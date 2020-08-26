import os

import numpy as np
import pandas as pd
from scipy import stats

from db_tools import ezfuncs
from core_maths.summarize import pct_change
from ihme_dimensions import dimensionality, gbdize
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from dataframe_io.pusher import SuperPusher
from dataframe_io.io_control.csv_io import CSVIO
from hierarchies.dbtrees import sextree, agetree
from aggregator.aggregators import AggSynchronous
from aggregator.operators import WtdSum, Sum

from como.common import get_population


def fill_square(df, index_cols, square_col, square_col_vals, fill_val=0):
    """make data square across a column for a set of index columns"""
    # get index dimensions
    index_cols = [col for col in index_cols if col != square_col]
    index_dct = {
        tuple(index_cols): list(set(
            tuple(x) for x in df[index_cols].values)),
        square_col: square_col_vals
    }

    # get data dimensions
    data_dct = {
        "non_draw_cols": [col for col in df.columns
                          if col not in index_cols + [square_col]]}

    # make it square
    gbdizer = gbdize.GBDizeDataFrame(
        dimensionality.DataFrameDimensions(index_dct, data_dct))
    df = gbdizer.fill_empty_indices(df, fill_val)
    return df


def add_metric(df):
    df["metric_id"] = 3
    return df


def sort_index_columns(df, index_cols):
    return df.sort_values(index_cols)


def compute_estimates(df, point_estimate="mean"):
    """ Compute summaries """
    indices = [col for col in df.columns if "draw" not in col]

    if point_estimate == "mean":
        df['mean'] = df.filter(like='draw').mean(axis=1)
    elif point_estimate == "median":
        df['median'] = np.median(df.filter(like='draw').values, axis=1)
    elif point_estimate is None:
        pass
    elif "draw" in point_estimate:
        df["mean"] = df[[point_estimate]]
    else:
        raise ValueError("point_estimate must be one of ['mean', 'median']")

    df['lower'] = stats.scoreatpercentile(
        df.filter(like='draw').values, per=2.5, axis=1)
    df['upper'] = stats.scoreatpercentile(
        df.filter(like='draw').values, per=97.5, axis=1)
    nondraw_cols = set(df.columns) - \
        set(df.filter(like='draw').columns)
    df = df[list(nondraw_cols)]
    df = df.sort_values(indices)
    return df


def broadcast(broadcast_onto_df, broadcast_df, index_cols,
              operator="*"):
    if operator not in ["*", "+", "-", "/"]:
        raise ValueError(
            "operator must be one of the strings ('*', '+', '-', '/'")
    if not set(broadcast_df.columns).issuperset(set(index_cols)):
        raise ValueError("'broadcast_df' must contain all 'index_cols'")
    if not set(broadcast_onto_df.columns).issuperset(set(index_cols)):
        raise ValueError("'broadcast_onto_df' must contain all 'index_cols'")
    if any(broadcast_df.duplicated(index_cols)):
        raise ValueError(
            "'broadcast_df' must be unique by the columns in declared in"
            "'index_cols'")

    data_cols = [col for col in broadcast_df.columns if col not in index_cols]

    if not set(broadcast_onto_df.columns).issuperset(set(data_cols)):
        raise ValueError(
            "'broadcast_onto_df' must contain all non 'index_cols' from "
            "'broadcast_df'")

    # sort and index the dataframes
    broadcast_onto_df.sort_values(index_cols, inplace=True)
    broadcast_onto_df.set_index(index_cols, inplace=True, drop=True)
    broadcast_df.sort_values(index_cols, inplace=True)
    broadcast_df.set_index(index_cols, inplace=True, drop=True)

    # subset the broadcast_df
    broadcast_df = broadcast_df[
        broadcast_df.index.isin(broadcast_onto_df.index)]

    # Broadcast the accross the data.
    if operator == "*":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] * broadcast_df[data_cols])
    if operator == "+":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] + broadcast_df[data_cols])
    if operator == "-":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] - broadcast_df[data_cols])
    if operator == "/":
        broadcast_onto_df[data_cols] = (
            broadcast_onto_df[data_cols] / broadcast_df[data_cols])

    return broadcast_onto_df.reset_index()


def convert_to_counts(df, pop_df, draw_cols):
    df = df.merge(pop_df)
    df[draw_cols] = (df[draw_cols].values.T * df["population"].values).T
    df = df.drop("population", axis=1)
    return df


def convert_to_rates(df, pop_df, draw_cols):
    df = df.merge(pop_df)
    df[draw_cols] = (df[draw_cols].values.T / df["population"].values).T
    df = df.drop("population", axis=1)
    return df


def rescale_enn(df, ds, population, index_cols, value_cols):
    asked_for = ds.content_kwargs.get('filters').copy()
    asked_for["age_group_id"] = 164

    birth_df = ds.content(filters=asked_for)
    enn_df = df[df.age_group_id == 2]
    df = df[df.age_group_id != 2]

    # aggregate enn and birth
    birth_df = convert_to_counts(birth_df, population, value_cols)
    enn_df = convert_to_counts(enn_df, population, value_cols)
    agg_df = pd.concat([birth_df, enn_df])
    agg_df["age_group_id"] = 2
    agg_df = agg_df.groupby(index_cols).sum().reset_index()
    agg_df = convert_to_rates(agg_df, population, value_cols)

    df = pd.concat([df, agg_df])
    return df


class _FinalizeComponent(object):

    component = None
    birth_ages = [1, 22, 28, 158]

    def __init__(self, io_mock, dimensions, population, std_age_weights):
        self._io_mock = io_mock
        self.dimensions = dimensions
        self.dimensions.index_dim.add_level(name="metric_id", val=[3])

        # get references to population and weights and construct aggregator
        self.population = population
        self.std_age_weights = std_age_weights

    @classmethod
    def get_finalizer_by_component(cls, component):
        """get the formula associated with the provided name"""
        sublcass_component_map = {
            SubClass.component: SubClass for SubClass in cls.__subclasses__()
        }
        return sublcass_component_map[component]

    @property
    def _mem_io_params(self):
        return {"draw_dict": self._io_mock, "name": self.component}

    def gen_draw_source(self):
        source = DrawSource(self._mem_io_params, mem_read_func)
        return source

    def gen_draw_sink(self):
        sink = DrawSink(self._mem_io_params, mem_write_func)
        sink.add_transform(sort_index_columns, self.dimensions.index_names)
        return sink

    def _agg_pop_wtd_ages_birth(self, age_group_id):
        age_tree = agetree(age_group_id)
        age_tree.add_node(164, {}, age_tree.root.id)

        # make the source and sink
        source = self.gen_draw_source()
        source.add_transform(convert_to_counts, self.population,
                             self.dimensions.data_list())
        sink = self.gen_draw_sink()
        sink.add_transform(convert_to_rates, self.population,
                           self.dimensions.data_list())

        # constuct aggregator obj
        operator = Sum(
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            value_cols=self.dimensions.data_list())
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            aggregate_col="age_group_id",
            operator=operator)

        aggregator.run(age_tree)

    def _agg_pop_wtd_ages(self, age_group_id):
        age_tree = agetree(age_group_id)

        # make the source and sink
        source = self.gen_draw_source()
        source.add_transform(
            fill_square,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            square_col="age_group_id",
            square_col_vals=[node.id for node in age_tree.leaves()])
        sink = self.gen_draw_sink()

        # constuct aggregator obj
        operator = WtdSum(
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            value_cols=self.dimensions.data_list(),
            weight_df=self.population,
            weight_name="population",
            merge_cols=["location_id", "year_id", "age_group_id", "sex_id"])
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            aggregate_col="age_group_id",
            operator=operator)

        aggregator.run(age_tree)

    def _agg_age_std_ages_birth(self):
        age_tree = agetree(27)

        # make the source and sink
        source = self.gen_draw_source()
        source.add_transform(
            rescale_enn,
            source,
            self.population,
            self.dimensions.index_names,
            self.dimensions.data_list())
        source.add_transform(
            fill_square,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            square_col="age_group_id",
            square_col_vals=[node.id for node in age_tree.leaves()])
        sink = self.gen_draw_sink()

        # constuct aggregator obj
        operator = WtdSum(
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            value_cols=self.dimensions.data_list(),
            weight_df=self.std_age_weights,
            weight_name="age_group_weight_value",
            merge_cols=["age_group_id"])
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            aggregate_col="age_group_id",
            operator=operator)

        # run the tree
        aggregator.run(age_tree)

    def _agg_age_std_ages(self):
        age_tree = agetree(27)

        # make the source and sink
        source = self.gen_draw_source()
        source.add_transform(
            fill_square,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            square_col="age_group_id",
            square_col_vals=[node.id for node in age_tree.leaves()])
        sink = self.gen_draw_sink()

        # constuct aggregator obj
        operator = WtdSum(
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            value_cols=self.dimensions.data_list(),
            weight_df=self.std_age_weights,
            weight_name="age_group_weight_value",
            merge_cols=["age_group_id"])
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "age_group_id"],
            aggregate_col="age_group_id",
            operator=operator)

        # run the tree
        aggregator.run(age_tree)

    def _compute_percent(self, denom_df):
        # get
        source = self.gen_draw_source()
        rate_df = source.content()

        # compute pct
        index_cols = [col for col in self.dimensions.index_names
                      if col in denom_df]
        pct_df = broadcast(rate_df, denom_df, index_cols, "/")
        pct_df.fillna(0, inplace=True)
        pct_df.replace(np.inf, 0, inplace=True)
        pct_df = pct_df.set_index(self.dimensions.index_names)
        pct_df[pct_df > 1] = 1
        pct_df = pct_df.reset_index()
        pct_df["metric_id"] = 2

        # push
        sink = self.gen_draw_sink()
        sink.push(pct_df)

    def compute_sex_aggregates(self):
        sex_tree = sextree()

        # make the source and sink
        source = self.gen_draw_source()
        source.add_transform(
            fill_square,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "sex_id"],
            square_col="sex_id",
            square_col_vals=[node.id for node in sex_tree.leaves()])
        sink = self.gen_draw_sink()

        # construct aggregator obj
        operator = WtdSum(
            index_cols=[col for col in self.dimensions.index_names
                        if col != "sex_id"],
            value_cols=self.dimensions.data_list(),
            weight_df=self.population,
            weight_name="population",
            merge_cols=["location_id", "year_id", "age_group_id", "sex_id"])
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in self.dimensions.index_names
                        if col != "sex_id"],
            aggregate_col="sex_id",
            operator=operator)

        # run the tree
        aggregator.run(sex_tree)

    def compute_age_aggregates(self, age_group_list):
        for age_group_id in age_group_list:
            if age_group_id != 27:
                self._agg_pop_wtd_ages(age_group_id)
            else:
                self._agg_age_std_ages()

    def compute_percent(self):
        raise NotImplementedError

    def compute_count(self):
        # get
        source = self.gen_draw_source()
        rate_df = source.content()

        # compute counts
        rate_df = rate_df[rate_df["metric_id"] == 3]
        rate_df = rate_df[rate_df["age_group_id"] != 27]
        count_df = rate_df.merge(self.population)

        if len(rate_df) != len(count_df):
            raise ValueError("not enough populations to convert to rate")

        count_df[self.dimensions.data_list()] = (
            count_df[self.dimensions.data_list()].values.T *
            count_df["population"].values).T
        count_df = count_df.drop("population", axis=1)
        count_df["metric_id"] = 1

        sink = self.gen_draw_sink()
        sink.push(count_df)

    def compute_percent_change(self, year_start, year_end):
        source = self.gen_draw_source()
        change_df = source.content(filters={"year_id": [year_start, year_end]})
        df = pct_change(
            df=change_df,
            start_year=year_start,
            end_year=year_end,
            time_col="year_id",
            data_cols=self.dimensions.data_list())
        df.fillna(0, inplace=True)
        return df


class FinalizeCause(_FinalizeComponent):

    component = "cause"

    def compute_percent(self):
        mem_source = self.gen_draw_source()
        denom = mem_source.content(filters={"cause_id": [294]})
        denom = denom.drop("cause_id", axis=1)
        self._compute_percent(denom_df=denom)

    def compute_age_aggregates(self, age_group_list):
        measure_id = self.dimensions.index_dim.get_level("measure_id")[0]
        for age_group_id in age_group_list:
            if measure_id == 6:
                if age_group_id != 27:
                    if age_group_id in self.birth_ages:
                        self._agg_pop_wtd_ages_birth(age_group_id)
                    else:
                        self._agg_pop_wtd_ages(age_group_id)
                else:
                    self._agg_age_std_ages_birth()
            else:
                if age_group_id != 27:
                    self._agg_pop_wtd_ages(age_group_id)
                else:
                    self._agg_age_std_ages()


class FinalizeImpairment(_FinalizeComponent):

    component = "impairment"

    def compute_percent(self):
        mem_source = DrawSource(
            {"draw_dict": self._io_mock, "name": "cause"}, mem_read_func)
        denom = mem_source.content(filters={"metric_id": 3})
        self._compute_percent(denom_df=denom)


class FinalizeInjuries(_FinalizeComponent):

    component = "injuries"

    def compute_percent(self):
        mem_source = DrawSource(
            {"draw_dict": self._io_mock, "name": "cause"}, mem_read_func)
        denom = mem_source.content(filters={"metric_id": 3})
        self._compute_percent(denom_df=denom)


class FinalizeSequela(_FinalizeComponent):

    component = "sequela"

    def compute_percent(self):
        pass

    def compute_age_aggregates(self, age_group_list):
        measure_id = self.dimensions.index_dim.get_level("measure_id")[0]
        for age_group_id in age_group_list:
            if measure_id == 6:
                if age_group_id != 27:
                    if age_group_id in self.birth_ages:
                        self._agg_pop_wtd_ages_birth(age_group_id)
                    else:
                        self._agg_pop_wtd_ages(age_group_id)
                else:
                    self._agg_age_std_ages_birth()
            else:
                if age_group_id != 27:
                    self._agg_pop_wtd_ages(age_group_id)
                else:
                    self._agg_age_std_ages()


class ComoSummaries(object):

    _gbd_compare_age_group_list = [
        1, 21, 23, 24, 25, 26, 159, 158, 28, 22, 27]
    # 162, 163, 169, 188

    def __init__(self, como_version, measure_id, year_id, location_id):

        self.como_version = como_version
        self.dimensions = self.como_version.nonfatal_dimensions
        self.measure_id = measure_id

        self.dimensions.simulation_index["year_id"] = year_id
        self.dimensions.simulation_index["location_id"] = location_id

        # inits
        self.io_mock = {}
        self._population = None
        self._std_age_weights = None

    @property
    def gbd_round_id(self):
        return self.como_version.gbd_round_id

    @property
    def components(self):
        return self.como_version.components

    @property
    def population(self):
        if self._population is None:
            dim = self.dimensions.get_simulation_dimensions(self.measure_id)
            pop = get_population(
                self.como_version,
                age_group_id=(
                    dim.index_dim.get_level("age_group_id") +
                    self._gbd_compare_age_group_list + [164]),
                location_id=dim.index_dim.get_level("location_id"),
                year_id=dim.index_dim.get_level("year_id"),
                sex_id=dim.index_dim.get_level("sex_id") + [3])
            pop = pop[["age_group_id", "location_id", "year_id", "sex_id",
                       "population"]]
            self._population = pop
        return self._population

    @property
    def std_age_weights(self):
        if self._std_age_weights is None:
            query = """
                SELECT age_group_id, age_group_weight_value
                FROM shared.age_group_weight
                WHERE gbd_round_id = {}""".format(self.gbd_round_id)
            aws = ezfuncs.query(query, conn_def='cod')
            self._std_age_weights = aws
        return self._std_age_weights

    def _import_static_draws_by_component(self, component):
        dim = self.dimensions.get_dimension_by_component(
            component, self.measure_id)
        draw_dir = os.path.join(self.como_version.como_dir, "draws", component,
                                str(dim.index_dim.get_level("location_id")[0]))
        real_source = DrawSource(
            {"draw_dir": draw_dir,
             "file_pattern": "{measure_id}_{year_id}_{sex_id}.h5"
             })
        real_source.add_transform(add_metric)
        fake_sink = DrawSink(
            {"draw_dict": self.io_mock, "name": component}, mem_write_func)

        # get the filters and data
        sim_dim = self.dimensions.get_simulation_dimensions(self.measure_id)
        filters = sim_dim.index_dim.levels.copy()
        filters["age_group_id"] = dim.index_dim.get_level("age_group_id")
        df = real_source.content(filters=filters)
        fake_sink.push(df)

    def _get_finalizer_by_component(self, component):
        Finalizer = _FinalizeComponent.get_finalizer_by_component(component)
        dimensions = self.dimensions.get_dimension_by_component(
            component, self.measure_id)
        finalizer = Finalizer(
            io_mock=self.io_mock,
            dimensions=dimensions,
            population=self.population,
            std_age_weights=self.std_age_weights)
        return finalizer

    def age_sex_agg_single_component(self, component):
        # read in data for this component
        print("importing: {}".format(component))
        self._import_static_draws_by_component(component)

        # initialize the finalizer for this component
        finalizer = self._get_finalizer_by_component(component)

        # compute all aggregates and summarize
        print("computing sex aggregate: {}".format(component))
        finalizer.compute_sex_aggregates()
        print("computing age aggregate: {}".format(component))
        finalizer.compute_age_aggregates(self._gbd_compare_age_group_list)

    def metric_convert_single_component(self, component):
        # initialize the finalizer for this component
        finalizer = self._get_finalizer_by_component(component)

        finalizer.compute_percent()
        finalizer.compute_count()

    def percent_change_single_component(self, component):
        finalizer = self._get_finalizer_by_component(component)
        df_list = []
        for year_start, year_end in self.como_version.change_years:
            df = finalizer.compute_percent_change(year_start, year_end)
            df = compute_estimates(df, point_estimate=None)
            df_list.append(df)
        df = pd.concat(df_list)
        df.fillna(0, inplace=True)
        df.rename(columns={"pct_change_means": "val"}, inplace=True)
        return df

    def estimate_single_component(self, component):
        # data to summarize
        draw_source = DrawSource(
            {"draw_dict": self.io_mock, "name": component},
            mem_read_func)
        df = draw_source.content()
        df = compute_estimates(df, "mean")
        df.rename(columns={"mean": "val"}, inplace=True)
        return df

    def export_summary(self, component, year_type, df):
        if year_type == "single_year":
            pattern = "{measure_id}/single_year/{location_id}/{year_id}.csv"
        if year_type == "multi_year":
            pattern = "{measure_id}/multi_year/{location_id}.csv"

        # path to use in summaries
        directory = os.path.join(self.como_version.como_dir, "summaries",
                                 component)
        pusher = SuperPusher(directory=directory,
                             spec={"file_pattern": pattern})
        pusher.push(df)