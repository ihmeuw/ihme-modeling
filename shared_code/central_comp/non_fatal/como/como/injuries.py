import os
import errno
from copy import deepcopy
import pandas as pd

from transmogrifier.super_gopher import SuperGopher
from hierarchies import dbtrees
from adding_machine.summarizers import transform_metric
from adding_machine import agg_locations as al
from ihme_dimensions import gbdize

from como.common import apply_restrictions, agg_hierarchy, cap_val


def agg_injuries(como_version, year, sex, measure_id, location_set_id):
    drawdir = os.path.join(como_version.como_dir, 'draws', 'injuries')
    al.agg_all_locs_mem_eff(
        drawdir=drawdir,
        stagedir=drawdir,
        location_set_id=location_set_id,
        index_cols=['location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'cause_id', 'rei_id'],
        year_ids=year,
        sex_ids=sex,
        measure_id=measure_id,
        custom_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5",
        output_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5")


class InjuryInputContainer(object):

    io_method_mapping = {
        3: {
            "read": [
                "add_long_term_ncodes_prevalence_to_reader",
                "add_short_term_ecode_ylds_to_reader"],
            "set": [
                "set_long_term_ncodes_prevalence_via_me_reader",
                "set_short_term_ecode_ylds_via_me_reader"],
            "get": ["get_en_matrices", "get_short_term_EN_ylds"]
        },
        5: {
            "read": ["add_ecode_prevalence_to_reader"],
            "set": ["set_ecode_prevalence_via_me_reader"],
            "get": []
        },
        6: {
            "read": ["add_short_term_ecode_incidence_to_reader"],
            "set": ["set_short_term_ecode_incidence_via_me_reader"],
            "get": []}
    }

    def __init__(self, como_version, dimensions):
        self.como_version = como_version
        self.dimensions = dimensions

        # draw containers
        self.long_term_ncode_prevalence = None
        self.short_term_ecode_ylds = None
        self.short_term_ecode_incidence = None
        self.ecode_prevalence = None
        self.en_matrices = None
        self.short_term_en_ylds = None

    def get_io_methods(self, measure_id, method_type):
        method_list = []
        for method in self.io_method_mapping[measure_id][method_type]:
            method_list.append(getattr(self, method))
        return method_list

    @property
    def long_term_ncode_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.injury_sequela, on='modelable_entity_id')
        arglist = zip(
            list(memv_df.modelable_entity_id),
            list(memv_df.model_version_id))
        return list(set(arglist))

    @property
    def short_term_ecode_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.st_injury_by_cause, on='modelable_entity_id')
        arglist = zip(
            list(memv_df.modelable_entity_id),
            list(memv_df.model_version_id))
        return list(set(arglist))

    @property
    def ecode_set(self):
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.injury_prev_by_cause, on='modelable_entity_id')
        arglist = zip(
            list(memv_df.modelable_entity_id),
            list(memv_df.model_version_id))
        return list(set(arglist))

    def add_long_term_ncodes_prevalence_to_reader(self, reader):
        if self.dimensions is None:
            raise AttributeError(
                "cannot add ncode prev to reader without defining dimensions")
        dimensions = deepcopy(self.dimensions)
        dimensions.index_dim.replace_level("measure_id", 5)
        for arguments in self.long_term_ncode_set:
            reader.add_reader_to_q(*arguments, dimensions=dimensions)
        return reader

    def add_short_term_ecode_ylds_to_reader(self, reader):
        if self.dimensions is None:
            raise AttributeError(
                "cannot add ecode ylds to reader without defining dimensions")
        dimensions = deepcopy(self.dimensions)
        dimensions.index_dim.replace_level("measure_id", 3)
        for arguments in self.short_term_ecode_set:
            reader.add_reader_to_q(*arguments, dimensions=dimensions)
        return reader

    def add_short_term_ecode_incidence_to_reader(self, reader):
        if self.dimensions is None:
            raise AttributeError(
                "cannot add ecode inci to reader without defining dimensions")
        dimensions = deepcopy(self.dimensions)
        dimensions.index_dim.replace_level("measure_id", 6)
        for arguments in self.short_term_ecode_set:
            reader.add_reader_to_q(*arguments, dimensions=dimensions)
        return reader

    def add_ecode_prevalence_to_reader(self, reader):
        if self.dimensions is None:
            raise AttributeError(
                "cannot add ecode prev to reader without defining dimensions")
        dimensions = deepcopy(self.dimensions)
        dimensions.index_dim.replace_level("measure_id", 5)
        for arguments in self.ecode_set:
            reader.add_reader_to_q(*arguments, dimensions=dimensions)
        return reader

    def set_long_term_ncodes_prevalence_via_me_reader(
            self, modelable_entity_reader):
        df = self._get_long_term_ncode_measure(modelable_entity_reader, 5)
        self.long_term_ncode_prevalence = df

    def set_short_term_ecode_ylds_via_me_reader(self, modelable_entity_reader):
        df = self._get_short_term_ecode_measure(modelable_entity_reader, 3)
        df = transform_metric(df, 3, 1)
        df = apply_restrictions(
            self.como_version.cause_restrictions, df,
            self.dimensions.data_list())
        self.short_term_ecode_ylds = df

    def set_short_term_ecode_incidence_via_me_reader(
            self, modelable_entity_reader):
        df = self._get_short_term_ecode_measure(modelable_entity_reader, 6)
        df = apply_restrictions(
            self.como_version.cause_restrictions, df,
            self.dimensions.data_list())
        self.short_term_ecode_incidence = df

    def set_ecode_prevalence_via_me_reader(self, modelable_entity_reader):
        df = self._get_ecode_measure(modelable_entity_reader, 5)
        df = apply_restrictions(
            self.como_version.cause_restrictions, df,
            self.dimensions.data_list())
        self.ecode_prevalence = df

    def get_en_matrices(self):
        if self.dimensions is None or self.como_version is None:
            raise AttributeError(
                "cannot get ncodes via this method if como_version is None or"
                " dimensions is None")
        df_list = []
        years = list(set(cap_val(self.dimensions.index_dim.levels.year_id,
                                 [1990, 1995, 2000, 2005, 2010, 2016])))
        parallelism = ["location_id", "sex_id"]
        for slices in self.dimensions.index_slices(parallelism):
            for year in years:
                nemat = pd.read_csv(
                    "FILEPATH/"
                    "NEmatrix_{location_id}_{year_id}_{sex_id}.csv".format(
                        location_id=slices[0],
                        year_id=year,
                        sex_id=slices[1]))
                nemat = nemat.merge(self.como_version.cause_list,
                                    left_on="ecode", right_on="acause")
                nemat = nemat.merge(self.como_version.injury_dws_by_sequela,
                                    left_on="ncode", right_on="n_code")
                nemat = nemat[["cause_id", "age_group_id", "sequela_id"] +
                              self.dimensions.data_list()]
                nemat["location_id"] = slices[0]
                nemat["sex_id"] = slices[1]
                nemat["year_id"] = year
                df_list.append(nemat)

        df = pd.concat(df_list)
        dims = deepcopy(self.dimensions)
        dims.index_dim.add_level("sequela_id", df.sequela_id.unique().tolist())
        dims.index_dim.add_level("cause_id", df.cause_id.unique().tolist())
        dims.index_dim.drop_level("measure_id")
        gbdizer = gbdize.GBDizeDataFrame(dims)
        df = gbdizer.random_choice_resample(df)
        df = gbdizer.fill_year_from_nearest_neighbor(df)
        self.en_matrices = df

    @staticmethod
    def resample_if_needed(df, dimensions, gbdizer):
        df = df.set_index(dimensions.index_names)
        # subset data columns
        draw_cols = ["draw_{}".format(i) for i in range(1000)]
        draw_cols = [d for d in draw_cols if d in df.columns]
        df = df[draw_cols]
        df = df.reset_index()

        # resample if ndraws is less than 1000
        if len(dimensions.data_list()) != len(draw_cols):
            df = gbdizer.random_choice_resample(df)
        return df

    def get_short_term_EN_ylds(self):
        if self.dimensions is None or self.como_version is None:
            raise AttributeError(
                "cannot get en ylds via this method if como_version is None or"
                " dimensions is None")
        en_dimensions = deepcopy(self.dimensions)
        en_dimensions.index_dim.replace_level("measure_id", 3)
        en_dimensions.index_dim.add_level(
            "cause_id",
            self.como_version.cause_restrictions.cause_id.unique().tolist())
        en_dimensions.index_dim.add_level(
            "rei_id",
            self.como_version.ncode_hierarchy.rei_id.unique().tolist())

        estim_df = self._get_short_term_EN_estimation(en_dimensions)
        annual_df = self._get_short_term_EN_annual(en_dimensions)
        df = estim_df.append(annual_df)
        df = apply_restrictions(
            self.como_version.cause_restrictions, df,
            self.dimensions.data_list())
        self.short_term_en_ylds = df

    def _get_short_term_EN_estimation(self, dim):
        # get non interpolated values
        estim_sg = SuperGopher(
            {'file_pattern': '{location_id}/ylds_{year_id}_{sex_id}.dta'},
            os.path.join(
                "filepath",
                "03_outputs/01_draws/ylds"
            ))
        years = list(set(
            cap_val(dim.index_dim.levels.year_id,
                    [1990, 1995, 2000, 2005, 2010, 2016]) + [2005]
        ))
        estim_df = estim_sg.content(
            location_id=dim.index_dim.get_level("location_id"),
            year_id=years,
            sex_id=dim.index_dim.get_level("sex_id"))

        # clean data
        estim_df = estim_df.merge(self.como_version.cause_list,
                                  left_on="ecode", right_on="acause")
        estim_df = estim_df.merge(self.como_version.ncode_hierarchy,
                                  left_on="ncode", right_on="rei")
        estim_df["age"] = estim_df["age"].round(2).astype(str)
        ridiculous_am = {
            '0.0': 2, '0.01': 3, '0.1': 4, '1.0': 5, '5.0': 6, '10.0': 7,
            '15.0': 8, '20.0': 9, '25.0': 10, '30.0': 11, '35.0': 12,
            '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17,
            '65.0': 18, '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31,
            '90.0': 32, '95.0': 235}
        estim_df["age"] = estim_df["age"].replace(ridiculous_am).astype(int)
        estim_df.rename(columns={"age": "age_group_id"}, inplace=True)

        # transform to rate
        estim_df = transform_metric(estim_df, 3, 1)

        # collapse inpatient
        estim_df = estim_df.groupby(
            ["location_id", "year_id", "age_group_id", "sex_id", "cause_id",
             "rei_id"]).sum().reset_index()

        # fill demographics
        data_cols = ["draw_{}".format(i) for i in range(1000)]
        gbdizer = gbdize.GBDizeDataFrame(dim)
        estim_df = gbdizer.add_missing_index_cols(estim_df)
        estim_df = gbdizer.gbdize_any_by_dim(estim_df, "age_group_id")
        estim_df.fillna(0, inplace=True)

        if gbdizer.missing_values(estim_df, "year_id"):
            estim_df = gbdizer.fill_year_by_interpolating(
                df=estim_df,
                rank_df=estim_df[estim_df["year_id"] == 2005],
                data_cols=data_cols)
        estim_df = estim_df[
            estim_df.year_id.isin(dim.index_dim.get_level("year_id"))
        ]

        # resample if necessary
        estim_df = self.resample_if_needed(estim_df, dim, gbdizer)
        return estim_df

    def _get_short_term_EN_annual(self, dim):
        # get non interpolated values
        annual_sg = SuperGopher(
            {'file_pattern': '{location_id}/ylds_{year_id}_{sex_id}.dta'},
            os.path.join(
                "filepath",
                "FILEPATH"
            ))
        annual_df = annual_sg.content(
            location_id=dim.index_dim.get_level("location_id"),
            year_id=dim.index_dim.get_level("year_id"),
            sex_id=dim.index_dim.get_level("sex_id"))

        # clean data
        annual_df = annual_df.merge(self.como_version.cause_list,
                                    left_on="ecode", right_on="acause")
        annual_df = annual_df.merge(self.como_version.ncode_hierarchy,
                                    left_on="ncode", right_on="rei")
        annual_df["age"] = annual_df["age"].round(2).astype(str)
        ridiculous_am = {
            '0.0': 2, '0.01': 3, '0.1': 4, '1.0': 5, '5.0': 6, '10.0': 7,
            '15.0': 8, '20.0': 9, '25.0': 10, '30.0': 11, '35.0': 12,
            '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17,
            '65.0': 18, '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31,
            '90.0': 32, '95.0': 235}
        annual_df["age"] = annual_df["age"].replace(ridiculous_am).astype(int)
        annual_df.rename(columns={"age": "age_group_id"}, inplace=True)

        # transform to rate
        annual_df = transform_metric(annual_df, 3, 1)

        # collapse inpatient
        annual_df = annual_df.groupby(
            ["location_id", "year_id", "age_group_id", "sex_id", "cause_id",
             "rei_id"]).sum().reset_index()

        # fill demographics
        gbdizer = gbdize.GBDizeDataFrame(dim)
        annual_df = gbdizer.add_missing_index_cols(annual_df)
        annual_df = gbdizer.gbdize_any_by_dim(annual_df, "age_group_id")
        annual_df.fillna(0, inplace=True)

        # resample if necessary
        annual_df = self.resample_if_needed(annual_df, dim, gbdizer)
        return annual_df

    def _get_long_term_ncode_measure(
            self, modelable_entity_reader, measure_id):

        if self.dimensions is None or self.como_version is None:
            raise AttributeError(
                "cannot set ncodes via this method if como_version is None or"
                " dimensions is None")

        df_list = []
        for key in self.long_term_ncode_set:
            maybe_df = modelable_entity_reader.reader_results[key]
            try:
                df = maybe_df[maybe_df.measure_id == measure_id]
            except AttributeError:
                raise maybe_df

            df_list.append(df)
        df = pd.concat(df_list)

        # attach sequela ids
        df = df.merge(
            self.como_version.injury_sequela, on='modelable_entity_id',
            how='left')

        # aggregate N codes
        df = df.groupby(
            ['sequela_id', 'cause_id', 'healthstate_id'] +
            self.dimensions.index_names).sum()
        df = df[self.dimensions.data_list()].reset_index()
        df = df.reset_index(drop=True)

        return df

    def _get_short_term_ecode_measure(
            self, modelable_entity_reader, measure_id):

        if self.dimensions is None or self.como_version is None:
            raise AttributeError(
                "cannot set short term ecodes via this method if como_version"
                "is None or dimensions is None")

        df_list = []
        for key in self.short_term_ecode_set:
            maybe_df = modelable_entity_reader.reader_results[key]
            try:
                df = maybe_df[maybe_df.measure_id == measure_id]
            except AttributeError:
                raise maybe_df
            df_list.append(df)
        df = pd.concat(df_list)

        # add cause_ids
        df = df.merge(self.como_version.st_injury_by_cause,
                      on='modelable_entity_id')

        # aggregate E codes
        df = df.groupby(
            ["cause_id"] + self.dimensions.index_names).sum()
        df = df[self.dimensions.data_list()].reset_index()
        df = df.reset_index(drop=True)

        return df

    def _get_ecode_measure(
            self, modelable_entity_reader, measure_id):

        if self.dimensions is None or self.como_version is None:
            raise AttributeError(
                "cannot set ecodes via this method if como_version is None or"
                " dimensions is None")

        df_list = []
        for key in self.ecode_set:
            maybe_df = modelable_entity_reader.reader_results[key]
            try:
                df = maybe_df[maybe_df.measure_id == measure_id]
            except AttributeError:
                raise maybe_df
            df_list.append(df)
        df = pd.concat(df_list)

        # add cause_ids
        df = df.merge(self.como_version.injury_prev_by_cause,
                      on='modelable_entity_id')

        # aggregate E codes
        df = df.groupby(
            ["cause_id"] + self.dimensions.index_names).sum()
        df = df[self.dimensions.data_list()].reset_index()
        df = df.reset_index(drop=True)

        return df


class InjuryResultContainer(object):

    results_mapping = {
        3: {"data_attr": "ylds",
            "compute": ["compute_ylds"]},
        5: {"data_attr": "prevalence",
            "compute": []},
        6: {"data_attr": "incidence",
            "compute": []}
    }

    def __init__(self, como_version, dimensions, injury_inputs=None,
                 como_sim=None):
        # inputs
        self.como_version = como_version
        self.injury_inputs = injury_inputs
        self.como_sim = como_sim

        # add cause to dimensions
        self.dimensions = deepcopy(dimensions)
        self.dimensions.index_dim.add_level(
            "cause_id",
            self.como_version.cause_restrictions.cause_id.unique().tolist())
        self.dimensions.index_dim.add_level(
            "rei_id",
            self.como_version.ncode_hierarchy.rei_id.unique().tolist())

        self._prevalence = None
        self._incidence = None
        self._ylds = None

    def get_compute_methods(self, measure_id):
        method_list = []
        for method in self.results_mapping[measure_id]["compute"]:
            method_list.append(getattr(self, method))
        return method_list

    def get_result_attr(self, measure_id):
        return getattr(self, self.results_mapping[measure_id]["data_attr"])

    def get_cause_tree(self):
        ct = dbtrees.causetree(self.como_version.cause_set_version_id, None,
                               None)
        return deepcopy(ct)

    @property
    def index_cols(self):
        return self.dimensions.index_names

    @property
    def draw_cols(self):
        return self.dimensions.data_list()

    @property
    def prevalence(self):
        return self._prevalence

    @prevalence.setter
    def prevalence(self, val):
        self._prevalence = val

    @property
    def incidence(self):
        return self._incidence

    @incidence.setter
    def incidence(self, val):
        self._incidence = val

    @property
    def ylds(self):
        return self._ylds

    @ylds.setter
    def ylds(self, val):
        self._ylds = val

    @property
    def _NE_ylds(self):
        nemat = self.injury_inputs.en_matrices
        nemat = nemat.merge(
            self.como_sim.ylds,
            on=["sequela_id", "age_group_id", "location_id", "year_id",
                "sex_id"])
        nemat = nemat.join(pd.DataFrame(
            data=(
                nemat.filter(regex="draw.*x").values *
                nemat.filter(regex="draw.*y").values),
            index=nemat.index,
            columns=self.draw_cols))
        nemat = apply_restrictions(
            self.como_version.cause_restrictions, nemat,
            self.dimensions.data_list())
        nemat = nemat.merge(self.como_version.injury_dws_by_sequela)
        nemat = nemat.merge(self.como_version.ncode_hierarchy,
                            left_on="n_code", right_on="rei")
        return nemat[self.index_cols + self.draw_cols]

    def compute_ylds(self):
        df = self._NE_ylds
        df = df.append(self.injury_inputs.short_term_en_ylds)

        # aggregate cases
        cause_tree = self.get_cause_tree()
        df = agg_hierarchy(
            tree=cause_tree,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")
        df = df[self.index_cols + self.draw_cols]

        # aggregate ncodes
        df = df.merge(self.como_version.ncode_hierarchy)
        df_agg = df.copy()
        df_agg = df.groupby(
            ["age_group_id", "location_id", "year_id", "sex_id", "measure_id",
             "cause_id", "parent_id"])[self.draw_cols].sum().reset_index()
        df_agg = df_agg.rename(columns={"parent_id": "rei_id"})

        # set attribute
        df = df.append(df_agg)
        df = df[self.index_cols + self.draw_cols]
        self.ylds = df

    def write_result_draws(self, df, measure_id):
        parallelism = ["location_id", "year_id", "sex_id"]
        for slices in self.dimensions.index_slices(parallelism):
            filename = "{mid}_{yid}_{sid}.h5".format(
                mid=measure_id, yid=slices[1], sid=slices[2])
            directory = os.path.join(
                self.como_version.como_dir, "draws", "injuries",
                str(slices[0]))
            try:
                os.makedirs(directory)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise
            filepath = os.path.join(directory, filename)

            out_df = df.loc[
                (df.location_id == slices[0]) &
                (df.year_id == slices[1]) &
                (df.sex_id == slices[2]),
                self.index_cols + self.draw_cols]
            out_df.to_hdf(
                filepath,
                "draws",
                mode="w",
                format="table",
                data_columns=self.index_cols)
