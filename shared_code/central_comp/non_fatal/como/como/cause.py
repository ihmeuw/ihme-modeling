import os
import errno
from copy import deepcopy

import pandas as pd

from ihme_dimensions import gbdize
from hierarchies import dbtrees
from adding_machine import agg_locations as al

from como import residuals
from como import common


def agg_causes(como_version, year, sex, measure_id, location_set_id):
    drawdir = os.path.join(como_version.como_dir, 'draws', 'cause')
    al.agg_all_locs_mem_eff(
        drawdir=drawdir,
        stagedir=drawdir,
        location_set_id=location_set_id,
        index_cols=['location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'cause_id'],
        year_ids=year,
        sex_ids=sex,
        measure_id=measure_id,
        custom_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5",
        output_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5")


class CauseResultContainer(object):

    results_mapping = {
        3: {"data_attr": "ylds",
            "compute": ["compute_ylds"]},
        5: {"data_attr": "prevalence",
            "compute": ["compute_prevalence"]},
        6: {"data_attr": "incidence",
            "compute": ["compute_incidence"]}
    }

    def __init__(self, como_version, dimensions, sequela_inputs=None,
                 injury_inputs=None, como_sim=None):
        # inputs
        self.como_version = como_version
        self.sequela_inputs = sequela_inputs
        self.injury_inputs = injury_inputs
        self.como_sim = como_sim

        # add cause to dimensions
        self.dimensions = deepcopy(dimensions)
        self.dimensions.index_dim.add_level(
            "cause_id",
            self.como_version.cause_restrictions.cause_id.unique().tolist())

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

    def get_index_level(self, level):
        return self.dimensions.index_dim.get_level(level)

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
    def _most_detailed_non_injury_prevalence(self):
        if self.sequela_inputs is None:
            raise AttributeError(
                "cannot access _most_detailed_non_injury_prevalence without "
                "setting sequela prevalence")
        df = self.sequela_inputs.sequela_prevalence.copy()
        df = df.groupby(self.index_cols).sum()
        return df[self.draw_cols].reset_index()

    @property
    def _simulated_aggregate_prevalence(self):
        if self.como_sim is None:
            raise AttributeError(
                "cannot access _simulated_aggregate_prevalence without "
                "setting como_sim")
        df = self.como_sim.agg_causes.copy()
        gbdizer = gbdize.GBDizeDataFrame(self.dimensions)
        df = gbdizer.random_choice_resample(df)
        return df[self.index_cols + self.draw_cols]

    @property
    def _aggregate_prevalence(self):
        if self.sequela_inputs is None:
            raise AttributeError(
                "cannot access _aggregate_prevalence without setting sequela"
                " prevalence")
        df = self.sequela_inputs.sequela_prevalence.copy()
        df = df.groupby(self.index_cols).sum().reset_index()
        df = df.merge(self.como_version.agg_cause_exceptions)
        df.drop("cause_id", axis=1, inplace=True)
        df.rename(columns={"parent_id": "cause_id"}, inplace=True)
        df = df.groupby(self.index_cols).sum().reset_index()
        return df[self.index_cols + self.draw_cols]

    @property
    def _ecode_prevalence(self):
        if self.injury_inputs is None:
            raise AttributeError(
                "can't access _ecode_prevalence without setting injury_inputs")
        # get the injuries hierarchy
        cause_tree = self.get_cause_tree()
        for node in cause_tree.level_n_descendants(1):
            if node.id != 687:
                cause_tree.prune(node.id)

        # aggregate from most detailed
        df = self.injury_inputs.ecode_prevalence.copy()
        df = common.agg_hierarchy(
            tree=cause_tree,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")
        df = df[df.cause_id != 294]  # drop all cause from injuries
        return df[self.index_cols + self.draw_cols]

    def compute_prevalence(self):
        # non injuries
        prev = self._most_detailed_non_injury_prevalence
        prev = prev.append(self._simulated_aggregate_prevalence)
        prev = prev.append(self._aggregate_prevalence)

        # get the non injuries hierarchy
        cause_tree = self.get_cause_tree()
        cause_tree.prune(687)
        prev = common.maximize_hierarchy(
            tree=cause_tree,
            df=prev,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")

        # add injuries
        prev = prev.append(self._ecode_prevalence)

        self.prevalence = prev

    @property
    def _most_detailed_incidence(self):
        df = self.sequela_inputs.sequela_incidence.copy()
        df = df.groupby(self.index_cols).sum().reset_index()
        return df[self.index_cols + self.draw_cols]

    @property
    def _short_term_injury_incidence(self):
        df = self.injury_inputs.short_term_ecode_incidence.copy()
        return df[self.index_cols + self.draw_cols]

    def compute_incidence(self):
        cause_tree = self.get_cause_tree()
        inci = self._most_detailed_incidence.append(
            self._short_term_injury_incidence)
        inci = common.agg_hierarchy(
            tree=cause_tree,
            df=inci,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")
        self.incidence = inci

    @property
    def _most_detailed_NE_injuries(self):
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
        nemat = common.apply_restrictions(
            self.como_version.cause_restrictions, nemat,
            self.dimensions.data_list())

        parallelism = ["location_id", "year_id", "sex_id"]
        for slices in self.dimensions.index_slices(parallelism):
            filename = "{yid}_{sid}.csv".format(yid=slices[1], sid=slices[2])
            directory = os.path.join(
                "FILEPATH", str(slices[0]))
            try:
                os.makedirs(directory)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise
            filepath = os.path.join(directory, filename)
            out_df = nemat.loc[
                (nemat.location_id == slices[0]) &
                (nemat.year_id == slices[1]) &
                (nemat.sex_id == slices[2])]
            out_df.to_csv(filepath, index=False)

        nemat = nemat[self.index_cols + self.draw_cols]
        nemat = nemat.groupby(self.index_cols).sum().reset_index()
        return nemat[self.index_cols + self.draw_cols]

    @property
    def _short_term_injury_ylds(self):
        df = self.injury_inputs.short_term_ecode_ylds.copy()
        return df[self.index_cols + self.draw_cols]

    def _get_simulated_ylds(self, with_seq_id=False):
        df = self.como_sim.ylds.copy()
        df = df.merge(self.como_version.sequela_list, on="sequela_id")
        index_cols = self.index_cols
        if with_seq_id:
            index_cols = index_cols + ["sequela_id"]
        return df[index_cols + self.draw_cols]

    def _other_drug(self, yld_df):
        """
        Use proportion of other drug users, exclusive of those who also use
        cocaine or amphetamines.
        """
        other_drug_prop = .0024216
        other_drug_se = .00023581
        other_drug_draws = common.draw_from_beta(
            other_drug_prop, other_drug_se, len(self.dimensions.data_list()))

        amph_coc_prop = .00375522
        amph_coc_se = .00029279
        amph_coc_draws = common.draw_from_beta(
            amph_coc_prop, amph_coc_se, len(self.dimensions.data_list()))

        ratio_draws = other_drug_draws / amph_coc_draws

        other_drug_ylds = yld_df[yld_df.cause_id.isin([563, 564])]
        other_drug_ylds = other_drug_ylds.groupby(
            ["location_id", "year_id", "sex_id", "age_group_id"]).sum()
        other_drug_ylds = other_drug_ylds.reset_index()
        dcs = other_drug_ylds.filter(like="draw").columns
        other_drug_ylds.loc[:, dcs] = other_drug_ylds.loc[
            :, dcs].values * ratio_draws

        other_drug_ylds["cause_id"] = 566
        other_drug_ylds["measure_id"] = 3
        return other_drug_ylds[self.index_cols + self.draw_cols]

    def _get_residuals(self, cause_ylds):
        df_list = []
        parallelism = ["location_id"]
        for slices in self.dimensions.index_slices(parallelism):
            ratio_df = residuals.calc(
                location_id=slices[0],
                ratio_df=self.como_version.global_ratios,
                output_type="cause_id",
                drawcols=self.dimensions.data_list(),
                seq_ylds=self._get_simulated_ylds(with_seq_id=True),
                cause_ylds=cause_ylds)
            df_list.append(ratio_df)
        df = pd.concat(df_list)
        return df[self.index_cols + self.draw_cols]

    def compute_ylds(self):
        df = self._most_detailed_NE_injuries
        df = df.append(self._short_term_injury_ylds)
        df = df.groupby(self.index_cols).sum().reset_index()
        df = df.append(self._get_simulated_ylds())
        df = df.groupby(self.index_cols).sum().reset_index()
        df = df.append(self._other_drug(df))
        df = df.append(self._get_residuals(df))

        # aggregate
        cause_tree = self.get_cause_tree()
        df = common.agg_hierarchy(
            tree=cause_tree,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension="cause_id")
        df = df[self.index_cols + self.draw_cols]
        self.ylds = df

    def write_result_draws(self, df, measure_id):
        parallelism = ["location_id", "year_id", "sex_id"]
        for slices in self.dimensions.index_slices(parallelism):
            filename = "{mid}_{yid}_{sid}.h5".format(
                mid=measure_id, yid=slices[1], sid=slices[2])
            directory = os.path.join(
                self.como_version.como_dir, "draws", "cause", str(slices[0]))
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
                self.index_cols + self.dimensions.data_list()]
            out_df.to_hdf(
                filepath,
                "draws",
                mode="w",
                format="table",
                data_columns=self.index_cols)
