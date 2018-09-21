import os
import errno
from copy import deepcopy

import pandas as pd

from db_tools import ezfuncs
from adding_machine import agg_locations as al

from como import residuals
from como.common import apply_restrictions


def agg_sequelae(como_version, year, sex, measure_id, location_set_id):
    drawdir = os.path.join(como_version.como_dir, 'draws', 'sequela')
    al.agg_all_locs_mem_eff(
        drawdir=drawdir,
        stagedir=drawdir,
        location_set_id=location_set_id,
        index_cols=['location_id', 'year_id', 'age_group_id', 'sex_id',
                    'measure_id', 'sequela_id'],
        year_ids=year,
        sex_ids=sex,
        measure_id=measure_id,
        custom_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5",
        output_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5")


class SequelaInputContainer(object):

    io_method_mapping = {
        3: {"read": ["add_sequela_prevalence_to_reader"],
            "set": ["set_sequela_prevalence_via_me_reader"],
            "get": []},
        5: {"read": ["add_sequela_prevalence_to_reader"],
            "set": ["set_sequela_prevalence_via_me_reader"],
            "get": []},
        6: {"read": ["add_sequela_incidence_to_reader"],
            "set": ["set_sequela_incidence_via_me_reader"],
            "get": []}
    }

    def __init__(self, como_version=None, dimensions=None):
        self.como_version = como_version
        self.dimensions = dimensions
        self.sequela_prevalence = None
        self.sequela_incidence = None

    def get_io_methods(self, measure_id, method_type):
        method_list = []
        for method in self.io_method_mapping[measure_id][method_type]:
            method_list.append(getattr(self, method))
        return method_list

    @property
    def sequela_set(self):
        if self.como_version is None:
            raise AttributeError(
                "cannot sequela set without adding a como version")
        memv_df = self.como_version.mvid_list.merge(
            self.como_version.sequela_list, on='modelable_entity_id')
        memv_df = memv_df[memv_df.sequela_id > 0]
        arglist = zip(
            list(memv_df.modelable_entity_id),
            list(memv_df.model_version_id))
        return list(set(arglist))

    def add_sequela_prevalence_to_reader(self, reader):
        if self.dimensions is None:
            raise AttributeError(
                "cannot add sequela to reader without defining dimensions")
        dimensions = deepcopy(self.dimensions)
        dimensions.index_dim.replace_level("measure_id", 5)
        for arguments in self.sequela_set:
            reader.add_reader_to_q(*arguments, dimensions=dimensions)
        return reader

    def add_sequela_incidence_to_reader(self, reader):
        if self.dimensions is None:
            raise AttributeError(
                "cannot add sequela to reader without defining dimensions")
        dimensions = deepcopy(self.dimensions)
        dimensions.index_dim.replace_level("measure_id", 6)

        for arguments in self.sequela_set:
            reader.add_reader_to_q(*arguments, dimensions=dimensions)
        return reader

    def set_sequela_prevalence_via_me_reader(self, modelable_entity_reader):
        df = self._get_sequela_measure(modelable_entity_reader, 5)
        self.sequela_prevalence = df

    def set_sequela_incidence_via_me_reader(self, modelable_entity_reader):
        df = self._get_sequela_measure(modelable_entity_reader, 6)
        self.sequela_incidence = df

    def _get_sequela_measure(self, modelable_entity_reader, measure_id):

        if self.dimensions is None or self.como_version is None:
            raise AttributeError(
                "cannot set sequela via this method if como_version is None or"
                " dimensions is None")

        df_list = []
        for key in self.sequela_set:
            maybe_df = modelable_entity_reader.reader_results[key]
            try:
                df = maybe_df[maybe_df.measure_id == measure_id]
            except AttributeError:
                raise maybe_df

            df_list.append(df)
        df = pd.concat(df_list)

        # attach sequela ids
        df = df.merge(
            self.como_version.sequela_list, on='modelable_entity_id',
            how='left')

        # restrict
        df = apply_restrictions(
            self.como_version.cause_restrictions, df,
            self.dimensions.data_list())
        df = df[['sequela_id', 'cause_id', 'healthstate_id'] +
                self.dimensions.index_names + self.dimensions.data_list()]

        return df


class SequelaResultContainer(object):

    results_mapping = {
        3: {"data_attr": "ylds",
            "compute": ["compute_ylds"]},
        5: {"data_attr": "prevalence",
            "compute": []},
        6: {"data_attr": "incidence",
            "compute": []}
    }

    def __init__(self, como_version, dimensions, sequela_inputs=None,
                 como_sim=None):
        # inputs
        self.como_version = como_version
        self.dimensions = dimensions
        self.sequela_inputs = sequela_inputs
        self.como_sim = como_sim

        self._ylds = None

    def get_compute_methods(self, measure_id):
        method_list = []
        for method in self.results_mapping[measure_id]["compute"]:
            method_list.append(getattr(self, method))
        return method_list

    def get_result_attr(self, measure_id):
        return getattr(self, self.results_mapping[measure_id]["data_attr"])

    @property
    def index_cols(self):
        return ["sequela_id"] + self.dimensions.index_names

    @property
    def draw_cols(self):
        return self.dimensions.data_list()

    @property
    def prevalence(self):
        if self.sequela_inputs is None:
            raise AttributeError(
                "cannot access sequela prevalence without setting"
                " sequela_inputs attribute")
        return self.sequela_inputs.sequela_prevalence

    @property
    def incidence(self):
        if self.sequela_inputs is None:
            raise AttributeError(
                "cannot access sequela incidence without setting"
                " sequela_inputs attribute")
        return self.sequela_inputs.sequela_incidence

    @property
    def ylds(self):
        return self._ylds

    @ylds.setter
    def ylds(self, val):
        self._ylds = val

    @property
    def _simulated_ylds(self):
        if self.como_sim is None:
            raise AttributeError(
                "can't access sequela ylds without setting como_sim attribute")
        return self.como_sim.ylds.copy()

    @property
    def _residuals(self):
        cause_ylds = self._simulated_ylds
        cause_ylds = cause_ylds.merge(
            self.como_version.sequela_list, on="sequela_id")
        cause_ylds = cause_ylds[
            self.dimensions.index_names + ["cause_id"] + self.draw_cols]
        seq_ylds = self._simulated_ylds

        df_list = []
        parallelism = ["location_id"]
        for slices in self.dimensions.index_slices(parallelism):
            ratio_df = residuals.calc(
                location_id=slices[0],
                ratio_df=self.como_version.global_ratios,
                output_type="sequela_id",
                drawcols=self.dimensions.data_list(),
                seq_ylds=seq_ylds,
                cause_ylds=cause_ylds)
            df_list.append(ratio_df)
        df = pd.concat(df_list)
        return df[self.index_cols + self.draw_cols]

    def compute_ylds(self):
        df = self._simulated_ylds
        df = df.append(self._residuals)
        df = df.groupby(self.index_cols).sum()
        self._ylds = df.reset_index()

    def write_result_draws(self, df, measure_id):
        parallelism = ["location_id", "year_id", "sex_id"]
        for slices in self.dimensions.index_slices(parallelism):
            filename = "{mid}_{yid}_{sid}.h5".format(
                mid=measure_id, yid=slices[1], sid=slices[2])
            directory = os.path.join(
                self.como_version.como_dir, "draws", "sequela", str(slices[0]))
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
