import os
from copy import deepcopy
import pandas as pd
import numpy as np

from gbd.constants import measures
from hierarchies import dbtrees

from como import common

THIS_PATH = os.path.abspath(os.path.dirname(__file__))


class ImpairmentResultComputer:

    _gbd_cols = ["rei_id", "sequela_id"]

    def __init__(self, como_version, como_df):
        self.como_version = como_version
        self.dimensions = self.como_version.nonfatal_dimensions
        self.imp_df = como_df

        # here we standardize incoming datasets
        for col in ["cause_id", "healthstate_id"]:
            try:
                self.imp_df = self.imp_df.drop(col, axis=1)
            except KeyError:
                pass
        self.imp_df = self.imp_df.merge(
            self.seq_cause, how="inner", on="sequela_id")

    @property
    def draw_cols(self):
        return self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False).data_list()

    @property
    def index_cols(self):
        return self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False).index_names

    @property
    def gbd_cols(self):
        return self._gbd_cols

    @property
    def seq_rei_hie(self):
        return self.seq_rei.merge(
            self.imp_hie, on="rei_id")[self.gbd_cols + ["parent_id"]]

    @property
    def seq_rei(self):
        return self.como_version.impairment_sequela

    @property
    def imp_hie(self):
        return self.como_version.impairment_hierarchy

    @property
    def seq_cause(self):
        return self.como_version.sequela_list[["sequela_id", "cause_id"]]

    def _draw_from(self, df, mean_col, se_col):
        """draws from a normal distribution and returns {num} draws"""

        # take draws for each row of the dataframe
        num = len(self.draw_cols)
        draws = pd.DataFrame(index=self.draw_cols)
        for row in range(len(df)):
            temp = pd.DataFrame(
                data=np.random.normal(
                    df.loc[row, f'{mean_col}'],
                    df.loc[row, f'{se_col}'], num),
                index=self.draw_cols)
            draws = pd.concat([draws, temp], axis=1, join='outer',
                              join_axes=None, ignore_index=False, keys=None,
                              levels=None, names=None, verify_integrity=False)

        # reshape data and merge back onto master by index
        draws = draws.transpose().reset_index()
        draws = draws.drop('index', 1)
        df = pd.concat([df, draws], axis=1, join='inner', join_axes=None,
                       ignore_index=False, keys=None, levels=None, names=None,
                       verify_integrity=False)
        return df

    def _scale(self, df, group):
        """define function to scale values to 100% for arbitrary grouping"""
        xtra = [col for col in df if col not in group + self.draw_cols]

        # get total
        sums = df[group + self.draw_cols].groupby(group).sum().reset_index()
        sums.sort_values(group, inplace=True)
        sums.set_index(group, inplace=True, drop=False)

        # set up index for broadcast
        df.sort_values(group, inplace=True)
        df.set_index(group, inplace=True, drop=False)

        # scale
        df[self.draw_cols] = df[self.draw_cols] / sums[self.draw_cols]
        df = df.fillna(0)

        return df[group + xtra + self.draw_cols].reset_index(drop=True)

    def _broadcast(self, broadcast_df, broadcast_onto_df, broadcast_index,
                   broadcast_onto_index):
        assert set(broadcast_onto_index).issuperset(set(broadcast_index)), (
            "'broadcast_onto_index' must be a superset of 'broadcast_index'")
        assert not any(broadcast_df.duplicated(broadcast_index)), (
            "'broadcast_df' must be unique by the columns in declared in"
            "'broadcast_index'")
        assert not any(broadcast_onto_df.duplicated(broadcast_onto_index)), (
            "'broadcast_onto_df' must be unique by the columns in declared in"
            "'broadcast_onto_index'")

        # sort and index the dataframes
        broadcast_df.sort_values(broadcast_index, inplace=True)
        broadcast_df.set_index(broadcast_index, inplace=True, drop=True)
        broadcast_onto_df.sort_values(broadcast_onto_index, inplace=True)
        broadcast_onto_df.set_index(broadcast_index, inplace=True)

        # Broadcast the accross the data.
        # We have already sorted the data so to get the demographic indicators
        # back we just need to reset the index and then reassign the draws
        # into the original dataframe
        tmp_df = (
            broadcast_onto_df[self.draw_cols] * broadcast_df[self.draw_cols])
        tmp_df = tmp_df.reset_index(drop=True)
        result_df = broadcast_onto_df.reset_index()
        result_df[self.draw_cols] = tmp_df[self.draw_cols]
        return result_df

    def _compute_epil_props(self):
        """generate epilepsy split proportions
         458 208 545 imp_epilepsy_treat  generic_medication 559
         459 209 545 imp_epilepsy_mod    epilepsy_year 746
         460 210 545 imp_epilepsy_sev    epilepsy_month 745
        """
        # cause_id 545 - Idiopathic epilepsy
        epilepsy_id = 545
        epilep_props = self.imp_df[self.imp_df.cause_id == epilepsy_id]
        epilep_props = epilep_props[self.index_cols + self.gbd_cols +
                                    self.draw_cols]
        epilep_props = self._scale(epilep_props, self.index_cols)
        return epilep_props.drop(['sequela_id'], axis=1)

    def _import_id_props(self):
        """generate intellectual disability split proportions"""
        id_props = pd.read_csv(f"{THIS_PATH}/../config/ID_split_dist.csv")
        id_props = self._draw_from(id_props, 'mean', 'se')
        id_props = id_props.drop(['mean', 'se'], axis=1)
        return id_props

    def _find_split_sequela(self, parent_rei_id):
        """find instances of impairment where a sequela is mapped to
        multiple severities. This means we must apply the split"""
        parent_col = ["parent_id"]
        size_df = self.seq_rei_hie.groupby(
            ["sequela_id"] + parent_col
        ).size().rename("count").reset_index()
        size_df = size_df[
            (size_df["count"] > 1) &
            (size_df["parent_id"] == parent_rei_id)]
        split_seq = self.seq_rei_hie[
            (self.seq_rei_hie.sequela_id.isin(size_df.sequela_id.unique())) &
            (self.seq_rei_hie.parent_id == parent_rei_id)]
        return split_seq

    def split_epilepsy(self):
        # rei_id 193 - Epilepsy
        epil_rei_id = 193
        split_seq = self._find_split_sequela(parent_rei_id=epil_rei_id)

        # get the epilepsy splits data
        epil_split_bool = (
            self.imp_df.sequela_id.isin(  # sequela_ids with epilepsy
                split_seq.sequela_id.unique()) &
            self.imp_df.rei_id.isin(  # only epilepsy reis
                self.seq_rei_hie[
                    self.seq_rei_hie.parent_id == epil_rei_id]["rei_id"]))
        to_split = self.imp_df[epil_split_bool]
        self.imp_df = self.imp_df[~epil_split_bool]

        # prep for math
        idx = self.index_cols + ["rei_id"]
        to_split.sort_values(idx, inplace=True)
        to_split.set_index(idx, inplace=True, drop=False)

        # get split proportions
        split_props = self._compute_epil_props()
        split_props.sort_values(idx, inplace=True)
        split_props.set_index(idx, inplace=True, drop=False)

        # split them
        to_split[self.draw_cols] = (
            to_split[self.draw_cols] * split_props[self.draw_cols])
        split = to_split.reset_index(drop=True)
        split = split[["rei_id", "cause_id"] +
                      self.index_cols + self.draw_cols]

        self.imp_df = self.imp_df.append(split)

    def split_id(self):
        # rei_id 197 - Developmental intellectual disability
        id_rei_id = 197
        split_seq = self._find_split_sequela(parent_rei_id=id_rei_id)

        # each base proportion is scaled to be specific to reis in the split
        base_props = self._import_id_props()
        sequela_props = split_seq.merge(base_props, how="inner", on="rei_id")
        split_props = self._scale(sequela_props, ["sequela_id"])

        # get the intellectual disability splits data
        id_split_bool = (
            self.imp_df.sequela_id.isin(split_seq.sequela_id.unique()) &
            self.imp_df.rei_id.isin(  # only id reis
                self.seq_rei_hie[
                    self.seq_rei_hie.parent_id == id_rei_id]["rei_id"]))
        to_split = self.imp_df[id_split_bool]
        self.imp_df = self.imp_df[~id_split_bool]

        # broadcast the split proportions accross the data.
        split = self._broadcast(
            broadcast_df=split_props,
            broadcast_onto_df=to_split,
            broadcast_index=self.gbd_cols,
            broadcast_onto_index=self.gbd_cols + self.index_cols)

        split = split[["rei_id", "cause_id"] +
                      self.index_cols + self.draw_cols]

        self.imp_df = self.imp_df.append(split)

    def aggregate_reis(self):
        # aggregate sequela up to the most detailed rei/cause combo
        cgroup = ["cause_id", "rei_id"] + self.index_cols
        child_agg_df = self.imp_df[
            cgroup + self.draw_cols].groupby(cgroup).sum().reset_index()
        to_agg = child_agg_df.merge(
            self.imp_hie[["rei_id", "parent_id"]], on="rei_id", how="inner")

        # aggregate all most detailed rei/cause combos to the parent
        pgroup = ["cause_id", "parent_id"] + self.index_cols
        # rei_id 194 - Guillain-Barré syndrome
        # excluded because most-detailed at level 1 already, no agg needed
        parent_agg_df = to_agg.loc[
            to_agg.rei_id != 194, pgroup + self.draw_cols
        ].groupby(pgroup).sum().reset_index()
        parent_agg_df.rename(columns={"parent_id": "rei_id"}, inplace=True)
        self.imp_df = parent_agg_df.append(child_agg_df)

    def aggregate_causes(self):
        ct = dbtrees.causetree(
            cause_set_version_id=self.como_version.cause_set_version_id)
        ct = deepcopy(ct)
        self.imp_df = common.agg_hierarchy(
            tree=ct,
            df=self.imp_df,
            index_cols=self.index_cols + ["cause_id", "rei_id"],
            data_cols=self.draw_cols,
            dimension="cause_id")
