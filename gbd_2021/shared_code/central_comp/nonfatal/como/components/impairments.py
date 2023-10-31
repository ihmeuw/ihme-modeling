import os
from copy import deepcopy
import pandas as pd
import numpy as np

from gbd.constants import measures
from hierarchies import dbtrees

from como.legacy import common

THIS_PATH = os.path.abspath(os.path.dirname(__file__))


class ImpairmentResultComputer:

    _gbd_cols = ["rei_id", "sequela_id"]

    def __init__(self, como_version, como_df):
        self.como_version = como_version
        self.dimensions = self.como_version.nonfatal_dimensions

        if "rei_id" in como_df.columns:
            self.imp_df = como_df
        else:
            self.imp_df = como_df.merge(self.seq_rei, how="inner", on="sequela_id")

        for col in ["cause_id", "healthstate_id"]:
            try:
                self.imp_df = self.imp_df.drop(col, axis=1)
            except KeyError:
                pass
        self.imp_df = self.imp_df.merge(self.seq_cause, how="inner", on="sequela_id")

    @property
    def draw_cols(self):
        return self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).data_list()

    @property
    def index_cols(self):
        return self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def gbd_cols(self):
        return self._gbd_cols

    @property
    def seq_rei_hie(self):
        return self.seq_rei.merge(self.imp_hie, on="rei_id")[self.gbd_cols + ["parent_id"]]

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

        num = len(self.draw_cols)
        draws = pd.DataFrame(index=self.draw_cols)
        for row in range(len(df)):
            temp = pd.DataFrame(
                data=np.random.normal(
                    df.loc[row, f"{mean_col}"], df.loc[row, f"{se_col}"], num
                ),
                index=self.draw_cols,
            )
            draws = pd.concat(
                [draws, temp],
                axis=1,
                join="outer",
                join_axes=None,
                ignore_index=False,
                keys=None,
                levels=None,
                names=None,
                verify_integrity=False,
            )

        draws = draws.transpose().reset_index()
        draws = draws.drop("index", 1)
        df = pd.concat(
            [df, draws],
            axis=1,
            join="inner",
            join_axes=None,
            ignore_index=False,
            keys=None,
            levels=None,
            names=None,
            verify_integrity=False,
        )
        return df

    def _scale(self, df, group):
        """define function to scale values to 100% for arbitrary grouping"""
        xtra = [col for col in df if col not in group + self.draw_cols]

        sums = df[group + self.draw_cols].groupby(group).sum().reset_index()
        sums.sort_values(group, inplace=True)
        sums.set_index(group, inplace=True, drop=False)

        df.sort_values(group, inplace=True)
        df.set_index(group, inplace=True, drop=False)

        df[self.draw_cols] = df[self.draw_cols] / sums[self.draw_cols]
        df = df.fillna(0)

        return df[group + xtra + self.draw_cols].reset_index(drop=True)

    def _broadcast(
        self, broadcast_df, broadcast_onto_df, broadcast_index, broadcast_onto_index
    ):
        assert set(broadcast_onto_index).issuperset(
            set(broadcast_index)
        ), "'broadcast_onto_index' must be a superset of 'broadcast_index'"
        assert not any(broadcast_df.duplicated(broadcast_index)), (
            "'broadcast_df' must be unique by the columns in declared in" "'broadcast_index'"
        )
        assert not any(broadcast_onto_df.duplicated(broadcast_onto_index)), (
            "'broadcast_onto_df' must be unique by the columns in declared in"
            "'broadcast_onto_index'"
        )

        broadcast_df.sort_values(broadcast_index, inplace=True)
        broadcast_df.set_index(broadcast_index, inplace=True, drop=True)
        broadcast_onto_df.sort_values(broadcast_onto_index, inplace=True)
        broadcast_onto_df.set_index(broadcast_index, inplace=True)

        tmp_df = broadcast_onto_df[self.draw_cols] * broadcast_df[self.draw_cols]
        tmp_df = tmp_df.reset_index(drop=True)
        result_df = broadcast_onto_df.reset_index()
        result_df[self.draw_cols] = tmp_df[self.draw_cols]
        return result_df

    def _import_id_props(self):
        """generate intellectual disability split proportions using
        same proportions from GBD 2013"""
        id_props = pd.read_csv(f"{THIS_PATH}/../config/ID_split_dist.csv")
        id_props = self._draw_from(id_props, "mean", "se")
        id_props = id_props.drop(["mean", "se"], axis=1)
        return id_props

    def _find_split_sequela(self, parent_rei_id):
        """find instances of impairment where a sequela is mapped to
        multiple severities. This means we must apply the split"""
        parent_col = ["parent_id"]
        size_df = (
            self.seq_rei_hie.groupby(["sequela_id"] + parent_col)
            .size()
            .rename("count")
            .reset_index()
        )
        size_df = size_df[(size_df["count"] > 1) & (size_df["parent_id"] == parent_rei_id)]
        split_seq = self.seq_rei_hie[
            (self.seq_rei_hie.sequela_id.isin(size_df.sequela_id.unique()))
            & (self.seq_rei_hie.parent_id == parent_rei_id)
        ]
        return split_seq

    def split_id(self):
        id_rei_id = 197
        split_seq = self._find_split_sequela(parent_rei_id=id_rei_id)

        base_props = self._import_id_props()
        sequela_props = split_seq.merge(base_props, how="inner", on="rei_id")
        split_props = self._scale(sequela_props, ["sequela_id"])

        id_split_bool = self.imp_df.sequela_id.isin(
            split_seq.sequela_id.unique()
        ) & self.imp_df.rei_id.isin(
            self.seq_rei_hie[self.seq_rei_hie.parent_id == id_rei_id]["rei_id"]
        )
        to_split = self.imp_df[id_split_bool]
        self.imp_df = self.imp_df[~id_split_bool]

        split = self._broadcast(
            broadcast_df=split_props,
            broadcast_onto_df=to_split,
            broadcast_index=self.gbd_cols,
            broadcast_onto_index=self.gbd_cols + self.index_cols,
        )

        split = split[["rei_id", "cause_id"] + self.index_cols + self.draw_cols]

        self.imp_df = self.imp_df.append(split)

    def aggregate_reis(self):
        cgroup = ["cause_id", "rei_id"] + self.index_cols
        child_agg_df = (
            self.imp_df[cgroup + self.draw_cols].groupby(cgroup).sum().reset_index()
        )
        to_agg = child_agg_df.merge(
            self.imp_hie[["rei_id", "parent_id"]], on="rei_id", how="inner"
        )


        pgroup = ["cause_id", "parent_id"] + self.index_cols
        most_detailed = [193, 194]
        parent_agg_df = (
            to_agg.loc[~to_agg["rei_id"].isin(most_detailed), pgroup + self.draw_cols]
            .groupby(pgroup)
            .sum()
            .reset_index()
        )
        parent_agg_df.rename(columns={"parent_id": "rei_id"}, inplace=True)
        self.imp_df = parent_agg_df.append(child_agg_df)

    def aggregate_causes(self):
        ct = dbtrees.causetree(
            cause_set_version_id=self.como_version.cause_set_version_id,
            gbd_round_id=self.como_version.gbd_round_id,
        )
        ct = deepcopy(ct)
        self.imp_df = common.agg_hierarchy(
            tree=ct,
            df=self.imp_df,
            index_cols=self.index_cols + ["cause_id", "rei_id"],
            data_cols=self.draw_cols,
            dimension="cause_id",
        )
