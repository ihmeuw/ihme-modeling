from copy import deepcopy
from typing import List, Optional

import numpy as np
import pandas as pd

import db_queries
from aggregator import aggregators, operators
from draw_sources import draw_sources, io
from gbd.constants import measures
from hierarchies import dbtrees
from hierarchies.legacy import tree

from como.legacy import common
from como.lib.resource_file_io import get_intellectual_disability_split
from como.lib.version import ComoVersion


def _draw_from(
    df: pd.DataFrame,
    mean_col: str,
    se_col: str,
    draw_cols: List[str],
    seed: Optional[int] = None,
) -> pd.DataFrame:
    """Draws from a normal distribution and returns {num} draws."""
    # take draws for each row of the dataframe
    num = len(draw_cols)
    draws = pd.DataFrame(index=draw_cols)

    rng = np.random.default_rng(seed=seed)

    for row in range(len(df)):
        temp = pd.DataFrame(
            data=rng.normal(
                loc=df.loc[row, f"{mean_col}"], scale=df.loc[row, f"{se_col}"], size=num
            ),
            index=draw_cols,
        )
        draws = pd.concat(
            [draws, temp],
            axis=1,
            join="outer",
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
        ignore_index=False,
        keys=None,
        levels=None,
        names=None,
        verify_integrity=False,
    )
    return df


class ImpairmentResultComputer:
    """Class for computing impairment aggregation results."""

    _gbd_cols = ["rei_id", "sequela_id"]

    def __init__(self, como_version: ComoVersion, como_df: pd.DataFrame) -> None:
        self.como_version = como_version
        self.dimensions = self.como_version.nonfatal_dimensions

        # this operation will expand the data for any sequela that are mapped
        # to multiple reis. (eg: id)
        if "rei_id" in como_df.columns:
            self.imp_df = como_df
        else:
            self.imp_df = como_df.merge(self.seq_rei, how="inner", on="sequela_id")

        # here we standardize incoming datasets
        for col in ["cause_id", "healthstate_id"]:
            try:
                self.imp_df = self.imp_df.drop(col, axis=1)
            except KeyError:
                pass
        self.imp_df = self.imp_df.merge(self.seq_cause, how="inner", on="sequela_id")

    @property
    def draw_cols(self) -> List[str]:
        """Draw column names."""
        return self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).data_list()

    @property
    def index_cols(self) -> List[str]:
        """Index column names."""
        return self.dimensions.get_simulation_dimensions(
            measure_id=measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def gbd_cols(self) -> List[str]:
        """Hard-coded rei_id and sequela_id."""
        return self._gbd_cols

    @property
    def seq_rei_hie(self) -> pd.DataFrame:
        """Merge of impairment sequelae and impairment hierarchy."""
        return self.seq_rei.merge(self.imp_hie, on="rei_id")[self.gbd_cols + ["parent_id"]]

    @property
    def seq_rei(self) -> pd.DataFrame:
        """Renaming of ComoVersion impariment sequela from database call."""
        return self.como_version.impairment_sequela

    @property
    def imp_hie(self) -> pd.DataFrame:
        """Renaming of ComoVersion impariment hierarchy from database call."""
        return self.como_version.impairment_hierarchy

    @property
    def seq_cause(self) -> pd.DataFrame:
        """Sequela and cause mapping from database call in ComoVersion."""
        return self.como_version.sequela_list[["sequela_id", "cause_id"]]

    def _scale(self, df: pd.DataFrame, group: List[str]) -> pd.DataFrame:
        """Define function to scale values to 100% for arbitrary grouping."""
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

    def _broadcast(
        self,
        broadcast_df: pd.DataFrame,
        broadcast_onto_df: pd.DataFrame,
        broadcast_index: List[str],
        broadcast_onto_index: List[str],
    ) -> pd.DataFrame:
        """Broadcast utility function."""
        if not set(broadcast_onto_index).issuperset(set(broadcast_index)):
            raise ValueError("'broadcast_onto_index' must be a superset of 'broadcast_index'")
        if any(broadcast_df.duplicated(broadcast_index)):
            raise ValueError(
                "'broadcast_df' must be unique by the columns in declared in"
                "'broadcast_index'"
            )
        if any(broadcast_onto_df.duplicated(broadcast_onto_index)):
            raise ValueError(
                "'broadcast_onto_df' must be unique by the columns in declared "
                "in 'broadcast_onto_index'"
            )

        # sort and index the dataframes
        broadcast_df.sort_values(broadcast_index, inplace=True)
        broadcast_df.set_index(broadcast_index, inplace=True, drop=True)
        broadcast_onto_df.sort_values(broadcast_onto_index, inplace=True)
        broadcast_onto_df.set_index(broadcast_index, inplace=True)

        # Broadcast the accross the data.
        # We have already sorted the data so to get the demographic indicators
        # back we just need to reset the index and then reassign the draws
        # into the original dataframe
        tmp_df = broadcast_onto_df[self.draw_cols] * broadcast_df[self.draw_cols]
        tmp_df = tmp_df.reset_index(drop=True)
        result_df = broadcast_onto_df.reset_index()
        result_df[self.draw_cols] = tmp_df[self.draw_cols]
        return result_df

    def _import_id_props(self) -> pd.DataFrame:
        """Generate intellectual disability split proportions using same proportions
        from GBD 2013.
        """
        id_props = get_intellectual_disability_split()
        id_props = _draw_from(id_props, "mean", "se", self.draw_cols, seed=42)
        id_props = id_props.drop(["mean", "se"], axis=1)
        return id_props

    def _find_split_sequela(self, parent_rei_id: int) -> pd.DataFrame:
        """Find instances of impairment where a sequela is mapped to multiple
        severities.

        This means we must apply the split
        """
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

    def split_id(self) -> None:
        """Proporition split for developmental intellectual disability."""
        # rei_id 197 - Developmental intellectual disability
        id_rei_id = 197
        split_seq = self._find_split_sequela(parent_rei_id=id_rei_id)

        # each base proportion is scaled to be specific to reis in the split
        base_props = self._import_id_props()
        sequela_props = split_seq.merge(base_props, how="inner", on="rei_id")
        split_props = self._scale(sequela_props, ["sequela_id"])

        # get the intellectual disability splits data
        id_split_bool = self.imp_df.sequela_id.isin(
            split_seq.sequela_id.unique()
        ) & self.imp_df.rei_id.isin(  # only id reis
            self.seq_rei_hie[self.seq_rei_hie.parent_id == id_rei_id]["rei_id"]
        )
        to_split = self.imp_df[id_split_bool]
        self.imp_df = self.imp_df[~id_split_bool]

        # broadcast the split proportions accross the data.
        split = self._broadcast(
            broadcast_df=split_props,
            broadcast_onto_df=to_split,
            broadcast_index=self.gbd_cols,
            broadcast_onto_index=self.gbd_cols + self.index_cols,
        )

        split = split[["rei_id", "cause_id"] + self.index_cols + self.draw_cols]

        self.imp_df = pd.concat([self.imp_df, split])

    def aggregate_reis(self, sex_id: List[int]) -> None:
        """Aggregate sequela up to the most detailed rei/cause combo."""
        cgroup = ["cause_id", "rei_id"] + self.index_cols
        child_agg_df = (
            self.imp_df[cgroup + self.draw_cols].groupby(cgroup).sum().reset_index()
        )
        child_agg_df = child_agg_df.merge(
            self.imp_hie[["rei_id", "parent_id"]], on="rei_id", how="left"
        )
        # NOTE: There are some impairments that are sex restricted, and won't exist in data
        # if the restricted sex is the only sex present. In these cases we modify the
        # aggregation hierarchy to not include the restricted impairment REI IDs so that
        # AggSynchronous doesn't complain about missing IDs.
        child_agg_reis = set(child_agg_df["rei_id"]).union(set(child_agg_df["parent_id"]))
        restricted_imp_hie = self.imp_hie.loc[self.imp_hie["rei_id"].isin(child_agg_reis)]
        if len(sex_id) == 1:
            restricted_reis = db_queries.get_restrictions(
                restriction_type="sex",
                rei_id=list(child_agg_reis),
                release_id=self.como_version.release_id,
                sex_id=sex_id[0],
                rei_set_id=self.como_version.impairment_rei_set_id,
            )["rei_id"].unique()
            restricted_imp_hie = restricted_imp_hie.loc[
                ~restricted_imp_hie["rei_id"].isin(restricted_reis)
            ]
        # Create a hierarchies REI tree, using a potentially restricted impairments hierarchy
        rei_tree = tree.parent_child_to_tree(
            df=restricted_imp_hie[["rei_id", "parent_id"]],
            parent_col="parent_id",
            child_col="rei_id",
        )
        # Actually aggregate
        cache = {"draws": child_agg_df}
        temp_source = draw_sources.DrawSource(
            {"draw_dict": cache, "name": "draws"}, read_func=io.mem_read_func
        )
        temp_sink = draw_sources.DrawSink(
            {"draw_dict": cache, "name": "draws"}, write_func=io.mem_write_func
        )
        pgroup = ["cause_id", "parent_id"] + self.index_cols
        operator = operators.Sum(index_cols=pgroup, value_cols=self.draw_cols)
        aggregator = aggregators.AggSynchronous(
            draw_source=temp_source,
            draw_sink=temp_sink,
            index_cols=pgroup,
            aggregate_col="rei_id",
            operator=operator,
        )
        aggregator.run(rei_tree)
        agg_df = temp_source.content()
        # Drop the all-impairment REI ID
        agg_df = agg_df.loc[agg_df["rei_id"] != 191]
        self.imp_df = agg_df

    def aggregate_causes(self) -> None:
        """Aggregate causes up the cause hierarchy determined by cause_set_version_id
        and release_id.
        """
        ct = dbtrees.causetree(
            cause_set_version_id=self.como_version.cause_set_version_id,
            release_id=self.como_version.release_id,
        )
        ct = deepcopy(ct)
        self.imp_df = common.agg_hierarchy(
            tree=ct,
            df=self.imp_df,
            index_cols=self.index_cols + ["cause_id", "rei_id"],
            data_cols=self.draw_cols,
            dimension="cause_id",
        )
