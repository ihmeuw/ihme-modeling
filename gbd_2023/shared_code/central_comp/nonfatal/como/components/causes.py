from typing import List

import numpy as np
import pandas as pd

from gbd import constants as gbd_constants
from hierarchies import dbtrees

from como.legacy import common, residuals
from como.lib import version


class CauseResultComputer:
    """Computes cause aggregates and residuals.

    Args:
        como_verson (version.ComoVersion): ComoVersion object associated with the current
            COMO being run.
    """

    def __init__(self, como_version: version.ComoVersion) -> None:
        # inputs
        self.como_version = como_version

        # set up the dimensions we are using
        self.dimensions = self.como_version.nonfatal_dimensions

    @property
    def index_cols(self) -> List[str]:
        """Returns the index column names for cause models."""
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_cause_dimensions(
            measure_id=gbd_constants.measures.PREVALENCE, at_birth=False
        ).index_names

    @property
    def draw_cols(self) -> List[str]:
        """Returns the draw column names for cause models."""
        # measure/birth dimension doesn't matter for columns
        return self.dimensions.get_cause_dimensions(
            measure_id=gbd_constants.measures.PREVALENCE, at_birth=False
        ).data_list()

    def aggregate_cause(self, df: pd.DataFrame, cause_set_version_id: int) -> pd.DataFrame:
        """Generates and returns cause aggregates for a given cause set version."""
        ct = dbtrees.causetree(
            cause_set_version_id=cause_set_version_id, release_id=self.como_version.release_id
        )
        df = common.agg_hierarchy(
            tree=ct,
            df=df,
            index_cols=self.index_cols,
            data_cols=self.draw_cols,
            dimension=gbd_constants.columns.CAUSE_ID,
        )
        df = df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            df[col] = df[col].astype(int)
        all_done = df.cause_id.unique().tolist()

        if self.como_version.include_reporting_cause_set:
            cts = dbtrees.causetree(
                cause_set_version_id=self.como_version.reporting_cause_set_version_id,
                release_id=self.como_version.release_id,
                return_many=True,
            )

            reporting_dfs = []
            for ct in cts:
                rep_only = common.agg_hierarchy(
                    tree=ct,
                    df=df.copy(deep=True),
                    index_cols=self.index_cols,
                    data_cols=self.draw_cols,
                    dimension="cause_id",
                )
                reporting_dfs.append(rep_only)
            reporting_df = pd.concat(reporting_dfs)
            reporting_df = reporting_df[~reporting_df["cause_id"].isin(all_done)]
            reporting_df = reporting_df[self.index_cols + self.draw_cols]

            for col in self.index_cols:
                reporting_df[col] = reporting_df[col].astype(int)
            return pd.concat([df, reporting_df]).reset_index(drop=True)
        return df.reset_index(drop=True)

    def residuals(
        self, cause_ylds: pd.DataFrame, simulated_ylds: pd.DataFrame
    ) -> pd.DataFrame:
        """Calculates cause/sequelae residuals."""
        location_id = cause_ylds[gbd_constants.columns.LOCATION_ID].unique().item()
        ratio_df = residuals.calc(
            location_id=location_id,
            ratio_df=self.como_version.global_ratios,
            output_type=gbd_constants.columns.CAUSE_ID,
            drawcols=self.draw_cols,
            seq_ylds=simulated_ylds,
            cause_ylds=cause_ylds,
        )
        ratio_df = ratio_df[self.index_cols + self.draw_cols]
        for col in self.index_cols:
            ratio_df[col] = ratio_df[col].astype(int)
        return ratio_df

    def aggregate_cause_prevalence(
        self, prev_df: pd.DataFrame, cause_set_version_id: int
    ) -> pd.DataFrame:
        """Calculate non-most-detailed parent cause aggregate prevalence.

        Combines each cause aggregate parents child cause draws as 1 - (the
        product of (1 - combined most-detailed cause prevalence)).
        """
        cause_prev = []
        ct = dbtrees.causetree(
            cause_set_version_id=cause_set_version_id, release_id=self.como_version.release_id
        )
        lvl = ct.max_depth() - 1
        agg_cause_exceptions = (
            self.como_version.agg_cause_exceptions.parent_id.unique().tolist()
        )

        def combine_draws(df: pd.DataFrame) -> pd.DataFrame:
            return pd.Series(
                1 - np.prod(1 - df[self.draw_cols].values, axis=0), index=self.draw_cols
            )

        while lvl >= 0:
            for node in ct.level_n_descendants(lvl):
                child_ids = [c.id for c in node.leaves()]
                cause_id = node.id

                # If there are child causes and the parent is not in the exception list,
                # combine draws.
                if child_ids and cause_id not in agg_cause_exceptions:
                    df = prev_df.loc[
                        (prev_df[gbd_constants.columns.CAUSE_ID].isin(child_ids))
                    ]
                    if df.empty:
                        # pandas < 1.0.0 did not complain about these lines with an empty
                        # DataFrame, but newer versions raise an exception.
                        continue
                    df[gbd_constants.columns.CAUSE_ID] = cause_id
                    df = df.groupby(self.index_cols).apply(combine_draws).reset_index()

                    cause_prev.append(df)
            lvl -= 1

        agg_cause_df = pd.concat(cause_prev)
        return agg_cause_df[self.index_cols + self.draw_cols]
