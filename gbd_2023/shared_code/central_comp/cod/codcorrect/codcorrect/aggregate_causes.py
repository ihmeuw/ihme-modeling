import gc
import logging

import pandas as pd

from gbd.constants import measures

from codcorrect.legacy.utils.constants import Columns
from codcorrect.legacy.parameters.machinery import MachineParameters

logger = logging.getLogger(__name__)


class AggregateCauses:

    def __init__(self, parent_dir: str,
                 location_id: int,
                 sex_id: int,
                 version: MachineParameters
                 ) -> None:
        """
        Aggregate causes up one or more cause hierarchies.

        Arguments:
            parent_dir (str): parent codcorrect directory
                e.g. FILEPATH
            location_id (int): draws location_id
            sex_id (int): draws sex_id
            year_ids (List[int]): year_ids
            version (MachineParameters): machinery parameters for the given
                 codcorrect run.
        """
        self.parent_dir = parent_dir
        self.location_id = location_id
        self.sex_id = sex_id
        self.version = version
        self.most_detailed_cause_list = (
            self._most_detailed_cause_list_from_hierarchies()
        )
        self._rescaled_data = None
        self._shocks_data = None
        self._unscaled_data = None
        self._aggregated_rescaled_data = None
        self._aggregated_shocks_data = None
        self._aggregated_unscaled_data = None
        self._aggregated_data_for_diagnostics = None

    @property
    def rescaled_data(self):
        if not isinstance(self._rescaled_data, pd.DataFrame):
            self._rescaled_data = self.version.file_system.read_unaggregated_rescaled_draws(
                self.location_id, self.sex_id
            )
        return self._rescaled_data

    @property
    def shocks_data(self):
        if not isinstance(self._shocks_data, pd.DataFrame):
            shocks_data = self.version.file_system.read_unaggregated_shocks_draws(
                self.location_id, self.sex_id
            )
            self._shocks_data = shocks_data[Columns.INDEX + self.version.draw_cols]
        return self._shocks_data

    @property
    def unscaled_data(self):
        if not isinstance(self._unscaled_data, pd.DataFrame):
            self._unscaled_data = self.version.file_system.read_unaggregated_unscaled_draws(
                self.location_id, self.sex_id
            )
        return self._unscaled_data

    @property
    def aggregated_rescaled_data(self):
        if not isinstance(self._aggregated_rescaled_data, pd.DataFrame):
            self._aggregated_rescaled_data = self._aggregate_rescaled()
        return self._aggregated_rescaled_data

    @property
    def aggregated_shocks_data(self):
        if not isinstance(self._aggregated_shocks_data, pd.DataFrame):
            self._aggregated_shocks_data = self._aggregate_shocks()
        return self._aggregated_shocks_data

    @property
    def aggregated_unscaled_data(self):
        if not isinstance(self._aggregated_unscaled_data, pd.DataFrame):
            self._aggregated_unscaled_data = self._aggregate_unscaled()
        return self._aggregated_unscaled_data

    @property
    def aggregated_data_for_diagnostics(self):
        if not isinstance(self._aggregated_data_for_diagnostics, pd.DataFrame):
            aggregated_all_data = pd.concat([
                self.aggregated_rescaled_data, self.aggregated_shocks_data],
                sort=False).reset_index(drop=True)
            self._aggregated_data_for_diagnostics = aggregated_all_data[
                Columns.INDEX + self.version.draw_cols].groupby(
                Columns.INDEX).sum().reset_index()
        return self._aggregated_data_for_diagnostics

    def run(self):
        logger.info("Beginning cause aggregation.")
        logger.info("Saving aggregated rescaled draws.")
        self.aggregate_and_save_rescaled()

        logger.info("Saving aggregated shock draws.")
        self.aggregate_and_save_shocks()

        logger.info("Saving aggregated unscaled draws.")
        self.aggregate_and_save_unscaled()

        logger.info("Saving diagnostics.")
        self.save_diagnostics()

    def aggregate_and_save_rescaled(self):
        self.aggregated_rescaled_data[Columns.MEASURE_ID] = measures.DEATH
        self.version.file_system.save_aggregated_rescaled_draws(self.aggregated_rescaled_data)

    def aggregate_and_save_shocks(self):
        self.aggregated_shocks_data[Columns.MEASURE_ID] = measures.DEATH
        self.version.file_system.save_aggregated_shocks_draws(self.aggregated_shocks_data)

    def aggregate_and_save_unscaled(self):
        self.aggregated_unscaled_data[Columns.MEASURE_ID] = measures.DEATH
        self.version.file_system.save_aggregated_unscaled_draws(self.aggregated_unscaled_data)

    def save_diagnostics(self):
        diagnostics = create_diagnostics(
            self.aggregated_unscaled_data,
            self.aggregated_data_for_diagnostics,
            self.version.draw_cols
        )
        self.version.file_system.save_diagnostics(diagnostics, self.location_id, self.sex_id)

    def _aggregate_rescaled(self):
        data = (
            self.rescaled_data[Columns.INDEX + self.version.draw_cols]
            .groupby(Columns.INDEX)
            .sum()
            .reset_index()
        )
        # filter down to most detailed causes
        data = data.loc[
            data[Columns.CAUSE_ID].isin(self.most_detailed_cause_list)]
        df_list = [data]
        for cause_set_id in self.version.cause_set_ids:
            temp = data.copy(deep=True)
            hierarchy_df = self.version._cause_parameters[
                cause_set_id].hierarchy
            aggs = aggregate_causes(
                temp,
                hierarchy_df,
                self.version.draw_cols)
            df_list.append(aggs)
            del temp
            gc.collect()
        return pd.concat(df_list, sort=False).reset_index(drop=True)

    def _aggregate_shocks(self):
        data = self.shocks_data.copy(deep=True)
        # filter down to most detailed causes
        data = data.loc[
            data[Columns.CAUSE_ID].isin(self.most_detailed_cause_list)]
        # Collapse draws for cause with multiple models (shock)
        data = data.groupby(Columns.INDEX).sum().reset_index()
        df_list = [data]
        for cause_set_id in self.version.cause_set_ids:
            temp = data.copy(deep=True)
            hierarchy_df = self.version._cause_parameters[
                cause_set_id].hierarchy
            aggs = aggregate_causes(
                temp,
                hierarchy_df,
                self.version.draw_cols)
            df_list.append(aggs)
            del temp
            gc.collect()
        return pd.concat(df_list, sort=False).reset_index(drop=True)

    def _aggregate_unscaled(self):
        full_index_set = self.aggregated_rescaled_data.loc[
            :, Columns.INDEX]
        full_index_set = full_index_set.drop_duplicates()
        data = pd.merge(
            self.unscaled_data,
            full_index_set,
            on=Columns.INDEX,
            how='outer')
        df_list = []
        for cause_set_id in self.version.cause_set_ids:
            temp = data.copy(deep=True)
            hierarchy_df = self.version._cause_parameters[
                cause_set_id].hierarchy
            aggs = aggregate_blanks(temp, hierarchy_df, self.version.draw_cols)
            df_list.append(aggs)
            del temp
            gc.collect()
        data = data.loc[data[self.version.draw_cols].notnull().all(axis=1)]
        df_list.append(data)
        return pd.concat(df_list, sort=False).reset_index(drop=True)

    def _most_detailed_cause_list_from_hierarchies(self):
        most_detailed = []
        for cause_set_id in self.version.cause_set_ids:
            most_detailed += self.version._cause_parameters[
                cause_set_id
            ].most_detailed_ids
        return list(set(most_detailed))


def aggregate_causes(df, hierarchy_df, draw_cols):
    """
    Aggregate causes up one or more cause hierarchies.
    Returns only the aggregates, mutates the original dataframe,
    so pass only copies.
    """

    # filter to most detailed in case a cause is most_detailed in
    # one hierarchy but not another
    data = _merge_cause_hierarchy_columns(
        df, hierarchy_df)
    data = data.loc[data[Columns.MOST_DETAILED] == 1]
    if data.empty:
        df_list = [data]
    else:
        # Loop through and aggregate
        min_level = int(hierarchy_df[Columns.LEVEL].min())
        max_level = int(data[Columns.LEVEL].max())
        df_list = []
        for level in range(max_level, min_level, -1):
            data = data[Columns.INDEX + draw_cols]
            # some hierarchies have causes with multiple parents and
            # are represented more than once so we need to drop duplicates
            # after subsetting to needed columns
            data = data.drop_duplicates(keep='first')
            data = _merge_cause_hierarchy_columns(data, hierarchy_df)
            level_df = data.loc[data[Columns.LEVEL] == level].copy()
            level_df = level_df.loc[
                level_df[Columns.CAUSE_ID] != level_df[Columns.PARENT_ID]
            ]
            level_df.loc[:, Columns.CAUSE_ID] = level_df[Columns.PARENT_ID]
            level_df = level_df[Columns.INDEX + draw_cols]
            level_df = level_df.groupby(Columns.INDEX).sum().reset_index()
            df_list.append(level_df)
            data = pd.concat([data, level_df], sort=False)

    return pd.concat(df_list).reset_index(drop=True)[
        Columns.INDEX + draw_cols]


def aggregate_blanks(df, hierarchy_df, draw_cols):
    """This function is to fill in gaps and to preserve existing data,
    specifically for shocks that don't exist at every level of the hierarchy.
    """
    # Get min and max level where we need to aggregate
    data = _merge_cause_hierarchy_columns(df, hierarchy_df)
    min_level = data.loc[
        data[draw_cols].isnull().any(axis=1),
        Columns.LEVEL].min() - 1
    max_level = data.loc[
        data[draw_cols].isnull().any(axis=1),
        Columns.LEVEL].max()

    df_list = []
    # Loop through and aggregate things that are missing
    for level in range(max_level, min_level, -1):
        # we have to wipe and then merge on hierarchy data for each level
        data = data[Columns.INDEX + draw_cols]
        # some hierarchies have causes with multiple parents and
        # are represented more than once so we need to drop duplicates
        # after subsetting to needed columns
        data = data.drop_duplicates(keep='first')
        data = _merge_cause_hierarchy_columns(data, hierarchy_df)
        # Get data that needs to get aggregated
        level_df = data.loc[(data[draw_cols].isnull().any(axis=1)) &
                            (data[Columns.LEVEL] == level),
                            Columns.INDEX]
        level_df = level_df.rename(
            columns={Columns.CAUSE_ID: Columns.PARENT_ID})
        level_df = pd.merge(
            level_df,
            data,
            on=list(set(Columns.INDEX) - set([Columns.CAUSE_ID])) +
            [Columns.PARENT_ID])
        # Collapse to parent if parent does not equal cause
        level_df = level_df.loc[
            level_df[Columns.CAUSE_ID] != level_df[Columns.PARENT_ID]
        ]
        level_df.loc[:, Columns.CAUSE_ID] = level_df[Columns.PARENT_ID]
        level_df = (
            level_df.groupby(Columns.INDEX)[draw_cols].sum().reset_index()
        )
        df_list.append(level_df)
        data = pd.concat(
            [
                data.loc[
                    ((~data[draw_cols].isnull().any(axis=1)) &
                     (data[Columns.LEVEL] == level)) |
                    (data[Columns.LEVEL] != level)
                ],
                level_df
            ],
            sort=False
        )
    data = pd.concat(df_list, sort=False)
    data = data[Columns.INDEX + draw_cols]
    return data


def create_diagnostics(before_data, after_data, draw_cols):
    before_data['mean_before'] = before_data[draw_cols].mean(axis=1)
    after_data['mean_after'] = after_data[draw_cols].mean(axis=1)

    data = pd.merge(before_data[Columns.INDEX + ['mean_before']],
                    after_data[Columns.INDEX + ['mean_after']],
                    on=Columns.INDEX, how='outer')
    # unscaled data will have NaN's for any shocks
    data.fillna(0, inplace=True)
    return data


def _merge_cause_hierarchy_columns(df: pd.DataFrame,
                                   hierarchy_df: pd.DataFrame):
    return pd.merge(df,
                    hierarchy_df[Columns.CAUSE_HIERARCHY_AGGREGATION],
                    on=Columns.CAUSE_ID,
                    how='inner')
