import logging

import pandas as pd

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)


class ApplyPAFs(ComputationElement):
    """ Apply PAFs to cause level data

    The math of applying PAFs is very simple:
        Cause-level data * PAFs = Risk attributable data
    """
    def __init__(self, paf_data_frame, cause_data_frame,
                 paf_data_columns, cause_data_columns,
                 paf_index_columns=['location_id', 'year_id', 'sex_id',
                                    'age_group_id', 'cause_id', 'rei_id',
                                    'star_id', 'measure_id'],
                 cause_index_columns=['location_id', 'year_id', 'sex_id',
                                      'age_group_id', 'cause_id', 'measure_id',
                                      'metric_id'],
                 merge_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'measure_id'],
                 index_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'rei_id',
                                'star_id', 'measure_id', 'metric_id']):
        self.paf_data_frame = paf_data_frame
        self.cause_data_frame = cause_data_frame
        self.paf_index_columns = paf_index_columns
        self.paf_data_columns = paf_data_columns
        self.cause_index_columns = cause_index_columns
        self.cause_data_columns = cause_data_columns
        self.merge_columns = merge_columns
        self.index_columns = index_columns

    def generate_data_columns(self, data_columns, prefix):
        new_col_names = {x: '{}_{}'.format(prefix, i)
                         for i, x in enumerate(data_columns)}
        new_draw_cols = ['{}_{}'.format(prefix, i)
                         for i, x in enumerate(data_columns)]
        return new_col_names, new_draw_cols

    def get_data_frame(self):
        logger.info("BEGIN apply_pafs")
        # Get data
        logger.debug("  read pafs")
        pafs_df = self.paf_data_frame
        logger.debug("  read cause data")
        cause_data_df = self.cause_data_frame
        # Merge paf and cause data together to get risk attributable dataframe
        logger.debug("  merge pafs and cause data")
        new_col_names, paf_cols = self.generate_data_columns(
            self.paf_data_columns, 'paf')
        pafs_df.rename(columns=new_col_names, inplace=True)
        new_col_names, cause_cols = self.generate_data_columns(
            self.cause_data_columns, 'draw')
        cause_data_df.rename(columns=new_col_names, inplace=True)
        ra_df = pd.merge(pafs_df, cause_data_df, on=self.merge_columns)

        # Apply PAFs
        attributable_burden_df = pd.DataFrame(
            ra_df[paf_cols].values * ra_df[cause_cols].values,
            columns=cause_cols,
            index=ra_df.index)

        ra_df = ra_df[self.index_columns].join(attributable_burden_df)
        logger.info("END apply_pafs")

        return ra_df
