import logging

import pandas as pd
import gbd.constants as gbd

from cluster_utils.pandas_utils import get_index_columns

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)


def risk_attr_burden_to_paf(risk_cause_df, hundred_percent_pafs_df, value_cols,
                            demographic_cols=['location_id', 'year_id',
                                              'age_group_id', 'sex_id',
                                              'measure_id', 'metric_id'],
                            ):
    """Takes a dataframe whose values represent risk-attributable burden
    and convert those to PAFs"""
    # Get the cause-level envelope
    if 'star_id' in risk_cause_df:
        burden_by_cause = risk_cause_df.query(
            'rei_id == @gbd.risk.TOTAL_ATTRIBUTABLE and '
            'star_id == @gbd.star.ANY_EVIDENCE_LEVEL')
    else:
        burden_by_cause = risk_cause_df.query(
            'rei_id == @gbd.risk.TOTAL_ATTRIBUTABLE')

    logger.info("APPLY PAFS BEGIN burden_by_cause {}"
                .format(get_index_columns(burden_by_cause)))
    # Merge cause-level envelope onto data
    # Do not merge on star_id. For pafs we don't care about the star,
    # just it proportion.
    # Left hand side will be in 1..5, RHS will be == 6
    paf_df = risk_cause_df.merge(burden_by_cause,
                                 on=demographic_cols + ['cause_id'],
                                 suffixes=('', '_bbc'))
    # Divide attributable burden by cause-level envelope
    bbc_vcs = ["{}_bbc".format(col) for col in value_cols]
    paf_df[value_cols] = paf_df[value_cols].values / paf_df[bbc_vcs].values
    paf_df[value_cols] = paf_df[value_cols].fillna(0)

    # Set certain cause-risk pairs to 100 % pafs
    # This should not happen on age standardized pafs
    if hundred_percent_pafs_df.empty:
        logger.debug("No hundred-percent PAFs detected")
    else:
        hundred_percent_pafs_df['full_paf'] = 1
        paf_df = pd.merge(paf_df, hundred_percent_pafs_df,
                          on=['cause_id', 'rei_id'], how='left')

        set_to_one = (
            (paf_df['full_paf'] == 1) &
            (paf_df['age_group_id'] != gbd.age.AGE_STANDARDIZED)
        )
        paf_rows = paf_df.loc[set_to_one].index.tolist()
        # for all the 100% pafs, make sure that the draws arent all equal
        # to 0. If they are all 0 they are not 100% attributable
        should_be_one_rows = paf_df.index.isin(paf_rows)
        not_actually_one_rows = (
            paf_df.loc[should_be_one_rows, value_cols] == 0).all(axis=1)
        paf_rows = list(
            set(paf_rows) - set(
                not_actually_one_rows[not_actually_one_rows].index))
        paf_df.loc[paf_rows, value_cols] = 1.0

    # Change metric to percent
    paf_df['metric_id'] = gbd.metrics.PERCENT
    logger.info("APPLY PAFS END")
    # Keep only the columns we need
    if 'star_id' in paf_df:
        keep_cols = demographic_cols + ['cause_id', 'rei_id', 'star_id'] + value_cols
    else:
        keep_cols = demographic_cols + ['cause_id', 'rei_id'] + value_cols
    return paf_df[keep_cols]


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
        # Notice that the columns in this new dataframe are named draw_i,
        # and are attributable burden draws
        attributable_burden_df = pd.DataFrame(
            ra_df[paf_cols].values * ra_df[cause_cols].values,
            columns=cause_cols,
            index=ra_df.index)

        ra_df = ra_df[self.index_columns].join(attributable_burden_df)
        logger.info("END apply_pafs")

        return ra_df


class RecalculatePAFs(ComputationElement):
    """ Recalculates PAFs using PAF-attributed and cause level data

    The math of applying PAFs is very simple:
        Risk attributable data / Cause-level data = PAFs
    """
    def __init__(self, risk_data_frame, cause_data_frame,
                 risk_data_columns, cause_data_columns,
                 risk_index_columns=['location_id', 'year_id', 'sex_id',
                                     'age_group_id', 'cause_id', 'rei_id',
                                     'star_id',
                                     'measure_id', 'metric_id'],
                 cause_index_columns=['location_id', 'year_id', 'sex_id',
                                      'age_group_id', 'cause_id', 'measure_id',
                                      'metric_id'],
                 merge_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'measure_id',
                                'metric_id'],
                 index_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'rei_id',
                                'measure_id', 'metric_id']):
        self.risk_data_frame = risk_data_frame
        self.cause_data_frame = cause_data_frame
        self.risk_index_columns = risk_index_columns
        self.risk_data_columns = risk_data_columns
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
        logger.info("BEGIN calculate_pafs")
        # Get data
        logger.debug("  read risk-attributed data")
        risk_data_df = self.risk_data_frame

        risk_data_df = risk_data_df.loc[(
            risk_data_df['metric_id'] == gbd.metrics.NUMBER,
            self.risk_index_columns + self.risk_data_columns)]
        logger.debug("  read cause data")
        cause_data_df = self.cause_data_frame

        cause_data_df = cause_data_df.loc[(
            cause_data_df['metric_id'] == gbd.metrics.NUMBER,
            self.cause_index_columns + self.cause_data_columns)]
        # Merge paf and cause data together to get risk attributable dataframe
        logger.debug("  merge pafs and cause data")
        new_col_names, risk_cols = self.generate_data_columns(
            self.risk_data_columns, 'risk')
        risk_data_df.rename(columns=new_col_names, inplace=True)
        new_col_names, cause_cols = self.generate_data_columns(
            self.cause_data_columns, 'draw')
        cause_data_df.rename(columns=new_col_names, inplace=True)
        paf_df = pd.merge(risk_data_df, cause_data_df, on=self.merge_columns,
                          how='left', indicator=True)
        assert len(paf_df.loc[paf_df['_merge'] != 'both']) == 0
        paf_df.drop('_merge', axis=1, inplace=True)
        # Apply PAFs
        logger.debug("  calculate pafs")
        new_vals = pd.DataFrame(
            paf_df[risk_cols].values / paf_df[cause_cols].values,
            columns=cause_cols,
            index=paf_df.index)
        paf_df = paf_df[self.index_columns].join(new_vals)
        paf_df['metric_id'] = gbd.metrics.PERCENT
        logger.info("END calculate_pafs")
        return paf_df
