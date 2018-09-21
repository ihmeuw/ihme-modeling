import logging

import pandas as pd
import gbd.constants as gbd

from computation_element import ComputationElement

logger = logging.getLogger(__name__)


def get_hundred_percent_pafs():
    cause_risk_pairs = [
        [381, 92], [381, 169], [381, 203], [381, 247], [381, 248], [381, 249],
        [381, 334], [381, 335], [381, 339], [387, 92], [387, 94], [387, 169],
        [387, 203], [387, 239], [387, 240], [387, 247], [387, 248], [387, 249],
        [389, 92], [389, 96], [389, 169], [389, 203], [389, 247], [389, 248],
        [389, 249], [390, 92], [390, 95], [390, 169], [390, 203], [390, 247],
        [390, 248], [390, 249], [394, 169], [394, 170], [394, 203], [394, 247],
        [394, 248], [394, 249], [395, 169], [395, 170], [395, 203], [395, 247],
        [395, 248], [395, 249], [396, 169], [396, 170], [396, 203], [396, 247],
        [396, 248], [396, 249], [397, 169], [397, 170], [397, 203], [397, 247],
        [397, 248], [397, 249], [398, 169], [398, 170], [398, 203], [398, 247],
        [398, 248], [398, 249], [399, 169], [399, 170], [399, 203], [399, 247],
        [399, 248], [399, 249], [420, 101], [420, 102], [420, 169], [420, 203],
        [420, 247], [420, 248], [420, 249], [432, 169], [432, 170], [432, 203],
        [432, 247], [432, 248], [432, 249], [498, 104], [498, 107], [498, 169],
        [498, 246], [498, 248], [498, 249], [511, 126], [511, 161], [511, 169],
        [511, 202], [511, 246], [511, 247], [511, 249], [512, 126], [512, 150],
        [512, 169], [512, 202], [512, 246], [512, 247], [512, 249], [513, 126],
        [513, 129], [513, 169], [513, 202], [513, 246], [513, 247], [513, 249],
        [514, 126], [514, 129], [514, 169], [514, 202], [514, 246], [514, 247],
        [514, 249], [524, 101], [524, 102], [524, 169], [524, 203], [524, 247],
        [524, 248], [524, 249], [560, 101], [560, 102], [560, 169], [560, 203],
        [560, 247], [560, 248], [560, 249], [562, 101], [562, 103], [562, 138],
        [562, 169], [562, 203], [562, 247], [562, 248], [562, 249], [563, 101],
        [563, 103], [563, 138], [563, 169], [563, 203], [563, 247], [563, 248],
        [563, 249], [564, 101], [564, 103], [564, 138], [564, 169], [564, 203],
        [564, 247], [564, 248], [564, 249], [565, 101], [565, 103], [565, 138],
        [565, 169], [565, 203], [565, 247], [565, 248], [565, 249], [566, 101],
        [566, 103], [566, 138], [566, 169], [566, 203], [566, 247], [566, 248],
        [566, 249], [587, 104], [587, 105], [587, 141], [587, 169], [587, 246],
        [587, 248], [587, 249], [590, 104], [590, 105], [590, 169], [590, 246],
        [590, 248], [590, 249], [590, 341], [591, 104], [591, 107], [591, 169],
        [591, 246], [591, 248], [591, 249], [591, 341], [592, 104], [592, 169],
        [592, 246], [592, 248], [592, 249], [592, 341], [593, 104], [593, 169],
        [593, 246], [593, 248], [593, 249], [593, 341], [938, 101], [938, 102],
        [938, 169], [938, 203], [938, 247], [938, 248], [938, 249]
    ]
    df = pd.DataFrame(cause_risk_pairs, columns=['cause_id', 'rei_id'])
    return df


def risk_attr_burden_to_paf(risk_cause_df, value_cols,
                            demographic_cols=['location_id', 'year_id',
                                              'age_group_id', 'sex_id',
                                              'measure_id', 'metric_id'],
                            ):
    """Takes a dataframe whose values represent risk-attributable burden
    and convert those to PAFs"""
    # Get the cause-level envelope
    burden_by_cause = risk_cause_df.query('rei_id == 0')
    # Merge cause-level envelope onto data
    paf_df = risk_cause_df.merge(burden_by_cause,
                                 on=demographic_cols+['cause_id'],
                                 suffixes=('', '_bbc'))
    # Divide attributable burden by cause-level envelope
    bbc_vcs = ["{}_bbc".format(col) for col in value_cols]
    paf_df[value_cols] = paf_df[value_cols].values / paf_df[bbc_vcs].values
    paf_df[value_cols] = paf_df[value_cols].fillna(0)
    # Set certain cause-risk pairs to 100 % pafs
    # This should not happen on age standardized pafs
    hundred_percent_pafs_df = get_hundred_percent_pafs()
    hundred_percent_pafs_df['full_paf'] = 1
    paf_df = pd.merge(paf_df, hundred_percent_pafs_df,
                      on=['cause_id', 'rei_id'], how='left')
    set_to_one = (
        (paf_df['full_paf'] == 1) &
        (paf_df['age_group_id'] != gbd.age.AGE_STANDARDIZED)
    )
    paf_df.loc[set_to_one, value_cols] = 1.0
    # Change metric to percent
    paf_df['metric_id'] = gbd.metrics.PERCENT
    # Keep only the columns we need
    keep_cols = demographic_cols + ['cause_id', 'rei_id'] + value_cols
    return paf_df[keep_cols]


class ApplyPAFs(ComputationElement):
    """ Apply PAFs to cause level data

    The math of applying PAFs is very simple:
        Cause-level data * PAFs = Risk attributable data
    """



    def __init__(self, paf_data_frame, cause_data_frame,
                 paf_index_columns = ['location_id', 'year_id', 'sex_id',
                                      'age_group_id', 'cause_id', 'rei_id',
                                      'measure_id'],
                 paf_data_columns = ['draw_{}'.format(x) for x in xrange(1000)],
                 cause_index_columns = ['location_id', 'year_id', 'sex_id',
                                        'age_group_id', 'cause_id',
                                        'measure_id', 'metric_id'],
                 cause_data_columns = ['draw_{}'.format(x) for x in xrange(1000)],
                 merge_columns = ['location_id', 'year_id', 'sex_id',
                                  'age_group_id', 'cause_id', 'measure_id'],
                 index_columns = ['location_id', 'year_id', 'sex_id',
                                  'age_group_id', 'cause_id', 'rei_id',
                                  'measure_id', 'metric_id']):
        self.paf_data_frame = paf_data_frame
        self.cause_data_frame = cause_data_frame
        self.paf_index_columns = paf_index_columns
        self.paf_data_columns = paf_data_columns
        self.cause_index_columns = cause_index_columns
        self.cause_data_columns = cause_data_columns
        self.merge_columns = merge_columns
        self.index_columns = index_columns

    def generate_data_columns(self, data_columns, prefix):
        new_col_names = {x: '{}_{}'.format(prefix, i) for i, x in enumerate(data_columns)}
        new_draw_cols = ['{}_{}'.format(prefix, i) for i, x in enumerate(data_columns)]
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
        new_col_names, paf_cols = self.generate_data_columns(self.paf_data_columns, 'paf')
        pafs_df.rename(columns=new_col_names, inplace=True)
        new_col_names, cause_cols = self.generate_data_columns(self.cause_data_columns, 'draw')
        cause_data_df.rename(columns=new_col_names, inplace=True)
        ra_df = pd.merge(pafs_df, cause_data_df, on=self.merge_columns)

        # Apply PAFs
        new_vals = pd.DataFrame(
            ra_df[paf_cols].values * ra_df[cause_cols].values,
            columns=cause_cols,
            index=ra_df.index)

        ra_df = ra_df[self.index_columns].join(new_vals)
        logger.info("END apply_pafs")

        return ra_df


class RecalculatePAFs(ComputationElement):
    """ Recalculates PAFs using PAF-attributed and cause level data

    The math of applying PAFs is very simple:
        Risk attributable data / Cause-level data = PAFs
    """


    def __init__(self, risk_data_frame, cause_data_frame,
                 risk_index_columns=['location_id', 'year_id', 'sex_id',
                                     'age_group_id', 'cause_id', 'rei_id',
                                     'measure_id', 'metric_id'],
                 risk_data_columns=['draw_{}'.format(x) for x in xrange(1000)],
                 cause_index_columns=['location_id', 'year_id', 'sex_id',
                                      'age_group_id', 'cause_id', 'measure_id',
                                      'metric_id'],
                 cause_data_columns=['draw_{}'.format(x) for x in xrange(1000)],
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
        new_col_names = {x: '{}_{}'.format(prefix, i) for i, x in enumerate(data_columns)}
        new_draw_cols = ['{}_{}'.format(prefix, i) for i, x in enumerate(data_columns)]
        return new_col_names, new_draw_cols

    def get_data_frame(self):
        logger.info("BEGIN calculate_pafs")
        # Get data
        logger.debug("  read risk-attributed data")
        risk_data_df = self.risk_data_frame

        risk_data_df = risk_data_df.loc[(
            risk_data_df['metric_id']==gbd.metrics.NUMBER,
            self.risk_index_columns + self.risk_data_columns)]
        logger.debug("  read cause data")
        cause_data_df = self.cause_data_frame

        cause_data_df = cause_data_df.loc[(
            cause_data_df['metric_id']==gbd.metrics.NUMBER,
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
        assert len(paf_df.loc[paf_df['_merge']!='both']) == 0
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
