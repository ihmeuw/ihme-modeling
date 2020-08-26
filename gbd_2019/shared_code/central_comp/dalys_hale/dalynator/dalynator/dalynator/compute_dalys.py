import logging

import gbd.constants as gbd

from dalynator.computation_element import ComputationElement
from dalynator.data_source import DataSource

logger = logging.getLogger(__name__)


class ComputeDalys(ComputationElement):
    """Compute DALYs from YLL and YLD.
     The math is simple:  daly = yll + yld
    The data source must have exactly the following indexes:
        ['location_id', 'year_id', 'age_group_id', 'sex_id', 'cause_id',
         'measure_id', 'metric_id']
        Any other 'extra' non-draw columns (e.g. "pop") will be carried
        through unchanged.

    Input validation:
        for CoD (YLL) input:
        measure must be YLL (==4)
        metric must be number (==1)

        for Epi (YLD) input:
        measure must be YLD (==3)
        metric must be number space (==1)

        All numbers must be non-negative, and not None, and not NaN
        Each unique index_cols must have exactly one row.

    Output:
        measure will be DALY (==2)
        metric will be number (==1)
    """

    END_MESSAGE = "END compute daly"

    MINIMUM_INDEXES = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                       'cause_id', 'measure_id', 'metric_id']
    NOT_DEMO_COLUMNS = ['envelope', 'model_version_id', 'model_version_type_id']
    """Must have at least these indexes"""

    def __init__(self,
                 yll_data_frame, yld_data_frame,
                 data_cols,
                 index_cols=MINIMUM_INDEXES,
                 ):
        self.yll_data_frame = yll_data_frame
        self.yld_data_frame = yld_data_frame
        self.data_cols = data_cols
        self.index_cols = index_cols

    def get_data_frame(self):

        # Validate inputs
        logger.info("BEGIN compute_dalys")
        yll_df = self.yll_data_frame
        logger.debug("  read yll")
        ComputeDalys.validate_measure_and_metric(yll_df, "yll",
                                                 gbd.measures.YLL,
                                                 gbd.metrics.NUMBER)
        logger.debug("  validated yll {}".format(yll_df.shape))

        yld_df = self.yld_data_frame
        logger.debug("  read yld")
        ComputeDalys.validate_measure_and_metric(yld_df, "yld",
                                                 gbd.measures.YLD,
                                                 gbd.metrics.NUMBER)
        logger.debug("  validated yld {}".format(yld_df.shape))

        # Chained data frame operations to get YLL and YLD frames into
        # shapes/indexing required for summation
        to_drop = self.NOT_DEMO_COLUMNS + ['measure_id']
        to_sum_indexes = list(set(self.index_cols) - set(
            to_drop))
        ylls_to_sum = yll_df.drop(
            to_drop, axis=1, errors='ignore').set_index(
                to_sum_indexes)
        ylds_to_sum = yld_df.drop(
            to_drop, axis=1, errors='ignore').set_index(
                to_sum_indexes)

        # The line that actually computes DALYs. Pandas does it all for us!
        daly_df = ylls_to_sum.add(ylds_to_sum, fill_value=0.0)
        logger.info("  added yll and yld {}".format(daly_df.shape))

        daly_df.reset_index(inplace=True)
        daly_df['measure_id'] = gbd.measures.DALY

        # Framewise addition often reorders the columns. Order them back again
        ds = DataSource('Computed Dalys', data_frame=daly_df)
        daly_df = ds._normalize_columns()

        logger.debug("  final shape {}".format(daly_df.shape))
        logger.info(ComputeDalys.END_MESSAGE)
        return daly_df

    @staticmethod
    def log_and_raise(error_message):
        logger.error(error_message)
        raise ValueError(error_message)

    @staticmethod
    def validate_measure_and_metric(df, df_name, measure, metric):
        """Check that measure_id and metric are present and correct.
        Raise a ValueError if either does not match."""

        missing_indexes = [x for x in ComputeDalys.MINIMUM_INDEXES
                           if x not in df.columns]
        if missing_indexes:
            ComputeDalys.log_and_raise("Missing required index {} "
                                       "from dataframe {}"
                                       .format(missing_indexes, df_name))

        if 'measure_id' in df.columns:
            bad = df['measure_id'] != measure
            if any(bad):
                ComputeDalys.log_and_raise("measure_id for {} must be {}, "
                                           "found bad rows"
                                           .format(df_name, measure))
        else:
            ComputeDalys.log_and_raise("column 'measure_id' missing in {}"
                                       .format(df_name))

        if 'measure_id' in df.columns:
            bad = df['metric_id'] != metric
            if any(bad):
                ComputeDalys.log_and_raise("metric_id for {} must be {}, "
                                           "found bad rows"
                                           .format(df_name, metric))
        else:
            ComputeDalys.log_and_raise("column 'metric_id' missing in {}"
                                       .format(df_name))
