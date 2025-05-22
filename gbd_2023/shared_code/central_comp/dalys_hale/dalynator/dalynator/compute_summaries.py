import logging

from core_maths.summarize import get_summary

from dalynator.computation_element import ComputationElement
from dalynator.lib.utils import get_index_draw_columns

logger = logging.getLogger(__name__)


class ComputeSummaries(ComputationElement):
    """Compute the summaries (mean, median, lower, upper) of ALL draw columns.

    The data source must have exactly the following indexes:
        ['location_id', 'year_id', 'age_group_id', 'sex_id',
        'cause_id', 'measure_id', 'metric_id']
        Any other 'extra' non-draw columns (e.g. "pop") will be carried
        through unchanged.
    """

    END_MESSAGE = "END compute summaries"

    MINIMUM_INDEXES = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                       'cause_id', 'measure_id', 'metric_id']
    """Must have at least these indexes"""

    def __init__(self,
                 in_df,
                 write_out_columns,
                 index_cols=MINIMUM_INDEXES,
                 ):
        self.in_df = in_df
        self.write_out_columns = write_out_columns
        self.index_cols = index_cols

    def get_data_frame(self):
        logger.info("BEGIN compute summaries")

        self.validate_measure_and_metric(self.in_df, "incoming dataframe")
        logger.debug("validated")

        _, draw_cols = get_index_draw_columns(self.in_df)
        sumdf = get_summary(self.in_df, draw_cols)
        sumdf = sumdf.reset_index()
        del sumdf['index']
        del sumdf['median']

        if 'pct_change_means' in sumdf:
            logger.info("replacing mean of pct change distribution with pct "
                        "change of means")
            sumdf['mean'] = sumdf['pct_change_means']
        sumdf = sumdf[self.write_out_columns]

        return sumdf

    @staticmethod
    def log_and_raise(error_message):
        logger.error(error_message)
        raise ValueError(error_message)

    def validate_measure_and_metric(self, df, df_name):
        """Set the index
        TODO rename?"""

        non_draw_indexes, _ = get_index_draw_columns(df)
        if non_draw_indexes:
            logger.debug(
                "dataframe {} has non-draw indexes {}, re-indexing".format(
                    df_name, non_draw_indexes))
            df.reset_index(inplace=True)
            # The set of index columns will now typically be longer
            index_cols, _ = get_index_draw_columns(df)
            df.set_index(index_cols, inplace=True)
            self.index_cols = index_cols
