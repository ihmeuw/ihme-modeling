import logging

from core_maths.summarize import pct_change

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)


class ComputePctChange(ComputationElement):
    """Compute pct change based on calculation in core_maths.

    """

    def __init__(self,
                 data_frame,
                 start_year,
                 end_year,
                 index_columns,
                 data_columns
                 ):
        self.data_frame = data_frame
        self.start_year = start_year
        self.end_year = end_year
        self.index_columns = index_columns
        self.data_columns = data_columns

    @staticmethod
    def log_and_raise(error_message):
        logger.error(error_message)
        raise ValueError(error_message)

    def get_data_frame(self):
        logger.info("BEGIN compute pct change")
        # Check to make sure years are correct
        logger.debug("  check year information")
        if self.start_year >= self.end_year:
            msg = "Start year ({}) must come before end year ({})".format(
                self.start_year, self.end_year)
            self.log_and_raise(msg)
        logger.debug("  read data")
        data = self.data_frame
        logger.debug("  calculate pct change")
        pct_data_df = pct_change(data,
                                 self.start_year,
                                 self.end_year,
                                 time_col='year_id',
                                 data_cols=self.data_columns,
                                 change_type='pct_change',
                                 index_cols=list(set(self.index_columns) -
                                                 set(['year_id'])))
        logger.info("END compute pct change")
        return pct_data_df
