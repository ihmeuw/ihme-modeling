import logging

logger = logging.getLogger(__name__)

from adding_machine.summarizers import pct_change

from dalynator.computation_element import ComputationElement


class ComputePercentageChange(ComputationElement):
    """Compute percent change based on calculation in adding_machine.

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
        logger.info("BEGIN compute percent change")
        # Check to make sure years are correct
        logger.debug("  check year information")
        if self.start_year >= self.end_year:
            msg = "Start year ({}) must come before end year ({})".format(
                self.start_year, self.end_year)
            self.log_and_raise(msg)
        logger.debug("  read data")
        data = self.data_frame
        logger.debug("  calculate percent change")
        pct_data_df = pct_change(data,
                                 self.start_year,
                                 self.end_year,
                                 change_type='pct_change')
        logger.info("END compute percent change")
        return pct_data_df
