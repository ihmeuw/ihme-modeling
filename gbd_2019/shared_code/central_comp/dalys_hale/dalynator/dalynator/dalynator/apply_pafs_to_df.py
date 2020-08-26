import logging

from dalynator.computation_element import ComputationElement
from dalynator.apply_pafs import ApplyPAFs

logger = logging.getLogger(__name__)


class ApplyPafsToDf(ComputationElement):
    def __init__(self, pafs_filter_df, data_frame, n_draws):
        self.pafs_filter_df = pafs_filter_df
        self.df = data_frame
        self.n_draws = n_draws

    def get_data_frame(self):
        logger.info("BEGIN apply PAFs")
        paf_dcs = ['draw_{}'.format(x) for x in range(self.n_draws)]
        cause_dcs = ['draw_{}'.format(x) for x in range(self.n_draws)]
        ce = ApplyPAFs(
            self.pafs_filter_df,
            self.df,
            paf_data_columns=paf_dcs,
            cause_data_columns=cause_dcs,
        )
        paf_df = ce.get_data_frame()

        logger.debug(" paf_df shape {}".format(paf_df.shape))

        return paf_df
