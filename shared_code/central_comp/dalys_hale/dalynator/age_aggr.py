import logging

from adding_machine import summarizers as sm
from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)


class AgeAggregator(ComputationElement):
    """ Age aggregation using adding machine """
    def __init__(self, data_frame, data_cols, index_cols, data_container=None,
                 age_groups={22: (0, 200), 27: (0, 200)}, include_pre_df=True,
                 gbd_compare_ags=True):
        self.data_frame = data_frame
        self.data_cols = data_cols
        self.index_cols = index_cols
        self.data_container = data_container
        self.age_groups = age_groups
        self.include_pre_df = include_pre_df
        self.gbd_compare_ags = gbd_compare_ags

    def get_data_frame(self):
        df = self.data_frame
        if self.data_container:
            sm.Globals.pop = self.data_container['pop']
            sm.Globals.aw = self.data_container['age_weights']
            sm.Globals.ags = self.data_container['age_spans']

        # Make sure DF only contains 1 metric. Cannot use combine-ages on
        # mixed metrics
        metrics = df.metric_id.unique()
        if len(metrics) > 1:
            raise ValueError("Can only combine_ages() on one metric at a "
                             "time. Got {}".format(metrics))
        else:
            metric = metrics[0]

        if self.data_container:
            new_result = sm.combine_ages(
                df, self.gbd_compare_ags, metric, self.age_groups,
                force_cartesian_product=True,
                force_use_global_age_group_set=True)
        else:
            new_result = sm.combine_ages(df, self.gbd_compare_ags, metric,
                                         self.age_groups)

        if self.include_pre_df:
            df = df.append(new_result)
            return df
        else:
            return new_result
