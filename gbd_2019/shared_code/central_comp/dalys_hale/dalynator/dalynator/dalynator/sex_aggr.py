import logging

import gbd.constants as gbd

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)


class SexAggregator(ComputationElement):
    """ sex aggregation by location, age, year, cause, set sex_id to 3
    (gbd.sex.BOTH)
    """

    def __init__(self, data_frame, data_cols, index_cols, include_pre_df=True,
                 data_container=None):
        self.data_frame = data_frame
        self.data_cols = data_cols
        self.index_cols = index_cols
        self.include_pre_df = include_pre_df
        self.data_container = data_container

    def get_data_frame(self):
        pre_sex_df = self.data_frame
        metrics = pre_sex_df.metric_id.unique()
        if len(metrics) > 1:
            raise ValueError("Can only combine sexes for one metric at a "
                             "time. Got {}".format(metrics))
        else:
            in_metric = metrics[0]

        # Summation should always be done in NUMBER space, so make sure
        # the input data frame is properly transformed ...
        if in_metric == gbd.metrics.RATE:
            if not self.data_container:
                raise ValueError("SexAggregator requires a data_container if "
                                 "working with a dataframe in RATE space")
            to_sum_df = self.data_container._convert_rate_to_num(pre_sex_df)
        elif in_metric == gbd.metrics.NUMBER:
            to_sum_df = pre_sex_df
        else:
            raise ValueError("SexAggregator can only work with metric_ids "
                             "{r} or {n}. Got {m}".format(r=gbd.metrics.RATE,
                                                          n=gbd.metrics.NUMBER,
                                                          m=in_metric))

        self.index_cols = self.create_index_column_list(pre_sex_df)

        sex_df = to_sum_df.groupby(self.index_cols).sum()
        sex_df['sex_id'] = gbd.sex.BOTH
        sex_df = sex_df.reset_index()

        # ... and back-transformed accordingly
        if in_metric == gbd.metrics.RATE:
            sex_df = self.data_container._convert_num_to_rate(sex_df)
        sex_df['metric_id'] = in_metric

        if self.include_pre_df:
            sex_df = sex_df.append(pre_sex_df)
        return sex_df

    def add_index_column(self, group_index_cols, column_name, df):
        if column_name in df.columns:
            group_index_cols.append(column_name)

    def create_index_column_list(self, df):
        cols = ['location_id', 'year_id', 'age_group_id']
        for col_name in ['cause_id', 'sequela_id', 'rei_id', 'star_id',
                         'measure_id']:
            if col_name in df.columns:
                cols.append(col_name)
        return cols
