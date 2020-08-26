import logging
import time

import pandas as pd

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)


class AggregateCauses(ComputationElement):
    """ Aggregate data up a specified cause hierarchy """

    def __init__(self, cause_tree, data_frame,
                 index_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'rei_id',
                                'star_id', 'measure_id', 'metric_id']):
        self.cause_tree = cause_tree
        self.data_frame = data_frame
        self.index_columns = index_columns

    def aggregate(self):
        """
        NOTE: .groupby().sum() on an empty dataframe can cause a change in the
        dtypes of the index columns.  This is a pandas bug.
        """
        # Get data
        data = self.data_frame
        # Aggregate up cause hierarchy
        max_depth = self.cause_tree.max_depth()
        level = max_depth - 1
        while level >= 0:
            logger.info("  aggregating level {le} of {md}".format(
                le=level, md=max_depth))
            aggs = []
            for cause in self.cause_tree.level_n_descendants(level):
                child_ids = [c.id for c in cause.children]
                if len(child_ids) > 0:
                    agg = data.loc[data['cause_id'].isin(child_ids)
                                   ].copy(deep=True)
                    if not agg.empty:
                        agg['cause_id'] = cause.id
                        agg = agg.groupby(
                            self.index_columns).sum().reset_index()
                        aggs.append(agg)
            if aggs:
                aggs = pd.concat(aggs)
                data = pd.concat([data, aggs], sort=True)
            level -= 1
        data = data.groupby(self.index_columns).sum().reset_index()
        return data

    def get_data_frame(self):
        # Logging the epoch time makes it easier to extract profile times
        # from a log
        logger.info("BEGIN aggregate_causes, epoch-time {}".format(time.time()))
        data = self.aggregate()
        logger.info("END aggregate_causes, epoch-time {}".format(time.time()))
        return data
