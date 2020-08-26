import logging
import pandas as pd

import gbd.constants as gbd

from dalynator.data_source import DataSource

logger = logging.getLogger(__name__)


class DataFilter(DataSource):
    """A kind of DataSource that conceptually does not compute any new data,
    just changes the format. Always has set_input_data_frame"""


class AddColumnsFilter(DataFilter):
    def __init__(self, columns_to_add):
        self.columns_to_add = columns_to_add

    def set_input_data_frame(self, input_df):
        self.input_df = input_df

    def get_data_frame(self):
        draw_index = self.input_df.columns.get_loc("draw_0")
        for column_name, column_value in self.columns_to_add.items():
            if column_name not in self.input_df.columns:
                self.input_df.insert(draw_index - 1, column_name, column_value)
                draw_index += 1
        return self.input_df


class PAFInputFilter(DataFilter):
    """ Transforms incoming PAF to match format expected for attribution """
    MORBIDITY_MORTALITY_MEASURES = {'yll': [gbd.measures.DEATH, gbd.measures.YLD], 'yld': [gbd.measures.YLL]}

    def __init__(self,
                 index_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'rei_id',
                                'measure_id', 'star_id'],
                 draw_columns=['draw_{}'.format(x) for x in range(1000)]):
        super(PAFInputFilter, self).__init__(name='paf filter')
        self.index_columns = index_columns
        self.draw_columns = draw_columns

    def set_input_data_frame(self, input_df):
        """
        If the star_id column is not present, then add it, default value 0.
        """
        keep_columns = self.index_columns + self.draw_columns
        if 'star_id' not in input_df.columns.values.tolist():
            keep_columns.remove('star_id')
            self.input_df = input_df[keep_columns]
            logger.debug("Adding star_id column to input paf df")
            self.input_df['star_id'] = gbd.star.UNDEFINED
        else:
            self.input_df = input_df[keep_columns]

    def _load_data_frame(self):
        death_df = self.input_df.query('measure_id == {}'.format(gbd.measures.YLL))
        death_df = death_df.replace(
            {'measure_id': {gbd.measures.YLL: gbd.measures.DEATH}})
        output = pd.concat([self.input_df, death_df])
        # Final format
        keep_columns = self.index_columns + self.draw_columns
        output = output[keep_columns]
        return output
