import pandas as pd

from data_source import DataSource


class DataFilter(DataSource):
    """A kind of DataSource that conceptually does not compute any new data, just changes the format.
    Always has set_input_data_frame"""


class AddColumnsFilter(DataFilter):
    def __init__(self, columns_to_add):
        self.columns_to_add = columns_to_add

    def set_input_data_frame(self, input_df):
        self.input_df = input_df

    def get_data_frame(self, desired_index):
        draw_index = self.input_df.columns.get_loc("draw_0")
        for column_name, column_value in self.columns_to_add.items():
            if column_name not in self.input_df.columns:
                self.input_df.insert(draw_index - 1, column_name, column_value)
                draw_index += 1
        return self.input_df


class PAFInputFilter(DataFilter):
    """ Transforms incoming PAF to match format expected for attribution """
    MORBIDITY_MORTALITY_MEASURES = {'yll': [1, 4], 'yld': [3]}

    def __init__(self,
                 index_columns=['location_id', 'year_id', 'sex_id',
                                'age_group_id', 'cause_id', 'rei_id'],
                 yll_columns=['paf_yll_{}'.format(x) for x in xrange(1000)],
                 yld_columns=['paf_yld_{}'.format(x) for x in xrange(1000)],
                 draw_columns=['draw_{}'.format(x) for x in xrange(1000)]):
        self.index_columns = index_columns
        self.yll_columns = yll_columns
        self.yld_columns = yld_columns
        self.draw_columns = draw_columns

    def set_input_data_frame(self, input_df):
        keep_columns = self.index_columns + self.yll_columns + self.yld_columns
        self.input_df = input_df[keep_columns]

    def slice_data_columns(self, data, measure_columns):
        # Find columns to keep
        keep_columns = self.index_columns + measure_columns
        # Slice DataFrame
        data = data[keep_columns]
        # Rename draw columns
        new_names = {k: v for k, v in zip(measure_columns, self.draw_columns)}
        data = data.rename(columns=new_names)
        return data

    def reshape_data(self):
        reshaped_data = []
        # YLLs
        temp = self.slice_data_columns(self.input_df, self.yll_columns)
        temp['yll_yld'] = 'yll'
        reshaped_data.append(temp)
        # YLDs
        temp = self.slice_data_columns(self.input_df, self.yld_columns)
        temp['yll_yld'] = 'yld'
        reshaped_data.append(temp)
        # Combine
        reshaped_data = pd.concat(reshaped_data)
        return reshaped_data

    def get_data_frame(self, desire_index_ignore=[]):
        # Reshape DataFrame
        data = self.reshape_data()
        # Expand data for each measure
        output = []
        for key, measure_ids in self.MORBIDITY_MORTALITY_MEASURES.iteritems():
            for measure_id in measure_ids:
                temp = data.loc[data['yll_yld']==key].copy(deep=True)
                temp['measure_id'] = measure_id
                output.append(temp)
        output = pd.concat(output)
        # Final format
        keep_columns = self.index_columns + ['measure_id'] + self.draw_columns
        output = output[keep_columns]
        return output
