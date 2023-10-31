from winnower.custom.base import TransformBase
import pandas
import numpy


class Transform(TransformBase):
    def output_columns(self, input_columns):
        result = list(input_columns)
        result.append("inj_in_fall")
        return result

    def execute(self, df):
        df['inj_in_fall'] = numpy.nan
        mask1 = (df['fall'].notna()) | (df['fall_inj'].notna())
        df.loc[mask1, 'inj_in_fall'] = 0
        mask2 = (df['fall'] == 1 ) & (df['fall_inj'] == 1)
        df.loc[mask2, 'inj_in_fall'] = 1

        return df