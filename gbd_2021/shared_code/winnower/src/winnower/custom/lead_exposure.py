from .base import TransformBase

class Transform(TransformBase):
    
    def execute(self, df):
        df = self.convert_bll_using_multiplier(df)
        return df

    def convert_bll_using_multiplier(self, df):
        if bool(self.config['multiplier']):
            df['bll'] *= float(self.config['multiplier'])
        return df
    
    def output_columns(self, input_columns):
        return input_columns