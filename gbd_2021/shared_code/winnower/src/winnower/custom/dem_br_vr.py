from .base import TransformBase
from winnower.util.age_calculator import (
    AgeCalculator,
    AgeKeys
)


class Transform(TransformBase):
    def __init__(self, columns, config, extraction_metadata):
        super().__init__(columns, config, extraction_metadata)
        # date_type='reg' is to process reg_date and output (reg_year, reg_month, reg_day) columns
        self.keys = AgeKeys(prefix='', date_type='reg')
        self.reg_date_calc = AgeCalculator(columns, config, self.keys)

    def output_columns(self, input_columns):
        "Add registration columns to list of output_columns."
        output_columns = list(input_columns)

        # Add columns to process 'reg_date'.
        output_columns.extend(col for col in self.reg_date_calc.calculated_columns() if col not in input_columns)
        return(output_columns)

    def execute(self, df):
        """
        Calculate 'reg_year', 'reg_month' and 'reg_day'.
        """
        df = self.reg_date_calc.calculate_ages(df)
        return(df)
