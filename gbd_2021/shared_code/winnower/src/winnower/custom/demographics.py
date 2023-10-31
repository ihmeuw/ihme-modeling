"""
The Demographics module.

This module has a wart in that it advertises columns in execute()'s returned
DataFrame that may not be present. This can occur under one known condition:
    all values in the resulting column are NaN

If this is an issue the logical place to fix the problem is
_merge_output_columns_into_df, likely by merely returning columns even if they
are entirely NaN values.
"""
from attr import attrib
import pandas

from winnower.constants import (
    Sex,
    SurveyModule,
)
from winnower.transform.base import attrs_with_logger
from winnower.util.age_calculator import (
    AgeKeys as DemographicsKeys,
    AgeCalculator,
)
from winnower.util.categorical import (
    category_codes,
    is_categorical,
)
from winnower.util.dataframe import as_column_type
from .base import TransformBase
from winnower import errors


# Constants
# Helper classes
@attrs_with_logger
class SexCalculator:
    """
    Calculates sex_id field.
    """
    inputs = ('sex', 'sex_male', 'sex_female')
    outputs = ('sex_id',)

    sex_col = attrib()
    male_values = attrib()
    female_values = attrib()

    def sex_id_from_sex_fields(self, df):
        sex_id = pandas.Series(float('nan'), index=df.index, name='sex_id')
        sex_id[self._male_mask(df)] = Sex.MALE
        sex_id[self._female_mask(df)] = Sex.FEMALE
        return sex_id

    def _male_mask(self, df):
        return self._gender_mask(self.male_values, df)

    def _female_mask(self, df):
        return self._gender_mask(self.female_values, df)

    def _gender_mask(self, sex_values, df):
        # convert input values into type determined by pandas when loading
        # sort of a chicken-and-egg problem: users input text which may be str
        # or int values
        col = df[self.sex_col]
        if is_categorical(col):
            col = category_codes(col)
        values = as_column_type(col, sex_values)
        return col.isin(values)


class Transform(TransformBase):
    """
    Processes Demographics inputs to provide output indicators.

    Of primary interest are determining the age of subjects as year/month/day
    values. Efforts are also made to provide the interview day/month/year
    as integers.

    Finally the sex of each participant is provided.
    """
    def __init__(self, columns, config, extraction_metadata):
        super().__init__(columns, config, extraction_metadata)
        demographics_keys_prefix = ''
        self.keys = DemographicsKeys(demographics_keys_prefix)
        self.age_calc = AgeCalculator(self.columns, self.config, self.keys)
        self.uses_extra_columns = []

    def validate(self, input_columns):
        """
        Appends columns to self.uses_extra_columns
        Validates that the transform can actually be performed.
        """
        if self.columns:
            self.record_uses_column_if_configured('sex')
            if self.keys.columns():
                for k in self.keys.columns():
                    self.record_uses_column_if_configured(k)

            self.record_uses_column_if_configured('int_date')
            self.record_uses_column_if_configured('birth_date')

    def output_columns(self, input_columns):
        """
        Appends new columns to input_columns and returns it when new
        columns are generated during transform
        """
        res = list(input_columns)
        if self._any_sex_calculations_possible():
            res.append('sex_id')
        res.extend(self.age_calc.calculated_columns())
        return res

    def execute(self, df):
        if self._any_sex_calculations_possible():
            df = self._add_sex_id(df)
        df = self.age_calc.calculate_ages(df)
        return df

    # General utility
    def _any_sex_calculations_possible(self):
        "Predicate: are any sex calculations possible?"
        if self.config.get('sex'):
            return True

        if self.config['survey_module'] in \
                {SurveyModule.MEN, SurveyModule.WOMEN}:
            return True

        return False

    # functions facilitating specific indicators
    def _add_sex_id(self, df):
        if self.config.get('sex'):
            sex_calc = SexCalculator(
                sex_col=self.get_column_name(self.config['sex']),
                male_values=self.config['sex_male'],
                female_values=self.config['sex_female'],
            )
            df = df.assign(sex_id=sex_calc.sex_id_from_sex_fields(df))
        else:
            sm = self.config['survey_module']
            if sm == SurveyModule.MEN:
                df['sex_id'] = Sex.MALE
            elif sm == SurveyModule.WOMEN:
                df['sex_id'] = Sex.FEMALE
        return df
