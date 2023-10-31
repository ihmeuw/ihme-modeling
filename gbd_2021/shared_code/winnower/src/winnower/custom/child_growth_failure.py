import pandas

from winnower.util.age_calculator import (
    AgeCalculator,
    AgeKeys,
)
from .base import TransformBase
from winnower import errors


class Transform(TransformBase):
    ADJUSTED_COLUMNS = (
        'birth_weight', 'height_cm', 'weight_kg', 'height_age_perc',
        'height_age_sd', 'height_age_prm', 'weight_age_perc', 'weight_age_sd',
        'weight_age_prm', 'weight_height_perc', 'weight_height_sd',
        'weight_height_prm', 'mother_weight', 'mother_height', 'wealth_factor',
    )

    def __init__(self, columns, config, extraction_metadata):
        super().__init__(columns, config, extraction_metadata)
        self.keys = AgeKeys(prefix='mother_')

        # mother_birth_month is set to code_custom, which means no form processing
        # is done. Manually fix the form data by making it a sequence of values
        if 'mother_birth_month_missing' in config and config['mother_birth_month_missing']:  # noqa
            config['mother_birth_month_missing'] = config['mother_birth_month_missing'].split(',') # noqa

        self.mother_age_calc = AgeCalculator(columns, config, self.keys)
        self.uses_extra_columns = []

    def validate(self, input_columns):
        """
        Appends columns to read from data in self.uses_extra_columns
        """
        if self.keys.columns():
            for k in self.keys.columns():
                self.record_uses_column_if_configured(k)

        self.record_uses_column_if_configured('mother_int_date')
        self.record_uses_column_if_configured('mother_birth_date')

    def output_columns(self, input_columns):
        res = input_columns

        for col in self.ADJUSTED_COLUMNS:
            adjustment = self.config.get(f"{col}_adjust")
            if adjustment:
                res.extend(col)
        return res

    def execute(self, df):
        df = self.mother_age_calc.calculate_ages(df)
        df = self._adjust_columns(df)
        df = self._remove_misattributed_birth_weights(df)
        if not self._special_age_exception():
            df = self._drop_age_over_5(df)
        return df

    def _adjust_columns(self, df):
        for col in self.ADJUSTED_COLUMNS:
            adjustment = self.config.get(f"{col}_adjust")
            if adjustment:
                df[col] = df[col] / float(adjustment)

        return df

    def _remove_misattributed_birth_weights(self, df):
        """
        Remove birth weights misattributed to older children in MICS.

        From child_growth_failure.do
        """
        if self.config['survey_name'] not in {'UNICEF_MICS', 'UNICEF_MICS_ROMA'}:  # noqa
            return df
        if not self.config['birth_weight']:
            return df

        # calculate last child age so we can infer *not* last children
        last_child_age = pandas.Series(float('nan'), index=df.index)
        cols = ['psu', 'hh_id', 'birth_weight']
        grouper = df[cols].fillna('!_--_!').groupby(cols)  # NaNs break groupby
        for index in grouper.groups.values():
            if len(index) == 1:  # Performance shortcut
                if df.loc[index, 'birth_weight'].notna().all():
                    last_child_age.loc[index] = df.loc[index, 'age_month']
                continue
            g = df.loc[index, ['birth_weight', 'age_month']]
            g = g[g.birth_weight.notna()]
            last_child_age.loc[g.index] = g.age_month.min()

        not_last_child = ~(last_child_age == df.age_month)
        df.loc[not_last_child, 'birth_weight'] = float('NaN')
        if 'birth_weight_card' in df:
            df.loc[not_last_child, 'birth_weight_card'] = float('NaN')

        return df

    def _special_age_exception(self):
        "Special cases where birth weights from mothers are needed."
        s_path = str(self.config['file_path'])
        if s_path.endswith("<FILEPATH>"):
            return True
        elif s_path.endswith("<FILEPATH>"):  # noqa
            return True

        if self.config['nid'] in {234353, 4779, 23172, 323944}:
            return True

        return False

    def _drop_age_over_5(self, df):
        over_5 = pandas.Series(False, index=df.index)
        # Limit to under five, except where we need to take birth weights from
        # mothers in the same file, and some cases of birth weight only
        if 'age_day' in df:
            over_5 |= (df.age_day > 1825)
        if 'age_month' in df:
            over_5 |= (df.age_month > 60)
        if 'age_year' in df:
            over_5 |= (df.age_year > 5)

        n_dropped = over_5.sum()
        if n_dropped:
            over_5_index = over_5[over_5].index
            self.logger.info(f"Dropping {n_dropped} rows where age > 5")
            df = df.drop(index=over_5_index)
        return df
