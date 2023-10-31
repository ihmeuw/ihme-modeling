from pathlib import Path

import pandas
import decimal

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive
from winnower.extract import get_column
from winnower.transform.indicators import Continuous
from winnower.util.stata import MISSING_STR_VALUE
from winnower.util.dates import encode_century_month_code

from .base import TransformBase


class Transform(TransformBase):
    code_custom_ind = ['ceb', 'ceb_male', 'ceb_female',
                       'ced', 'ced_male', 'ced_female', 'children_alive',
                       'males_alive', 'females_alive',
                       'males_in_house', 'males_elsewhere',
                       'females_in_house', 'females_elsewhere']

    def output_columns(self, input_columns):
        res = input_columns
        extra_columns = []

        extra_columns.extend(['int_month', 'interview_date_cmc',
                              'males_alive', 'females_alive',
                              'children_alive', 'ceb', 'ced',
                              'ceb_male', 'ceb_female', 'ced_male',
                              'ced_female', 'births', 'births2', 'earlyneo', 
                              'timefb', 'firstbirth_month', 'firstbirth_cmc'])

        if self.config['age_year']:
            extra_columns.append('age_group_of_woman')

        res.extend(X for X in extra_columns if X not in input_columns)
        return res

    def execute(self, df):
        df = self.recode_nulls_cont_vars(df)
        df = self.impute_survey_midpoint(df)
        df = self.gen_interview_date_cmc(df)
        df = self.impute_ceb_ced(df)
        df = self.correct_extraneous_ceb_ced(df)
        df = self.calc_timefb(df)
        return df

    def recode_nulls_cont_vars(self, df):
        """
        generate custom code continuous indicators
        get missing values for all indicators from child_count_missing
        - if not in config, generate an empty series, a series of nulls
        """
        miss_vals = self.config.get('child_count_missing', ())
        for cont_var in self.code_custom_ind:
            cont_var_input = get_column(self.config[cont_var], df.columns)[0] \
                if bool(self.config[cont_var]) else None
            # if the custom continuous indicator is configured for survey
            if bool(cont_var_input):
                # missing values are read in topic_forms.py, see SbhForm
                cont_fixer = Continuous(
                    output_column=cont_var,
                    input_column=cont_var_input,
                    missing=miss_vals,
                    map_indicator=0,
                    code_custom=0,
                    required=0
                )
                df[cont_var] = cont_fixer.execute(df[[cont_var_input]])[cont_var]
            else:
                # if there is no configuration, set to a column of nulls
                df[cont_var] = float('NaN')
        return df
    

    def impute_survey_midpoint(self, df):
        """
        If no interview month available, impute midpoint of survey
        - either June or January if possible to impute
        """
        if 'int_month' not in df:
            df['int_month'] = float('NaN')
            mask_june = df.year_end - df.year_start == 0
            mask_jan = df.year_end - df.year_start == 1
            df.loc[mask_june, 'int_month'] = 6
            df.loc[mask_jan, 'int_month'] = 1
        return df
    

    def gen_interview_date_cmc(self, df):
        "Generate century month code for interview date"
        cmc = [encode_century_month_code(year, month)
               for year, month in
               df[['int_year', 'int_month']].itertuples(index=False)]
        df['interview_date_cmc'] = cmc
        return df


    def _do_impute(self, ind_name, *args):
        """
        helper method for impute_ceb_ced to check config if the indicator
        should be imputed
        - if ind_name not in config and each variable in *args are configured
        """
        if self.config[ind_name]:
            return False
        for var in args:
            if not self.config[var]:
                return False
        return True

    
    def impute_ceb_ced(self, df):
        """
        Goal: calculate children ever born (ceb), and children ever died (ced)
        - based on what information is provided in the survey
        - addition uses _impute_from_cols
        - subtraction is always null if one of the two columns in subtraction
          operation is null
        """
        ## 1 imput males_alive and females_alive if the
        # _in_house and _elsewhere counts are available
        for gen in ['male', 'female']:
            ind_name = gen+'s_alive'
            var1, var2 = (gen + x for x in ['s_in_house', 's_elsewhere'])
            if self._do_impute(ind_name, var1, var2):
                df[ind_name] = self._impute_from_cols(df[var1], df[var2])
        
        ## 2 Two cases when we should impute total children alive
        # in both, children_alive is not configured
        # - case 1: males_alive, females_alive are configured
        # - case 2: _in_house, _elsewhere both configured for males and females
        ind_name = 'children_alive'
        var1, var2 = ('males_alive', 'females_alive')
        child_alive_vars = ['males_in_house', 'males_elsewhere', 
                            'females_in_house', 'females_elsewhere']
        if self._do_impute(ind_name, var1, var2) or \
            self._do_impute(ind_name, *child_alive_vars):
            df[ind_name] = self._impute_from_cols(df[var1], df[var2])

        ## 3 impute ceb (if not configured) from ceb_male and ceb_female
        if self._do_impute('ceb', 'ceb_male', 'ceb_female'):
            df['ceb'] = self._impute_from_cols(df.ceb_male, df.ceb_female)
        ## 4 impute ced (if not configured) from ced_male and ced_female
        if self._do_impute('ced', 'ced_male', 'ced_female'):
            df['ced'] = self._impute_from_cols(df.ced_male, df.ced_female)

        ## 5 if either are not configured, calculate ced_male and ced_female 
        # by subtracting the gender-respective children_alive from ceb,
        for gen in ['male', 'female']:
            ind_name = 'ced_' + gen
            var1, var2 = f"ceb_{gen}", f"{gen}s_alive"
            if self._do_impute(ind_name, var1, var2):
                df[ind_name] = df[var1] - df[var2]
        
        ## 6 replace null instances of ceb when ced OR children_alive is not null
        df.loc[df.ceb.isna(), 'ceb'] = self._impute_from_cols(df.ced, df.children_alive)

        ## 7 replace null instances of ced by subtracting children_alive from
        # ceb IF both are not null
        df.loc[df.ced.isna(), 'ced'] = (df['ceb'] - df['children_alive'])[df.ced.isna()]
        
        # check for problematic ceb and ced totals
        for ind_name in ['children_alive', 'ced']:
            num_invalid = sum((df[ind_name] > df.ceb) & \
                (df[ind_name].notna() & df.ceb.notna()))
            if num_invalid > 0:
                msg = f"Please check outputs! There are {num_invalid} " + \
                    f"instances where {ind_name} is greater than ceb."
        
        ## 8 for ced and children_alive, if 0, then the corresponding _male and
        # _female counts should switch from null to 0
        for gen in ['male', 'female']:
            ind_name = 'ced_' + gen
            df.loc[(df.ced == 0) & df[ind_name].isna(), ind_name] = 0
            # check if resulting column is only 0's and nan's
            if set(df[ind_name].value_counts().index) == {0}:
                msg = f"Please check {ind_name} output. Column only " + \
                    "contains 0 and null values"
                self.logger.warning(msg)
        df.loc[(df.children_alive == 0) & df.males_alive.isna(), \
            'males_alive'] = 0
        df.loc[(df.children_alive == 0) & df.females_alive.isna(), \
            'females_alive'] = 0
        
        ## 9 if ceb_male or ceb_female are not configured, replace with sum
        # of ced_ and children_alive_ IF both exist
        for gen in ['male', 'female']:
            ind_name = 'ceb_' + gen
            if self._do_impute(ind_name):
                var1, var2 = f"{gen}s_alive", f"ced_{gen}"
                df[ind_name] = \
                    self._impute_from_cols(df[var1], df[var2], fill_one_na=False)
                
            # check if resulting column is only 0's and nan's
            if set(df[ind_name].value_counts().index) == {0}:
                msg = f"Please check {ind_name} output. Column only " + \
                    "contains 0 and null values"
                self.logger.warning(msg)

        # drop variables that are not ceb or ced
        vars_to_drop = ['males_in_house', 'males_elsewhere', 
                        'females_in_house', 'females_elsewhere',
                        'children_alive', 'males_alive',
                        'females_alive']

        to_drop = [X for X in vars_to_drop if X in df]
        df.drop(to_drop, axis=1, inplace=True)

        return df


    def _impute_from_cols(self, col1, col2, fill_one_na=True):
        """
        adding two columns together
        """
        mask_null = col1.isna() & col2.isna()
        if fill_one_na:
            col1, col2 = (col.fillna(0) for col in [col1, col2])
        result = col1 + col2
        result[mask_null] = float('NaN')
        return result

    
    def correct_extraneous_ceb_ced(self, df):
        "if either ceb or ced are over 30 or if ced is negative, set to null"
        large_ceb = df.ceb > 30
        large_ced = df.ced > 30
        df.loc[large_ceb, 'ceb'] = float('NaN')
        df.loc[large_ced, 'ceb'] = float('NaN')
        df.loc[large_ceb, 'ced'] = float('NaN')
        df.loc[large_ced, 'ced'] = float('NaN')
        df.loc[df.ced < 0, 'ced'] = float('NaN')
        return df


    def calc_timefb(self, df):
        "return time since a woman's first birth in cmc format"
        def _right_round(num):
            "helper function to round 0.5 to 1 instead of 0"
            blah = decimal.Decimal(num).quantize(decimal.Decimal('1'), \
                rounding=decimal.ROUND_HALF_UP)
            if (not num.is_integer()) & (num < 0) & (num % 0.5 == 0):
                return pandas.to_numeric(blah) + 1
            return pandas.to_numeric(blah)
        
        if 'firstbirth_cmc' in df:
            df['timefb'] = (df.interview_date_cmc - df.firstbirth_cmc) / 12
            df['timefb'] = df.timefb.apply(_right_round)
        # if no month provided, take the middle of the year
        elif ('firstbirth_month' not in df) & ('firstbirth_year' in df):
            df['firstbirth_month'] = 7
            df['firstbirth_cmc'] = (df.firstbirth_year - 1900) * 12 \
                + df.firstbirth_month
            df['timefb'] = (df.interview_date_cmc - df.firstbirth_cmc) / 12
            df['timefb'] = df.timefb.apply(_right_round)
        # convert month and year in integer format to cmc
        elif ('firstbirth_month' in df) & ('firstbirth_year' in df):
            df['firstbirth_cmc'] = (df.firstbirth_year - 1900) * 12 \
                + df.firstbirth_month
            df['timefb'] = (df.interview_date_cmc - df.firstbirth_cmc) / 12
            df['timefb'] = df.timefb.apply(_right_round)
        
        # only keeping firstbirth_cmc
        for var in ["firstbirth_year", "firstbirth_month"]:
            if var in df:
                df.drop(var, axis=1, inplace=True)
        return df

    