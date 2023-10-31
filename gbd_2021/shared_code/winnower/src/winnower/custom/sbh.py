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

from .base import TransformBase


class Transform(TransformBase):
    code_custom_ind = ['mothers_age', 'ceb', 'ceb_male', 'ceb_female',
                       'ced', 'ced_male', 'ced_female', 'children_alive',
                       'children_alive_male', 'children_alive_female',
                       'males_in_house', 'males_elsewhere',
                       'females_in_house', 'females_elsewhere', 'lastborn_cmc',
                       'lastborn_year_ccyy', 'lastborn_month_mm',
                       'lastborn_day_dd', 'stillbirth_number']

    def output_columns(self, input_columns):
        res = input_columns
        extra_columns = []

        extra_columns.extend(['int_month', 'interview_date_cmc',
                              'children_alive_male', 'children_alive_female',
                              'children_alive', 'ceb', 'ced',
                              'ceb_male', 'ceb_female', 'ced_male',
                              'ced_female', 'stillbirths', 'births', 'births2',
                              'earlyneo', 'stillbirth_definition',
                              'stillbirths_calculated_5yr',
                              'births_calculated_5yr',
                              'timefb', 'firstbirth_month', 'firstbirth_cmc'])

        if self.config['age_year']:
            extra_columns.append('age_group_of_woman')

        res.extend(X for X in extra_columns if X not in input_columns)
        return res

    def execute(self, df):
        df = self.recode_nulls_cont_vars(df)
        df = self.impute_survey_midpoint(df)
        df = self.gen_interview_date_cmc(df)
        df = self.filter_individuals(df)
        df = self.correct_0_ceb_cases(df)
        df = self.impute_ceb_ced(df)
        df = self.correct_extraneous_ceb_ced(df)
        df = self.gen_age_group_of_woman(df)
        df = self.calc_timefb(df)
        df = self.calc_stillbirths(df)
        return df

    def recode_nulls_cont_vars(self, df):
        """
        generate custom code continuous indicators
        - if not in config, generate an empty series, a series of nulls
        """
        for cont_var in self.code_custom_ind:
            cont_var_input = get_column(self.config[cont_var], df.columns)[0] \
                if bool(self.config[cont_var]) else None
            # if the custom continuous indicator is configured for survey
            if bool(cont_var_input):
                miss_vals = self.config[cont_var + '_missing'] \
                    if (cont_var + '_missing') in self.config else ()
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
        df['interview_date_cmc'] = 12 * (df.int_year - 1900) + df.int_month
        return df


    def filter_individuals(self, df):
        """
        drop any remaining males and women not aged 15-49
        for some birth registry (BR) and child (CH) survey modules
        - recode male children to prevent these indiv. from getting dropped
        - if mother's age is provided, replace overall age with mother's age
        TODO Ask about restricting to BR and CH modules
        - ubCov if mothers_age and age_year already exist, then ubCov does not
          replace age_year and uses the original
        - winnower if mothers age and age_year both exist, winnower renames
          and uses mothers_year
        - need to decide what to do about women's (WN) modules
        """
        if self.config['survey_module'] in ['BR', 'CH']:
            df.loc[df.sex_id == 1, 'sex_id'] = 2
        if bool(self.config['mothers_age']):
            df.age_year = df.mothers_age
            df = df.drop('mothers_age', axis=1)
        # only keep women who are between the ages of 15 and 49 (inclusive)
        if 'sex_id' in df:
            df = df.loc[df.sex_id != 1]
        if 'age_year' in df:
            df = df.loc[(df.age_year >= 15) & (df.age_year <= 49)]
        return df.reset_index(drop=True)
    

    def correct_0_ceb_cases(self, df):
        """
        if a woman reports she has never given birth before, then variables 
            such as children_ever_born, _alive, _elsewhere, should all be 0
        """
        has_bin_ceb_info = bool(self.config['given_birth_bin']) \
            & bool(self.config['inbirth_bin'])
        if has_bin_ceb_info:
            ceb_vars = ['ceb', 'ceb_male', 'ceb_female', 'children_alive',
                        'children_alive_male', 'children_alive_female', 'ced',
                        'ced_male', 'ced_female', 'males_in_house',
                        'females_in_house', 'males_elsewhere',
                        'females_elsewhere', 'inbirth_bin']
            mask_no_births = df.given_birth_bin == 0
            for ceb_var in ceb_vars:
                if bool(self.config[ceb_var]):
                    df.loc[mask_no_births, ceb_var] = 0
        return df


    def _do_impute(self, ind_name, *args):
        """
        helper method for impute_ceb_ced to check config if the indicator
        should be imputed
        - if ind_name not in config and each variable in *args are configured
        """
        if self.config[ind_name]:
            return False
        if len(args) > 0:
            if isinstance(args[0], list):
                args = [i for i in args[0]]
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
        ## 1 imput children_alive_male and children_alive_female if the
        # _in_house and _elsewhere counts are available
        for gen in ['male', 'female']:
            ind_name = 'children_alive_' + gen
            var1, var2 = (gen + x for x in ['s_in_house', 's_elsewhere'])
            if self._do_impute(ind_name, var1, var2):
                df[ind_name] = self._impute_from_cols(df[var1], df[var2])
        
        ## 2 Two cases when we should impute total children alive
        # in both, children_alive is not configured
        # - case 1: children_alive_male, children_alive_female are configured
        # - case 2: _in_house, _elsewhere both configured for males and females
        ind_name = 'children_alive'
        var1, var2 = ('children_alive_male', 'children_alive_female')
        child_alive_vars = ['males_in_house', 'males_elsewhere', 
                            'females_in_house', 'females_elsewhere']
        if self._do_impute(ind_name, var1, var2) or \
            self._do_impute(ind_name, child_alive_vars):
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
            var1, var2 = (x + gen for x in ['ceb_', 'children_alive_'])
            if self._do_impute(ind_name, var1, var2):
                df[ind_name] = df[var1] - df[var2]
        
        ## 6 replace null instances of ceb when ced OR children_alive is not null
        df['ceb'][df.ceb.isna()] = \
            self._impute_from_cols(df['ced'], df['children_alive'])[df.ceb.isna()]

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
        df.loc[(df.children_alive == 0) & df.children_alive_male.isna(), \
            'children_alive_male'] = 0
        df.loc[(df.children_alive == 0) & df.children_alive_female.isna(), \
            'children_alive_female'] = 0
        
        ## 9 if ceb_male or ceb_female are not configured, replace with sum
        # of ced_ and children_alive_ IF both exist
        for gen in ['male', 'female']:
            ind_name = 'ceb_' + gen
            if self._do_impute(ind_name):
                var1, var2 = (x + gen for x in ['children_alive_', 'ced_'])
                df[ind_name] = \
                    self._impute_from_cols(df[var1], df[var2], fill_na=False)
            # check if resulting column is only 0's and nan's
            if set(df[ind_name].value_counts().index) == {0}:
                msg = f"Please check {ind_name} output. Column only " + \
                    "contains 0 and null values"
                self.logger.warning(msg)

        # drop variables that are not ceb or ced
        vars_to_drop = ['males_in_house', 'males_elsewhere', 
                        'females_in_house', 'females_elsewhere',
                        'children_alive', 'children_alive_male',
                        'children_alive_female']
        for var in vars_to_drop:
            if var in df:
                df.drop(var, axis=1, inplace=True)


        return df


    def _impute_from_cols(self, col1, col2, replace_na=True, fill_na=True):
        """
        adding two columns together
        - replace_na, used when wanting a result when one value is null and 
          the other is non-null, but the result of two non-null values should
          still be null
        """
        if replace_na:
            mask_null = col1.isna() & col2.isna()
        if fill_na:
            col1, col2 = (col.fillna(0) for col in [col1, col2])
        result = col1 + col2
        if replace_na:
            result[mask_null] = float('NaN')
        return result
    
    def correct_extraneous_ceb_ced(self, df):
        "if either ceb or ced are over 30 or if ced is negative, set to null"
        large_ceb = df.ceb > 30
        large_ced = df.ced > 30
        df.loc[large_ceb, 'ceb'] = float('NaN')
        df.loc[large_ced, 'ced'] = float('NaN')
        df.loc[large_ceb, 'ced'] = float('NaN')
        df.loc[large_ced, 'ced'] = float('NaN')
        df.loc[df.ced < 0, 'ced'] = float('NaN')
        return df


    def gen_age_group_of_woman(self, df):
        "create an age_group categorical based on the row's (woman's) age"
        df['age_group_of_woman'] = float('NaN')
        cats = ['15-19', '20-24', '25-29', '30-34', '35-39', '40-44', '45-49']
        if bool(self.config['age_year']):
            bins = [14, 19, 24, 29, 34, 39, 44, 49]
            df['age_group_of_woman'] = \
                pandas.cut(df.age_year, bins=bins, labels=cats)
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

    def calc_stillbirths(self, df):
        sb_cols = ['v018', 'vcal_1', 'v008', 'b3_01', 'b6_01', 'v005', 
                   'v021', 'v024']
        """
        v018 is row of month of interview - which spot in the calendar
        vcal_1 is a calendar, each character represents one month
        v008 is date of interview in CMC format
        b3_01 is date of birth for child 1(CMC)
        b6_01 is age at death (what are the units?)
        v005 is sample weight
        v021 is primary sampling unit
        v024 is region
        """
        # note from sbh.do:Certain surveys have these variables but don't
        # have content within them 
        incomplete_dhs_nids = \
            {19683, 19963, 56021, 56063, 76706, 77384, 286781}
        if self.config['nid'] in incomplete_dhs_nids:
            return df
        # handle capitalization differences
        inputs = [get_column(input_col, df.columns)[0] \
            for input_col in sb_cols]
        # DHS surveys must have all these imput variables to proceed
        if not all(inputs):
            return df
        v018, vcal_1, v008, b3_01, b6_01, v005, v021, v024 = tuple(inputs) 

        df['stillbirths'] = 0
        df['births'] = 0 # I don't think this is used, artifact of sbh.do
        df['births2'] = 0
        df['earlyneo'] = 0 # I don't think this is used, artifact of sbh.do
        beg = df[v018]
        end = beg + 60 # I think inrange is inclusive in both ends in Stata

        # NOTE calendar vcal_1 is a long string, so each character represents
        # one month
        # Create a five year window using the beg and end indices
        # Supposed to be 60 chars long, but 66 to help count stillbirths which
        # is trying to find a seven character string
        try:
            window = pandas.Series([cal[(b - 1):(e - 1 + 6)] for cal, b, e \
                in zip(df[vcal_1], beg, end)])
        except Exception as e:
            # this is creating an exception for nid 19315
            window = df[vcal_1]
            msg = "Please check stillbirths output and VCAL_1 and V018 " + \
                "variables. Could not build 5 year window."
            self.logger.warning(msg)
            
        
        # 'births' not used for births_calculated_5yr indicator, births2 is
        df['births'] = window.str[:60].str.count('B')
        df['stillbirths'] = window.str.count('TPPPPPP')

        # Reset beginning and end dates
        beg = df[v008] - 59 # .between is inclusive on both ends too
        end = df[v008]

        # each column represents a child (if no 3rd, 4th, etc child, then null)
        birthdate_cols = [x for x in df.columns if x.startswith(b3_01[:3])]
        birthdate_cols = \
            [x for x in birthdate_cols if x.split('_', 1)[1].isdigit()]
        death_records = df[[x for x in df.columns if x.startswith(b6_01[:3])]]

        # for each row, find # of children whose birthdate finds within that
        # row's beginning and end date
        for i, row in df.iterrows():
            row = row[birthdate_cols]
            birth_in_time = \
                row.between(beg[i], end[i] - 1).reset_index(drop=True)
            df['births2'][i] = row.between(beg[i], end[i]).sum()
            death_record = death_records.loc[i]
            df['earlyneo'][i] = sum(birth_in_time & \
                death_record.between(100,106).reset_index(drop=True))
        
        sb_def = self.config['stillbirth_definition']
        df['stillbirth_definition'] = sb_def if bool(sb_def) else "28_weeks"
        df['stillbirths_calculated_5yr'] = df['stillbirths']
        df['births_calculated_5yr'] = df['births2']
        return df
