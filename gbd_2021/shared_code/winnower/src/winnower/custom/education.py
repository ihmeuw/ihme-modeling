from winnower.custom.base import TransformBase
from winnower.config.models.fields import separated_values
from winnower.util.dataframe import get_column_type_factory
import pandas
import numpy


class Transform(TransformBase):
    def output_columns(self, input_columns):
        return input_columns

    def execute(self, df):
        df = self._recode_edu_age_finish(df)
        df = self._na_for_edu_level_categ(df)
        df = self._missing_for_no_education(df)
        df = self._calc_edu_level_categ_by_edu_years_in_level(df)
        df = self._calc_edu_age_finish(df)
        return df
    

    def _recode_edu_age_finish(self, df):
        """
        A large number of Eurobarameter surveys need to be recoded
        ex. from education.do for nid 419948
            replace edu_age_finish = 99 if edu_age_finish == 0
            replace edu_age_finish = 0 if edu_age_finish == 97
        Use edu_age_finish_recode_x where x is the correctly recoded value
        and the configuration is the original edu_age_finish that needs to
        be recoded.
        """
        recode_map = {}
        def _add_recode_to_map(config_name, remap_val):
            "Handle case when multiple values need are recoded to 0, 97, etc."
            user_input = self.config.get(config_name, "")
            for val in separated_values(user_input):
                recode_map[int(val)] = remap_val
        # so in this case the config value stored at 'edu_age_finish_recode_97'
        # will be recoded/re-mapped to 97
        _add_recode_to_map("edu_age_finish_recode_97", 97)
        _add_recode_to_map("edu_age_finish_recode_99", 99)
        _add_recode_to_map("edu_age_finish_recode_0", 0)
        _add_recode_to_map("edu_age_finish_recode_missing", float("NaN"))
        if recode_map:
            df.edu_age_finish = df.edu_age_finish.replace(recode_map)
        return df


    def _na_for_edu_level_categ(self, df):
        "for edu_level_categ, replace value na to an empty string"
        if 'edu_level_categ' in df:
            if df['edu_level_categ'].dtype != numpy.float64:
                mask_categ_lower_na = df['edu_level_categ'].str.lower() == 'na'
                df.loc[mask_categ_lower_na, 'edu_level_categ'] = ""
        return df


    def _missing_for_no_education(self, df):
        """
        when no_education is true, set edu_level_cont and edu_level_categ to 0
        """
        if 'no_education' in df:
            if 'edu_level_cont' in df:
                df.loc[df['no_education'] == 1, "edu_level_cont"] = 0
            if 'edu_level_categ' in df:
                df.loc[df['no_education'] == 1, "edu_level_categ"] = "0"
            del df['no_education']
        return df


    def _calc_start_pt(self, edu_level_1):
        """
        helper method for _calc_edu_level_categ_by_edu_years_in_level
        used for calculating edu_level_cont to backfill edu_level_categ
        """
        return max(edu_level_1 - 1, 0)

    def _calc_edu_level_categ_by_edu_years_in_level(self, df):
        """
        calculate edu_level_categ by using edu_years_in_level
        """
        if 'edu_years_in_level' in df:
            # set edu_years_in_level outliers and 0's in UNICEF_MICS to null
            mask_edu_years_level = df['edu_years_in_level'] > 90
            if self.config['survey_name'] == "UNICEF_MICS":
                mask_edu_years_level |= df.edu_level_categ == "0"
            df.loc[mask_edu_years_level, 'edu_years_in_level'] = float('NaN')

            if 'edu_level_categ' in df:
                ## impute edu_level_cont from string range in edu_level_categ
                temp_value = \
                    df['edu_level_categ'].str.split("-", n = 1, expand = True)
                # drop null values to avoid error before converting to numeric
                # save the splitee values as individual columns
                df['edu_min'] = pandas.to_numeric(temp_value[0])
                df['edu_max'] = pandas.to_numeric(temp_value[1])
                start_pt = df['edu_min'].apply(self._calc_start_pt)
                df['edu_level_cont'] = start_pt + df['edu_years_in_level']
                
                # only for survey ARG/PERMANENT_HH_SURVEY_EPH
                if self.config['survey_name'] == 'ARG/PERMANENT_HH_SURVEY_EPH':
                    categ2_mask = (df.ch13 == 1) & (df.edu_max.notna())
                    df.loc[categ2_mask, 'edu_level_cont'] = df.loc[categ2_mask, 'edu_max']
                
                ## for all values we have edu_level_cont, replace the original
                ## edu_level_categ values
                cont_mask = df.edu_level_cont.notna()
                df.loc[cont_mask, "edu_level_categ"] = \
                    df.loc[cont_mask, "edu_level_cont"].astype(int).astype(str)
                
                del df['edu_years_in_level']
                del df['edu_level_cont']
        return df


    def _calc_edu_age_finish(self, df): 
        if 'edu_age_finish' in df:
            """
            Commenting out
            replace edu_age_finish = 0 if edu_age_finish == .
            from education.do (around line 1274)
            """
            # df.loc[df.edu_age_finish.isna(), 'edu_age_finish'] = 0

            # need edu_level_cont_missing for the rest of the method
            if not self.config['edu_level_cont_missing']:
                return df

            to_number = get_column_type_factory(df.edu_age_finish)
            config_missing = self.config['edu_level_cont_missing']
            edu_level_missing_list = [to_number(val) for val in config_missing]
            mask_na = df.edu_age_finish.isin(edu_level_missing_list)
            df.loc[mask_na, 'edu_age_finish'] = float('NaN')
            df['edu_age_finish'] = df['edu_age_finish'] - df['edu_age_start']
            df.loc[df.edu_age_finish <= 0, 'edu_age_finish'] = 0
            del df['edu_age_start']
        return df

