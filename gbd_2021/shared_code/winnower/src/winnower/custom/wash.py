from pathlib import Path

import pandas
import pdb

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive

from .base import TransformBase


class Transform(TransformBase):
    added_w_source_surveys = {141948, 367995, 243548, 243537, 41856, 41851,
                              41844, 41837, 41830, 41823, 286233, 336799,
                              243562, 243564, 243566, 265194, 345238, 345239,
                              272498, 134876, 134125, 134124, 134123, 134122,
                              13411, 32203, 341667, 240604, 403655, 148346,
                              148345, 10377, 10373, 153674, 137328, 238389,
                              165017, 137328, 238389, 404407, 12146, 46480}
    piped_w_source_surveys = {385743, 385749, 385765, 386690, 386688, 385723}
    acs_soap_surveys = {412455, 412538, 438795, 412541}

    uses_extra_columns = None

    def execute(self, df):
        # df = self._correct_shared_san_num(df)
        df = self._gen_hw_soap_mics(df)
        df = self._pull_hw_soap_pma(df)
        df = self._pull_hw_water_pma(df)
        df = self._gen_hw_soap_acs(df)
        df = self._concatenate_w_source(df)
        df = self._convert_source_to_str(df)
        return df

    def _is_pma_survey_with_hw_obs(self):
        "check to determine if this is a pma survey"
        return 'hw_obs' in self.columns

    def _is_acs_survey_needing_hw_soap(self):
        "need to generate hw_soap indicator for these surveys"
        return self.config['nid'] in self.acs_soap_surveys

    def _is_mics_survey(self):
        return ('hw_soap1' in self.columns) | ('hw_soap2' in self.columns) | ('hw_soap3' in self.columns)

    def _will_concat_w_source(self):
        return self.config['nid'] in self.added_w_source_surveys

    def validate(self, input_columns):
        soap_water_cols = ['hw_soap1', 'hw_soap2', 'hw_soap3', 'hw_obs',
                           'w_source_drink', 'w_source_other']
        extra_columns = [X for X in soap_water_cols if X in input_columns]
        self.uses_extra_columns = extra_columns

    def output_columns(self, input_columns):
        """
        add/remove column names to/from retention list if adding/removing
            columns in this custom code file
        """
        result = list(input_columns)
        if self._is_pma_survey_with_hw_obs():
            result.extend(['hw_soap_pma', 'hw_water_pma'])
        if self._is_mics_survey():
            result.append('hw_soap_mics')
        if self._will_concat_w_source():
            result.remove('w_source_other')
        return result

    def _gen_hw_soap_mics(self, df):
        """
        create the hw_soap_mics by combining the responses for up to three soap
            variables in MICS surveys
        """
        if self._is_mics_survey():
            soap_cols = ['hw_soap1', 'hw_soap2', 'hw_soap3']
            # remove any columns if they are not in the data frame
            soap_cols = [col for col in soap_cols if col in self.columns]
            df['hw_soap_mics'] = float('NaN')
            # first create a dataframe of boolean columns, then consolidate
            df.loc[(df[soap_cols] == 1).any(axis='columns'), 'hw_soap_mics'] = 1
            # set instances of hand_washing station but not soap to zero
            df.loc[df.hw_soap_mics.isna() & (df.hw_station == 1), 'hw_soap_mics'] = 0
        return df

    def _pull_hw_soap_pma(self, df):
        """
        look at string descriptions of handwashing (hw_obs) to determine if
            this household has access to soap
        applies to PMA surveys
        returns new variables hw_soap_pma
        """
        if self._is_pma_survey_with_hw_obs():
            df['hw_soap_pma'] = float('NaN')
            df.loc[df.hw_obs.str.contains('soap', case=False), 'hw_soap_pma'] = 1
            df.loc[df.hw_soap_pma.isna() & df.hw_station.notna(), 'hw_soap_pma'] = 0
        return df

    def _pull_hw_water_pma(self, df):
        """
        look at string descriptions of handwashing (hw_obs) to determine if
            this household has access to water for washing hands
        applies to PMA surveys
        returns new variables hw_water_pma
        """
        if self._is_pma_survey_with_hw_obs():
            df['hw_water_pma'] = float('NaN')
            df.loc[df.hw_obs.str.contains('water', case=False), 'hw_water_pma'] = 1
            df.loc[df.hw_water_pma.isna() & df.hw_station.notna(), 'hw_water_pma'] = 0
        return df

    def _gen_hw_soap_acs(self, df):
        if self._is_acs_survey_needing_hw_soap():
            df['hw_soap_acs'] = float('NaN')
            df.loc[(df.hw_station == 1) & (df.hw_water == 1), 'hw_soap_acs'] = 1
        return df

    def _concatenate_w_source(self, df):
        """
        if there is w_source_other
        want to concatenate with w_source_drink, and then drop this column
        from <FILEPATH>
        """
        if self._will_concat_w_source():
            temp = df.w_source_drink.str.strip() + ' ' + df.w_source_other.str.strip()
            df['w_source_drink'] = temp.str.strip()
            df = df.drop('w_source_other', axis=1)
        return df

    def _convert_source_to_str(self, df):
        """
        replace instances of 1's as 'piped', 0's to 'not piped'
        from <FILEPATH>
        """
        if self.config['nid'] in self.piped_w_source_surveys:
            temp = df.w_source_drink
            df['w_source_drink'] = ''
            df.loc[temp == 1, "w_source_drink"] = 'piped'
            df.loc[temp == 0, "w_source_drink"] = "not piped"
        return df

    def _correct_shared_san_num(self, df):
        """
        NOTE shared_san_num is no longer extracted, so this method is not needed
        if the number of people sharing a toilet is greater than 10, set the
            shared_san_num equal to 11
        """
        if self.config['shared_san_num_greater_ten'] != None:
            if len(self.config['shared_san_num_greater_ten']) > 0:
                many_people = self.config['shared_san_num_greater_ten']
                many_people_vals = [int(i) for i \
                    in many_people.replace(" ", "").split(', ')]
                mask = df.shared_san_num.isin(many_people_vals)
                df.loc[mask, 'shared_san_num'] = 11
        return df
