from pathlib import Path

import pandas

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive
from winnower.extract import get_column
from winnower.transform.indicators import Binary
from winnower.transform.indicators import MetaNum

from .base import TransformBase


class Transform(TransformBase):
    uses_extra_columns = []

    def validate(self, input_columns):
        """
        Appends columns to self.uses_extra_columns
        """
        gout_configs = ['goutvar', 'gout_true', 'gout_false',
                        'goutvar', 'gout_combined_type',
                        'gout_combined_case_1', 'gout_combined_case_1_true']

        self.uses_extra_columns.extend(X for X in gout_configs if X in self.columns)  # noqa

        for gout_config in gout_configs:
            self.record_uses_column_if_configured(gout_config)

    def output_columns(self, input_columns):
        res = input_columns
        res.extend(['goutvar', 'gout_recall_period_days', 'cv_bias_mod_high'])
        return res

    def execute(self, df):
        df = self.gen_gout(df)
        df = self.get_recall_period(df)
        df = self.get_cv_bias_mod_high(df)
        return df

    def gen_gout(self, df):
        """
        Generate goutvar indicator, whether or not a person has gout
        2 kinds of configurations
        - binary with the typical true/false values
        - combined_case where there are two binary columns to consider
        """
        df['goutvar'] = float('NaN')
        gout_bin_inputs = ['goutvar', 'gout_true', 'gout_false']
        if all([self.config[input] for input in gout_bin_inputs]) & \
            (not self.config['gout_combined_type']):
            df = self._gen_gout_binary(df)
        gout_combo_inputs = ['goutvar', 'gout_combined_type', 
                             'gout_combined_case_1']
        if all([self.config[input] for input in gout_combo_inputs]):
            df = self._gen_gout_combined(df)
        return df

    def _gen_gout_binary(self, df):
        "Create goutvar from a binary configuration"
        gout = self.get_column_name(self.config['goutvar'])
        bin_fixer = self._setup_binary_fixer('gout', (gout,), \
            self.config['gout_true'], self.config['gout_false'])
        new_gout = bin_fixer.execute(df)['gout']
        df.loc[new_gout.isin([0, 1]), 'goutvar'] = \
            new_gout[new_gout.isin([0, 1])]
        return df

    def _gen_gout_combined(self, df):
        """
        Create goutvar from a combined configuration
        - look at gout and gout_combined_case_1 input columns and process
          each as a binary indicator
        - combined_type determines a case when gout and gout_combined_case_1
          are cases, or when at least one is a case
        """
        gout = self.get_column_name(self.config['goutvar'])
        bin_fixer = self._setup_binary_fixer('gout', (gout,), \
            self.config['gout_true'], ())
        new_gout1 = bin_fixer.execute(df)['gout']

        gout_cc = self.get_column_name(self.config['gout_combined_case_1'])
        # Change input column and true value for the 2nd case we're considering
        bin_fixer.input_columns = (gout_cc,)
        bin_fixer.true_values = self.config['gout_combined_case_1_true']
        new_gout2 = bin_fixer.execute(df)['gout']

        if self.config['gout_combined_type'].lower() == 'and':
            df['goutvar'] = (new_gout1 == 1) & (new_gout2 == 1)
        elif self.config['gout_combined_type'].lower() == 'or':
            df['goutvar'] = (new_gout1 == 1) | (new_gout2 == 1)
        return df

    def _setup_binary_fixer(self, output_column, input_columns, \
        true_values, false_values):
        "return Binary indicator with user provided input, true/false values"
        return Binary(
            output_column=output_column,
            input_columns=input_columns,
            true_values=true_values,
            false_values=false_values,
            map_indicator=0,
            code_custom=0,
            required=0
        )

    def get_recall_period(self, df):
        "return the recall period only for non-null goutvar observations"
        ind_name = 'gout_recall_period_days'
        df[ind_name] = float('NaN')
        if self.config[ind_name]:
            metanum_fixer = self._setup_metanum_fixer(ind_name)
            df[ind_name] = metanum_fixer.execute(df)[ind_name]
            df[ind_name] = df[ind_name].mask(df.goutvar.isna())
        return df

    def get_cv_bias_mod_high(self, df):
        "return the covariates only for non-null goutvar observations"
        ind_name = 'cv_bias_mod_high'
        df[ind_name] = float('NaN')
        if self.config[ind_name]:
            metanum_fixer = self._setup_metanum_fixer(ind_name)
            df[ind_name] = metanum_fixer.execute(df)[ind_name]
            df[ind_name] = df[ind_name].mask(df.goutvar.isna())
        return df

    def _setup_metanum_fixer(self, ind_name):
        "return MetaNum indicator based on the indicator name and its config"
        return MetaNum(
            output_column=ind_name,
            value=float(self.config[ind_name]),
            map_indicator=0,
            code_custom=0,
            required=0
        )
