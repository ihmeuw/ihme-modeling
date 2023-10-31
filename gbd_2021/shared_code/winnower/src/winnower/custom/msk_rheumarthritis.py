from pathlib import Path

import pandas
import pdb

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive
from winnower.transform.indicators import Binary
from winnower.transform.indicators import MetaNum

from .base import TransformBase


class Transform(TransformBase):
    uses_extra_columns = []

    def validate(self, input_columns):
        """
        Appends columns to self.uses_extra_columns
        """
        bin_configs = ['rheum_arth', 'new_ra1', 'new_ra2']

        for bin_config in bin_configs:
            if bin_config in input_columns:
                self.uses_extra_columns.append(bin_config)
            self.record_uses_column_if_configured(bin_config)

    def output_columns(self, input_columns):
        res = input_columns
        out_cols = ['rheum_arth', 'new_ra1', 'new_ra2',
                    'cv_diagn_admin_data', 'cv_not_acr87_ra_criteria',
                    'cv_not_represent', 'rheum_arth_recall_period_days']
        res.extend(X for X in out_cols if X in input_columns)
        return res

    def execute(self, df):
        df = self.gen_rheum_arth(df)
        df = self.get_recall_period(df)
        df = self.gen_rheum_arth_covariates(df)
        return df

    def gen_rheum_arth(self, df):
        """
        Indicator of interest: whether an individual has rheumatoid arthritis
        """
        df['rheum_arth'] = float('NaN')
        ## Binary case
        bin_inputs = ['rheum_arth', 'rheum_arth_true', 'rheum_arth_false']
        if all([self.config[input] for input in bin_inputs]):
            df = self._gen_rheum_arth_binary(df)

        ## Combined case
        # NOTE In msk_rheumarthritis.do, combination case will override binary
        # even though comment on code says it is supposed to be if/elif
        combined_inputs = bin_inputs[0:1] + \
            ['rheum_arth_combined_flag', 'rheum_arth_combined_case_1']
        if all([self.config[input] for input in combined_inputs]):
            df = self._gen_rheum_arth_combined(df)
        return df

    def _gen_rheum_arth_binary(self, df):
        ra_input = self.get_column_name(self.config['rheum_arth'])
        bin_fixer = self._setup_binary_fixer('rheum_arth', (ra_input,), \
            self.config['rheum_arth_true'], self.config['rheum_arth_false'])
        new_rheum_arth = bin_fixer.execute(df)['rheum_arth']
        df.loc[new_rheum_arth.isin([0, 1]), 'rheum_arth'] = \
            new_rheum_arth[new_rheum_arth.isin([0, 1])]
        return df

    def _gen_rheum_arth_combined(self, df):
        ra_input = self.get_column_name(self.config['rheum_arth'])
        bin_fixer1 = self._setup_binary_fixer('new_ra1', (ra_input,), \
            self.config['rheum_arth_true'], self.config['rheum_arth_false'])
        new_ra1 = bin_fixer1.execute(df)['new_ra1']

        cc_input = \
            self.get_column_name(self.config['rheum_arth_combined_case_1'])
        bin_fixer2 = self._setup_binary_fixer('new_ra2', (cc_input,), \
            self.config['rheum_arth_combined_case_1_true'], ())
        new_ra2 = bin_fixer2.execute(df)['new_ra2']

        mask_both_ra = (new_ra1 == 1) & (new_ra2 == 1)
        df.loc[mask_both_ra, 'rheum_arth'] = 1
        df.loc[~mask_both_ra, 'rhuem_arth'] = 0
        # rheum_arth is null whenever binary inputs are null
        # null values are not determined by combined case
        # NOTE need this step because combining two binary indicators
        df['rheum_arth'] = df['rheum_arth'].mask(new_ra1.isna())
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
        # set the recall period, also constant
        ind_name = 'rheum_arth_recall_period_days'
        if self.config[ind_name]:
            metanum_fixer = self._setup_metanum_fixer(ind_name)
            df[ind_name] = metanum_fixer.execute(df)[ind_name]
            df[ind_name] = df[ind_name].mask(df.rheum_arth.isna())
        return df

    def gen_rheum_arth_covariates(self, df):
        # assign covariate values, these are constants
        covars = ['cv_diagn_admin_data', 'cv_not_acr87_ra_criteria',
                  'cv_not_represent']
        mask_null_rheum_arth = df.rheum_arth.isna()
        for covar in covars:
            if self.config[covar]:
                metanum_fixer = self._setup_metanum_fixer(covar)
                df[covar] = metanum_fixer.execute(df)[covar]
                df[covar] = df[covar].mask(mask_null_rheum_arth)
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
