from pathlib import Path

import pandas
import pdb

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive
from winnower.extract import get_column
from winnower.transform.indicators import Binary
from winnower.transform.indicators import MetaNum

from .base import TransformBase


class Transform(TransformBase):
    covars = ['cv_dx_oa_selfreport', 'cv_dx_radiographic_only',
              'cv_dx_selfreported_pain', 'cv_dx_symptomatic_only']

    uses_extra_columns = []

    def validate(self, input_columns):
        """
        Appends columns to self.uses_extra_columns
        """
        bin_config = 'ost_arth'

        if bin_config in input_columns:
            self.uses_extra_columns.append(bin_config)
        self.record_uses_column_if_configured(bin_config)

    def output_columns(self, input_columns):
        res = input_columns
        if 'ost_arth' not in res:
            res.append('ost_arth')
        res.extend(X for X in self.covars if X not in input_columns)
        return res

    def execute(self, df):
        df = self.gen_ost_arth(df)
        df = self.gen_ost_arth_covariates(df)
        return df

    def gen_ost_arth(self, df):
        """
        Generate binary indicator ost_arth
        In custom code to allow for future flexibility for this indicator,
        Originally not always a binary indicator according to 
        msk_osteoarthritis.do
        """
        ost_arth = self.get_column_name(self.config['ost_arth'])
        bin_fixer = Binary(
            output_column='ost_arth',
            input_columns=(ost_arth,),
            true_values=self.config['ost_arth_true'],
            false_values=self.config['ost_arth_false'],
            map_indicator=0,
            code_custom=0,
            required=0
        )
        df['ost_arth'] = bin_fixer.execute(df).ost_arth
        return df

    def gen_ost_arth_covariates(self, df):
        for covar in self.covars:
            if bool(self.config[covar]):
                metanum_fixer = MetaNum(
                    output_column=covar,
                    value=float(self.config[covar]),
                    map_indicator=0,
                    code_custom=0,
                    required=0
                )
                df[covar] = metanum_fixer.execute(df)[covar]
                df.loc[df.ost_arth.isna(), covar] = float('NaN')
        return df
