from pathlib import Path

import pandas
import numpy

from winnower import errors
from winnower.util.dataframe import set_default_values
from winnower.util.dtype import is_numeric_dtype
from winnower.util.path import J_drive

from .base import TransformBase


def present_and_numeric(df, col):
    "Is col present in df and of a numeric type?"
    if col in df:
        if is_numeric_dtype(df[col]):
            return True
        # include this possibility to account for dtype('O')
        # test_1232 to test_1235 entries are type float instead of float64
        # test_1236 to test_1238 entries are strings
        try:
            float(df[col][0])
            return True
        except (TypeError, ValueError):
            return False
    else:
        return False


class UnitConverter:
    "Converts units."
    @classmethod
    def for_bmi(cls):
        path = ("<FILEPATH>")
        # TODO: type check columns
        df = pandas.read_csv(path)
        return cls(df)

    def __init__(self, conversion_df):
        self.conversion_df = conversion_df

    def convert_to_cm(self, column, in_unit):
        return self._convert(column, in_unit, 'cm')

    def convert_to_kg(self, column, in_unit):
        return self._convert(column, in_unit, 'kg')

    def _convert(self, column, in_unit, out_unit):
        "Check that only one conversion exists or STOP ALL THINGS"
        cdf = self.conversion_df
        # cdf.var is a method - must use __getitem__ syntax
        mask = ((cdf['var'] == column.name) &
                (cdf.unit_from == in_unit) &
                (cdf.unit_to == out_unit))
        conversion_row = cdf[mask]
        if len(conversion_row) == 1:
            factor = conversion_row.conversion_factor.iloc[0]
            return column * factor
        # error conditions
        if len(conversion_row):
            msg = (f"Multiple conversion values for input {column.name} "
                   f"from unit {in_unit!r} to unit {out_unit!r}")
        else:
            msg = (f"No conversion values for input {column.name} "
                   f"from unit {in_unit!r} to unit {out_unit!r}")
        raise errors.Error(msg)


class Transform(TransformBase):
    uses_extra_columns = None
    cm_measures = (
        'metab_height',
        'metab_height_rep',
        'metab_height2',
        'metab_height2_rep',
    )
    kg_measures = (
        'metab_weight',
        'metab_weight_rep',
        'metab_weight2',
        'metab_weight2_rep',
    )

    def validate(self, input_columns):
        """
        Populate required columns for performing this transform
        and assign to self.uses_extra_columns
        """
        measures = list(self.cm_measures)
        measures.extend(self.kg_measures)

        extra_columns = [X for X in measures if X in input_columns]
        self.uses_extra_columns = extra_columns

    def output_columns(self, input_columns):
        """
        Returns the list of input_columns appended to
        new columns generated in this transform.
        """
        measures = list(self.cm_measures)
        measures.extend(self.kg_measures)
        res = input_columns
        for measure in measures:
            if measure in self.columns:
                unit_field = f"{measure}_unit"
                res.append(unit_field)

        if 'metab_height' in input_columns and 'metab_weight' in input_columns:
            res.append('bmi')
        if 'metab_height_rep' in input_columns:
            res.append('bmi_rep')

        return res

    def execute(self, df):
        df = self._convert_to_common_units(df)
        df = self._combine_multi_variable_inputs(df)
        df = self._calculate_bmi(df)
        return df

    def _convert_to_common_units(self, df):
        uc = UnitConverter.for_bmi()
        cm_measures = self.cm_measures
        kg_measures = self.kg_measures

        for measure in cm_measures:
            if measure not in df:
                continue
            unit_field = f"{measure}_unit"
            unit = self.config[unit_field]

            if unit is not None and df[measure].notna().any() and unit != 'cm':
                df[measure] = uc.convert_to_cm(df[measure], unit)
                df[unit_field] = 'cm'

        for measure in kg_measures:
            if measure not in df:
                continue
            unit_field = f"{measure}_unit"
            unit = self.config[unit_field]
            if unit is not None and df[measure].notna().any() and unit != 'kg':
                df[measure] = uc.convert_to_kg(df[measure], unit)
                df[unit_field] = 'kg'

        return df

    def _combine_multi_variable_inputs(self, df):
        for var in 'metab_height', 'metab_weight':
            second = f"{var}2"
            if present_and_numeric(df, second):
                df[var] += df[second].fillna(0)
                del df[second]

            rep2 = f"{var}2_rep"
            if present_and_numeric(df, rep2):
                rep = f"{var}_rep"
                df[rep] += df[rep2].fillna(0)
                del df[rep2]
        return df

    def _calculate_bmi(self, df):
        if all(present_and_numeric(df, X) for X in ('metab_height', 'metab_weight')):  # noqa
            if df.metab_weight.dtype != 'float64':
                df.metab_weight = pandas.to_numeric(df.metab_weight)
            if df.metab_height.dtype != 'float64':
                df.metab_height = pandas.to_numeric(df.metab_height)
            df['bmi'] = (df.metab_weight / df.metab_height ** 2) * 10_000

        if present_and_numeric(df, 'metab_height_rep'):
            df['bmi_rep'] = (df.metab_weight_rep / df.metab_height_rep ** 2) * 10_000  # noqa

        if present_and_numeric(df, 'metab_bmi'):
            set_default_values('bmi', df.metab_bmi, df)

        if present_and_numeric(df, 'metab_bmi_rep'):
            set_default_values('bmi_rep', df.metab_bmi_rep, df)

        if 'bmi' in df:
            df.loc[numpy.isinf(df.bmi), 'bmi'] = numpy.nan
            round_bmi = df.bmi.round(decimals=5)
            df.loc[df.bmi.isna(), 'overweight'] = numpy.nan
            df.loc[df.bmi.isna(), 'obese'] = numpy.nan
            df.loc[round_bmi >= 25, 'overweight'] = 1
            df.loc[round_bmi < 25, 'overweight'] = 0
            df.loc[round_bmi >= 30, 'obese'] = 1
            df.loc[round_bmi < 30, 'obese'] = 0
        if 'bmi_rep' in df:
            df.loc[numpy.isinf(df.bmi_rep), 'bmi_rep'] = numpy.nan
            round_bmi_rep = df.bmi_rep.round(decimals=5)
            df.loc[round_bmi_rep >= 25, 'overweight_rep'] = 1
            df.loc[round_bmi_rep < 25, 'overweight_rep'] = 0
            df.loc[round_bmi_rep >= 30, 'obese_rep'] = 1
            df.loc[round_bmi_rep < 30, 'obese_rep'] = 0
        return df
