import re

import pandas

from winnower.util.categorical import (
    category_codes,
)
from winnower.util.dtype import (
    is_categorical_dtype,
    is_string_dtype,
)

_MISSING_MATCH = re.compile('^[.][a-z]?$')
MISSING_STR_VALUE = ''


class StataNameValidator:
    """
    Validates Stata variable names.
    """
    reserved = frozenset([
        '_all', '_b', 'byte', '_coef', '_cons', 'double', 'float', 'if', 'in',
        'int', 'long', '_n', '_N', '_pi', '_pred', '_rc', '_skip', 'strL',
        'using', 'with',
        'str0', 'str1', 'str2', 'str3', 'str4', 'str5', 'str6', 'str7', 'str8',
        'str9',
    ])

    # 1-32 letters
    name_match = re.compile('^[A-Za-z0-9_]{1,32}$')

    @classmethod
    def is_valid_column_name(cls, s):
        """
        Predicate: indicates if `s` is a valid stata name.

        See 11.3 http://www.stata.com/manuals13/u11.pdf
        """
        if s in cls.reserved:
            return False
        return bool(cls.name_match.match(s))


is_valid_column_name = StataNameValidator.is_valid_column_name


def is_missing_str(val: str):
    """
    Predicate: is `val` a missing value in Stata?
    """
    if not isinstance(val, str):
        return False
    if val == MISSING_STR_VALUE:
        return True
    return bool(_MISSING_MATCH.match(val))


def is_missing_value(value):
    if isinstance(value, pandas.Series):
        if is_string_dtype(value):
            return (value == '') | pandas.isnull(value)
        else:  # categorical, datetime, numeric
            return pandas.isnull(value)
    else:  # single scalar value
        return pandas.isnull(value) or value == MISSING_STR_VALUE


def group(df, columns: list):
    """
    Provide group() functionality per Stata built-in egen with group option.

    Args:
        df: pandas DataFrame containing all columns specified by columns.
        columns: a list of column names present in df.

    Generates a series of numeric values numbered 1-N for the N distinct sets
    of values in all rows of df[columns]. These sets of values are numbered in
    sorted order.

    Any row with 1 or more NaN values will have a group value of NaN.

    https://www.stata.com/manuals13/degen.pdf
    """
    result = pandas.Series(float('NaN'), index=df.index)

    tmp = df[columns].dropna()  # drop all rows with 1 or more NaNs
    for col_name in columns:
        if is_categorical_dtype(tmp[col_name]):
            tmp[col_name] = category_codes(tmp[col_name])

    for group_val, index in enumerate(tmp.groupby(columns).groups.values(),
                                      start=1):
        result[index] = group_val

    return result
