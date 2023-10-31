import numpy
from pandas.core.dtypes.generic import (
    ABCDatetimeIndex,
    ABCSeries,
)
from pandas.api.types import (  # noqa
    is_categorical_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_numeric_dtype,
    is_string_dtype as _is_string_dtype,
    is_datetime64_any_dtype as _is_datetime64_any_dtype,
    is_object_dtype,
    infer_dtype as _infer_dtype,
)
from pandas.core.dtypes.dtypes import ExtensionDtype


def is_float_value(val):
    "Predicate: is this value a floating point type?"
    # https://docs.scipy.org/doc/numpy/reference/arrays.scalars.html
    return isinstance(val, (float, numpy.floating))


def is_datetime_dtype(column):
    """
    Predicate: is this a datetime column.
    """
    # normally all you need
    if _is_datetime64_any_dtype(column):
        return True

    # This is the body of is_datetime_arraylike which was removed from pandas
    if isinstance(column, ABCDatetimeIndex):
        return True
    elif isinstance(column, (numpy.ndarray, ABCSeries)):
        return (
            is_object_dtype(column.dtype)
            and _infer_dtype(column, skipna=False) == "datetime"
        )
    return getattr(column, "inferred_type", None) == "datetime"


def is_string_dtype(column, *, mixed_ok=False):
    """
    Slower implementation of is_string_dtype from pandas that works per name.

    Args:
        column: pandas.Series to check dtype of
    Keyword Args:
        mixed_ok (default False): whether to consider object columns that
        contain at least 1 str value a str.

    https://github.com/pandas-dev/pandas/issues/15585
    """
    # negative result is dependable
    if not _is_string_dtype(column):
        return False

    # check if `column` is a dtype (API compatible, but ambiguous)
    # TODO: should we warn users who pass column.dtype instead of column?
    # it is ambiguous whether object dtypes contain strings, but we have to
    # break API to have a perfect answer
    if isinstance(column, numpy.dtype):
        return column.type == numpy.object_

    if column is numpy.object_:
        return True

    if isinstance(column, ExtensionDtype):  # pandas custom dtypes
        return False

    # if `column` is a Series and returned True for _is_string_dtype...
    vals = column.dropna()
    if len(vals):  # Truth value of a series is ambiguous
        type_requirement = any if mixed_ok else all
        return type_requirement(isinstance(val, str) for val in vals)
    else:
        return True  # all NaN an acceptable equivalent to string column
