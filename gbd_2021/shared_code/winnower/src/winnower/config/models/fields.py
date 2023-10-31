"""
Fields for processing ubcov configuration from online spreadsheets.
"""
from pathlib import Path
import re

import attr
import pandas

from winnower import constants
from winnower import errors
from winnower.util.path import fix_survey_path


# Helper methods
def required_fields(form_cls):
    """Returns list of required fields in a form."""
    return [X.name for X in attr.fields(form_cls) if X.default is attr.NOTHING]


def list_or_expression(s):
    """
    Tokenizes `s` depending on if it is a value list or an expression.

    Used as a `converter` argument in attr-based forms.
    """
    if pandas.isnull(s):
        return ()

    if re.match(">|<", s):
        return (s,)
    else:
        return tuple(s.replace(",", " ").split())


# Validator methods
def validate_not_empty(instance, attribute, val):
    if not val:
        name = attribute.name
        raise errors.ValidationError(f"{name} must define at least 1 value")


def validate_not_null(inst, attr, value):
    """
    Validates `value` is not None or NaN
    """
    if pandas.isnull(value):
        raise errors.ValidationError(f"{attr} must not be None/NaN")


# Field cleaners/converters
def _convert_path(path: str):
    return Path(fix_survey_path(path))


def comma_separated_values(val: str):
    """
    Returns a tuple of values, performing minor cleaning.

    >>> comma_separated_values('foo')
    ('foo',)
    >>> comma_separated_values(' foo   ')
    ('foo',)
    >>> comma_separated_values('foo, bar')
    ('foo', 'bar')
    >>> comma_separated_values('foo,bar')
    ('foo', 'bar')
    >>> comma_separated_values('  foo , bar   ')
    ('foo', 'bar')
    >>> comma_separated_values('"foo","bar"')
    ('foo', 'bar')
    >>> comma_separated_values(float('nan'))
    ()
    """
    if pandas.isnull(val):
        return ()
    return tuple(X.strip() for X in val.replace('"', '').split(','))


def pandas_bool(val, default=False):
    """
    Conve
    """
    if pandas.isnull(val):
        return default
    if isinstance(val, str):
        val = int(val)
    return bool(val)


def nan_to_None(val):
    """
    Converts nan values None and returns. All other values returned as-is.

    >>> nan_to_None(None)
    >>> nan_to_None(float('nan'))
    >>> nan_to_None(42)
    42
    """
    if pandas.isnull(val):
        return None
    return val


def nan_or(converter, default=None):
    """
    Returns conversion function for use in attrib "convert" kwarg.

    Function first checks if input valuse is NaN or None. If so, the `default`
    is returned. Otherwise, converter(val) is returned.

    Pass optional kwarg `default` to use a non-None value
    """
    def func(val):
        if pandas.isnull(val) or val == constants.MISSING_STR:
            return default
        return converter(val)

    return func


nan_or_str = nan_or(str.strip)
pandas_int = nan_or(int)
pandas_str = nan_or(str)


def separated_values(val: str, delimiters=' |,&'):
    """
    Returns a tuple of values from a delimited list.

    >>> separated_values('foo bar')
    ('foo', 'bar')
    >>> separated_values('foo,bar')
    ('foo', 'bar')
    >>> separated_values('foo|bar')
    ('foo', 'bar')
    >>> separated_values('foo&bar')
    ('foo', 'bar')
    >>> separated_values(' foo &   bar  ')
    ('foo', 'bar')
    >>> separated_values(float('NaN'))
    ()
    """
    if pandas.isnull(val):
        return ()
    return tuple(X for X in re.sub(f"[{delimiters}]", ' ', val).split() if X)


# Logical field processors
def ubcov_id_field():
    return attr.attrib(converter=int)


def winnower_id_field():
    """
    Unique str that is constructed from keys configured for file.

    Codebooks and merge generate a winnower_id from a md5 hash of
    the concatenated keys in the IHME survey key.
    See: constants/SURVEY_KEY.
    """
    return attr.attrib(converter=str)


def required_attrib(**kwargs):
    """
    A required attribute.
    """
    md = kwargs.get('metadata', {})
    md['required'] = True
    kwargs['metadata'] = md

    # Set validate_not_null as first validator
    validator = [validate_not_null]
    if 'validator' in kwargs:
        v = kwargs['validator']
        if callable(v):
            validator.append(v)
        else:  # assume sequence, the other valid value
            validator.extend(v)

    kwargs['validator'] = validator

    return attr.ib(**kwargs)


def optional_attrib(**kwargs):
    """
    An optional attribute.

    Sets `default` value to None if not specified.
    """
    kwargs.setdefault('default', None)
    kwargs.setdefault('converter', nan_to_None)
    return attr.ib(**kwargs)


def optional_varlist(converter=comma_separated_values):
    """
    An optional variable list. May contain multiple values comma separated.

    Provide optional kwarg convert to use a different method to split values.
    """
    return optional_attrib(converter=converter)


def space_separated_values(val: str):
    """
    Returns a tuple of values form a space delimited list.

    Performs minor cleaning.

    >>> space_separated_values('foo bar')
    ('foo', 'bar')
    >>> space_separated_values('  foo     bar ')
    ('foo', 'bar')
    """
    if pandas.isnull(val):
        return ()
    return tuple(X.strip() for X in val.split())


def value(*, converter=nan_or_str, required=False):
    if required:
        return attr.ib(converter=converter, validator=validate_not_null)
    else:
        return optional_attrib(converter=converter)


def values_list(*, converter=separated_values, required=False):
    """
    A sequence of values, separated by delimiters.

    Delimiters may be one or more of space ( ), pipe (|), comma (,) or
    ampersand (&).
    """
    if required:
        return attr.ib(converter=converter, validator=validate_not_empty)
    else:
        return optional_attrib(converter=converter)


def required_shared_filesystem_path():
    "A required attribute that represents a shared file system path."
    return required_attrib(converter=_convert_path)
