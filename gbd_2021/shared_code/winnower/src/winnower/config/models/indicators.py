"""
Provides method for_indicator, which returns a dict of all information needed
to generate a form class which can process inputs for the indicator.

An "indicator" is represented as a set of related form fields. For example,
the indicator has_tb_now is a "bin" indicator which implies it has 3 fields:

has_tb_now_true     values indicator the subject has tb.
has_tb_now_false    values indicating the subject does NOT have tb.
has_tb_now          column containing subject data, which should only contain
                    the set of values in has_tb_now_true and has_tb_now_false.
"""
from .fields import (
    value,
    values_list,
    comma_separated_values,
    list_or_expression,
)


# Primary dispatch method
def fields_for_indicator(ind) -> dict:
    if ind.code_custom:
        return {}

    try:
        method = INDICATOR_TYPE_FIELDS[ind.indicator_type]
    except KeyError:
        msg = f"No support for indicator type {ind.indicator_type}"
        raise RuntimeError(msg)
    else:
        return method(ind)


# Logical groups for processing all fields related to an Indicator.
def _fields_for_bin(ind):
    name = ind.indicator_name

    return {
        name: values_list(required=ind.indicator_required,
                         converter=comma_separated_values),
        f"{name}_true": values_list(required=ind.indicator_required,
                                    converter=list_or_expression),
        f"{name}_false": values_list(required=ind.indicator_required,
                                     converter=list_or_expression),
    }


def _fields_for_categ_str(ind):
    name = ind.indicator_name
    return {name: value(required=ind.indicator_required)}


def _fields_for_categ_multi_bin(ind):
    name = ind.indicator_name
    return {
        name: values_list(required=ind.indicator_required,
                         converter=comma_separated_values),
        f"{name}_true": values_list(required=ind.indicator_required),
        f"{name}_false": values_list(required=ind.indicator_required),
    }


def _fields_for_cont(ind):
    name = ind.indicator_name
    required = ind.indicator_required
    return {
        name: value(required=required),
        # the _missing values are never required
        f"{name}_missing": values_list(required=False),
    }


def _fields_for_meta_num(ind):
    name = ind.indicator_name
    return {name: value(required=ind.indicator_required)}


def _fields_for_meta_str(ind):
    name = ind.indicator_name
    return {name: value(required=ind.indicator_required)}


INDICATOR_TYPE_FIELDS = {
    'bin': _fields_for_bin,
    'categ_str': _fields_for_categ_str,
    'categ_multi_bin': _fields_for_categ_multi_bin,
    'cont': _fields_for_cont,
    'meta': _fields_for_meta_str,  # identical to meta_str
    'meta_num': _fields_for_meta_num,
    'meta_str': _fields_for_meta_str,
    'num': _fields_for_cont,  # identical to cont
}
