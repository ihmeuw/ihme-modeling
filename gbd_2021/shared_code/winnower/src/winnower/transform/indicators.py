"""
Transform components for indicators.

Indicators are the primary means of producing output in survey extractions.

The main entry point to this will be transform_from_indicator. This method
takes a winnower.config.models.ubcov.IndicatorConfig instance and returns
an IndicatorBase subclass ready for procesing ... or it returns None.

None indicates the IndicatorConfig should not be used. Currently this is only
the case when the indicator defines no input_vars.

Note that there is a strong correlation between input_vars being empty and
code_custom being True. However the inverse is not the case, so the check is
always for if input_vars is empty, rather than if code_custom is True.
"""
import functools
import inspect

from attr import attrs, attrib
import numpy
import pandas

from winnower import constants
from winnower import errors
from winnower.util.categorical import (
    category_codes,
    is_categorical,
    CategoryFixer,
)
from winnower.util.dataframe import get_column_type_factory
from winnower.util.dates import datetime_to_numeric
from winnower.util.dtype import (
    is_numeric_dtype,
    is_string_dtype,
    is_datetime_dtype,
)
from winnower.util.expressions import (
    build_expression_query,
    is_expression,
)
from winnower.util.stata import is_missing_str, MISSING_STR_VALUE
from winnower.extract import get_column
from winnower.config.models.fields import comma_separated_values

from .base import (
    Component,

    attrs_with_logger,
)

from winnower.globals import eg  # Extraction context globals.


@attrs  # necessary for subclasses to inherit the attrib()s
class IndicatorBase(Component):
    """
    Base class for Indicator transforms.

    Performs checks to ensure that Indicator classes have consistent behavior.
    Conceptually similar to ABC, but for @classmethods

    Subclasses must be sure to decorate the class with @attr.attrs.
    """
    # Attributes common to all indicators
    output_column = attrib()
    map_indicator = attrib()
    code_custom = attrib()
    required = attrib()

    @classmethod
    def indicator_factory(cls, indicator, config):
        """
        Return Indicator ready to be used in an ExtractionChain or None.

        Returns None if the indicator isn't configured. In other words,
        mandatory inputs necessary to process have been left blank.
        """
        # Using @abstractmethod doesn't work - only prevents __init__ calls,
        # not classmethod calls.
        raise NotImplementedError("Subclasses must define indicator_factory")

    def _validate(self, input_columns):
        """
        Default _validate implemention, suitable for most indicators.

        Some subclasses will need to overwrite this.
        """
        self.input_column = self._fix_column(self.input_column, input_columns)

    def _fix_column(self, column, columns):
        """
        Helper method for resolving column names.

        Returns: case-insensitive equivalent of `column` which is present in
            columns.

        Raises: errors.ValidationError if no case-insensitive name is present.

        ubCov allows users to enter their configuration in all lower-case.
        The original program allowed this because it automatically renamed all
        columns to their lower-case values. In rare cases this led to errors,
        so winnower will not mimic the behavior.
        """
        self.logger.debug(f"fixing column: {column}")
        self.logger.debug(f"columns: {columns}")
        result, log_inexact, err = get_column(column, columns)
        if err:  # no inexact match found
            # sometimes users input previously created indicators in as vars
            # for other indicators.
            # ie: current_contra -> any_contra
            msg = f"{self.output_column}: missing input column {column!r}"
            raise errors.ValidationError(msg)

        if log_inexact:
            self.logger.info(f"{self.__class__.__name__} aliased {column!r} "
                             f"to {result!r}")
        return result

    def output_columns(self, source_output_columns):
        res = source_output_columns
        res.append(self.output_column)
        return res


def ind_NotImplementedError(ind: IndicatorBase, field, name=None):
    # provide name IFF passing a class
    # else picks up name from ind.output_column
    name = name or ind.output_column

    clsname = (ind if inspect.isclass(ind) else type(ind)).__name__
    msg = f"{name} ({clsname}) - no support for {field!r}"
    return NotImplementedError(msg)


# TODO: BinaryMixin subclasses don't require the map_indicator, code_custom,
# and required arguments
class BinaryMixin:
    """
    Mixin class for handling logic shared between Binary and CategMultiBinary.
    """
    def _get_expression_mask(self, col, expr, err_name) -> numpy.array:
        if is_numeric_dtype(col):
            col = col.astype(numpy.float64)
        # convert to DataFrame to use .query()
        col_frame = col.to_frame()
        match_expr = build_expression_query(expr, col.name)
        try:
            match = col_frame.query(expr=match_expr)
        except Exception as e:
            raise self._execute_expression_error(err_name, match_expr, e)
        else:
            # match is a subsetted data frame *not* a mask. it keeps the
            # original index values, so we can compare to col for our mask.
            return col.index.isin(match.index)

    def _get_values_mask(self, col, raw_values, use_category_dtype=False):
        factory = get_column_type_factory(
            col, use_category_dtype=use_category_dtype)
        values = [factory(X) for X in raw_values]
        return col.isin(values)

    def _try_get_true_false_masks(self, col, *, use_category_dtype):
        try:
            true_values = getattr(self, 'true_values')
            if is_expression(true_values):
                t_mask = self._get_expression_mask(col, true_values, err_name='true_values')
            else:
                t_mask = self._get_values_mask(col, true_values,
                                         use_category_dtype=use_category_dtype)

            if hasattr(self, 'false_values'):
                false_values = getattr(self, 'false_values')
                if is_expression(false_values):
                    f_mask = self._get_expression_mask(col, false_values, err_name='false_values')
                else:
                    f_mask = self._get_values_mask(col, false_values,
                                         use_category_dtype=use_category_dtype)
            else:
                f_mask = None

        except errors.InvalidExpression as e:
            t_mask = None
            f_mask = None
            has_issue = e
        else:
            if hasattr(self, 'false_value'):
                has_issue = not (t_mask | f_mask).any()
            else:
                has_issue = not (t_mask).any()
        return t_mask, f_mask, has_issue

    def _execute_expression_error(self, config_name, expression, e):
        "Return an informative error message for raising."
        if isinstance(e, pandas.core.computation.ops.UndefinedVariableError):
            err = e.args[0]
            # self.output_column
            msg = (f"Configuration for '{self.output_column}' {config_name}"
                   f" is '{expression}' which created error: {err}")
        else:
            msg = ("An unexpected error occurred when calculating "
                   f"{self.output_column} due to {config_name} being set to "
                   f"'{expression}' - '{e}'")
        return errors.InvalidExpression(msg)

    def get_masks(self, in_col, in_col_name):
        if is_categorical(in_col):
            # assume user provided labels
            t_mask, f_mask, has_issue = self._try_get_true_false_masks(
                in_col, use_category_dtype=True)

            # if there was an issue, try again assuming codes are provided
            if has_issue:
                msg = (f"Generating {self.output_column}: input column "
                       f"{in_col_name!r} is categorical whose labels do "
                       "not match any true/false values. Attempting to "
                       "match codes instead")
                self.logger.info(msg)

                t_mask, f_mask, has_issue = self._try_get_true_false_masks(
                    category_codes(in_col), use_category_dtype=False)

                if has_issue:
                    if isinstance(has_issue, Exception):
                        raise has_issue
                    else:
                        msg = (f"Generating {self.output_column}: input "
                               f"column {in_col_name!r} is categorical "
                               "whose codes AND labels do not match any "
                               "true/false values. All outputs are NaN. "
                               "Check your configuration")
                        self.logger.warning(msg)
        else:
            t_mask, f_mask, has_issue = self._try_get_true_false_masks(
                in_col, use_category_dtype=False)

            if has_issue:
                if isinstance(has_issue, Exception):
                    raise has_issue
                else:
                    msg = (f"Generating {self.output_column}: input "
                           f"column {in_col_name!r} is numeric whose "
                           "codes do not match any true/false values. "
                           "All outputs are NaN. Check your configuration")
                    self.logger.warning(msg)

        return t_mask, f_mask

    def log_missing(self, df, input_columns):
        # Handle logging of missing true/false values in a separate function
        # Should work for both child classes of BinaryMixin
        # This function does not handle expressions
        in_cols = [df[in_col] for in_col in input_columns]
        true_values = getattr(self, 'true_values')
        true_missing = numpy.copy(true_values)
        false_missing = []
        msg = ''

        for in_col in in_cols:
            true_missing = numpy.intersect1d(true_missing,
                                             get_missing(in_col, true_values))

        # If all values given as True values exist in the data, we skip
        # this section of the log
        if len(true_missing) > 0:
            msg += (f"True values: {true_missing} \n")

        # Since CategMultiBinary does not require false values to be given,
        # we need to check that the attribute exists before trying to compare
        # the data against False values, otherwise logic is the same as
        # for True values
        if hasattr(self, 'false_values'):
            false_values = getattr(self, 'false_values')
            false_missing = numpy.copy(false_values)
            for in_col in in_cols:
                false_missing = numpy.intersect1d(false_missing,
                                                  get_missing(in_col, false_values))

        # If all values given as True values exist in the data, we skip
        # this section of the log
        if len(false_missing) > 0:
            msg += (f"False values: {false_missing} \n")

        # We only log the message if there are either true or false
        # values missing, otherwise, no log is output
        if msg:
            full_msg = (f"Generating {self.output_column}: input columns do "
                        "not contain any values matching the following "
                        f"true/false values \n") + msg
            self.logger.warning(full_msg)


@attrs_with_logger
class Binary(IndicatorBase, BinaryMixin):
    """
    A binary True/False value supporting NULLs.
    """
    input_columns = attrib()  # Note plural!
    true_values = attrib()
    false_values = attrib()

    def get_uses_columns(self):
        return self.input_columns

    def _validate(self, input_columns):
        "Overridden to support multiple input columns."
        self.input_columns = [self._fix_column(col, input_columns)
                              for col in self.input_columns]

    @classmethod
    def indicator_factory(cls, indicator, config):
        input_columns = getattr(config, indicator.indicator_name)

        if not input_columns:
            return  # not filled in - blank in codebook

        name = indicator.indicator_name
        false_values, true_values = f"{name}_false", f"{name}_true"

        # Original ubCov code hard-coded the _true and _false values in to
        # processing such that input_meta wasn't used
        # Users often omit entered input_meta into the indicators db
        # when left blank, binary should assume the _true _false syntax
        if set(indicator.input_meta) != {false_values, true_values}:
            indicator.input_meta = (true_values, false_values)

        return cls(output_column=name,
                   true_values=getattr(config, true_values),
                   false_values=getattr(config, false_values),
                   input_columns=input_columns,
                   map_indicator=indicator.map_indicator,
                   code_custom=indicator.code_custom,
                   required=indicator.indicator_required)

    def execute(self, df):
        col = pandas.Series(float('NaN'),
                            index=df.index, name=self.output_column)

        for in_col_name in self.input_columns:
            in_col = df[in_col_name]

            t_mask, f_mask = self.get_masks(in_col, in_col_name)
            nan_mask = numpy.logical_not(numpy.logical_or(f_mask, t_mask))

            if nan_mask.any():
                self._log_col_nans(in_col, in_col_name, nan_mask)

            col = self._update_bin_output(col, t_mask, f_mask)

        self.log_missing(df, self.input_columns)

        df[self.output_column] = col
        return df

    def _update_bin_output(self, col, t_mask, f_mask):
        # get mask - True == replace; False == conditional replace
        col[t_mask] = 1
        f_mask = numpy.logical_and(f_mask, pandas.isnull(col))
        col[f_mask] = 0
        return col

    def _log_col_nans(self, col, col_name, nan_mask):
        # numpy.unique does not work on str categorical labels
        if is_categorical(col):
            vals, _ = numpy.unique(col.cat.codes[nan_mask], return_index=True)
            categ_map = dict(enumerate(col.cat.categories))
            unmapped_labels = [categ_map[i] for i in vals if i in categ_map]
        else:
            vals, _ = numpy.unique(col[nan_mask], return_index=True)

        if not is_numeric_dtype(vals):
            vals = numpy.setdiff1d(vals, numpy.array(['']))
        else:
            vals = vals[~numpy.isnan(vals)]

        if len(vals) > 0:
            msg = (f"Generating {self.output_column}: input column"
                   f" {col_name!r} had the following values not matching"
                   f" either true or false conditions, interpreted as NaN \n"
                   f"{vals}")
            if is_categorical(col):
                # also want to know the labels for which values went unmatched
                msg += f"\ncorresponding labels:  {unmapped_labels}"
            msg += (" \nCheck your configuration \n")
            self.logger.warning(msg)


@attrs_with_logger
class CategoricalStr(IndicatorBase):
    """
    A series of categorical values.

    Categorical values, similar to an enumeration, use compact numeric codes to
    store information that is much more expensive to store naively e.g., string
    values.
    """
    input_column = attrib()

    @classmethod
    def indicator_factory(cls, indicator, config):
        input_column = getattr(config, indicator.indicator_name)

        if not input_column:
            return  # not filled in

        res = cls(output_column=indicator.indicator_name,
                  input_column=input_column,
                  map_indicator=indicator.map_indicator,
                  code_custom=indicator.code_custom,
                  required=indicator.indicator_required)
        return res

    def get_uses_columns(self):
        return [self.input_column]

    def execute(self, df):
        col = df[self.input_column]

        if is_numeric_dtype(col):
            msg = (f"Trying to generate {self.output_column} but "
                   f"{self.input_column} is a numeric with no labels. Please "
                   "specify labels in the Labels Database.")
            self.logger.warning(msg)
            df[self.output_column] = col
        elif is_categorical(col):
            # Drop category codes and convert to a fully string value
            df[self.output_column] = CategoryFixer().extract_labels(col)
        else:  # string
            try:  # if all values can be converted to numeric, do so.
                df[self.output_column] = pandas.to_numeric(col, errors='raise')
            except ValueError:
                df[self.output_column] = col.fillna(MISSING_STR_VALUE)
                msg = f"{self.output_column} could not convert to numeric"
                self.logger.info(msg)
            else:
                msg = f"Converted {self.output_column} to numeric"
                self.logger.info(msg)

        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f'Set indicator column {self.output_column}',
        }
        self.logger.info(self._execute_metadata['detail'])
        return df


@attrs_with_logger
class CategMultiBinary(IndicatorBase, BinaryMixin):
    """
    A ###-separated list of categorical values.

    Categorical Multi Binaries are configured very similarly to Binary objects
    but have very different outputs. Whereas a Binary object uses these inputs
    to generate 1/0 values representing True/False, a Categorical Multi Binary
    uses these values to generate a string label with the *column label* of
    every input var which evaluates to a True value. This label is delimited
    with '###'.

    Foe example given 3 inputs saw_clinic, saw_pharm, saw_hosp with these truth
    values for a particular row:

    input var   column label        row evaluates to true?

    saw_clinic  Visited Clinic      no
    saw_pharm   Visited Pharmacy    yes
    saw_hosp    Visited Hospital    yes

    The output for this row would be 'Visited Pharmacy###Visited Hospital'.
    """
    input_columns = attrib()
    true_values = attrib()
    column_labels = attrib()

    @classmethod
    def indicator_factory(cls, indicator, config, column_labels):
        input_columns = getattr(config, indicator.indicator_name)

        if not input_columns:
            return  # not filled in

        name = indicator.indicator_name
        true_values = getattr(config, f"{name}_true")
        false_values = getattr(config, f"{name}_false")

        if is_CategoricalStr_indicator(indicator, config):
            # CategoricalStr expects a single value instead of a tuple of
            # values (which is valid for CategMultiBinary). This mutation
            # fixes the configuration so that CategoricalStr will process
            # correctly.
            setattr(config, indicator.indicator_name, input_columns[0])
            return CategoricalStr.indicator_factory(indicator, config)

        res = cls(input_columns=input_columns,
                  output_column=name,
                  true_values=true_values,
                  column_labels=column_labels,
                  map_indicator=indicator.map_indicator,
                  code_custom=indicator.code_custom,
                  required=indicator.indicator_required)

        return res

    def get_uses_columns(self):
        return self.input_columns

    def _validate(self, input_columns):
        "Validate every input column has a label."
        # Fix input columns
        self.input_columns = [self._fix_column(col, input_columns)
                              for col in self.input_columns]
        # Ensure all columns are labeled
        without_label = [X for X in self.input_columns
                         if X not in self.column_labels]
        if without_label:
            msg = f"No labels for columns {without_label}"
            raise errors.ValidationError(msg)

    def execute(self, df):
        parts = pandas.Series([[] for _ in df.index], index=df.index)

        for in_col in self.input_columns:
            label = self.column_labels[in_col]

            t_mask, f_mask = self.get_masks(df[in_col], in_col)

            for idx in numpy.where(t_mask)[0]:
                # numpy.where returns 0-index values. If the DataFrame has a
                # non-zero based index, this will error without iloc.
                parts.iloc[idx].append(label)
        self.log_missing(df, self.input_columns)

        df[self.output_column] = parts.map("###".join)
        return df


@attrs_with_logger
class Continuous(IndicatorBase):
    """
    A continuous numeric value series.

    Accounts for missing values.
    """
    input_column = attrib()
    missing = attrib()

    @classmethod
    def indicator_factory(cls, indicator, config):
        name = indicator.indicator_name
        input_column = getattr(config, name)

        if not input_column:
            return

        missing = getattr(config, f"{name}_missing", None)

        return cls(output_column=name,
                   input_column=input_column,
                   map_indicator=indicator.map_indicator,
                   code_custom=indicator.code_custom,
                   required=indicator.indicator_required,
                   missing=missing)

    def get_uses_columns(self):
        return [self.input_column]

    def execute(self, df):
        col = df[self.input_column]
        # Original source
        # <ADDRESS>
        # http://www.stata.com/help.cgi?destring

        # by default, destring produces double values (float64)
        # we'll settle for any numeric value
        # https://stackoverflow.com/a/29519728
        if is_numeric_dtype(col.dtype):
            # convert to float64 explicitly to reduce rounding issues
            # do this for the input column as well for consistency
            df[self.input_column] = col = col.astype(numpy.float64)
        elif is_categorical(col):
            col = category_codes(col)
        elif is_string_dtype(col, mixed_ok=True):
            # convoluted - replicate stata "destring" behavior which converts
            # numbers but also treats empty strings as missing (~NaN) values
            nan_mask = col.map(is_missing_str)
            # a bit of a hack - we can't convert to_numeric with "missing"
            # values still present, but we don't set "missing" values to NaN
            # until later in this function
            if self.missing is not None:
                miss_selector = self.missing_selector(df)
                nan_mask[miss_selector] = True
            try:
                converted = pandas.to_numeric(col[~nan_mask], errors='raise')
            except Exception:
                msg = ("Generating Continuous indicator "
                       f"{self.output_column!r} as number but input is "
                       "string. Please report error.")
                self.logger.error(msg)
                col = col.fillna(MISSING_STR_VALUE)
            else:
                col = pandas.Series(index=col.index, dtype='float64')
                col[nan_mask] = float('NaN')
                col[~nan_mask] = converted

        elif is_datetime_dtype(col):
            self.logger.warning(f"Column {self.input_column} is already "
                                "datetime, attempting to convert to stata "
                                "days since Jan, 1960. Check data as this "
                                "might not work!!")
            col = datetime_to_numeric(col)
            if eg.more_debug:
                self.logger.debug(f"Days since 1960 for {self.input_column} "
                                  f"{col}")
        else:
            msg = (f"({self.output_column} source {self.input_column} is non-"
                   f"numeric: {col.dtype}. Contact <USERNAME> to discuss fix.")
            raise NotImplementedError(msg)

        df[self.output_column] = col

        if self.missing is not None:
            selector = self.missing_selector(df)
            df.loc[selector, self.output_column] = float('NaN')
            self.log_missing(df)

        detmsg = (f"Generate continuous values from {self.input_column!r} "
                  f"to {self.output_column!r}")
        self.logger.info(detmsg)
        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': detmsg,
        }
        return df

    def missing_selector(self, df):
        """
        Returns selector for missing data.

        Useful with e.g., DataFrame.loc[selector, ...]
        """
        if is_expression(self.missing):
            query = build_expression_query(self.missing, self.input_column)
            # TODO: handle errors - see Binary
            index = df.query(query).index
            return index
        else:  # if not expression, then list of values to set to NaN
            # NOTE: originally we used pandas.Series.isin() instead of casting
            # this fails silently if the missing values do not cast
            tf = get_column_type_factory(df[self.input_column])
            missing_vals = []

            if isinstance(self.missing, str):
                values = [X for X in comma_separated_values(self.missing)]
            else:
                values = self.missing

            for val in values:
                try:
                    converted = tf(val)
                except ValueError:
                    dtype = df[self.input_column].dtype
                    msg = (f"Could not convert {val!r} to {dtype} - "
                           "missing values may be incorrect")
                    self.logger.error(msg)
                    missing_vals.append(val)
                else:
                    missing_vals.append(converted)

            mask = df[self.input_column].isin(missing_vals)
            # https://stackoverflow.com/a/52173171
            return mask[mask].index

    def log_missing(self, df):
        # Handle logging of missing values in a separate function
        # This function does not handle expressions
        missing = get_missing(df[self.input_column], getattr(self, 'missing'))

        # If all values given as missing values exist in the data, we skip
        # this section of the log
        if len(missing) > 0:
            msg = (f"Generating {self.output_column}: input column does "
                   "not contain any values matching the following "
                   f"missing values \nMissing values: {missing} \n")
            self.logger.warning(msg)

    def ouput_columns(self, input_columns):
        return input_columns


@attrs_with_logger
class Meta(IndicatorBase):
    """
    Meta indicators directly pass through input data to the output.

    Meta is effectively a marker indicating "use this var as an indicator."
    """
    # Note: no input column!
    value = attrib()

    @classmethod
    def indicator_factory(cls, indicator, config):
        value = getattr(config, indicator.indicator_name)

        if value is None:
            return

        output_column = indicator.indicator_name

        return cls(output_column=output_column,
                   value=value,
                   map_indicator=indicator.map_indicator,
                   code_custom=indicator.code_custom,
                   required=indicator.indicator_required)

    def get_uses_columns(self):
        "No input column. Return empty list"
        return []

    def _validate(self, input_columns):
        "Convert value to str if it is not."
        self.value = str(self.value)

    def execute(self, df):
        df[self.output_column] = self.value

        if self.map_indicator:
            raise ind_NotImplementedError(self, 'map_indicator')

        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f'Set column {self.output_column!r} to {self.value}',
        }
        self.logger.info(self._execute_metadata['detail'])
        return df


class MetaNum(Meta):
    """
    MetaNum indicators directly pass through input data to the output.

    MetaNum is identical to Meta except that output is to be numeric.
    """
    def _validate(self, input_columns):
        "Validate self.value can be converted to a numeric."
        try:
            self.value = int(self.value)
        except ValueError:
            if self.value == constants.MISSING_STR:
                self.value = float('NaN')
            else:
                try:
                    self.value = float(self.value)
                except ValueError:
                    return


# Maps ubCov indicator types to handling classes
# <ADDRESS>
INDICATOR_TYPE_MAPPING = {
    'bin': Binary,
    'categ_str': CategoricalStr,
    'categ_multi_bin': CategMultiBinary,
    'cont': Continuous,
    'meta': Meta,
    'meta_str': Meta,  # no known difference between "meta" and "meta_str"
    'meta_num': MetaNum,
    # cont and num are generated identically; cont has optional '_missing'
    'num': Continuous,
}


def transform_from_indicator(ind, config, label_helper):
    """
    Main API method

    Returns transform component for the indicator or None.
    """
    ind_type = ind.indicator_type
    mapped_type = INDICATOR_TYPE_MAPPING.get(ind_type)
    if mapped_type is None:
        ind_name = ind.indicator_name
        msg = f"Indicator {ind_name} is {ind_type!r} - not yet supported"
        raise NotImplementedError(msg)

    # This may return None, which indicates there is no transformation to
    # perform for the indicator `ind`
    if ind.indicator_type == 'categ_multi_bin':
        column_labels = label_helper.get_column_labels()
        return mapped_type.indicator_factory(ind, config,
                                             column_labels)
    else:
        return mapped_type.indicator_factory(ind, config)


def is_CategoricalStr_indicator(ind, config=None):
    '''
    API method

    Checks if an indicator should be handled as a
    CategoricalStr which is the case if the given
    indicator is of type CategoricalStr or if it
    is of type CategMultiBinary with a single column

    We allow Config to be null, since there exists a weird
    edge case in which basic indicators do not include a
    configuration
    '''
    if ind.indicator_type == 'categ_str':
        return bool(ind.input_vars)
    elif ind.indicator_type == 'categ_multi_bin':
        # CategMultiBinary objects will be accompanied by a valid configuration
        if config is None:
            raise errors.Error("Cannot determine if a categ_mutli_bin will become a categ_str without config")
        return all([
            len(getattr(config, ind.indicator_name)) == 1,
            not getattr(config, f"{ind.indicator_name}_true"),
            not getattr(config, f"{ind.indicator_name}_false"),
        ])
    else:
        return False


def get_missing(in_col, values):

    def setdiff(ar1, ar2, out):
        # Define a new function that returns all items in ar1 which
        # are not contained in ar2. Similar to numpy's setdiff1d
        # However, in this function ar1 and out hold the same values,
        # but potentially of different types, so we perform the
        # set difference logic using ar1 and ar2, but return the
        # value in array out
        return out[numpy.isin(ar1, ar2,
                              assume_unique=True, invert=True)]

    missing = numpy.array([])
    if not is_expression(values):
        if is_categorical(in_col):
            # For categorical data, we first try to find missing
            # true/false values by checking the data as is,
            # we then try to cast the given true values to
            # numeric type to compare against codes, in case the
            # categorical data is given as codes instead of labels
            factory = get_column_type_factory(in_col,
                                              use_category_dtype=True)

            factorized = numpy.array([factory(X) for X in values])

            # NOTE Need to a 1-dimensional array for single value true/false
            # configurations for binary indicators, i.e. when not a tuple
            missing = setdiff(factorized,
                              in_col.unique(),
                              numpy.array(values, ndmin=1))

            # We can now filter again against the codes, in
            # case the true values were given as codes.
            # If we fail we simply ignore the exception
            try:
                # Since true/false/missing values can be a combination
                # of strings and numbers for categorical data,
                # the best we can do is try all of them
                factory = get_column_type_factory(category_codes(in_col),
                                                  use_category_dtype=True)

                factorized = numpy.array([factory(X) for X in values])

                missing = setdiff(factorized,
                                  category_codes(in_col).unique(),
                                  numpy.array(values, ndmin=1))
            except ValueError:
                pass
        else:
            # If value is not categorical, we can simply try to cast the
            # values to whatever data type the data column is
            # Again, we use try/except since values can be a mix of
            # numeric and strings
            try:
                factory = get_column_type_factory(in_col,
                                                  use_category_dtype=False)
                if isinstance(values, str):
                    factorized = numpy.array([factory(values)])
                else:
                    factorized = numpy.array([factory(X) for X in values])

                missing = setdiff(factorized,
                                  in_col.unique(),
                                  numpy.array(values, ndmin=1))
            except ValueError:
                pass

    return missing
