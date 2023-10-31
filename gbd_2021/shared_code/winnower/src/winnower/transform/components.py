#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8 :
"""
Components for transformation of datasets.
"""
import re

from attr import attrib
import numpy
import pandas

from winnower import constants
from winnower.errors import Error, ValidationError
from winnower.extract import get_column
from winnower.util.categorical import (
    CategoryFixer,
    category_codes,
    is_categorical,
    standin_label,
    undefined_label,
)
from winnower.util.dataframe import (
    get_column_type_factory,
    rows_with_nulls_mask,
)
from winnower.util.dtype import (
    is_numeric_dtype,
    is_integer_dtype,
)
from winnower.util.stata import (
    MISSING_STR_VALUE,
    is_missing_value,
)

from .base import (
    Component,

    attrs_with_logger,
)

from winnower.globals import eg  # Extraction context globals.


@attrs_with_logger
class AliasColumn(Component):
    """
    Alias a column, making it available under another name.
    """
    alias_from = attrib()
    alias_to = attrib()

    def get_uses_columns(self):
        return [self.alias_from]

    def _validate(self, input_columns):
        if self.alias_from in input_columns:
            return
        msg = f"(AliasColumn) column {self.alias_from} not in input_columns"
        raise ValidationError(msg)

    def output_columns(self, input_columns):
        input_columns.append(self.alias_to)
        return input_columns

    def execute(self, df):
        df[self.alias_to] = df[self.alias_from]

        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f"Alias {self.alias_from!r} to {self.alias_to!r}",
        }
        self.logger.info(f"Aliased {self.alias_from} to {self.alias_to}")
        return df


@attrs_with_logger
class ConstantColumn(Component):
    """
    Adds a constant value as a column.
    """
    column = attrib()
    value = attrib()

    def _validate(self, input_columns):
        "No validation to perform."

    def get_uses_columns(self):
        "Nothing to return"
        return []

    def output_columns(self, input_columns):
        if self.column not in input_columns:  # TODO: O(n)
            input_columns.append(self.column)
        return input_columns

    def execute(self, df):
        res = df
        self.logger.debug(f"Adding constant column {self.column}")
        # odd case - we're assigning a sequence (likely a tuple) to a column
        # if we try the naive solution pandas will incorrectly assume we're
        # trying to assign the sequence VALUES to multiple COLUMNS and then
        # error
        if pandas.core.dtypes.inference.is_sequence(self.value):
            newcol = pandas.Series([self.value] * len(df))
            res[self.column] = newcol
        else:
            res[self.column] = self.value
        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f"Add column {self.column} as constant {self.value}",
        }
        return res


@attrs_with_logger
class DropEmptyRows(Component):
    """
    Scan resulting DataFrame and remove any effectively null rows.
    """
    indicators = attrib()

    def _validate(self, input_columns):
        "Nothing to validate."

    def output_columns(self, input_columns):
        return input_columns

    def get_uses_columns(self):
        "Return empty list"
        return []

    def execute(self, df):
        cols_to_check = [X.indicator_name for X in self.indicators
                         if 'meta' not in X.indicator_type
                         and X.indicator_name in df]  # noqa
        ncols = len(cols_to_check)
        rows_to_drop = pandas.isna(df[cols_to_check]).sum(axis=1) == ncols
        indices_in_index_to_drop = numpy.where(rows_to_drop)[0]
        indices_to_drop = df.index[indices_in_index_to_drop]
        if len(indices_to_drop):
            self.logger.info(f"Dropping {len(indices_to_drop)} null rows")

        res = df.drop(index=indices_to_drop)
        return res


@attrs_with_logger
class Keep(Component):
    """
    Keep rows based on the presence of data in a given column.
    """
    column = attrib()

    def _validate(self, input_columns):
        if self.column in input_columns:
            return
        msg = f"(Keep) column {self.column} not in input_columns"
        raise ValidationError(msg)

    def output_columns(self, input_columns):
        return input_columns

    def execute(self, df):
        null_mask = rows_with_nulls_mask([self.column], df)

        n_kept, n = null_mask.sum(), len(null_mask)

        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f"Kept {n_kept} of {n}",
        }

        return df[~null_mask]


@attrs_with_logger
class KeepColumns(Component):
    """
    Keep only specified columns, dropping all others.
    """
    columns_to_keep = attrib(converter=frozenset)
    drop_empty = attrib()

    def execute(self, df):
        columns_to_drop = [X for X in df if X not in self.columns_to_keep]
        self.logger.info(f"Dropping columns {columns_to_drop}")

        if self.drop_empty:
            null_cols = []
            for column in df:
                if is_missing_value(df[column]).all():
                    null_cols.append(column)

            columns_to_drop.extend(null_cols)
            self.logger.info(f"Dropping all-null columns {null_cols}")

        res = df.drop(columns_to_drop, axis='columns')
        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f"Dropped columns {columns_to_drop}",
        }
        return res

    def _validate(self, input_columns):
        """
        Validates that the columns_to_keep are in input_columns. Not all
        indicators particularly from custom code will make to input_columns
        and at this stage, it should be ok.
        """
        for col in self.columns_to_keep:
            if col not in input_columns:
                self.logger.warning(f"keep_col(indicator){col!r} not found "
                                    f"in input_columns.")

    def output_columns(self, input_columns):
        return([X for X in self.columns_to_keep if X in input_columns])

    def get_uses_columns(self):
        """
        columns_to_keep could have indicators of the same name as data column.
        In that case, it needs to be included in source.uses_columns. So, return
        for further processing.
        """
        return []


@attrs_with_logger
class MapNumericValues(Component):
    """
    Maps numeric values to string labels for a single column.

    Args:
        column: the name of the column to map values for.
        old_new_values: sequence of (old_value, new_label) tuples.

    Remapping values is made more complex by pandas requirement that 'category'
    dtypes map to unique values (so that e.g., '4' and '5' cannot both map to
    'Ages 4-5'). This is due to the fact that internally pandas stores this
    information in a way such that you cannot determine the "code" (unmapped
    value) of something given its "category" (mapped value) unless these
    mapping are unique.

    As a workaround this transform has three logical steps:
        Step 1: determine if multiple keys map to the same new value. If so,
                create unique labels by embedding the value in them.
        Step 2: reassign any negative values as new positive ones, as pandas
                does not support negative codes.
        Step 3: convert data to a pandas "category" dtype.
    """
    @classmethod
    def if_present(cls, columns_to_map, value_map, chain):
        """
        Convenience method to map numeric values if present.

        MapNumericValues has two major use cases - emulating Stata's value
        labels and applying the external "label database" labels. This method
        is meant to cover the former case, but only apply the Stata labels that
        are actually necessary.
        """
        for configured_col in columns_to_map:
            # columns_to_map is derived from user-input configuration and may
            # be a case-insenitive match only. Get the real column name
            try:
                col = chain.get_input_column(configured_col)
            except ValidationError:
                continue

            if col in value_map:
                mapper = cls(
                    column=col,
                    old_new_values=value_map[col].items())
                chain.append(mapper)

    column = attrib()
    old_new_values = attrib()

    def get_uses_columns(self):
        return [self.column]

    def _validate(self, input_columns):
        if self.column not in input_columns:
            msg = f"(MapNumericValues) {self.column!r} not in input_columns"
            raise ValidationError(msg)
        # Value labels from stata files should not be stripped if
        # extractions should exactly match ubcov extractions.
        if eg.strict:
            for old, new in self.old_new_values:
                if new != new.strip():
                    msg = ("STRICT: not stripping new names for "
                           f"old '{old}', new '{new}'")
                    self.logger.warning(msg)
        else:
            self.old_new_values = {old: new.strip()
                                   for old, new in self.old_new_values}.items()

    def output_columns(self, input_columns):
        return input_columns

    def execute(self, df):
        in_column = df[self.column]
        # Step 0 - is it even numeric? We can only map numeric values
        if not is_numeric_dtype(in_column.dtype):
            if is_categorical(in_column):
                in_column = category_codes(in_column)
            else:
                in_column = self._convert_to_numeric(in_column)

        # Step 1 (see docstring)
        value_map, remapped_values = self._build_value_map(in_column)

        # Step 2
        if remapped_values:
            msg = (f"{self.column}: Remapping negative categorical values "
                   f"{remapped_values}")
            self.logger.warning(msg)
            in_column = in_column.replace(remapped_values)

        # Step 3
        df[self.column] = self._map_values_in_series(value_map, in_column)

        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': f"Mapped values for {self.column}"
        }
        self.logger.info(f"Mapped values for {self.column}")
        return df

    def _convert_to_numeric(self, in_column):
        """
        Attempt to convert Series to numeric dtype following Stata conventions.

        Raises RuntimeError if non-missing values are present which cannot be
        converted to numeric floats.
        """
        values = in_column.unique()

        # Ensure well known "missing" values are converted to NaN
        # ''  values are "missing" in text fields
        # "." values are "missing" in numeric fields
        replacements = {}
        if MISSING_STR_VALUE in values:
            replacements[MISSING_STR_VALUE] = float('NaN')
        if constants.MISSING_STR in values:
            replacements[constants.MISSING_STR] = float('NaN')
        if replacements:
            body = " and ".join(f"{old!r} with {new!r}"
                                for old, new in replacements.items())
            msg = f"Replacing values {body} to convert column to numeric."
            self.logger.info(msg)
            in_column = in_column.replace(replacements)

        try:
            result = in_column.astype(numpy.float64)

            orig_dtype = str(in_column.dtype)
            if orig_dtype == 'object':  # strings are held by generic 'object'
                orig_dtype = 'string'
            msg = (f"Coerced column {self.column!r} from {orig_dtype} "
                   f"to {result.dtype}")
            self.logger.warning(msg)

            return result
        except ValueError:
            msg = (f"(MapNumericValues) column {self.column!r} contains "
                   "non-numeric values which cannot be converted. Coercing.")
            self.logger.error(msg)
            return pandas.to_numeric(in_column, errors='coerce')

    def _build_value_map(self, in_column):
        """
        Returns value_map, a mapping of unique values to unique labels.

        If self.old_new_values contains duplicated labels they will be altered
        to include the related key. For example, if old_new_values is

            {1: 'Foo', 2: 'Foo'}

        then _build_value_map returns

            {1: 'Foo (1)', 2: 'Foo (2)'}

        In addition, negative values are remapped to unused positive values.
        """
        in_val_type = get_column_type_factory(in_column,
                                              use_category_dtype=False)
        # This deviates slightly from the original ubCov implementation in that
        # ubCov coerced all in_vals (k) to numbers, setting non-numeric values
        # to missing (NaN).
        self.logger.debug(f"old_new_vals {self.old_new_values}")
        value_map = {in_val_type(k): v for k, v in self.old_new_values}
        # Check for presence of values not in value_map; consolidate
        unmapped = [X for X in in_column.dropna().unique()
                    if X not in value_map]
        if unmapped:
            msg = (f"{self.column}: no value map for values {unmapped}. "
                   "These have been mapped to <undefined category <value>>")
            self.logger.warning(msg)
        # Update value map with undefined labels.
        for u_val in unmapped:
            value_map[in_val_type(u_val)] = undefined_label(u_val)

        return CategoryFixer().clean_value_map(value_map)

    def _map_values_in_series(self, value_map, series):
        """
        Remap the category labels in `series` per `value_map`.
        """
        # pandas categorical requires storing data internally in two arrays:
        # codes and categories. codes will be identical in length to `series`
        # and each value in codes will be a valid index in categories.
        # categories will be an array of unique values used as labels.
        #
        # As part of converting to categories, all original values in `series`
        # will be mapped from their original values to a new value in the range
        # of [0, number_of_unique_values_in_series). This process is results in
        # the loss of the original values.
        #
        # To avoid losing the original values we instead define categories
        # for all values in the range of [0, maximum_value_in_series]. To
        # facilitate this stand-in catgories must be created.
        #
        # This is done in two steps. First, convert the series to categorical
        # data with `categories` defined such that all codes will perfectly
        # match the original data. Second, rename the categories to the mapped
        # values provided in `value_map`.
        #
        # https://pandas.pydata.org/pandas-docs/stable/categorical.html

        unique_vals = series.dropna().unique()
        # This only works if all our non-NaN values are integers - check
        fractional, _ = numpy.modf(unique_vals)
        if fractional.any():
            problems = [X for X in unique_vals if int(X) != X]
            msg = (f"MapNumericValues: column {self.column} contains float "
                   f"values {problems} which cannot be converted to "
                   "categorical/labeled data without loss of information.")
            raise RuntimeError(msg)

        # regardless of dtype, our non-NaN values are integers
        try:
            categories = range(int(unique_vals.max()) + 1)
        except ValueError:
            # This actually happened - a rather large survey in ARAB LEAGUE
            # was subsetted to only include Morocco. The result was an entire
            # column contained only NaN values
            if len(unique_vals) == 0:
                categories = []
            else:
                raise

        self.logger.debug(f"categories {categories}")
        cat_data = pandas.Categorical(series,
                                      categories=categories,
                                      ordered=True)

        cat_series = pandas.Series(cat_data, index=series.index)
        # create new categories. Start by populating with filler values
        new_categories = [standin_label(X) for X in cat_series.cat.categories]
        # Then actually substitute in our real values
        for val in series.dropna().unique():
            idx = int(val)  # creation of series_ints proves this is safe
            # NOTES: a side effect of this is that any columns that depend on
            # the unmapped values being an empty string will break.
            #
            # Indicator columns will have the <undefined category> labels auto
            # converted to empty strings when the indicator's output column is
            # generated.
            #
            # value_map should have labels for all values including undefined.
            # Undefined labels were added during the build_value_map step.
            #
            # KeyError should never occur ... if it does something went
            # terribly wrong.
            try:
                new_categories[idx] = value_map[val]
            except KeyError:
                msg = (f"MapNumericValues: for column {self.column}."
                       f"value map {value_map} has no value {val}.")
                raise RuntimeError(msg)

        # Finally, assign the new categories. None of our "stand-in" labels
        # should ever be visible to the end user
        self.logger.debug(f"New categories {new_categories}")
        return cat_series.cat.rename_categories(new_categories)


@attrs_with_logger
class RenameColumn(Component):
    """
    Renames a column without modifying underlying data.
    """
    rename_from = attrib()
    rename_to = attrib()

    def _validate(self, input_columns):
        if self.rename_from in input_columns:
            return
        msg = f"(RenameColumn) column {self.rename_from} not in input_columns"
        raise ValidationError(msg)

    def output_columns(self, input_columns):
        input_columns.remove(self.rename_from)  # TODO: O(N)
        input_columns.append(self.rename_to)
        return input_columns

    def get_uses_columns(self):
        return [self.rename_from]

    def execute(self, df):
        f = self.rename_from
        t = self.rename_to

        df = df.rename(columns={f: t})
        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': "Rename {!r} to {!r}".format(f, t),
        }
        self.logger.info(f"Renamed {f} to {t}")
        return df


@attrs_with_logger
class Subset(Component):
    """
    Subsets (filters) a dataset given the query criteria.
    """
    query = attrib()
    have_hook = attrib()

    # Error message pandas raises if you query a non-present column
    query_err_matcher = re.compile(r"name '(.+?)' is not defined")
    # Stata Query "<varname> != ." to pandas
    query_not_missing_matcher = re.compile(
        (r"(?P<before>[\s^]*)"
         r"(?P<varname>[\w][\w\d]*)"
         r"\s*!=\s*\.(?P<after>\s|$)"))

    query_not_missing_repl = r"\g<before>\g<varname> == \g<varname>\g<after>"
    # Stata Query "<varname> == ." to pandas
    query_missing_matcher = re.compile(
        (r"(?P<before>[\s^]*)"
         r"(?P<varname>[\w][\w\d]*)"
         r"\s*==\s*\.(?P<after>\s|$)"))

    query_missing_repl = r"\g<before>\g<varname> != \g<varname>\g<after>"

    def _validate(self, input_columns):
        # Hypothetically we could parse the query str and figure this out.
        self._prepare_query(df=None)
        return

    def output_columns(self, input_columns):
        return input_columns

    def execute(self, df):
        query = self._prepare_query(df)
        # Case-insensitive config strikes again!
        # Injecting a variant of pandas.computation.scope.Scope to fix
        # is infeasible as pandas.computation.eval.eval hard-codes
        # pandas.computation.expr.Expr, which itself hard-codes Scope.
        #
        # Instead, use a brute-force loop. This operation occurs at most once
        # per extraction, and 901 of 7940 configured surveys use this feature
        while True:
            try:
                # In order to handle both numeric and string queries for
                # categorical data, we have to determine if the query term
                # is numeric, queries can come in the form ~ (column_name == )
                # or query, where the query would be of the form
                # column_name == query_term or column_name != query_term,
                # spaces however are not required. Since queries may be
                # compound, we determine which columns to switch to
                # their code value, and keep track of the changed columns,
                # to change the column back to the label value after the query
                fix_cols = []
                if type(query) == str:
                    matched = re.match(r"\~\s*\((?P<query>.*)\)$", query)

                    def split_queries(q):
                        # First we dissect the query into components by splitting
                        # on either '&' or '|' delimitters. Then for each component
                        # we remove spaces and replace instances of "!" with
                        # another "=" so we can split the query by "==" this
                        # does not affect the actual query, we just do it
                        # to parse column names and query terms
                        return [Q.replace(' ', '')
                                 .replace('!', '=')
                                 .split("==") for Q in re.split('[|&]', q)]

                    if matched:
                        queries = split_queries(matched.group('query'))
                    else:
                        queries = split_queries(query)

                    for subset_col, query_term in queries:
                        # only run the modified query if the column data is
                        # categorical and the query term is numeric
                        if subset_col in df:
                            if query_term.isnumeric():
                                if df[subset_col].dtype.name == 'category':
                                    # Keep track of the columns we are fixing
                                    fix_cols.append((subset_col, df[subset_col].copy()))
                                    df[subset_col] = df[subset_col].cat.codes
                                elif not is_numeric_dtype(df[subset_col].dtype):
                                    # log error, return unchanged dataframe
                                    self.log_bad_query(query=query,
                                                       have_hook=self.have_hook,
                                                       logger=self.logger)
                                    return df
                            elif is_integer_dtype(df[subset_col].dtype):
                                # log error, return unchanged dataframe
                                self.log_bad_query(query=query,
                                                   have_hook=self.have_hook,
                                                   logger=self.logger)
                                return df

                result = df.query(query)

                # Iterate back over the fixed columns to change the values
                # in the resulting dataframe to their original state
                for subset_col, labeled_col in fix_cols:
                    result[subset_col] = labeled_col[result.index]

            except pandas.core.computation.ops.UndefinedVariableError as e:
                query = self._fix_case_problem_in_query(
                    df.columns, query, e.args[0])
            except (ValueError, SyntaxError):
                self.log_bad_query(query=self.query, have_hook=self.have_hook,
                                   logger=self.logger)
                return df
            else:
                return result

    def _prepare_query(self, df):
        q = self.query
        if q.startswith('keep if '):
            return self._fix_stata_missing_value(q[8:])
        if q.startswith('drop if '):
            return "~ ({})".format(self._fix_stata_missing_value(q[8:]))

        self.log_bad_query(query=q, have_hook=self.have_hook,
                           logger=self.logger)

    @staticmethod
    def log_bad_query(*, query, have_hook, logger):
        context = f"The subset {query!r} errored during processing."
        please_check = "PLEASE CHECK YOUR OUTPUTS!"
        hook_faq = ("See extraction hook documentation at "
                    "winnower/browse/docsource/extraction_hooks.md")

        if have_hook:
            hook_msg = ("This extraction is using a custom code hook so it is "
                        "probably OK.")
        else:
            hook_msg = ("This extraction has no custom code hook! This output "
                        "is probably not correct!")

        msg = f"{context} {hook_msg} {hook_faq} {please_check}"

        if have_hook:
            logger.warning(msg)

        else:
            logger.error(msg)

    def _fix_stata_missing_value(self, q):
        # Attempt to correctly interpret stata missing values i.e. "."
        #
        # Stata has the concept of a missing value which is similar but
        # not exactly the same as a NaN value. In order to use a subset
        # query that filters on values that are not missing the query
        # must be re-worked to use an equivalent pandas query. The following
        # takes advantage of the fact that one NaN is not equivalent to
        # another NaN.
        #
        # Currently only == and != are supported.
        # In the == case the part of the query will be re-written as follows:
        # var == . becomes var != var
        # var != . becomes var == var.
        #
        # Notes: There is no attempt made to handle stata's extended
        # missing values.

        q = re.sub(self.query_not_missing_matcher,
                   self.query_not_missing_repl,
                   q)
        q = re.sub(self.query_missing_matcher,
                   self.query_missing_repl,
                   q)
        return q

    def _fix_case_problem_in_query(self, columns, query, err_msg):
        """
        Resolve issue with ubCov config allowing case-insensitive definitions.
        """
        # Get offending variable name or error
        match = self.query_err_matcher.match(err_msg)
        if match is None:
            msg = (f"Unexpected error handling query {query} - contact "
                   f"<USERNAME> and include error message {err_msg!r}")
            raise Error(msg)

        # Find case-correct variable name or error
        bad_case_name = match.group(1)
        column_name, _, err = get_column(bad_case_name, columns)
        if column_name is None:
            msg = (f"Could not locate column {bad_case_name} for subset "
                   "operation")
            raise ValidationError(msg)

        # Fix all occurrences in query
        self.logger.warning(f"case mismatch in subset - using {column_name} "
                            f"instead of {bad_case_name}")
        return query.replace(bad_case_name, column_name)
