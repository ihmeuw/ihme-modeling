#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8 :
"""
Logic to merge (join) data sets.
"""
from attr import attrib
import pandas
import logging

from winnower import errors
from winnower.extract import (
    ExtractionChainBuilder,
    ValidatedExtractionChain,
)
from winnower.sources import (
    FileSource,
    DataFrameSource,
)
from winnower.util.categorical import (
    category_codes,
    update_with_compatible_categories,
    downcast_float_to_integer,
)
from winnower.util.dataframe import (
    duplicate_record_indices,
    rows_with_nulls_mask,
    set_default_values,
)
from winnower.util.path import resolve_path_case
from winnower.util.dtype import (
    is_categorical_dtype,
    is_numeric_dtype,
    is_string_dtype,
    is_float_dtype,
)
from winnower.util.stata import MISSING_STR_VALUE

from .base import (
    Component,

    attrs_with_logger,
)

from .components import MapNumericValues
from winnower.globals import eg

# For module level logging.
logger = logging.getLogger(__loader__.name)

# TODO: support reshape arg


@attrs_with_logger
class Merge(Component):
    """
    Merges two data sets together.

    Necessary for all surveys having multiple modules (files) necessary for
    logical processing.

    Args:
        right_chain: ExtractionChain representing the dataset to merge with.
        left_cols: columns in the left dataset which will be used in the merge
            operation.
        right_cols: columns in the right dataset which will be used in the
            merge operation.
        type: the type of merge. One of ('1:1', '1:m', 'm:1', 'm:m').
        keep: tuple of strs values in '123'. Each indicates a type of resulting
            merge record to keep.
            1: records from the left dataset only.
            2: records from the right dataset only.
            3: records from both datasets.

            keep=('1', '3') would keep all records from the left dataset
            (provided in .execute()). keep=('3',) would only keep records from
            the left dataset that successfully merged with data from the right
            dataset.
        drop_threshold: maximum percentage of records that can be automatically
            dropped due to non-uniqueness without throwing an error (1 = 100%).

            Default value of 0.05 (5%).

            On join operations with a "1" value ("1:1", "1:m", "m:1") the
            dataset(s) with a "1" value are pre-processed to remove all rows
            with non-unique combinations of the selected columns (either
            left_cols or right_cols).

            If the routine would drop more than drop_threshold's worth of data
            Merge will instead error.
        merged_file: optional description of the file that was merged with.
            Defaults to "unspecified data source".
    """
    right_chain = attrib()
    left_cols = attrib()
    right_cols = attrib()
    type = attrib()
    keep = attrib()
    column_labels = attrib()
    drop_threshold = attrib(default=0.05)
    merged_file = attrib(default='unspecified data source')

    merge_ind = 'merge_type'
    keep_trans = {
        '1': 'left_only',
        '2': 'right_only',
        '3': 'both',
    }

    @classmethod
    def from_chain_and_config(cls, chain, config, columns_to_label=()):
        # Use this to instantiate for traditional ubcov merges.
        left_cols = [chain.get_input_column(X) for X in config.master_vars]
        logger.debug(f"Building merge transform from chain and config "
                     f"for {config.merge_file}")

        logger.debug(f"master_vars {config.master_vars}")
        logger.debug(f"left_cols {left_cols}")

        right_chain_builder = cls._get_merge_source(
            file_path=config.merge_file,
            reshape=config.reshape)

        value_labels = right_chain_builder.source.get_value_labels()
        MapNumericValues.if_present(columns_to_label,
                                    value_labels,
                                    right_chain_builder)

        right_cols = [right_chain_builder.get_input_column(X)
                      for X in config.using_vars]

        logger.debug(f"using_vars {config.using_vars}")
        logger.debug(f"right_cols {right_cols}")

        column_labels = right_chain_builder.source.get_column_labels()
        # adding column_labels to the Merge object so they can be used later
        # if the topic calls for categ_multi_bin
        return cls(merged_file=config.merge_file,
                   right_chain=right_chain_builder.get_chain(),
                   type=config.type,
                   keep=config.keep,
                   column_labels=column_labels,
                   left_cols=left_cols,
                   right_cols=right_cols)

    @classmethod
    def from_dataframe(cls, right_df,
                       master_cols, using_cols,
                       merge_type="m:1", merge_keep=None,
                       reshape=False, columns_to_label=()):

        # Use this to instantiate from a dataframe
        logger.debug(f"Building merge transform from dataframe.")
        logger.debug(f"left_cols {master_cols}")
        logger.debug(f"right_cols {using_cols}")

        right_chain_builder = cls._get_merge_source_from_dataframe(
            right_df,
            keep=merge_keep,
            reshape=reshape)

        value_labels = right_chain_builder.source.get_value_labels()
        MapNumericValues.if_present(columns_to_label,
                                    value_labels,
                                    right_chain_builder)

        column_labels = right_chain_builder.source.get_column_labels()

        # adding column_labels to the Merge object so they can be used later
        # if the topic calls for categ_multi_bin
        return cls(merged_file=None,
                   right_chain=right_chain_builder.get_chain(),
                   type=merge_type,
                   keep=merge_keep,
                   column_labels=column_labels,
                   left_cols=master_cols,
                   right_cols=using_cols)

    def _validate(self, input_columns):
        # column validation done in from_chain_and_config
        bad_keep = [X for X in self.keep if X not in self.keep_trans]
        if bad_keep:
            raise errors.ValidationError(f"Keep values {bad_keep} unsupported")

        if len(self.left_cols) != len(self.right_cols):
            msg = (f"Left merge columns {self.left_cols} not the same length "
                   f"as Right merge columns {self.right_cols}")
            raise errors.ValidationError(msg)

        miss_left = [X for X in self.left_cols if X not in input_columns]
        if miss_left:
            msg = f"Left merge columns {miss_left} not present!"
            raise errors.ValidationError(msg)

        right_output_cols = frozenset(self.right_chain.output_columns())
        miss_right = [X for X in self.right_cols if X not in right_output_cols]
        if miss_right:
            msg = f"Right merge columns {miss_right} not present!"
            raise errors.ValidationError(msg)

    def get_uses_columns(self):
        return self.left_cols

    def output_columns(self, input_columns):
        # Often there are common columns - do not accidentally include those
        # This also checks for columns that would match if they were
        # lower-cased.
        merge_cols = set(self.right_chain.output_columns())
        merge_cols_case = {x.lower(): x for x in merge_cols}
        # Get a set containing only lower case input column names.
        input_cols_case = {x.lower() for x in input_columns}
        for col in merge_cols_case.keys():
            if col not in input_cols_case:
                input_columns.append(merge_cols_case[col])

        return input_columns

    def execute(self, df):
        """
        Merges left and right data frames.
        """

        # Ubcov and Winnower perform m:m merges in a fundementally
        # different manner. This is a result of a fundemental difference
        # between stata and pandas.
        #
        # Ubcov performs m:m merges in the following way:
        # First, ubcov ensures that the matching columns for the left(master)
        # and right(using) df are equivalent. If they are not ubcov will
        # generate copies of the left cols in right.
        #
        # Second, ubcov uses stata's merge method to merge the two dfs. Stata
        # starts by finding the first match in left and then merging it with
        # the first match in right if there is no match in right then the right
        # vars are left empty. Next stata matches the second match in left with
        # the second match in right. If there is no second match but there is
        # a first match (for the matching cols) then the first match in right
        # will be used for any additional matches in left. This means that the
        # count of rows (for a particular match in left) will never exceed the
        # original count of rows in left for that particular match.
        #
        # On the other hand winnower uses pandas' merge function with how
        # defined as outer (a full outer join). Unlike stata this merge is
        # a true full outer join and so every match in left is merged with
        # every match in right resulting in a row count for resulting merged
        # matched rows of <number of left matches> * <number of right matches>.
        #
        # The end result is that there is the possibility of winnower and ubcov
        # creating results with different row counts.
        #
        # NOTES: There are currently only seven m:m merges in the codebooks and
        # so this difference is unlikely to be an issue. Additionally, it seems
        # that it is likely that the user did not really want a m:m merge in
        # the first place and probably wanted just a m:1 or 1:m where either
        # the left or right df had any duplicates dropped. If a true m:m merge
        # was wanted in ubcov then `joinby` would be the better choice as it
        # does a true full outer join.
        #
        # SEE: <ADDRESS>

        left = df
        right = self.right_chain.execute()

        # DROP ANY missing rows in right with missing values in right_cols
        right = self._drop_missing_values(self.right_cols, right)

        # check that {left,right}_cols define a unique row
        l, _, r = self.type
        if l == '1':  # noqa
            left = self._drop_duplicate_rows(
                self.drop_threshold, self.left_cols, left, 'master')

        if r == '1':
            right = self._drop_duplicate_rows(
                self.drop_threshold, self.right_cols, right, 'using')

        self.logger.debug("Removing categorical labels")
        col_categories = self._remove_categorical_labels(
            left, right, self.left_cols, self.right_cols)

        self._ensure_compatible_merge_dtypes(left, self.left_cols,
                                             right, self.right_cols)

        self.logger.debug("Doing Pandas merge")
        joined = pandas.merge(
            left, right,
            left_on=self.left_cols,
            right_on=self.right_cols,
            how='outer',
            suffixes=('', '_MERGE_CONFLICT'),
            indicator=self.merge_ind)

        # emulate Stata defaults on merge - set NaN values to "" for str cols
        for col_name in joined:
            if is_string_dtype(joined[col_name]) and not \
                    is_categorical_dtype(joined[col_name]):
                self.logger.debug(f"{col_name} is string dtype.")
                self.logger.debug("Setting NaN to ''")
                set_default_values(col_name, MISSING_STR_VALUE, joined)

        # Fill in all missing data from left columns with available data
        # left_is_nan is used to emulate Stata merge *without* update
        left_is_nan = joined[self.merge_ind] == 'right_only'
        self.logger.debug("Checking for _MERGE_CONFLICT columns.")
        for col in joined.columns:
            if col.endswith('_MERGE_CONFLICT'):
                if eg.more_debug:
                    # Using more_debug as the following can flood the log.
                    self.logger.debug(f"Resolving merge conflict for {col}")
                # STATA-style merge/update of NaN values
                # '_MERGE_CONFLICT' is 15 characters
                set_default_values(col[:-15], joined[col], joined, left_is_nan)
                # Removing this replicated STATA behavior, but it would
                # probably be better to keep it around ...
                del joined[col]

        # Fill in missing values in left merge columns with values from right
        # This is for backwards compatibility as STATA can only merge data sets
        # on identically named merge keys.
        # Alternatively, one could rename the columns before pandas.merge and
        # let the above loop do the filling.
        # This method preserves more information during processing by keeping
        # the original columns. It is hoped that this will aid in debugging,
        # should this ever come up
        for lcol, rcol in zip(self.left_cols, self.right_cols):
            if lcol != rcol:
                set_default_values(lcol, joined[rcol], joined)

        # Fill in missing values in left merge columns with values from right
        # This is for backwards compatibility with ubcov lowercasing left and
        # right column labels. A merge conflict would have arisen for these
        # columns if winnower had followed ubcov's convention of lowercasing
        # all columns labels upon loading of the data but because this doesn't
        # happen a check is done after the merge to update any of the left cols
        # whose labels would have matched if they were lowercase.
        case_cols = {X.lower(): [X] for X in left.columns
                     if X not in self.left_cols}
        for col in right.columns:
            if col not in self.right_cols and col.lower() in case_cols.keys():
                case_cols[col.lower()].append(col)
        for left_col, right_col in [X for X in case_cols.values()
                                    if len(X) > 1 and X[0] != X[1]]:
            self.logger.debug(f"Resolving merge conflict between {left_col} "
                              f"and {right_col}.")
            set_default_values(left_col, joined[right_col], joined)
            # remove the right_col so that it isn't picked up as an indicator
            del joined[right_col]

        joined = self._filter_records_with_keep(self.keep, joined)
        self.logger.info(f"Merged file {self.merged_file}")

        self.logger.debug("Restoring categorical labels.")
        if col_categories:
            self._add_categorical_labels(col_categories, joined)

        return joined

    @classmethod
    def _get_merge_source(cls, file_path, reshape):
        # somewhat repeats UbcovExtractor.get_source
        source = FileSource(resolve_path_case(file_path))
        chain = ValidatedExtractionChain(source)
        builder = ExtractionChainBuilder(chain)
        if reshape:
            raise NotImplementedError("reshape not yet implemented for merge")
        return builder

    @classmethod
    def _get_merge_source_from_dataframe(cls, dataframe,
                                         keep=None, reshape=False):

        # Uses a dataframe as a source rather than a file path.
        source = DataFrameSource(dataframe, keep=keep)
        chain = ValidatedExtractionChain(source)
        builder = ExtractionChainBuilder(chain)
        if reshape:
            raise NotImplementedError("reshape not yet implemented for merge")
        return builder

    def _drop_missing_values(self, right_cols, right):
        right_rows_missing_values = rows_with_nulls_mask(right_cols, right)
        # We want True values, not all values
        n_right_rows_missing = right_rows_missing_values.sum()
        if n_right_rows_missing:
            self.logger.warning(
                f"Dropping {n_right_rows_missing} obs in using that are "
                f"missing one or more values in columns {right_cols}")
            right = right[~right_rows_missing_values]

        return right

    def _drop_duplicate_rows(self, drop_threshold, columns, df, label):
        """
        Returns df with only rows that are unique for the values in `columns`.

        Alternatively, if the percentage of rows which aren't unique exceeds
        drop_threshold, error instead.
        """
        duplicate_indices = duplicate_record_indices(columns, df)
        n = len(duplicate_indices)
        if n == 0:
            return df

        pct = len(duplicate_indices) / len(df)
        if pct <= drop_threshold:
            df = df.drop(duplicate_indices)
            self.logger.warning(f"Dropping {n} duplicate records in {label} "
                                "that aren't uniquely identifying "
                                f"out of {len(df)} obs")
        else:
            msg = (f"Merge columns in {label} aren't uniquely identifying. "
                   f"{n} duplicates out of {len(df)} obs")

            self.logger.error(msg)
            raise errors.Error(msg)

        return df

    def _ensure_compatible_merge_dtypes(self, left, lcols, right, rcols):
        for lcol, rcol in zip(lcols, rcols):
            l_dtype = left[lcol].dtype
            r_series = right[rcol]

            l_is_numeric = is_numeric_dtype(l_dtype)
            r_is_numeric = is_numeric_dtype(r_series.dtype)

            if l_is_numeric == r_is_numeric:
                continue

            # original implementation coerces `right` dtype to match `left`
            # checks only numeric/non-numeric status and coerces so that both
            # match `left`
            #
            # Unfortunately STATA (the language of the original implementation)
            # can tell that '0020103154' == '20103154' but Python cannot
            #
            # Instead, if either is numeric then make both numeric

            # Fundamentally this just attempts to confirm both colums are
            # EITHER numeric OR not numeric
            #
            # This is not smart enough. A categorical column is, basically,
            # both.
            #
            # If the non-categorical column is numeric, convert it to
            # a categorical and merge
            #
            # if the non-categorical column is non-numeric
            try:
                if not l_is_numeric:
                    left[lcol] = pandas.to_numeric(left[lcol])
                else:  # implies `not r_is_numeric`
                    right[rcol] = pandas.to_numeric(r_series)
            except Exception as e:
                msg = (f'Error in coercing {rcol!r} (dtype={r_series.dtype}) '
                       f'to match type of {lcol!r} (dtype={l_dtype}). '
                       f'Additional debug: l_is_numeric is {l_is_numeric}. '
                       f'Full error: {e}')
                self.logger.error(msg)

                emsg = ('Error in Merge when preparing to join columns '
                        f'{lcol!r} and {rcol!r}. Log message: {msg}')
                raise errors.Error(emsg)

    def _filter_records_with_keep(self, keep, joined):
        merge_mask = joined.pop(self.merge_ind)

        if not keep:
            return joined

        kept_merge_indicators = [self.keep_trans[X] for X in keep]
        mask = merge_mask.isin(kept_merge_indicators)
        return joined[mask]

    def _remove_categorical_labels(self, left, right, left_cols, right_cols):
        column_categories = {}

        for lcol, rcol in zip(left_cols, right_cols):
            l_series = left[lcol]
            r_series = right[rcol]
            l_is_cat = is_categorical_dtype(l_series.dtype)
            r_is_cat = is_categorical_dtype(r_series.dtype)

            if l_is_cat or r_is_cat:
                # scr: StandardizedCategoryResult
                scr = update_with_compatible_categories(l_series, r_series)
                left[lcol] = category_codes(scr.left, nan_val=-1)
                right[rcol] = category_codes(scr.right, nan_val=-1)
                # on merge left column name is kept/will need updating
                # in all cases update the result to a categorical
                column_categories[lcol] = scr.categories

        return column_categories

    def _add_categorical_labels(self, col_categories, joined):
        # Re-categorize any categoricals that needed to be converted for merge
        for col_name, categories in col_categories.items():
            # If series is float, try downcasting to integer
            if is_float_dtype(joined[col_name]):
                joined[col_name] = downcast_float_to_integer(
                    joined[col_name])
            categorized = pandas.Categorical.from_codes(
                joined[col_name], categories, ordered=True)
            joined[col_name] = categorized
