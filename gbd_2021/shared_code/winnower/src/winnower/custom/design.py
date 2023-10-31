"""
Custom code module - a port of design.do

<FILEPATH>
"""
import collections
import datetime as dt
from functools import reduce
import operator

import numpy
import pandas

from winnower import constants
from winnower import errors
from winnower.util.categorical import category_codes
from winnower.util.dtype import (
    is_categorical_dtype,
    is_numeric_dtype,
)
from winnower.util.dates import datetime_to_numeric
from winnower.util.stata import MISSING_STR_VALUE, group
from .base import TransformBase

from winnower.globals import eg  # Extraction context globals


class Transform(TransformBase):
    """
    Replicates logic in design.do

    Note: due to the logic of processing there is an unintuitive behavior
    present in three functions which may appear incorrect. These functions are:

        _gen_unique_group_ids
        _copy_or_group_indicator
        _handle_weights

    In all three cases the functions have a related pattern of logic.

    * CASE 1 - not configured. Return immediately
    * CASE 2 - single value - already copied to df under key ind_name in the
               UbcovExtractor.apply_vars(). Logic uses `ind_name`
    * Case 3 - multiple values. Not copied in UbcovExtractor.apply_vars().
               Logic manually retrieves the values.

    Cases 2 and 3 utilize different methods of accessing the data. The author
    judged the functions were clearer as written, so this quirk is documented
    once here instead.
    """
    INDICATORS = ('strata', 'psu', 'geospatial_id', 'pweight', 'hhweight',
                  'hh_id', 'line_id')

    uses_extra_columns = None

    def output_columns(self, input_columns):
        """
        Appends new columns to input_columns and returns it when new
        columns are generated during transform
        """
        for col in self.INDICATORS:
            if self.config[col]:
                input_columns.append(col)

        # specially handled
        if self.config['psu'] and not self.config['geospatial_id']:
            input_columns.append('geospatial_id')

        return input_columns

    def validate(self, input_columns):
        """
        Validates that the transform can actually be performed.
        """
        extra_columns = []
        for indicator in self.INDICATORS:
            fixed_columns = []
            if self.config[indicator]:
                for in_col in self.config[indicator]:
                    try:
                        fixed = self.get_column_name(in_col)
                    except errors.ValidationError:
                        msg = (f"Column {in_col!r} for indicator {indicator!r} "  # noqa
                               f"not present in data: {self.columns}")
                        raise errors.ValidationError(msg) from None
                    else:
                        fixed_columns.append(fixed)
                        extra_columns.append(fixed)
                self.config[indicator] = tuple(fixed_columns)

        # Indicators could be named after columns, so add them
        # to extra columns if they are
        extra_columns.extend(X for X in self.INDICATORS if X in self.columns)
        self.uses_extra_columns = extra_columns

    def execute(self, df):
        # Design Vars
        df = self._gen_unique_group_ids('strata', df)
        df = self._gen_unique_group_ids('psu', df)
        df = self._copy_or_group_indicator('geospatial_id', df)

        for weight in ('pweight', 'hhweight'):
            df = self._handle_weights(weight, df)

        # geospatial id special case
        if self.config['psu'] and not self.config['geospatial_id']:
            df = self._copy_input_to_output_id(df, input_col_name='psu', output_col_name='geospatial_id')

        # psu_id special case
        if self.config['psu']:
            df = self._copy_input_to_output_id(df, input_col_name='psu', output_col_name='psu_id')

        # strata_id special case
        if self.config['strata']:
            df = self._copy_input_to_output_id(df, input_col_name='strata', output_col_name='strata_id')

        # household and line id
        df = self._copy_or_group_indicator('hh_id', df)
        df = self._copy_or_group_indicator('line_id', df)

        # PSU remapping
        if self.config['strata'] and self.config['psu']:
            df = self._psu_check(df)

        return df

    def _gen_unique_group_ids(self, ind_name, df):
        """
        Populates `ind_name` column given `ind_input` IFF multiple input cols.

        Method covers three logical branches:
        * indicator is unconfigured: no-op.
        * indicator is configured with a single input column:
            IF   column is numeric: assign values directly
            ELIF values can be losslessly converted to numbers (e.g., "2"),
                 assign converted values
            ELSE create a mapping of input values to output integers.
        * indicator is configured with two or more input columns: create a
            mapping of input values to output integers.
        """
        ind_input = self.config[ind_name]
        if not ind_input:
            return df

        if len(ind_input) == 1:  # single value
            input_name = ind_input[0]
            if input_name not in df.columns:
                raise errors.NotFound(f"{input_name} not found")

            df[ind_name] = df[input_name]
            # convert to a number if necessary
            self._ensure_numeric(ind_name, df)
            return df

        # 2+ values
        df[ind_name] = group(df, list(ind_input))

        return df

    def _copy_or_group_indicator(self, ind_name, df):
        """
        Populates indicator `ind_name` with concatenation of `ind_input`.
        """
        ind_input = self.config[ind_name]
        if not ind_input:
            return df

        if len(ind_input) == 1:
            column_name = ind_input[0]
            if column_name not in df.columns:
                raise errors.NotFound(f"{column_name} not found")

            column = df[column_name]

            # Ensure values are numeric IFF categoricals
            if is_categorical_dtype(column.dtype):
                self.logger.debug(f"ind_name {ind_name} is categorical")
                df[ind_name] = category_codes(column)
            else:
                # special case - geospatial_id should be numeric if possible
                if ind_name == 'geospatial_id':
                    column, = strip_leading_zeros([column])
                df[ind_name] = column
            return df

        # 2+
        columns = [df[column_name] for column_name in ind_input]
        if ind_name == 'geospatial_id':
            columns = strip_leading_zeros(columns)
        df[ind_name] = self._concat_columns(columns)
        return df

    def _concat_columns(self, columns):
        cleaned = []
        for column in columns:
            try:
                # Special case for categorical data. Categorical data handles
                # NaN values differently than regular series. Notably,
                # categorical_series.map does NOT run the map function on NaNs
                # and instead returns a new categorical series with NaNs in all
                # the places they were before.
                if is_categorical_dtype(column.dtype):
                    column = category_codes(column)
                s_column = column.map(self._to_str)
            except ValueError as e:
                msg = f"{e.args[0]} while converting column {column.name}"
                raise errors.Error(msg) from None
            else:
                cleaned.append(s_column)
        return ['_'.join(vals) for vals in zip(*cleaned)]

    def _to_str(self, val):
        # Return '.' for NaN values - consistent with STATA ubCov
        if pandas.isnull(val):
            return constants.MISSING_STR
        # Remove '.0' from result if possible
        if hasattr(val, 'is_integer') and val.is_integer():
            val = int(val)
        elif isinstance(val, (dt.datetime, pandas.Timestamp)):
            val = datetime_to_numeric(val)
        return str(val)

    def _handle_weights(self, ind_name, df):
        ind_input = self.config[ind_name]
        if not ind_input:
            return df

        if len(ind_input) == 1:
            input_name = ind_input[0]
            if input_name not in df.columns:
                raise errors.NotFound(f"{input_name} not found")

            df[ind_name] = df[input_name]
            # convert to a number if necessary
            self._ensure_numeric(ind_name, df)
            return df

        # 2+ - multiply weights together
        for col_name in ind_input:
            self._ensure_numeric(col_name, df)

        df[ind_name] = reduce(operator.mul, (df[col] for col in ind_input))
        return df

    def _copy_input_to_output_id(self, df, input_col_name='psu', output_col_name='geospatial_id'):
        input_config = self.config[input_col_name]

        if len(input_config) == 1:
            df[output_col_name] = df[input_col_name]
        else:
            columns = [df[col_name] for col_name in input_config]
            columns = strip_leading_zeros(columns)
            df[output_col_name] = self._concat_columns(columns)
        return df

    def _psu_check(self, df):
        holder = collections.defaultdict(set)

        for psu, strata in df[['psu', 'strata']].itertuples(index=False):
            if pandas.isnull(psu):
                continue  # don't remap NaN psu
            holder[psu].add(strata)

        psus_to_remap = {psu for psu, stratas in holder.items()
                         if len(stratas) > 1}

        if psus_to_remap:
            df = self._remap_psu(df, psus_to_remap)

        return df

    def _remap_psu(self, df, psus_to_remap):
        """
        Remaps the 'psu' value to match the most common strata for that psu.

        This is an error handling mechanism.
        """

        # If true enables detailed debugging messages.
        if eg.more_debug:
            _counts = df['strata'].value_counts()
            self.logger.debug(f"strata counts before reshape {_counts}")

        psus = df['psu']
        for psu in psus_to_remap:

            mask = psus == psu
            # Counts is now a series where the index corresponds to a psu value
            # and the value is the # of occurrences. Drops NaNs by default
            # TODO: df.loc[mask, 'strata'] ?
            counts = df[mask]['strata'].value_counts()

            # If true enables detailed debugging messages.
            if eg.more_debug:
                self.logger.debug(f"Current psu {psu}")
                _counts = df[mask]['strata'].value_counts(dropna=False)
                self.logger.debug(f"strata counts (mask) {_counts}")
                _counts = df['strata'].value_counts(dropna=False)
                self.logger.debug(f"strata counts (all) {_counts}")

            # by default the highest index (highest PSU value) will be selected
            # in the case where multiple psu values have the same max count.
            # ubcov defaults to the *lowest* index.
            # sort the index (psu value) for backwards compatibility
            # Note that this does not always work, as winnower does not yet
            # reliably keep data sorted the same as ubCov.
            scounts = counts.sort_index(ascending=True)
            try:
                best = scounts.idxmax()
            except ValueError:
                # all values were NaN. ubCov would error in this condition
                # log and continue (can't sanely remap anyway)
                msg = (f"All NaN values encountered when remapping psu {psu} "
                       "SOMETHING IS LIKELY VERY WRONG")
                self.logger.error(msg)
                continue
            else:
                max_count = scounts.loc[best]
                max_vals = counts.index[counts == max_count].tolist()
                if len(max_vals) > 1:
                    msg = (f"strata values {max_vals} tied for most frequent "
                           f"in psu {psu}. Selecting {best} to be used.")
                    self.logger.warning(msg)

            df.loc[mask, 'strata'] = best
            # `best` is the most common psu value.
            # `counts[best]` is the number of occurrences
            pct_obs = counts[best] / counts.sum()
            msg = (f"Remapping psu {psu} to strata {best} "
                   f"({pct_obs:.2f} of observations)")
            self.logger.info(msg)
        return df

    def _ensure_numeric(self, column_name, df):
        # TODO: after Continuous.execute is refactored to use not-yet-written
        # method destring consider using that method here. BE SURE TO TEST
        column = df[column_name]
        if is_numeric_dtype(column.dtype):
            return
        elif is_categorical_dtype(column.dtype):
            df[column_name] = category_codes(column)
            return

        try:
            column_as_num = pandas.to_numeric(column, errors='raise')
        except:  # noqa
            pass
        else:
            df[column_name] = column_as_num
            return

        # Manually convert
        # python3 - dicts are iterated on in order of insert.
        # thus list(dict.fromkeys(..)) will return unique items in order
        # this forces pandas.Categorical to use this order (default is to
        # use sorted order)
        values = column.replace({MISSING_STR_VALUE: numpy.nan}).dropna()
        dtype = pandas.api.types.CategoricalDtype(values.unique())
        codes = column.astype(dtype).cat.codes
        # codes are integer dtype by default AND 0-indexed
        # the code `-1` is assigned to unknown values (NaNs, if present)
        # Convert to using NaN's and 1-indexed codes
        df[column_name] = pandas.Series(codes.replace({-1: numpy.nan}) + 1,
                                        index=column.index)


def strip_leading_zeros(columns):
    """
    Handles special case of ubCov processing.

    Removes leading 0s from all columns where possible.
    """
    for i in range(len(columns)):
        col = columns[i]
        if not is_numeric_dtype(col):
            try:
                col = pandas.to_numeric(col, errors='raise')
            except Exception:
                pass
            else:
                columns[i] = col
    return columns
