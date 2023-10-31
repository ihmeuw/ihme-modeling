import itertools

import numpy
import pandas
from pandas.core.indexing import is_list_like

from winnower import errors
from winnower.logging import get_class_logger
from winnower.util.categorical import (
    category_codes,
    update_with_compatible_categories,
)
from winnower.util.dtype import (
    is_categorical_dtype,
    is_numeric_dtype,
    is_string_dtype,
)
from winnower.util.stata import is_missing_str, is_missing_value


# Used in place of NaN values for groupby operations (NaN values break things)
def get_fill_str(series):
    vals = set(series.dropna())
    s = '__fill_str__'
    while s in vals:
        s += '_'
    return s


def get_fill_float(series):
    values = set(series.dropna())
    dtype_info = numpy.finfo(series.dtype)
    f = dtype_info.min  # minium possible value
    while f in values:
        f += dtype_info.resolution
    return f


def duplicate_record_indices(columns, df, keep="first"):
    """
    Returns a list of row index values representing duplicate records.

    All records after the first with identical values in `columns` are
    considered duplicates. The first record is considered canonical.

    If keep is "first" then only the first duplicate will be kept.
    If keep is "last" then only the last duplicate will be kept.
    Any other value for keep results in no duplicates being kept.
    """
    if isinstance(columns, tuple):  # pandas wants list of columns, not tuple
        columns = list(columns)

    # we can't do a groupby on `df` because NaN values can break things
    # instead create a cleaned version
    start = end = None
    if keep == 'first':
        start = 1
        end = None
    elif keep == 'last':
        start = None
        end = -1
    grouper = pandas.DataFrame(index=df.index)
    for col_name in columns:
        col = df[col_name]
        if pandas.isna(col).any():
            # fill NaN value with something legal and non-conflicting
            if is_numeric_dtype(col.dtype):
                fill_val = get_fill_float(col)
                grouper[col_name] = col.fillna(fill_val)
            elif is_categorical_dtype(col.dtype):
                # -1 is the internal NaN value (codes are integer) so this
                # can not accidentally conflict
                grouper[col_name] = category_codes(col, nan_val=-1)
            elif is_string_dtype(col.dtype):
                grouper[col_name] = col.fillna(get_fill_str(col))
            else:
                msg = f"No strategy for filling NaN values in {col.dtype}"
                raise errors.Error(msg, col.dtype)

        else:
            grouper[col_name] = col

    duplicate_indices = list(itertools.chain.
            from_iterable(
                indexes[start:end]
                for indexes in grouper.groupby(columns).groups.values()
                if len(indexes) > 1))

    return duplicate_indices


def as_column_type(column, values, use_category_dtype=False):
    """
    Converts `values` into a list of values of type specified in `column`.

    This is an input-handling function. In this case users input text into
    a spreadsheet. This text is split by delimiter and provided to this
    function as `values`.

    These values are meant to represent actual values present in a DataFrame
    somewhere. As a result we use the DataFrame's column's inferred type
    (based on actual values) to convert our input text into the appropriate
    type.
    """
    type_factory =\
        get_column_type_factory(column, use_category_dtype=use_category_dtype)

    return [type_factory(val) for val in values]


def get_column_type_factory(column: pandas.Series, *,
                            use_category_dtype=False):
    """
    Returns the type factory for a column.

    This function is intended for use in converting form input into the
    appropriate type for the column it is being used with.

    Args:
        column: the pandas Series to return the type factory for.

    Keyword Args:
        use_category_dtype (False): boolean indicating what to do if `column`
            is a categorical series. If True, return the dtype of the category
            labels (likely numpy.object_). If False, return the dtype of the
            category values (likely numpy.intXX).

            Under most circumstances the caller will prefer `False`.
    """
    dtype = column.dtype
    if is_categorical_dtype(dtype):
        # we have a Categorical series - more work is necessary
        # there is some level of ambiguity of whether the caller desires the
        # dtype of the underlying values OR of the category labels
        if use_category_dtype:
            type_factory = column.cat.categories.dtype.type
        else:
            type_factory = column.cat.codes.dtype.type
    else:
        type_factory = dtype.type

    if is_numeric_dtype(type_factory):
        # Add wrapper function to handle stata missing values.
        # A "missing" value in Stata is similar to a NaN value in Python with
        # the notable exception that 27 exist and they are ordered.
        # . is the generic missing, with .a thru .z representing additional
        # missing values that are implicitly ordered (.a is less than .z)
        #
        # We will treat all NaN values as NaN
        def type_factory_handling_missing(val):
            if is_missing_str(val):
                return float('NaN')
            else:
                return type_factory(val)

        return type_factory_handling_missing

    if is_string_dtype(type_factory):
        # Add wrapper function to handle paired quotation marks if present.
        # These are occasionally provided by end users though not required.
        def type_factory_handling_quotes(val):
            if (val.startswith("'") and val.endswith("'")) or \
                    (val.startswith('"') and val.endswith('"')):
                val = val[1:-1]
            return type_factory(val)
        return type_factory_handling_quotes

    return type_factory


def rows_with_nulls_mask(columns, df):
    """
    Returns DataFrame mask for `df`.

    mask is True for a row if any column in `columns` is null for that row.
    """
    if len(columns) == 1:
        return is_missing_value(df[columns[0]])

    mask = numpy.logical_or.reduce([is_missing_value(df[col])
                                    for col in columns])
    return pandas.Series(mask, index=df.index)


def set_default_values(column, value, df, mask=None):
    """
    Set default values for a column in a DataFrame. Create column if necessary.
    """
    if is_list_like(value) and len(value) != len(df):
        msg = (f"list-like value {value} is length {len(value)}. "
               f"DataFrame length is {len(df)}. Lengths must match.")
        raise errors.Error(msg)

    if isinstance(value, (list, tuple)):
        value = pandas.Series(value, index=df.index)

    if mask is None:
        mask = pandas.Series(True, index=df.index)

    if column in df.columns:
        null_mask = rows_with_nulls_mask([column], df) & mask
        if not isinstance(value, pandas.Series):
            value = pandas.Series(value, index=df.index)

        df = NaNUpdater.update(df, column, value, null_mask)
    else:  # column does not exist - add new column
        df[column] = value


class NaNUpdater:
    """
    Updates NaN values in one Series with another.

    Utilizes the strategy pattern to determine how to perform the update based
    on the dtype (data type) of the values being updated and the defaults.

    Take care to provide a `df` and `default_values` with matching DataFrame
    index values or your results will be incorrect. See also
    tests/util/test_dataframe.py
    """
    @classmethod
    def update(cls, df, column, default_values, mask):
        "Main API method."
        nu = cls(df, column, default_values, mask)
        try:
            return nu.execute()
        except pandas.core.indexing.IndexingError:
            msg = "index for Series does not match DataFrame"
            raise errors.Error(msg)

    def __init__(self, df, column, default_values, mask):
        self.logger = get_class_logger(self)
        self.df = df
        self.column = column
        self.defaults = default_values
        self.mask = mask

    def execute(self):
        "Pick a strategy and return the result."
        strategy = self.select_strategy()
        return strategy()

    def select_strategy(self):
        base_is_cat = is_categorical_dtype(self.values.dtype)
        defaults_is_cat = is_categorical_dtype(self.defaults.dtype)

        if not base_is_cat and not defaults_is_cat:  # no categoricals
            return self.update_with_scalars
        elif base_is_cat and defaults_is_cat:
            return self.consolidate_categories_and_update  # all categoricals
        elif not base_is_cat and defaults_is_cat:
            return self.update_scalars_with_categoricals
        else:  # base_is_cat and not defaults_is_cat
            return self.update_categoricals_with_scalars

    def update_with_scalars(self):
        self.df.loc[self.mask, self.column] = self.defaults[self.mask]
        return self.df

    def update_scalars_with_categoricals(self):
        """
        Update scalar NULLs with categorical values.

        This is a minor variant of update_with_scalars - one merely has to
        determine whether to use the codes or the values.
        """
        defaults = self.defaults[self.mask]
        if is_numeric_dtype(self.df[self.column]):
            defaults = category_codes(defaults)
        self.df.loc[self.mask, self.column] = defaults
        return self.df

    def update_categoricals_with_scalars(self):
        scr = update_with_compatible_categories(self.values, self.defaults)
        self.df[self.column] = scr.left
        self.df.loc[self.mask, self.column] = scr.right[self.mask]
        return self.df

    def consolidate_categories_and_update(self):
        # create CategoricalDtype from the intersection

        # Stata's behavior is to only use the base data set's labels. This
        # takes advantage of the fact that in Stata value labels do not need
        # to include a label for every value

        # Pandas does not allow for partially labeled data in a Categorical -
        # it NaN's all unlabeled values

        # In addition, for simplicity and because it likely results in a
        # higher quality data set, use behavior resembling Stata merge with
        # update.

        # This means that, for columns existing in BOTH data sets, NaN values
        # in our base data will be replaced with the corresponding values in
        # our default data
        base, defaults = self._set_identical_categoricals(
            self.values, self.defaults)

        base.loc[self.mask] = defaults[self.mask]
        self.df[self.column] = base
        return self.df

    def _set_identical_categoricals(self, left, right):
        self.logger.debug("Consolidate categories and update")
        scr = update_with_compatible_categories(left, right)
        self.logger.debug(f"Categorical superset {scr.categories}.")
        return scr.left, scr.right

    @property
    def values(self):
        "Convenience method to provide symmetry with self.defaults."
        return self.df[self.column]
