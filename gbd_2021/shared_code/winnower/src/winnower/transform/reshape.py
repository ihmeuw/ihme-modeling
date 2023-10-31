#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set fileencoding=utf-8 :
"""
Code to reshape datasets.
"""
import collections
import itertools
import re
import logging

import pandas

from winnower.extract import get_column
from winnower import errors
from winnower.globals import eg  # Extraction context globals.
from winnower.logging import get_class_logger
from winnower.util.dataframe import (
    duplicate_record_indices,
    rows_with_nulls_mask,
)
from winnower.transform import Merge

from .base import Component

# For module level logging.
logger = logging.getLogger(__loader__.name)


def reshape2(df, *, pat=r'^(.+)_(\d+)$', under_delim=False, index_cols=[],
             keepid=None):
    """
    Reshape data set, likely from household-level to individual-level rows.

    IHME utilizes per-person data where possible. This transforms the source
    data sets with non-per-person data into something suitable.

    Args:
        df: the DataFrame to reshape.
    Keyword Args:
        pat: a str regular expression pattern OR a compiled regular expression.
        under_delim: a bool True indicates col parts are separated by _.
        index_cols: an array/Index like object which specifies the key columns.
        keepid: a str (reshape_keepid) indicating the reshape column that
                should be used to filter in reshaped rows if the keepid value
                for the row is not NaN.

    Returns the reshaped dataframe.

    reshape2 differs from reshape in several ways.

    1. It doesn't utilize the builtin Pandas vector operations that reshape
       uses and instead reshapes by renaming and appending leaf columns at the
       end of a temporary dataframe and then merging that dataframe with
       another temprorary dataframe which contains all of the non-reshaped
       columns. The merge is done on the index_cols provided. Doing a reshape
       in this fashion can be much quicker for data sets containing a large
       number of "stems" e.g. the columns that remain after the reshape.

    2. If keepid is provided then only those rows in the leaf columns which
       contain values for the reshape_keepid column are appended. This can
       greatly speed up the reshape operation especially with datasets that
       contain a large number of empty values in the leaf columns.

    3. If strict is provided then reshape2 will produce exactly the same result
       as ubcov would have when using the parallel reshape algorithm in
       reshape2.do. Specifically, strict mode allows the rename operation to
       fail silently when reshape2 attempts to rename a leaf column (to remove
       leading 0's) and a leaf column already exists with that name. For
       example if the data set contains a leaf column named stem1_01 and
       another named stem1_1 reshape2 would normally skip the rename of
       stem1_01 to stem1_1 because that column already exists. However, due to
       a bug in ubcov reshape2.do ubcov will fail silently on that rename and
       the data in stem1_01 would not appear in the reshaped dataframe.
    """
    # this is present in Reshape
    if isinstance(pat, str):
        matcher = re.compile(pat)
    else:
        matcher = pat
    assert matcher.groups == 2  # prefix, suffix (usually)

    # Used for recreating col names from pattern pieces.
    # TODO: Winnower currently treats the under_delim parameter as a bool.
    # If it is True then there is no underscore used between stem and leaf
    # otherwise there is. This may not be ideal behavior as it might be more
    # useful to allow for (and rename) under_delim to specifiy the character(s)
    # used to separate stem and leaf. For now reshape2 duplicates the existing
    # winnower behavior.
    delim = '' if under_delim else '_'

    # Helper function that returns stems, leaves and renamed df if keepid
    # If df.columns = ['foo_01', 'bar_02'], stems = {'foo', 'bar'} and
    # leaves = ['01', '02']
    def get_stems_leaves_and_cols_renamed_df(df, matcher, delim, keepid):
        tuples = tuple(X.groups() for X in map(matcher.match, df.columns) if X)
        # Stems are the first part of the match
        stems = set([x[0] for x in tuples])
        # while the leaf number is the second...
        # sorting is done to match ubcov and is not strictly necessary.
        leaves = sorted(set([x[1] for x in tuples]))

        if keepid is None:
            return stems, leaves, df

        # if keepid: rename the columns so that length of leaves are equal.
        # Find the longest leaf and rename all columns to have same length-
        # leaves by zero padding on the left.
        # e.g. rename 'foo_1' to 'foo_01', given the longest leaf-length is 2
        # This is required so that 'foo_01' is treated equal to 'foo_1' while
        # grouping columns that match to keepid column-leaves.
        maxlen = len(max(leaves, key=len))
        new_leaves = set()
        col_map = {}
        for tupl in tuples:
            col = tupl[0] + delim + tupl[1]
            new_leaf = tupl[1].zfill(maxlen)
            new_leaves.add(new_leaf)
            newcol = tupl[0] + delim + new_leaf
            if col != newcol:
                col_map.update({col: newcol})

        if col_map:
            df.rename(columns=col_map, inplace=True)
        return stems, sorted(new_leaves), df

    stems, leaves, df = get_stems_leaves_and_cols_renamed_df(
        df,
        matcher,
        delim,
        keepid)

    # Collect the columns that will not participate in the reshape. These will
    # be merged back into the dataframe at the end of the reshape process. If
    # know index_cols are provided then all of the norm_cols will be used as
    # indexes for the merge. This is not recommended as it could lead to a very
    # slow merge.
    norm_cols = pandas.Index([X for X in df.columns if not matcher.match(X)])

    if not index_cols:
        index_cols = norm_cols
    else:
        if not isinstance(index_cols, pandas.Index):
            # If the index_cols are not a pandas.Index then trust that the
            # user has provided a sane list like object that can be converted
            # to a pandas.Index.
            index_cols = pandas.Index(list(index_cols))
        # make sure the norm_cols include the index_cols so that the merge will
        # be successful.
        norm_cols = norm_cols.union(index_cols)

    all_cols = df.columns
    # Anything that is not a norm col must be a reshape col by definition.
    reshape_cols = all_cols.difference(norm_cols)

    # Our final reshaped data frame will contain these cols. The norm dataframe
    # will be merged back into this dataframe.
    reshaped_cols = pandas.Index(stems).union(index_cols)

    # keepid is a collection (currently only one keepid is supported) of
    # reshaped column names. The keepid will match the reshape column stem
    # rather than the entire column name i.e. keepid will be "child_age" if
    # stem+leaf column name is "child_age_01". It is used to drop rows with
    # NaNs, in the keepid column, during the reshape. The keepid case must
    # match the stem case for this reason the following step is performed to
    # coerce the keepid to match the reshape_col stem.
    if keepid:
        _keepid = [get_column(X, reshaped_cols.tolist())[0] for X in keepid]
        # verify that keepid is actually the name of a reshape column.
        # TODO: This validation work should be refactored into the Reshape
        # component. See LBDSE-451
        if _keepid[0] is None:
            msg = (f"reshape_keepid {keepid!r} could not be found in "
                   f"the reshaped_cols (stems) {reshaped_cols!r}")
            raise errors.ValidationError(msg)
        keepid = _keepid

    # Create our norm dataframe as a copy of the original dataframe but
    # including only the norm_cols.
    #
    # TODO: tolist becomes to_list in pandas version 0.25
    norm_df = df[norm_cols.tolist()].copy()

    # Helper function used to determine leaf column names from stem and leaf
    # number (leaf). In strict mode a check is made to see if renaming would
    # have failed i.e. a column already exists matching the renamed leaf
    # column.
    def to_leaf_name(cols, stem, leaf):
        leafn = stem + delim + str(leaf)
        if eg.strict:
            ileafn = stem + delim + str(int(leaf))
            if ileafn in cols and leafn != ileafn:
                msg = (f"STRICT: using leaf name {ileafn} "
                       "with preceding 0 removed.")
                logger.warning(msg)
                return ileafn

        leafn = stem + delim + str(leaf)
        return leafn

    # Helper function that checks if the leaf column exists. If it doesn't it
    # is not included in the current round of reshapes.
    def keep(cols, stem, leaf):
        leafn = to_leaf_name(cols, stem, leaf)

        if leafn in cols:
            return True

        return False

    # do the reshape ...
    reshaped_chunks = []
    for leaf in leaves:
        # leaf should be a string like '01', '02', '1', '10', '34', etc. They
        # will be sorted.

        # Build a leaf column to stem name map. Only include constructed leaf
        # column names that actually exist in the data set.
        stem_leaf_map = {to_leaf_name(reshape_cols, x, leaf): x for x in stems
                         if keep(reshape_cols, x, leaf)}

        # If there are no columns in this particular leaf round then continue
        # to the next round.
        if not stem_leaf_map:
            continue

        # Collect the columns to be appended. Keys will be the leaf column name
        # while values will be the stem names. Make sure the index cols are
        # included.
        to_append = index_cols.union(stem_leaf_map.keys())

        # Make a deep copy of these leaf columns ... This is done to prevent
        # changes to the original dataframe.
        #
        # TODO: is a deep copy necessary?
        append_df = df[to_append.tolist()].copy(deep=True)

        # Rename the leaf columns to their stem column name so that they are
        # correctly appended at the end of their stem columns in the
        # reshaped_df dataframe.
        append_df = append_df.rename(stem_leaf_map, axis='columns')

        # Use reshape_keepid to search for and drop rows that have NaN values
        # in the column that matches the value(s) in reshape_keepid config.
        #
        # TODO: keepid is a set but is forced to contain only one value ... is
        #       this correct?

        logger.debug(f"Reshape2 reshape_keepid {keepid}")
        if keepid:
            logger.debug(f"Reshape2 has a reshape_keepid")
            # If the keepid columns are not even present in this round of
            # reshapes then just skip ahead to the next leaf.
            if not set(keepid).issubset(append_df.columns.tolist()):
                continue

            # TODO: Is dropna sufficient to determine if the keepid column has
            # empty values?
            append_df.dropna(subset=list(keepid), inplace=True)

        # Add the reshapeid column, with the value of the current leaf, to the
        # the append_df dataframe.
        if len(append_df):
            append_df['reshapeid'] = pandas.Series(int(leaf), dtype='int64')
            reshaped_chunks.append(append_df)

    reshaped_df = pandas.concat(reshaped_chunks, sort=False)

    # Reshaping is done ... now just merge the norm cols back into our reshaped
    # dataframe.
    # Prepare the merge.
    master_cols = using_cols = index_cols.tolist()

    # Build the merge using the Merge transform ... this mimics the behavior of
    # a stata merge operation.
    merge = Merge.from_dataframe(norm_df, master_cols, using_cols)

    # Do the actual merge
    logger.debug("Reshape2 shape before merge {reshaped_df.shape}")
    merged = merge.execute(reshaped_df)
    logger.debug("Reshape2 shape after merge {reshaped_df.shape}")
    return merged


# TODO: unused but keeping for now in case ubCov's megastem reshape code is
# needed.
def reshape(df, *, pat=r'^(.+)_(\d+)$'):
    """
    Reshape data set, likely from household-level to individual-level rows.

    IHME utilizes per-person data where possible. This transforms the source
    data sets with non-per-person data into something suitable.

    Args:
        df: the DataFrame to reshape.
    Keyword Args:
        pat: a str regular expression pattern OR a compiled regular expression.

    Returns the reshaped dataframe.

    Credit to Kyle Heuton for a helpful response in manipulating pandas Index's
    with masterful precision.
    """
    # WARNING: no logic is present to ensure sanely grouped matches
    # this is present in Reshape
    if isinstance(pat, str):
        matcher = re.compile(pat)
    else:
        matcher = pat
    assert matcher.groups == 2  # prefix, suffix (usually)

    # 1 re-index the df on all normalized columns
    norm_cols = [X for X in df.columns if not matcher.match(X)]

    df.set_index(norm_cols, inplace=True)

    # 2 re-column the df on the pattern we're normalizing against
    df.columns = pandas.MultiIndex.from_tuples(
        # using the default `pat`, this will be
        # tuple(X.rsplit('_', 1) for X in df.columns)
        tuple(X.groups() for X in map(matcher.match, df.columns)))

    # 3 undo the last level of the column multi-index
    #   this multiplies the row count by len(df.columns.levels[-1]) and
    #   AND aligns the data.
    #
    #   This also creates a new level of index multi-index which is the
    #   second group in `pat`. This level of index is unnecessary...
    #
    # Note: -1 == last level only for stack() and reset_index()
    df = df.stack(level=-1)

    # 4 Remove the last level of index; convert to float and save as reshapeid
    df.reset_index(level=-1, inplace=True)
    # column added by reset_index uses procedurally generated name
    reshapeid_name = f"level_{len(df.index.levels)}"
    df['reshapeid'] = df.pop(reshapeid_name).astype(float)

    # 5 Remove all remaining indexes (`norm_cols`) but populate new columns
    #   with the values instead of dropping the data
    df.reset_index(inplace=True)

    return df


class Reshape(Component):
    """
    Performs a series of transforms to pivot a dataset to a new dimension.

    Classic example: reshaping from household-based data to person-based data.

    Args:
        id_columns: sequence of columns which define a unique row *before*
            pivoting the dataset.
    Optional Keyword Args:
        regex_pat: the regular expression pattern used to identify columns
            to be reshaped. Defaults to ".*_\\d+" and e.g., reshapes
            person_1, person_2, person_3 to several rows named person.
        under_delim (default: True) if False, regex_pat defaults to ".*?\\d+"
            (removes the underscore before the suffix digits).
        keepid: (default: None) an array of one (reshape_keepid) indicating the
                reshape column that should be used to filter in reshaped rows
                if the keepid value for the row is not NaN.
    """
    def __init__(self, id_columns,
                 *,
                 regex_pat=None,
                 under_delim=False,
                 keepid=None):
        self.id_columns = id_columns
        # Only used for default regex_pat; kept for introspection
        self.under_delim = under_delim
        self.keepid = keepid

        self.regex_pat = regex_pat  # Note: saved before mutation

        if regex_pat is None:
            regex_pat = r'^(.*?)(\d+)$' if under_delim else r'^(.*)_(\d+)$'
        elif not regex_pat.endswith('$'):
            # Add $ if not present (^ unnecessary as we're using re.match)
            regex_pat += '$'  # LOG?

        self.re = re.compile(regex_pat)

        self.logger = get_class_logger(self)
        self.column_renames = None  # populated in _validate
        self.input_columns = None  # populate in _validate

    def _validate(self, input_columns):
        if self.under_delim:
            raise NotImplementedError(
                f"Values for under_delim {self.under_delim} "
                "have not been tested.")

        # Validate reshape_keepid
        if self.keepid:
            # Note: all ubCov config has only a single column entered
            if len(self.keepid) > 1:
                msg = ("Provide only one column for reshape_keepid, not "
                       f"{self.keepid!r}")
                raise errors.ValidationError(msg)

        # Validate id_columns in input_columns
        missing_inputs = [X for X in self.id_columns if X not in input_columns]
        if missing_inputs:
            msg = f"Columns {missing_inputs} not in input data"
            raise errors.ValidationError(msg)

        # validate exactly 2 groups
        if not self.re.groups == 2:
            msg = (f"{self.regex_pat!r} defines {self.re.groups} groups. "
                   "Define exactly 2")
            raise errors.ValidationError(msg)

        matches = [X for X in map(self.re.match, input_columns)
                   if X is not None]
        if not any(matches):
            msg = f"{self.regex_pat} doesn't match any input columns"
            raise errors.ValidationError(msg)

        prefixed_groups = collections.defaultdict(list)
        for m in matches:
            prefix = self._get_prefix(m)
            prefixed_groups[prefix].append(m)

        length_groups = collections.defaultdict(list)
        for prefix, groups in prefixed_groups.items():
            length_groups[len(groups)].append(prefix)

        # if the groupings don't all have the same length error
        if len(set(length_groups)) != 1:
            most_common_len, supported_fields = max(length_groups.items(),
                             key=lambda item: len(item[1]))
            unsupported_fields = list(itertools.chain.from_iterable(
                fields for len_, fields in length_groups.items()
                if len_ != most_common_len))
            msg = ("The following fields will not reshape evenly, resulting "
                   "in potential data loss. Double check your outputs!. "
                   f"Effected fields are {unsupported_fields!r}")
            self.logger.error(msg)

        self.column_renames = prefixed_groups
        extra_columns = []
        extra_columns.extend(self.id_columns)
        for col in input_columns:
            m = self.re.match(col)
            if m:
                extra_columns.append(col)
        self.input_columns = extra_columns

    def get_uses_columns(self):
        return self.input_columns

    def output_columns(self, input_columns):
        output_cols = []
        reshaped_prefixes = {}  # used as an ordered set

        for col in input_columns:
            m = self.re.match(col)
            if m:
                prefix = self._get_prefix(m)
                reshaped_prefixes.setdefault(prefix, True)
            else:
                output_cols.append(col)
        output_cols.extend(reshaped_prefixes)
        output_cols.append('reshapeid')
        return output_cols

    def execute(self, df):
        os = df.shape  # old shape

        df = self._drop_records_with_missing_identifiers(df, self.id_columns)
        keep = 'first'
        if eg.strict:
            self.logger.warning("STRICT: All duplicate rows will be dropped, "
                                "even the first!!")
            keep = False
        df = self._drop_nonunique_records(df, self.id_columns, keep=keep)

        # reshape2 more closely matches ubCov's parallel reshape2.do method.
        res = reshape2(df, pat=self.re,
                       under_delim=self.under_delim,
                       index_cols=self.id_columns,
                       keepid=self.keepid)

        ns = res.shape  # new shape
        self._execute_metadata = {
            'action': self.__class__.__name__,
            'detail': (f"Reshaped from {os[1]} columns/{os[0]} rows to "
                       f"{ns[1]} columns/{ns[0]} rows")
        }
        return res

    def _get_prefix(self, re_match):
        # TODO: handled named patterns "prefix" and "suffix"
        # see self.re.groupindex (will not be empty iff named pats)
        return re_match.groups()[0]

    def _drop_records_with_missing_identifiers(self, df, id_columns):
        has_rows_with_nulls = rows_with_nulls_mask(id_columns, df)

        n = has_rows_with_nulls.sum()
        if n:
            msg = f"_drop_records_with_missing_identifiers - dropped {n} rows."
            self.logger.debug(msg)

        return df[~has_rows_with_nulls]

    def _drop_nonunique_records(self, df, id_columns, keep='first'):
        # foo = df.groupby(['col1', 'col2']).groups
        #   values are each an index of the index values that correspond to
        #   the key
        # >>> dfdup
        #           city   name state
        # 0    Shoreline   Mike    WA
        # 1  Wallingford  Geoff    WA
        # 2      Seattle   Erin    WA
        # 3      Seattle  Grant    WA
        #
        #
        # >>> pprint(dfdup.groupby(['city', 'state']).groups)
        # {('Seattle', 'WA')    : Int64Index([2, 3], dtype='int64'),
        #  ('Shoreline', 'WA')  : Int64Index([0], dtype='int64'),
        #  ('Wallingford', 'WA'): Int64Index([1], dtype='int64')}
        indexes_to_drop = duplicate_record_indices(id_columns, df, keep=keep)
        n = len(indexes_to_drop)
        if not n:
            return df

        self.logger.debug(f"(Reshape) dropped {n} duplicate records")
        return df.drop(indexes_to_drop)
