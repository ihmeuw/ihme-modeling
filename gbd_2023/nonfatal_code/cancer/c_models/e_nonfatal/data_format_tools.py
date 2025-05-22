# -*- coding: utf-8 -*-

'''
Name of Module: data_format_tools.py
Contents:
    collapse              (like STATA collapse)
    force_drop_columns    (drop columns that may not exist)
    long_to_wide          (like STATA reshape long)
    make_group_id_col  (like STATA egen group)
    wide_to_long          (like STATA reshape wide)

Description: Contains useful functions for data formatting, including
             python versions of common STATA commands.
Arguments: N/A -- to use, import cancer_prep.utils.data_format_tools at the
                  top of your file with the rest of your import statement
Output: N/A
Contributors: INDIVIDUAL_NAME
'''
import pandas as pd
import numpy as np
import re
from functools import reduce
import glob



def collapse(df, by_cols=None, func='sum', combine_cols=None, stub=None, keepNA = False):
    ''' Convenience function for STATA-like collapsing. Like STATA, removes
    any columns not specified in either by_cols or combine_cols.

    Arguments:
    df : DataFrame
         A pandas DataFrame
    by_cols: str or list-like
                Columns you want to use to group the data and collapse over
    func : str, default 'sum'
             The aggregation function you wish to perform (sum, mean, min, max)
    combine_cols: str or list-like, default None
                 Columns you want to compute the aggregation function over
                 If no columns passed all columns will be aggregated (except
                 group columns)
    stub : str (can be regex), default None
           For aggregating over columns with similar names, can specify a stub
           name instead to aggregate over all columns containing the stub.
    keepNA : bool, deault True
             For aggregating when some values are NA, preserves sum as NA if 
             all summed values are NA, otherwise NAs are regarded as 0s
    '''
    # Get columns and ensure proper var types
    if isinstance(by_cols, str):
        by_cols = [by_cols]
    # Get all columns other than by_cols if no combine_cols or stub given
    if not combine_cols:
        combine_cols = [c for c in list(df) if c not in by_cols]
    if isinstance(combine_cols, str):
        combine_cols = [combine_cols]
    if stub:
        combine_cols = df.filter(regex=stub).columns.tolist()

    # Remove columns that are not included in group or aggregation calculation
    # (mimics STATA behavior)
    df = df[by_cols + combine_cols]
    g = df.groupby(by_cols)
    if func == 'sum':
        g = g[combine_cols].sum(min_count = 1) if keepNA else g[combine_cols].agg(np.sum)
    elif func == 'mean':
        g = g[combine_cols].agg(np.mean)
    elif func == 'min':
        g = g[combine_cols].agg(np.min)
    elif func == 'max':
        g = g[combine_cols].agg(np.max)

    return g.reset_index()


def force_drop_columns(df, cols=None, stubnames=None):
    ''' Drops columns that may not exist without breaking the program
        -- Arguments:
            df : DataFrame
                a pandas DataFrame
            cols: str or list-like, default=None
                the columns of the DataFrame to delete (do not need to exist)
            stubnames: str or list-like (can be regex), default=None
                    stubnames for selection of variables to drop. Can specify both
                    columns and stubnames.
    '''
    def get_varnames(df, stub):
        return df.filter(regex=stub).columns.tolist()
    # Get into correct format
    if isinstance(cols, str):
        cols = [cols]
    else:
        list(cols)

    if stubnames:
        if isinstance(stubnames, str):
            stubnames = [stubnames]
        else:
            list(stubnames)
        for stub in stubnames:
            cols = cols + get_varnames(df, stub)
    # Delete columns, ignoring errors if columns don't exist
    for col in cols:
        try:
            del df[col]
        except KeyError:
            continue

    return df


def make_group_id_col(df, group_cols, id_col='uid'):
    ''' Adds group IDs to a dataset.
        -- Arguments:
            df : DataFrame
                a pandas DataFrame
            group_cols : str or list-like
                        The columns used to group the data
            id_col : str, default 'uid'
                    The name of the column where you want to store the group ids
    '''
    g = df.loc[:, group_cols].copy().drop_duplicates()
    g.reset_index(inplace=True)
    g[id_col] = g.index + 1
    output = df.merge(g, on=group_cols)
    return(output)


def long_to_wide(df, stub, i, j, drop_others=False):
    ''' Long to wide panel format. Less flexible but more user friendly than
    pivot tables to get a stata-like reshape.

    This function expects to find a `stub` column, with values in its rows
    uniquely identified by values in columns `i` and `j`. The values in `j`
    will be applied as suffixes to the stub column to generate a group of
    columns with format Asuffix1, Asuffix2,...AsuffixN.

    Note - will treat all extra columns not included in reshape as additional
    ID columns. The only reason they need to be passed is to ensure that the
    data SHOULD be reshaped, since given enough columns pretty much anything
    can uniquely identify a row.

    Arguments:
    df : DataFrame
         The long-format DataFrame
    stub : str
          The stub name. Contains values in long format. The wide format
          columns will start with this stub name.
    i : str or list
        Columns to use as id variables. Together with `j`, should uniquely
        identify an observation in a row in `stub`
    j : str
        Extant column with observations to use as suffix for the stub name.
    drop_others : bool, default=False
                If true, will drop any columns not specified in either i or j.
                Otherwise all columns will be included as additional
                identifier columns.
    '''
    if isinstance(i, str):
        i = [i]
    if isinstance(df.index, pd.core.index.MultiIndex):
        df = df.reset_index()
    # Error Checking
    if df[i + [j]].duplicated().any():
        raise ValueError("`i` and `j` don't uniquely identify each row")
    if df[j].isnull().any():
        raise ValueError("`j` column has missing values. cannot reshape")
    if df[j].astype(str).str.isnumeric().all():
        if df.loc[df[j] != df[j].astype(int), j].any():
            print(
                "decimal values cannot be used in reshape suffix. {} coerced to integer".format(j))
        df.loc[:, j] = df[j].astype(int)
    else:
        df.loc[:, j] = df[j].astype(str)
    # Perform reshape
    if drop_others:
        df = df[i + [j]]
    else:
        i = [x for x in list(df) if x not in [stub, j]]
    df = df.set_index(i + [j]).unstack(fill_value=np.nan)
    # Ensure all stubs and suffixes are strings and join them to make col names
    cols = pd.Index(df.columns)
    # for each s in each col in cols, e.g. cols = [(s, s1), (s, s2)]
    cols = map(lambda col: map(lambda s: str(s), col), cols)
    cols = [''.join(c) for c in cols]
    # Set columns to the stub+suffix name and remove MultiIndex
    df.columns = cols
    df = df.reset_index()
    return df


def wide_to_long(df, stubnames, i, j, new_index=False, drop_others=False):
    ''' Because pd.wide_to_long is broken...
    Wide panel to long format. Less flexible but more user friendly than melt.

    This function expects to find a series of columns with `stub`+suffix names,
    with rows uniquely identified by values in column(s) `i`. The function will
    strip the suffixes from the `stub` columns and compress the observations
    into a single column named `stub`. The suffixes be stored in a new column
    `j` and will identify the values previously stored in the wide format.

    If there are any extra variables that weren't passed as ID variables,
    they will be treated like ID variables automatically. The only reason
    they need to be passed is to ensure that the data SHOULD be reshaped, since
    given enough columns pretty much anything can uniquely identify a row.

    Currently only can reshape one stub but future development will allow
    reshaping multiple stubnames at a time.

    If new_index is set to false, will output a dataset indexed by numeric
    id index (similar to when a stata dataset is imported). Otherwise the
    DataFrame will be MultiIndexed.

    Arguments:
    df : DataFrame
         The wide-format DataFrame
    stub : str (can be regex)
           The stub name or a regex for getting columns with
           a stub name. This will match any column that contains the stub name
           by default. If you only want to match columns that start with
           stub name, for example, pass a regex (such as "^stub").
    i : str or list-like
        (Existing) Columns to use as id variables
    j: str
        (New) Column with sub-observation variables
    new_index : Boolean, default=False
                If true, will output a multi-indexed dataframe.
    drop_others : Boolean, default=False
                  If true, will drop columns not included in i, j, or stub.
                  Note that this won't work well with the desired future
                  development of being able to reshape multiple stubnames.
                  Consider which features are most important.

    '''

    def get_varnames(df, stub):
        return(df.filter(regex=stub).columns.tolist())

    def melt_stub(df, stub, newVarNm):
        varnames = get_varnames(df, stub)
        # Use all cols as ids
        ids = [c for c in df.columns.values if c not in varnames]
        newdf = pd.melt(df, id_vars=ids, value_vars=varnames,
                        value_name=stub, var_name=newVarNm)
        # remove 'stub' from observations in 'newVarNm' columns, then
        #   recast to int typeif numeric suffixes were used new
        try:
            if newdf[newVarNm].unique().str.isdigit().all():
                newdf[newVarNm] = newdf[newVarNm].str.replace(
                    stub, '').astype(int)
        except AttributeError:
            newdf[newVarNm] = newdf[newVarNm].str.replace(stub, '').astype(str)
        return newdf

    # Error Checking
    if isinstance(i, str):
        i = [i]
    if isinstance(stubnames, str):
        stubnames = [stubnames]
    if isinstance(j, str):
        j = [j]
    if len(j) != len(stubnames):
        raise ValueError("Stub list must be same length as j list")
    if any(map(lambda s: s in list(df), stubnames)):
        raise ValueError("Stubname can't be identical to a column name")
    if df[i].duplicated().any():
        raise ValueError("The id variables don't uniquely identify each row")

    # Start the reshaping (pop stub in prep for rewriting for multiple stubs)
    stubcols = []
    for s in stubnames:
        stubcols +=  get_varnames(df, s)
    non_stubs = [c for c in df.columns if c not in stubcols+i]
    for pos, stub in enumerate(stubnames):
        jval = j[pos]
        temp_df = df.copy()
        # Drop extra columns if requested
        if drop_others:
            temp_df = temp_df[i + get_varnames(df, stub)]
        else:
            temp_df = temp_df[i + get_varnames(df, stub) + non_stubs]
        # add melted data to output dataframe
        if pos == 0:
            newdf = melt_stub(temp_df, stub, jval)
        else:
            newdf = newdf.merge(melt_stub(temp_df, stub, jval))

    if new_index:
        return newdf.set_index(i + j)
    else:
        return newdf
