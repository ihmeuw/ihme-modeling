# -*- coding: utf-8 -*-
'''
Description: Functions that enhance the pandas module and are generic to the
    pandas module
Contents:
    read_files :
    load_large_dta :
    parse_list_col :
    tuple_unique_entries :
    convert_col :
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME
'''

# disable print as statement and enable print as function
from __future__ import print_function
import sys
import pandas as pd
import numpy as np
import os
import ast
from warnings import warn

# for parallel processing
from multiprocessing import Pool
import multiprocessing as mp
from itertools import product, repeat


def read_one_file(filepath, file_type, show_status=False):
    ''' Worker thread function for read_files to run in parallel
    '''
    df = load_large_file(filepath, file_type, show_status = False)
    return(df)


def read_files(list_of_filepaths, file_type="csv"):
    ''' Returns a dataframe containing the appended contents of all files in
            list_of_filepaths.
        Note: accepts only those filetypes accepted by the load_large_file
            function (see function for more details)
        -- Inputs
            list_of_filepaths: files containing data to be concatenated
            file_type : one of file_types accepted by load_large_file
    '''
    print("    appending files...")

    # reduce the list of dataframes to a single dataframe
    with Pool(processes= mp.cpu_count()) as pool: # or max your hardware can support
        # ma
        appended_outputs = pool.starmap(read_one_file, 
                                zip(list_of_filepaths, 
                                repeat(file_type, times = len(list_of_filepaths)), 
                                repeat(False, times = len(list_of_filepaths))))

    appended_outputs = pd.concat(appended_outputs, ignore_index=True)
    return(appended_outputs)


def load_large_file(fpath, file_type="dta", show_status=True):
    ''' Function to load large STATA files
        Contributors: INDIVIDUAL_NAME
        -- Inputs:
            fpath : path to file
            file_type : filetype of file
            show_status : boolean, should reader display stauts
    '''
    ok_types = ['dta', 'csv']
    assert file_type in ok_types, "File type must be one of " + ok_types
    #
    if not os.path.isfile(fpath):
        warn("{} not found".format(fpath))
        return(pd.DataFrame())
    elif os.path.getsize(fpath) == 0:
        return(pd.DataFrame())
    #
    if file_type == 'dta':
        reader = pd.read_stata(fpath, iterator=True)
    elif file_type == 'csv':
        reader = pd.read_csv(fpath, iterator=True, low_memory=False, encoding='utf-8')
    df_tup = {}
    chunk_num = 0
    try:
        chunk = reader.get_chunk(100*1000)
        while len(chunk) > 0:
            df_tup[chunk_num] = chunk
            chunk_num += 1
            chunk = reader.get_chunk(100 * 1000)
            if show_status:
                print('.', end="")
    except (StopIteration, KeyboardInterrupt):
        pass
    try:
        df = pd.concat(df_tup)
    except ValueError:
        df = pd.DataFrame()
    if show_status:
        print('\nloaded {} rows'.format(len(df)))
    return(df)


def parse_list_col(pd_series):
    ''' Accepts a pandas series of lists and outputs the unique members
        of all lists combined
    '''
    parsed = []
    for sub_l in pd_series:
        parsed = list(set(parsed + sub_l))
    return(sorted(parsed))


def tuple_unique_entries(series):
    ''' Returns a tuple of the unique entries in the series (or column of dataframe)
        With logic to manage tuple or list entries
    '''
    output = tuple()
    for val in series:
        if isinstance(val, tuple) | isinstance(val, list):
            output = tuple(sorted(set(output + val)))
        else:
            try:
                ast.literal_eval(val)
                val = ast.literal_eval(val)
                if isinstance(val, tuple) | isinstance(val, list):
                    output = tuple(sorted(set(output+val)))
                else:
                    output = tuple(
                        sorted(set([c for c in output] + [str(val)])))
            except (ValueError, SyntaxError):
                output = tuple(sorted(set([c for c in output] + [str(val)])))
    return(output)


def convert_col(pd_series, to_type):
    ''' converts a string format pandas series to the specified type, avoiding
            error encountered with empty strings
    '''
    assert to_type in ['float', 'int'], "wrong type sent"

    def convert_int(val):
        try:
            return int(val)
        except ValueError:
            return np.nan

    def convert_float(val):
        try:
            return float(val)
        except ValueError:
            return np.nan
    if to_type == "float":
        return(pd_series.apply(lambda x: convert_float(x)))
    elif to_type == "int":
        return(pd_series.apply(lambda x: convert_int(x)))