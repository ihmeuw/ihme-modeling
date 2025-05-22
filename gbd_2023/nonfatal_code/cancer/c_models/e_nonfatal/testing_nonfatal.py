'''
Description: Contains functions common to testing for the nonfatal model
How to Use: functions are called by other scripts in the nonfatal.tests module
Contributors: INDIVIDUAL_NAME

'''
import numpy as np
import pandas as pd

def check_diff(test_df, old_col, new_col, uid_cols):
    ''' Checks for significant differences between two columns
    '''
    old_fmt = test_df[old_col].astype('float64')
    new_fmt = test_df[new_col].astype('float64')
    diffs = test_df.loc[(old_fmt - new_fmt).abs().round(5) > 0.00001, 
                                            uid_cols + [old_col, new_col]]
    return(diffs.rename(columns={old_col:'old', new_col:'new'}))


def test_single_cols(test_df, uid_cols, check_cols):
    ''' Checks the differences in test_df between each pair of columns in the
        check_cols list
    '''
    for s in check_cols:
        col1 = s+'_x'
        col2 = s+'_y'
        diff = check_diff(test_df, col1, col2, uid_cols)
        diff_len = len(diff)
        if diff_len:
            print("Alert! {} values differ by {} rows".format(s,diff_len))
            print(diff.head())
            assert False
        else:
            print(s+" column ok")


def find_diffs(test_df, test_dict, uid_cols):
    ''' Checks the differences in test_df between each matching index of the 
            columns listed in the test_dict 
        -- Inputs:
            test_df : a merged version of the new and old outputs
            test_dict : a dictionary for which each key corresponds to a group of
                two columns lists: old and new (allows comparison of multiple groups of columns)
    '''
    for n, c in enumerate(test_dict):
        for i in range(0, 1000):
            old_list = sorted(test_dict[c][0])
            new_list = sorted(test_dict[c][1])
            col1 = old_list[i]
            col2 = new_list[i]
            diff_df = check_diff(test_df, col1, col2, uid_cols)
            num_diff = len(diff_df)
            if num_diff > 0:
                prop_diff = num_diff/float(len(test_df))
                print('found differences in ' + str(num_diff) + ' rows of ' + c + str(i))
                print('proportion not matching: {} ({} rows)'.format(prop_diff, num_diff))
                print("Examples:")
                print(diff_df.head())
                return
        print(c + " column group ok")
    print("all columns successfully passed test")


