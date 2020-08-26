'''
Name of Module: test_functions.py 
Description: Set of test functions for cause_disaggregation prep process  
Arguments: N/A - to use: import subroutines.test_functions at the 
                    top of your file with all other import statements 
Output: N/A  
'''

# import libraries
import pandas as pd
import numpy as np
import sys


def calc_total(df, dropped_data, metric):
    metric_cols = [m for m in df.columns.values if metric in m]
    df[metric + str(1)] = df[metric_cols].sum(axis=1)  # row summation
    total = df[metric + str(1)].sum(axis=0)
    dropped_data = df.merge(dropped_data, on='obs')
    dropped_data[metric + str(1)] = dropped_data[metric_cols].sum(axis=1)
    dropped_total = dropped_data[metric + str(1)].sum(axis=0)
    print("total after dropping _none wgt causes...{}".format(total)) 
    print("total of dropped data is...{}".format(dropped_total))
    return (total, dropped_total)


def test_garbage_remap(df, gbd_causes, del_merge=True):
    test_df =  df.loc[df['_merge'] =="left_only" & 
                        ~df['rdp_cause'].isin(gbd_causes), :]
    assert len(test_df) == 0, "Not all observations successfully merged"


def test_mapped_codes(df):
    tmp = df[(df.acause2 != "") | (
        (df.acause1 != df.gbd_cause) & (df.gbd_cause != "_gc"))]
    if len(tmp.index) > 0:
        tmp.to_csv('FILEPATH')
        raise AssertionError("ERROR: some disaggregated causes are mapped to more than one code. Please correct")
    else:
        return df


def check_calc_errors(df, metric, pre_total, dropped_data_total):
    metric_cols = [m for m in df.columns.values if metric in m]
    df[metric + str(1)] = df[metric_cols].sum(axis=1)  # row total
    df.to_csv('FILEPATH')
    total = df[metric + str(1)].sum()
    delta = (total + dropped_data_total) -  pre_total
    print("pre_total is... {pre_tot}".format(pre_tot=pre_total))
    print("total is... {tot}".format(tot=total))
    print("delta is... {delt}".format(delt=delta))
    assert abs(delta) <= 5, \
        "ERROR: total before disaggregation does not equal total after"
    return df


def check_miss_cause(df):
    missing_causeval = ((df['cause'] == "") | (df['acause'] == ""))
    assert len(df[missing_causeval]) == 0, \
        "ERROR: cannot continue with missing cause infomation. Error in cause_disaggregation suspected"
    return(None)
