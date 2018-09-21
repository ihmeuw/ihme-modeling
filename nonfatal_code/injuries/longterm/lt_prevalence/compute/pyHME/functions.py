"""This module includes functions commonly used in GBD"""

# Imports
import pandas as pd
from time import time
import csv

# Functions
def calc_stdev(df,ll_col,ul_col):
    """ Return a series of SD values given names of ll and ul columns in a DataFrame."""
    return (df[ul_col].sub(df[ll_col])) / 3.92
    
def create_summary_stats(input,output=None):
    """Create summary stats from a GBD-formatted draws file."""
    df = pd.read_csv(input,index_col='age').transpose()
    result = df.describe(percentile_width=95)
    result = result.transpose()
    result = result[['mean','2.5%','97.5%']]
    result = result.rename(columns={'2.5%':'ll',
                                    '97.5%':'ul'})
    if output: result.to_csv(output)
    return result

def start_timer():
    """
    Return the starting time (seconds since epoch) of a given process 
    (assuming that this is called before the process begins).
    """
    return time()
    
def end_timer(start_time,out_path,slots=1):
    """
    Write a csv that contains one value - the elapsed time, in slot-hours, since 
    start_time.
    """
    elapsed_hrs = (time() - start_time) * slots / 3600
    with open(out_path,'wb') as csvfile:
        cw = csv.writer(csvfile)
        cw.writerow([elapsed_hrs])
