
# standard libraries
import datetime
import time
import subprocess
import submitter
import pandas as pd
import numpy as np
import os
import sys

# Make sex_ids, 1 and 2, into names Male and Female
def sex_fix(sex_id):
	if sex_id == 1:
		sex = 'Male'
	else:
		sex = 'Female'
	return sex

# Make the age groups into age start to age end intervals
def age_fix(row):
	if row['age_end']%5:
		return row['age_end']
	else:
		return row['age_end'] - 1.0

def wait(pattern, seconds):
    '''
    Description: Pause the master script until certain sub-jobs are finished.

    Args:
        1. pattern: the pattern of the jobname that you want to wait for
        2. seconds: number of seconds you want to wait

    Output:
        None, just pauses the script
    '''
    seconds = int(seconds)
    while True:
        qstat = submitter.qstat()
        if qstat['name'].str.contains(pattern).any():
            print time.localtime()
            time.sleep(seconds)
            print time.localtime()
        else:
            break
			
def timestamp(time=datetime.datetime.now()):
    """
	Return a timestamp for the current moment in the CoD format.

    Times are made in the timezone of the computer that called it (likely PST)

    This is the format:
        {year}_{month}_{day}_{hourminuteseconds}

    Example:
        April 2nd, 1992, 7:12:39 PM -->
            1992_04_02_191239

    Arguments:
        time: datetime.datetime
            DUSERts to now()

    Returns:
        String, the timestamp
    """
    return '{:%Y_%m_%d_%H%M%S}'.format(time)
	


def make_square_matrix(df):
	"""Make sure the matrix doesn't have any missing data."""
	
	CAUSES = df[['cause_id']].drop_duplicates().reset_index()
	CAUSES['merge_col'] = 1
	CAUSES.drop('index', axis=1, inplace=True)

	AGE_GROUPS = df[['age_group_id']].drop_duplicates().reset_index()
	AGE_GROUPS['merge_col'] = 1
	AGE_GROUPS.drop('index', axis=1, inplace=True)

	SEXES = df[['sex_id']].drop_duplicates().reset_index()
	SEXES['merge_col'] = 1
	SEXES.drop('index', axis=1, inplace=True)

	LOCATIONS = df[['location_id']].drop_duplicates().reset_index()
	LOCATIONS['merge_col'] = 1
	LOCATIONS.drop('index', axis=1, inplace=True)

	square = LOCATIONS.merge(CAUSES, on='merge_col', how='inner')\
					  .merge(SEXES, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
				  
	df = square.merge(df, how='left')
	df.fillna(0, inplace=True)
	
	return df
		
def make_custom_causes(df, group_cols, measure_cols):
    """
	Make two custom cause aggregations and append to df
    
    Requires:
        df['cause_id'] exists
    """
    # original columns shouldn't change
    orig_cols = df.columns
    # set id columns as group_cols + cause_id
    id_cols = group_cols + ['cause_id']
    # Define cause aggregates
    # copd intersitial, all resp_pneum
    other_resp = {509, 510, 511, 512, 513, 514, 515, 516}
    other_resp_code = 520
    # all but ihd, cmp, htn, rhd and (copd/interstitial/pnuem)
    other_heart_failure = {390, 503, 614, 616, 618, 619, 643, 507, 388}
    other_hf_code = 385
	# alcoholic cardiomyopathy, myocarditis, and other cardiomyopathy
    other_cmp = {938, 942, 944}
    other_cmp_code = 499
    
    # drop existing cvd_other and resp_other
    df = df.ix[~df['cause_id'].isin([other_resp_code,
	                                 other_hf_code,
									 other_cmp_code])]
    
    # Collapse and append
    resp_other = df.fillna(0).ix[df['cause_id'].isin(other_resp)].groupby(\
	                               group_cols)[measure_cols].sum().reset_index()
    assert len(resp_other) > 0, 'resp other aggregations didnt work'
    resp_other['cause_id'] = other_resp_code
    cvd_other = df.fillna(0).ix[df['cause_id'].isin(other_heart_failure)]\
	                      .groupby(group_cols)[measure_cols].sum().reset_index()
    assert len(cvd_other) > 0, 'cvd other aggregation didnt work'
    cvd_other['cause_id'] = other_hf_code
    cmp_other = df.fillna(0).ix[df['cause_id'].isin(other_cmp)].groupby(\
	                               group_cols)[measure_cols].sum().reset_index()
    assert len(cmp_other) > 0, 'cmp other aggregation didnt work'
    cmp_other['cause_id'] = other_cmp_code
    df = df.append(cmp_other, ignore_index=True)
    df = df.append(cvd_other, ignore_index=True)
    df = df.append(resp_other, ignore_index=True)
    assert other_resp_code in set(df.cause_id.unique()), \
	       'resp other did not get added'
    assert other_hf_code in set(df.cause_id.unique()), \
	       'other hf did not get added'
    assert other_cmp_code in set(df.cause_id.unique()), \
	       'other cmp did not get added'
    # make sure columns didn't change
    assert set(df.columns) == set(orig_cols)
    # make sure duplicates weren't introduced
    assert not df[id_cols].duplicated().any(), \
	       'duplicates introduced in custom cause generation'
    return df
	
	