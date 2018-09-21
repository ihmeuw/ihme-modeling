# -*- coding: utf-8 -*-
"""
Parallel job for claims prep to--
reshape data long
map on bundle id
process 8 types of claims data
    4 for admissions
    4 for
"""
import time
start = time.time()

import pandas as pd
import numpy as np
import platform
import sys
import itertools
#from datetime import TimedeltaIndex

if platform.system() == "Linux":
    root = "FILEPATH"
    USERNAME_path = "FILEPATH"

# sys.path.append(USERNAME_path)
sys.path.append(USERNAME_path)
from hosp_prep import sanitize_diagnoses, stack_merger

#################################
# TAKE IN JOB ARGS
################################

year = sys.argv[1]
dataset = sys.argv[2]
age = sys.argv[3]
sex = sys.argv[4]
# measure = sys.argv[5]

if year == " " or year == "" or year == "test":
    year = 2000
    dataset = "ccae"
    age = 58
    sex = 2
    #measure = "inc"

###########################
# DURATION FUNCTION !!!!!!
###########################


def recursive_duration(data_sub, return_df, unique_cases, counter):
    #data_sub.sort_values(by='adm_date', inplace=True)
    if counter > 10000:
        return("counter is 10,000")
    if data_sub.shape[0] == 0:
        return(return_df)
    else:
        counter += 1
        unique_cases += 1
        #print(unique_cases)
        return_df = pd.concat([return_df, data_sub.iloc[0:1, :]])
        return(recursive_duration(data_sub[data_sub.adm_date >= data_sub.adm_limit.iloc[0]], return_df=return_df, unique_cases=unique_cases, counter=counter))


def report_if_merge_fail(df, check_col, id_cols):
    """Report a merge failure if there is one"""
    merge_fail_text = """
        Could not find {check_col} for these values of {id_cols}:

        {values}
    """
    if df[check_col].isnull().values.any():
        # 'missing' can be df or series
        missing = df.loc[df[check_col].isnull(), id_cols]
        raise AssertionError(
            merge_fail_text.format(check_col=check_col,
                                   id_cols=id_cols, values=missing)
        )
    else:
        pass

#################################
# READ IN DATA
################################
if root == "FILEPATH":
    df = pd.read_stata("FILEPATH")
else:
    df = pd.read_stata("FILEPATH")

# The Stata spiltting script creates a few empty dataframes when there isn't
# any data. If the dataframe is empty end the program
if df.shape[0] == 0:
    exit

# format MS to flow with our process
df['year_start'] = year
df['year_end'] = year
df['age'] = int(df['age_start'].unique())
df.drop(['age_start', 'age_end', 'claim_num'], axis=1, inplace=True)
df.rename(columns={'enrolid': 'enroUSER_id', 'sex': 'sex_id',
                   'platform': 'is_otp', 'date': 'adm_date',
                   'stdplac': 'facility_id'}, inplace=True)
df['is_otp'].replace(['otp', 'inp'], [1, 0], inplace=True)
df['sex_id'] = pd.to_numeric(df['sex_id'])
df['egeoloc'] = pd.to_numeric(df['egeoloc'])

# inpatient data doesn't have a facility_id, this was causing the inpatient
# data to be lost in the groupby. make a dummy inp facility id
df.loc[(df.facility_id.isnull()) & (df.is_otp == 0), 'facility_id'] = 99999

# int_feats = ['age', 'egeoloc', 'sex_id', 'is_otp', 'year_start', 'year_end']
# for feat in int_feats:
#    df[feat] = pd.to_numeric(df[feat], downcast='integer')

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('DX')]

new_dx_feats = ["dx_" + str(num) for num in np.arange(1, 16, 1)]

name_dict = dict(zip(diagnosis_feats, new_dx_feats))

df.rename(columns=name_dict, inplace=True)
# Remove non-alphanumeric characters from dx feats
# for feat in diagnosis_feats:
#    df[feat] = sanitize_diagnoses(df[feat])
#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

# if the dataframe is too large, split it up into 100 pieces and reshape long
# one at a time then concat back together
if df.shape[0] > 1000000:
    split_df = np.array_split(df, 100)
    del df
    df = []
    for each_df in split_df:
        each_df = each_df.set_index(['enroUSER_id', 'adm_date', 'age',
                                     'egeoloc', 'sex_id', 'is_otp',
                                     'year_start', 'year_end',
                                     'facility_id']).stack().reset_index()
        # drop blank diagnoses
        each_df = each_df[each_df[0] != ""]
        # rename cols
        each_df = each_df.rename(columns={'level_9': 'diagnosis_id',
                                          0: 'cause_code'})
        # replace diagnosis_id with codes, 1 for primary, 2 for secondary
        # and beyond
        each_df['diagnosis_id'] = np.where(each_df['diagnosis_id'] ==
                                    'dx_1', 1, 2)
        # append to list of dataframes
        df.append(each_df)

    df = pd.concat(df)
    df.reset_index(inplace=True)

else:
    df = df.set_index(['enroUSER_id', 'adm_date', 'age', 'egeoloc', 'sex_id',
                       'is_otp', 'year_start', 'year_end',
                       'facility_id']).stack().reset_index()

    df = df[df[0] != ""]

    df = df.rename(columns={'level_9': 'diagnosis_id', 0: 'cause_code'})

    # replace diagnosis_id with codes, 1 for primary, 2 for secondary
    # and beyond
    df['diagnosis_id'] = np.where(df['diagnosis_id'] ==
                                          'dx_1', 1, 2)

# DON'T DO A GROUPBY UNTIL AFTER MAPPING AND DETERMINING WHAT DATA YOU WANT
# JUST BUILD THE INFASTRUCTURE FOR ALL THESE CAUSE THEY'LL PROBABLY BE WANTED
# AT SOME POINT

# INP PRIMARY ADMISSIONS -- INPATIENT PRIMARY INDIVIDUALS
# INP ANY ADMISSIONS -- INP ANY INDIVIDUALS
# INP+OTP ANY ADMISSIONS -- INP+OTP ANY INDIVIDUALS
# OTP ANY ADMISSIONS - INDIVIDUALS

# these four don't make sense b/c outpatient data doesn't have a primary dx
# INP+OTP PRIMARY ADMISSIONS -- INP+OTP PRIMARY INDIVIDUALS
# OTP PRIMARY ADMISSIONS - AND INDIVIDUALS

# map data to bundle id
maps = pd.read_csv(root + "FILEPATH")
maps = maps[maps.code_system_id == 1]
maps = maps[['cause_code', 'bundle_id', 'bid_measure']]
maps.dropna(subset=['bundle_id'], inplace=True)

maps['bundle_id'] = pd.to_numeric(maps['bundle_id'])


# remove non-alphanumeric characters
df['cause_code'] = df['cause_code'].str.replace("\W", "")
# make sure all letters are capitalized
df['cause_code'] = df['cause_code'].str.upper()
maps['cause_code'] = maps['cause_code'].str.replace("\W", "")
# make sure all letters are capitalized
maps['cause_code'] = maps['cause_code'].str.upper()

# merge bundle id onto data
df = df.merge(maps, how='left', on='cause_code')

# drop null bundle id rows
df = df[df.bundle_id.notnull()]

# if the dataframe becomes empty after mapping end the program
if df.shape[0] == 0:
    exit

# merge on location IDs
loc_map = pd.read_csv(root + "FILEPATH")
loc_map = loc_map[['egeoloc', 'location_id']]
pre_shape = df.shape[0]
df = df.merge(loc_map, how='left', on='egeoloc')
assert pre_shape == df.shape[0]


def expandgrid(*itrs):
    # create a template df with every possible combination of
    #  age/sex/year/location to merge results onto
    # define a function to expand a template with the cartesian product
    product = list(itertools.product(*itrs))
    return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

# ages = np.arange(0, 151, 1)
ages = df.age.unique()
sexes = [1, 2]
location_id = df.location_id.unique()
year_start = df.year_start.unique()
year_end = df.year_end.unique()
bundles = df.bundle_id.unique()
facilities = df.facility_id.unique()
facilities = np.append(facilities, 1111)
# create the template df using pandas and the func above
template_df = pd.DataFrame(expandgrid(ages, sexes, location_id, year_start,
                                      year_end, bundles, facilities))
# rename columns
template_df.columns = ['age', 'sex_id', 'location_id', 'year_start',
                       'year_end', 'bundle_id', 'facility_id']

# read in the durations files
durations = pd.read_excel(root + "FILEPATH")

maps_short = maps[['bundle_id', 'bid_measure']].drop_duplicates()
maps_short = maps_short[maps_short.bundle_id.notnull()]
durations = maps_short.merge(durations, how='left', on='bundle_id')

# now loop over every possible way to sum up cases
agg_types = ['inp_pri', 'inp_any', 'inp_otp_any', 'otp_any']

for agg_type in agg_types:
    # drop DX depending on inp
    if agg_type == 'inp_pri':
        # drop all non inpatient primary data
        dat_indv = df[(df.diagnosis_id == 1) & (df.is_otp == 0)].copy()
        dat_claims = df[(df.diagnosis_id == 1) & (df.is_otp == 0)].copy()
    if agg_type == 'inp_any':
        # drop all non inpatient data
        dat_indv = df[df.is_otp == 0].copy()
        dat_claims = df[df.is_otp == 0].copy()
    if agg_type == 'inp_otp_pri':
        # drop all non inpatient/outpatient primary data
        dat_indv = df[df.diagnosis_id == 1].copy()
        dat_claims = df[df.diagnosis_id == 1].copy()
    if agg_type == 'inp_otp_any':
        # keep everything
        dat_indv = df.copy()
        dat_claims = df.copy()
    if agg_type == 'otp_pri':
        # drop all non outpatient primary data
        dat_indv = df[(df.diagnosis_id == 1) & (df.is_otp == 1)].copy()
        dat_claims = df[(df.diagnosis_id == 1) & (df.is_otp == 1)].copy()
    if agg_type == 'otp_any':
        # drop all non outpatient data
        dat_indv = df[df.is_otp == 1].copy()
        dat_claims = df[df.is_otp == 1].copy()

    # if the subset dataframe is empty move on to next set
    if dat_indv.shape[0] == 0:
        continue

    # we also want to go from claims data to estimates for individuals
    prev = dat_indv[dat_indv['bid_measure'] == 'prev'].copy()
    # drop all the duplicates for prev causes, equivalent to a 365 day duration
    # prev = prev[['enroUSER_id', 'age', 'sex_id', 'location_id',
    #             'year_start', 'year_end', 'bundle_id', 'bid_measure', 'facility_id']]
    prev.drop_duplicates(subset=['enroUSER_id', 'bundle_id'], inplace=True)
    prev['facility_id'] = 1111

    inc = dat_indv[dat_indv['bid_measure'] == 'inc'].copy()
    # drop the pharma and lab types 
    inc = inc[(inc.facility_id != 1) & (inc.facility_id != 98) & (inc.facility_id != 81)]
    facilities = inc.facility_id.unique()
    assert 1 not in facilities
    assert 98 not in facilities
    assert 81 not in facilities,\
        "Pharmacy and Lab facility types were not dropped for incidence data"

    ########################
    # CREATE DURATION LIMITS
    ########################
    final_inc = []
    if inc.shape[0] > 0:
        # merge on durations
        inc = inc.merge(durations[['bundle_id', 'duration']], how='left',
                        on='bundle_id')
        # convert a col of ints to a days type time object
        temp_inc = inc['duration'].apply(np.ceil).apply(lambda x: pd.Timedelta(x, unit='D'))
        # make sure adm date is a date time object
        inc['adm_date'] = pd.to_datetime(inc['adm_date'])
        # then add durations to adm_date to get the limit date
        inc['adm_limit'] = inc['adm_date'] + temp_inc
        #dat['flag_id'] = str(dat['enroUSER_id'] + "_" + str(dat['adm_date'])

        # compare pd concat to appending a list
        start = time.time()

        #dat_admis.sort_values(by=['enroUSER_id', 'bundle_id', 'adm_date'], inplace=True)
        # groupby enroUSER and bundle
        #dat_admis = dat_admis.groupby(['enroUSER_id', 'bundle_id'])

        inc.sort_values(by=['enroUSER_id', 'bundle_id', 'adm_date'], inplace=True)
        inc = inc.groupby(['enroUSER_id', 'bundle_id'])

        for enroUSER_id, bundle_id in inc:
            final_inc.append(recursive_duration(bundle_id, pd.DataFrame(), 0, 0))

    # alt method-
#    make_unique = lambda x: mark row as unique
#                            assign var adm_limit which is the adm_limit from unique row
#                            iterate to next row
#                            if row adm_date < var adm_limit then ignore:
#                            if row adm_date > var adm_limit then mark row as unique
#                            assign var adm_limit which is the adm_limit from unique row
#                            iterate to next row
#                            etc.
#    # alt method 2
#    def make_unique(adm_date, adm_limit):
#        # take the adm date from the row and pass it jan 1st of the year
#        if adm_date >= adm_limit:
#            return(1, adm_limit)
#        else:
#            return(0, adm_limit)

    # bring the data back together
    if len(final_inc) > 0:
        inc_df = pd.concat(final_inc)
        dat_indv = pd.concat([inc_df, prev])
        dat_indv.drop(labels=['adm_limit', 'duration'], axis=1, inplace=True)
    else:
        dat_indv = prev.copy()

    indv_loss = dat_indv.isnull().sum().max()
    claims_loss = dat_claims.isnull().sum().max()
    indv_sum = dat_indv.shape[0] - indv_loss
    claims_sum = dat_claims.shape[0] - claims_loss

    # now create cases
    col_name_a = agg_type + "_claims_cases"
    dat_claims[col_name_a] = 1

    col_name_i = agg_type + "_indv_cases"
    dat_indv[col_name_i] = 1

    # groupby and collapse summing cases
    groups = ['location_id', 'year_start', 'year_end',
                'age', 'sex_id', 'bundle_id', 'facility_id']
    # add these assertions because pandas groupby is very aggressive with
    # dropping NAs. set a 20% data loss threshold
    if dat_indv.shape[0] > 2000:
        assert (dat_indv[groups].isnull().sum() < dat_indv.shape[0] * .2).all()
        assert (dat_claims[groups].isnull().sum() < dat_claims.shape[0] * .2).all()
    dat_claims = dat_claims.groupby(groups).agg({col_name_a: 'sum'}).reset_index()
    dat_indv = dat_indv.groupby(groups).agg({col_name_i: 'sum'}).reset_index()

    # merge onto our template df craeted above
    template_df = template_df.merge(dat_claims, how='left', on = ['age', 'sex_id',
                            'location_id', 'year_start',
                            'year_end', 'bundle_id', 'facility_id'])
    template_df = template_df.merge(dat_indv, how='left', on = ['age', 'sex_id',
                            'location_id', 'year_start',
                            'year_end', 'bundle_id', 'facility_id'])

    # check sum of cases to ensure we're not losing beyond what's expected
    print(agg_type)
    assert template_df[col_name_a].sum() == claims_sum, "Some cases lost"
    assert template_df[col_name_i].sum() == indv_sum, "Some cases lost"
    # print(agg_type + " with the new method")
    # end = time.time()
    # print((end - start) / 60)

# remove rows where every value is NA
case_cols = template_df.columns[template_df.columns.str.endswith("_cases")]
col_sums = template_df[case_cols].sum()
template_df.dropna(axis=0, how='all', subset=case_cols,
                   inplace=True)
assert (col_sums == template_df[case_cols].sum()).all()

# write template_df to FILEPATH for use in correction factors
out_dir = root + "FILEPATH"
filepath = "FILEPATH"
template_df.to_csv(filepath, index=False)

# now format data for the marketscan claims process which uses 2015 Stata code
# vectors of prev and inc bundle IDs to write to proper directory
prev_buns = maps[maps.bid_measure == 'prev'].bundle_id.unique()
inc_buns = maps[maps.bid_measure == 'inc'].bundle_id.unique()

# merge egeoloc back onto data
template_df = template_df.merge(loc_map, how='left', on='location_id')

# rename sex_id to sex data type is int [1, 2]
template_df.rename(columns={'sex_id': 'sex'}, inplace=True)

if 'inp_otp_any_indv_cases' in template_df.columns:
    # take only the cases col we want, rename it to cases
    ms_all = template_df[['sex', 'egeoloc', 'age', 'bundle_id',
                          'inp_otp_any_indv_cases', 'year_start']].copy()
    ms_all.rename(columns={'inp_otp_any_indv_cases': 'cases',
                           'year_start': 'year',
                           'age': 'age_start'}, inplace=True)
    ms_all['age_end'] = ms_all['age_start']
    # write prev and inc
    ms_all[ms_all.bundle_id.isin(prev_buns)].to_stata("FILEPATH",
           write_index=False)
    ms_all[ms_all.bundle_id.isin(inc_buns)].to_stata("FILEPATH",
           write_index=False)


if 'inp_any_indv_cases' in template_df.columns:
    ms_inp_any = template_df[['sex', 'egeoloc', 'age', 'bundle_id',
                          'inp_any_indv_cases', 'year_start']].copy()
    ms_inp_any.rename(columns={'inp_any_indv_cases': 'cases',
                               'year_start': 'year',
                               'age': 'age_start'}, inplace=True)
    ms_inp_any['age_end'] = ms_inp_any['age_start']
    ms_inp_any[ms_inp_any.bundle_id.isin(prev_buns)].to_stata("FILEPATH",
           write_index=False)
    ms_inp_any[ms_inp_any.bundle_id.isin(inc_buns)].to_stata("FILEPATH",
           write_index=False)

################################
# DONE DONE DONE DONE DONE DONE
################################
