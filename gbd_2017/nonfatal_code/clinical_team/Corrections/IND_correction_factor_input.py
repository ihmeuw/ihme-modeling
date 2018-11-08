
# coding: utf-8

# ## formatting code

import pandas as pd
import platform
import numpy as np
import sys
import itertools
import time
import getpass

user = getpass.getuser()

# load our functions
hosp_path = r"filepath".format(user)
sys.path.append(hosp_path)

import hosp_prep

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

filepath = root + r"filepath"
df = pd.read_stata(filepath)

# convert dates to datetime objects
df.dateofdischarge = pd.to_datetime(df.dateofdischarge, dayfirst=True, errors='coerce')
df.dateofadmission = pd.to_datetime(df.dateofadmission, dayfirst=True, errors='coerce')

df['los'] = df['dateofdischarge'] - df['dateofadmission']
# drop day cases
df = df[df['los'] != '0 days']

df['year_start'], df['year_end'] = 2014, 2014

# Select features from raw data to keep
keep = ['patientcode', 'year_start', 'year_end', 'age_years', 'sex',
        'dateofadmission',
        'icd10coding_1', 'icd10coding_2', 'icd10coding_3', 'icd10coding_4',
        'icd10coding_5', 'icd10coding_6']

df = df[keep]

# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex': 'sex_id',
    'age_years': 'age',
    'facility_id': 'facility_id',
    'code_system_id': 'code_system_id',
    'patientcode': 'patient_id',
    'dateofadmission': 'adm_date',

    # measure_id variables
    'outcome_id': 'outcome_id',
    # diagnosis varibles
    'icd10coding_1': 'dx_1',
    'icd10coding_2': 'dx_2',
    'icd10coding_3': 'dx_3',
    'icd10coding_4': 'dx_4',
    'icd10coding_5': 'dx_5',
    'icd10coding_6': 'dx_6'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df['representative_id'] = 3  # Not representative
df['location_id'] = 4862

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'IND_SNH'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# facility_id can take one of multiple values
df['facility_id'] = 'hospital'

# discharge or death
df['outcome_id'] = 'discharge'

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df['location_id'] = 4862

df['source'] = 'IND_SNH'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# facility_id can take one of multiple values
df['facility_id'] = 'hospital'

# discharge or death
df['outcome_id'] = 'discharge'

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

# NULL DIAGNOSES BREAK STACK MERGER
df = df[df.dx_1.notnull()]
# Reshape diagnoses from wide to long
#   - review `hosp_prep.py` for additional function documentation
df = hosp_prep.stack_merger(df)

df['val'] = 1


# ## claims code

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

# age has str values for some reason
df.age = pd.to_numeric(df.age)


def clean_bad_ids(df):
    """
    remove patient IDs which have multiple sexes associated with them or
    have impossible ages with them, ie one admission the patient is 30,
    and the next they're 75
    """
    print("df shape with missing pat IDs is {}".format(df.shape))
    # drop missing patient IDs
    df = df[df.patient_id != "."]
    df = df[df.age.notnull()]
    if df.shape[0] == 0:
        print("{} is missing unique patient ids".format(fpath))
        sys.exit()

    print("df shape is", df.shape)
    # drop duplicates by ID and sex
    x = df[['patient_id', 'sex_id']].drop_duplicates()
    # keep only IDs that are duplicated i.e. that have multiple sex IDs for them
    x = x[x.duplicated(subset=['patient_id'])]
    # drop enrollee IDs with 2 sexes associated
    df = df[~df.patient_id.isin(x.patient_id)]
    print("there were {} bad patient IDs due to multiple sexes".format(x.shape[0]))

    dfG = df.copy()
    dfG['age_min'] = dfG['age']
    dfG['age_max'] = dfG['age']
    # create age min and max by enrollee ID groups
    dfG = dfG.groupby(['patient_id']).agg({'age_min': 'min', 'age_max': 'max'}).reset_index()
    max_age_df = dfG[['patient_id', 'age_max']].copy()
    # find the difference beween min and max
    dfG['age_diff'] = dfG['age_max'] - dfG['age_min']
    # drop where difference is greater than 1 year
    dfG = dfG[dfG.age_diff > 1]
    print("there were {} bad patient IDs due to large age differences".format(dfG.shape[0]))

    df = df[~df.patient_id.isin(dfG.patient_id)]
    del x


    df = df.merge(max_age_df, how='left', on='patient_id')
    df['age_diff'] = df['age_max'] - df['age']
    print("age diffs are", df['age_diff'].value_counts(dropna=False))
    assert df['age_diff'].max() <= 1
    df.drop(['age', 'age_diff'], axis=1, inplace=True)
    df.rename(columns={'age_max': 'age'}, inplace=True)
    del dfG

    print(df.patient_id.unique().size)
    return df

df = clean_bad_ids(df)


# INP PRIMARY ADMISSIONS -- INPATIENT PRIMARY INDIVIDUALS
# INP ANY ADMISSIONS -- INP ANY INDIVIDUALS

# map data to bundle id
maps = pd.read_csv(root + r"filepath")
assert hosp_prep.verify_current_map(maps)
maps = maps[maps.code_system_id.isin(df.code_system_id.unique())]

maps = maps[['cause_code', 'bundle_id', 'bid_measure', 'bid_duration']]
maps.dropna(subset=['bundle_id'], inplace=True)

# sort of odd- python env on cluster doesn't recognize keyword 'downcast'
maps['bundle_id'] = pd.to_numeric(maps['bundle_id'])

# remove non-alphanumeric characters
df['cause_code'] = df['cause_code'].str.replace("\W", "")
# make sure all letters are capitalized
df['cause_code'] = df['cause_code'].str.upper()
maps['cause_code'] = maps['cause_code'].str.replace("\W", "")
# make sure all letters are capitalized
maps['cause_code'] = maps['cause_code'].str.upper()

maps_no_dur = maps[['cause_code', 'bundle_id', 'bid_measure']].copy()
# merge bundle id onto data
df = df.merge(maps_no_dur, how='left', on='cause_code')

# drop null bundle id rows
df = df[df.bundle_id.notnull()]

# if the dataframe becomes empty after mapping end the program
if df.shape[0] == 0:
    exit

def expandgrid(*itrs):

    product = list(itertools.product(*itrs))
    return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

# ages = np.arange(0, 151, 1)
ages = df.age.unique()
sexes = [1, 2]
location_id = df.location_id.unique()
year_start = df.year_start.unique()
year_end = df.year_end.unique()
bundles = df.bundle_id.unique()
# create the template df using pandas and the func above
template_df = pd.DataFrame(expandgrid(ages, sexes, location_id, year_start,
                                      year_end, bundles))
# rename columns
template_df.columns = ['age', 'sex_id', 'location_id', 'year_start',
                       'year_end', 'bundle_id']

# read in the durations files
durations = pd.read_excel(root + filepath)

maps_short = maps[['bundle_id', 'bid_measure', 'bid_duration']].drop_duplicates()
maps_short = maps_short[maps_short.bundle_id.notnull()]
durations = maps_short.merge(durations, how='left', on='bundle_id')

durations.loc[(durations.bid_measure == 'inc') &              (durations.duration.isnull()), 'duration'] =              durations.loc[(durations.bid_measure == 'inc') &                            (durations.duration.isnull()), 'bid_duration']

assert durations[(durations.bid_measure == 'inc') & (durations.duration.isnull())].shape[0] == 0

durations.drop('bid_duration', axis=1, inplace=True)

# now loop over every possible way to sum up cases
agg_types = ['inp_pri', 'inp_any']

for agg_type in agg_types:
    # drop DX depending on inp/otp/primary/any
    if agg_type == 'inp_pri':
        # drop all non inpatient primary data
        dat_indv = df[(df.diagnosis_id == 1)].copy()
        dat_claims = df[(df.diagnosis_id == 1)].copy()
    if agg_type == 'inp_any':
        # drop all non inpatient data
        dat_indv = df.copy()
        dat_claims = df.copy()

    # if the subset dataframe is empty move on to next set
    if dat_indv.shape[0] == 0:
        continue

    # we also want to go from claims data to estimates for individuals
    prev = dat_indv[dat_indv['bid_measure'] == 'prev'].copy()
    # drop all the duplicates for prev causes, equivalent to a 365 day duration
    prev.drop_duplicates(subset=['patient_id', 'bundle_id'], inplace=True)

    inc = dat_indv[dat_indv['bid_measure'] == 'inc'].copy()
    
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
        #dat['flag_id'] = str(dat['patient_id'] + "_" + str(dat['adm_date'])

        # compare pd concat to appending a list
        start = time.time()

        #dat_admis.sort_values(by=['patient_id', 'bundle_id', 'adm_date'], inplace=True)
        # groupby enrollee and bundle
        #dat_admis = dat_admis.groupby(['patient_id', 'bundle_id'])

        inc.sort_values(by=['patient_id', 'bundle_id', 'adm_date'], inplace=True)
        inc = inc.groupby(['patient_id', 'bundle_id'])

        for patient_id, bundle_id in inc:
            final_inc.append(recursive_duration(bundle_id, pd.DataFrame(), 0, 0))
    
    # bring the data back together
    if len(final_inc) > 0:
        inc_df = pd.concat(final_inc)
        dat_indv = pd.concat([inc_df, prev])
        dat_indv.drop(labels=['adm_limit', 'duration'], axis=1, inplace=True)
    else:
        dat_indv = prev.copy()

    # null rows are lost in the groupby so these max cols are used
    # to make sure we're not losing any extra data beyond these nulls
    indv_loss = dat_indv.isnull().sum().max()
    claims_loss = dat_claims.isnull().sum().max()
    print("null claims", dat_claims.isnull().sum())
    print("the most null claims from any columns {}".format(claims_loss))
    indv_sum = dat_indv.shape[0] - indv_loss
    claims_sum = dat_claims.shape[0] - claims_loss

    # now create cases
    col_name_a = agg_type + "_claims_cases"
    dat_claims[col_name_a] = 1

    col_name_i = agg_type + "_indv_cases"
    dat_indv[col_name_i] = 1

    # groupby and collapse summing cases
    groups = ['location_id', 'year_start', 'year_end',
                'age', 'sex_id', 'bundle_id']
    # add these assertions because pandas groupby is very aggressive with
    # dropping NAs. set a 20% data loss threshold
    if dat_indv.shape[0] > 2000:
        assert (dat_indv[groups].isnull().sum() < dat_indv.shape[0] * .2).all()
        assert (dat_claims[groups].isnull().sum() < dat_claims.shape[0] * .2).all()
    dat_claims = dat_claims.groupby(groups).agg({col_name_a: 'sum'}).reset_index()
    dat_indv = dat_indv.groupby(groups).agg({col_name_i: 'sum'}).reset_index()

    # merge onto our template df created above
    template_df = template_df.merge(dat_claims, how='left', on = ['age', 'sex_id',
                            'location_id', 'year_start',
                            'year_end', 'bundle_id'])
    template_df = template_df.merge(dat_indv, how='left', on = ['age', 'sex_id',
                            'location_id', 'year_start',
                            'year_end', 'bundle_id'])

    # check sum of cases to ensure we're not losing beyond what's expected
    print(agg_type)
    assert template_df[col_name_a].sum() == claims_sum, "Some cases lost. claims sum is {} type is {} data col sum is {}".format(claims_sum, col_name_a, template_df[col_name_a].sum())
    assert template_df[col_name_i].sum() == indv_sum, "Some cases lost. claims sum {} {} sum {}".format(claims_sum, col_name_i, template_df[col_name_i].sum())
    # print(agg_type + " with the new method")
    end = time.time()
    print((end - start) / 60)

# remove rows where every value is NA
case_cols = template_df.columns[template_df.columns.str.endswith("_cases")]
col_sums = template_df[case_cols].sum()
template_df.dropna(axis=0, how='all', subset=case_cols,
                   inplace=True)
assert (col_sums == template_df[case_cols].sum()).all()

# write the data
filepath = root + r"filepath"
template_df.to_csv(filepath)
