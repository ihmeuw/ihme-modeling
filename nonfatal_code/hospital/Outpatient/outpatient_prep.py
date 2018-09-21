
# coding: utf-8

import pandas as pd
import numpy as np
from db_tools.ezfuncs import query
from db_queries import get_cause_metadata, get_population
import datetime
import os
import time
import re
import platform
pd.options.display.max_rows = 100
import datetime
import sys

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

hosp_path = "FILEPATH"
sys.path.append(hosp_path)

import hosp_prep

def drop_data_for_outpatient(df):
    """
    Keep only outpatient data from the USA and Sweden
    """

    print("###############################################################")
    print("                        DROPPING DATA")
    print("###############################################################")

    # facility type, exclusion stats
    # hospital : inpatient
    # day clinic : EXCLUDED
    # emergency : EXCLUDED
    # clinic in hospital : outpatient, but has different pattern than other Outpatient
    # outpatient clinic : outpatient
    # inpatient unknown : inpatient
    # outpatient unknown : outpatient

    # print how many rows we're starting wtih
    print("STARTING NUMBER OF ROWS = " + str(df.shape[0]))
    print("\n")

    # drop everything but outpatient
    print("DROPPING INPATIENT")
    df = df[(df['facility_id'] == 'outpatient unknown') |
            (df['facility_id'] == 'outpatient clinic') |
            (df['facility_id'] == 'clinic in hospital')]
    print("NUMBER OF ROWS = " + str(df.shape[0]))

    # drop canada data, they mix up inpatient/outpatient
    print("DROPPING CANADA DATA")
    df = df[df['source'] != 'CAN_NACRS_02_09']
    df = df[df['source'] != 'CAN_DAD_94_09']
    print("NUMBER OF ROWS = " + str(df.shape[0]))

    # drop data that is at national level for US
    print("DROPPING NATIONAL LEVEL USA DATA")
    df = df[df['location_id'] != 102]
    print("NUMBER OF ROWS = " + str(df.shape[0]))

    # check that we didn't somehow drop all rows
    assert df.shape[0] > 0, "All data was dropped, there are zero rows!"

    # DROP THE OTHER TWO
    print("DROPPING NORWAY AND BRAZIL")
    df = df[df.source != "NOR_NIPH_08_12"]
    df = df[df.source != 'BRA_SIA']
    df = df[df.source != 'PHL_HICC']
    print("NUMBER OF ROWS = " + str(df.shape[0]))

    print("DONE DROPPING DATA")

    return(df)


def outpatient_mapping(df):
    print("###############################################################")
    print("                           Mapping")
    print("###############################################################")

    maps = pd.read_csv(r"FILEPATH", dtype={'cause_code': object})

    maps = maps[['cause_code', 'bundle_id', 'code_system_id', 'bid_measure']]
    maps.rename(columns={'bid_measure': 'measure'}, inplace=True)

    # drop duplicate values
    maps = maps.drop_duplicates()

    # match on upper case
    df['cause_code'] = df['cause_code'].str.upper()
    maps['cause_code'] = maps['cause_code'].str.upper()

    # store variables for data check later
    before_values = df['cause_code'].value_counts()  # value counts before

    # merge the hospital data with excel spreadsheet maps
    df = df.merge(maps, how='left',
                  on=["cause_code", "code_system_id"])
    after_values = df['cause_code'].value_counts()  # value counts after

    # create data frame of rows without baby sequela
#     no_match_df = df[df.bundle_id.isnull()]
    # count how many rows didn't match
    no_match_count = df['bundle_id'].isnull().sum()
#     no_match_count = float(no_match_df.shape[0])
    no_match_per = round(no_match_count/df.shape[0] * 100, 4)
    print("###\n" + str(no_match_count) + " rows did not match the map\n###\n" +
    "This is " + str(no_match_per) + "% of total rows that did not match\n###")

    # drop missing bundle_ids
    print("dropping rows without bundles...")
    df = df[df.bundle_id.notnull()]
#     df.drop('diagnosis_id', axis=1, inplace=True)
    print df.columns
    groups = ['location_id', 'year_start', 'year_end', 'age_group_unit',
              'age_start', 'age_end', 'sex_id', 'source', 'nid',
              'facility_id', 'representative_id',
              'metric_id', 'bundle_id', 'measure']
    print("performing groupby...")
    df = df.groupby(groups).agg({'val': 'sum'}).reset_index()

    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
                'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
                'metric_id', 'bundle_id']
    float_cols = ['val']
    str_cols = ['source', 'facility_id']

    print("compressing...")
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='float')
    for col in str_cols:
        df[col] = df[col].astype(str)

    print("Done Mapping")

    return(df)


def get_parent_injuries(df):
    """
    roll up the child causes into parents add parent injuries
    """

    pc_injuries = pd.read_csv(root + r"FILEPATH")
    # take only parent causes from map
    # pc_injuries = pc_injuries[pc_injuries.parent==1]
    # create BID and ME ID dicts for parent causes
    me_dict = dict(zip(pc_injuries.e_code, pc_injuries['level1_meid']))
    bundle_dict = dict(zip(pc_injuries.e_code, pc_injuries['Level1-Bundle ID']))

    # loop over ecodes pulling every parent name
    df_list = []
    for parent in pc_injuries.loc[pc_injuries['parent']==1, 'e_code']:
        inj_df = pc_injuries[(pc_injuries['baby sequela'].str.contains(parent)) & (pc_injuries['parent'] != 1)]
        inj_df = inj_df[inj_df['Level1-Bundle ID'].notnull()]
        inj_df['me_id'] = me_dict[parent]
        inj_df['bundle_id'] = bundle_dict[parent]
        # inj_df['nonfatal_cause_name'] = parent
        df_list.append(inj_df)

    parent_df = pd.concat(df_list)

    # number of child injuries in csv should match new df
    assert pc_injuries.child.sum() == parent_df.child.sum(),        "sum of child causes doesn't match sum of parent causes"

    parent_df.drop(['baby sequela', 'ME name level 1', 'level 1 measure',
                    'e_code', 'parent', 'child'], axis=1,
                    inplace=True)

    parent_df.rename(columns={'me_id': 'parent_me_id',
                              'level1_meid': 'modelable_entity_id',  # child me_id
                              'bundle_id': 'parent_bundle_id',
                              'Level1-Bundle ID': 'bundle_id'},  # child bundle_id
                     inplace=True)

    # reorder cuz it's hard to look at
    col_before = parent_df.columns
    parent_df = parent_df[['modelable_entity_id', 'parent_me_id', 'bundle_id', 'parent_bundle_id']]
    assert set(col_before) == set(parent_df.columns), 'you dropped a column while reordering columns'


    # duplicate data for parent injuries
    parent_df = parent_df.merge(df, how='left', on='bundle_id')

    # drop child ME and bundle IDs
    parent_df.drop(['modelable_entity_id', 'bundle_id'], axis=1, inplace=True)
    # add parent bundle and ME IDs
    parent_df.rename(columns={'parent_me_id': 'modelable_entity_id',
                              'parent_bundle_id': 'bundle_id'}, inplace=True)

    parent_df.drop('modelable_entity_id', axis=1, inplace=True)

    assert set(parent_df.columns).symmetric_difference(set(df.columns)) == set()

    # rbind duped parent data back onto hospital data
    df = pd.concat([df, parent_df])

    df = df[df.age_start.notnull()]
    return(df)


def apply_outpatient_correction(df):
# load corrections
    corrections = pd.read_csv("FILEPATH")

    corrections = corrections.drop(['indv_cf', 'incidence', 'prevalence', 'injury_cf'], axis=1)

    corrections = corrections.rename(columns={'outpatient': 'smoothed_value'})
    corrections.rename(columns={'sex': 'sex_id'}, inplace=True)

    corrections.update(corrections['smoothed_value'].fillna(1))  # age and sex restricted cfs are null, should become one. data would be zero anyways

    assert not corrections.smoothed_value.isnull().any(), "shouldn't be any nulls in smoothed_value"

    corrections.loc[corrections.smoothed_value > 1, 'smoothed_value'] = 1

    pre_shape = df.shape[0]
    df = df.merge(corrections, how='left', on =['age_start', 'sex_id', 'bundle_id'])
    assert pre_shape == df.shape[0], "merge somehow added rows"
    print "these bundles didn't get corrections", sorted(df.loc[df.smoothed_value.isnull(), 'bundle_id'].unique()), " so we made their CF 1"
#     print df.isnull().sum()
    df.update(df['smoothed_value'].fillna(1))
    assert not df.isnull().any().any(), "there are nulls!"

    # apply correction
    df['val_corrected'] = df['val'] * df['smoothed_value']

    # drop correction
    df.drop("smoothed_value", axis=1, inplace=True)

    return(df)


def outpatient_restrictions(df):
    print("###############################################################")
    print("               Applying age and sex restrictions")
    print("###############################################################")

    cause = pd.read_excel(root + r"FILEPATH", sheetname='Sheet1')
    cause = cause[['bundle_id', 'male', 'female', 'yld_age_start', 'yld_age_end']].copy()
    cause = cause.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.
    cause['yld_age_start'].loc[cause['yld_age_start'] < 1] = 0

    # merge get_cause_metadata onto hospital using cause_id map
    pre_cause = df.shape[0]
    df = df.merge(cause, how='left', on = 'bundle_id')
    assert pre_cause == df.shape[0], "The merge duplicated rows unexpectedly"

    # set mean to zero where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'val'] = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'val_corrected'] = 0

    # set mean to zero where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'val'] = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'val_corrected'] = 0

    # set mean to zero where age end is smaller than yld age start
    df.loc[df['age_end'] < df['yld_age_start'], 'val'] = 0
    df.loc[df['age_end'] < df['yld_age_start'], 'val_corrected'] = 0

    # set mean to zero where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], 'val'] = 0
    df.loc[df['age_start'] > df['yld_age_end'], 'val_corrected'] = 0

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1, inplace=True)
    print("\n")
    print("Done with Restrictions")

    return(df)


def get_sample_size(df):
    print("###############################################################")
    print("                    Getting sample size")
    print("###############################################################")

    # pull age_group to age_start/age_end map
    age_group = query("SQL QUERY")
    # correct age groups
    age_group.loc[age_group['age_group_years_end'] > 1, 'age_group_years_end'] = age_group.loc[age_group['age_group_years_end'] > 1, 'age_group_years_end'] - 1

    age_group.rename(columns={'age_group_years_start': 'age_start',
                              'age_group_years_end': 'age_end'}, inplace=True)
    age_group = age_group[age_group.age_group_id != 161]  # this id is not in the population table

    df = df.merge(age_group, how='left', on=['age_start', 'age_end'])
    df.loc[df.age_end == 99, 'age_group_id'] = 235

    pop = get_population(age_group_id=[28, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31],
                         location_id=list(df.location_id.unique()),
                         sex_id=[1,2],
                         year_id=list(df.year_start.unique()))

    # # rename pop columns to match hospital data columns
    pop.rename(columns={'year_id': 'year_start'}, inplace=True)
    pop['year_end'] = pop['year_start']
    pop.drop("process_version_map_id", axis=1, inplace=True)

    demography = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id']

    pre_shape = df.shape[0]  # store for before comparison
    # then merge population onto the hospital data

    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert pre_shape == df.shape[0], "number of rows don't match after merge"

    df.drop('age_group_id', axis=1, inplace=True)

    print("Done getting sample size")

    return(df)

def outpatient_elmo(df):
    print("###############################################################")
    print("                   Starting ELMO formatting")
    print("###############################################################")

    # drop
    df = df.drop(['source', 'facility_id', 'metric_id'], axis=1)
    # rename columns for ELMO reqs
    df.rename(columns={'representative_id': 'representative_name',
                       'val_corrected': 'cases',
                       'val': 'cases_uncorrected',
                       'population': 'sample_size',
                       'sex_id': 'sex'},
              inplace=True)

    # adjust min age_end to 0.999 instead of 1
    df.loc[df['age_start'] == 0, 'age_end'] = 0.999

    # make dictionary for replacing representative id with representative name
    representative_dictionary = {-1: "Not Set",
                                 0: "Unknown",
                                 1: "Nationally representative only",
                                 2: "Representative for subnational " +
                                 "location only",
                                 3: "Not representative",
                                 4: "Nationally and subnationally " +
                                 "representative",
                                 5: "Nationally and urban/rural " +
                                 "representative",
                                 6: "Nationally, subnationally and " +
                                 "urban/rural representative",
                                 7: "Representative for subnational " +
                                 "location and below",
                                 8: "Representative for subnational " +
                                 "location and urban/rural",
                                 9: "Representative for subnational " +
                                 "location, urban/rural and below",
                                 10: "Representative of urban areas only",
                                 11: "Representative of rural areas only"}
    df.replace({'representative_name': representative_dictionary},
               inplace=True)

    # add elmo reqs
    df['source_type'] = 'Facility - outpatient'
    df['urbanicity_type'] = 'Unknown'
    df['recall_type'] = 'Not Set'
    df['unit_type'] = 'Person'
    df['unit_value_as_published'] = 1
    df['is_outlier'] = 0
    df['sex'].replace([1, 2], ['Male', 'Female'], inplace=True)
    df['measure'].replace(["prev", "inc"], ["prevalence", "incidence"], inplace=True)

    df['mean'] = np.nan
    df['upper'] = np.nan
    df['lower'] = np.nan
    df['seq'] = np.nan  # marked as auto
    df['underlying_nid'] = np.nan  # marked as optional
    df['sampling_type'] = np.nan  # marked as optional
    df['recall_type_value'] = np.nan  # marked as optional
    df['uncertainty_type'] = np.nan  # marked as auto
    df['uncertainty_type_value'] = np.nan  # marked as optional
    df['input_type'] = np.nan
    df['standard_error'] = np.nan  # marked as auto
    df['effective_sample_size'] = np.nan
    df['design_effect'] = np.nan  # marked as optional
    df['response_rate'] = np.nan
    df['extractor'] = "USER and USER"

    # human readable location names
    loc_map = query(SQL QUERY)
    df = df.merge(loc_map, how='left', on='location_id')

    # add bundle_name
    bundle_name_df = query(SQL QUERY)

    df = df.merge(bundle_name_df, how="left", on="bundle_id")

    print("DONE WITH ELMO")
    return(df)


df_orig = pd.read_csv(r"FILEPATH", dtype={'cause_code': object})

# ** copy df_orig **
df = df_orig.copy()

# ** drop data **
df = df[df.source.isin(['SWE_PATIENT_REGISTRY_98_12', 'USA_NAMCS'])]

df = drop_data_for_outpatient(df)

df = df[df.source.isin(['SWE_PATIENT_REGISTRY_98_12', 'USA_NAMCS'])]

# ** mapping **
df = outpatient_mapping(df)

# ** parent inj **
df = get_parent_injuries(df)

hosp_prep.check_parent_injuries(df, col_to_sum='val')

# ** correction factors **
df = apply_outpatient_correction(df)

# ** age sex restrictions **
df = outpatient_restrictions(df)

# # test making square
df = df[df.val != 0]

ages = hosp_prep.get_hospital_age_groups()

df = df.merge(ages, how='left', on=['age_start', 'age_end'])

df_square = hosp_prep.make_zeroes(df, level_of_analysis='bundle_id',
                             cols_to_square=['val', 'val_corrected'], icd_len=5)


np.abs(df.val.sum() - df_square.val.sum())

pre_check_val = df['val'].sort_values().reset_index(drop=True)
post_check_val = df_square.loc[df_square['val'] > 0, 'val'].sort_values().reset_index(drop=True)

merged = df_square.merge(df, how='left', on=['age_start', 'age_end', 'sex_id', 'location_id', 'year_start', 'source', 'nid',
                                            'age_group_id', 'bundle_id', 'year_end', 'facility_id', 'representative_id',
                                            ])


(merged.loc[merged.val_y.notnull(), 'val_y'] == merged.loc[merged.val_y.notnull(), 'val_x']).sum()

(merged.loc[merged.val_y.notnull(), 'val_y'] == merged.loc[merged.val_y.notnull(), 'val_x']).all()

np.abs(post_check_val - pre_check_val).sum()

check = pd.DataFrame({"pre": pre_check_val, "post": post_check_val})

check['diff']  = check.pre - check.post

# ** fix nans **
df_square['age_group_unit'] = 1
df_square['metric_id'] = 1
df_square.drop('measure', axis=1, inplace=True)

# ** recover measure **
maps = pd.read_csv(r"FILEPATH", dtype={'cause_code': object})

maps = maps[['bundle_id', 'bid_measure']].drop_duplicates()
maps.rename(columns={'bid_measure': 'measure'}, inplace=True)


df_square = df_square.merge(maps, how='left', on='bundle_id')

df_square.loc[df_square.measure.isnull(), 'measure'] = 'inc'


df_square.drop("age_group_id", axis=1, inplace=True)

df_square = get_sample_size(df_square)

done = outpatient_elmo(df_square)

done[done[['age_start', 'age_end', 'year_start', 'year_end', 'location_id', 'sex', 'bundle_id']].duplicated(keep=False)].shape[0]

done.to_hdf(r"FILEPATH", key='df', complib='blosc', complevel=5, mode='w')

done.to_csv(r"FILEPATH", index=False, encoding='utf-8')

done = pd.read_hdf(r"FILEPATH", key='df')

def outpatient_write_bundles(df):
    # CAUSE INFORMATION
    # get cause_id so we can write to an acause
    # have to go through cause_id to get to a relationship between BID & acause
    cause_id_info = query("QUERY")
    # get acause
    acause_info = query("QUERY")
    # merge acause, bid, cause_id info together
    acause_info = acause_info.merge(cause_id_info, how="left", on="cause_id")

    # REI INFORMATION
    # get rei_id so we can write to a rei
    rei_id_info = query("QUERY")
    # get rei
    rei_info = query("QUERY")
    # merge rei, bid, rei_id together into one dataframe
    rei_info = rei_info.merge(rei_id_info, how="left", on="rei_id")

    #COMBINE REI AND ACAUSE
    # rename acause to match
    acause_info.rename(columns={'cause_id': 'cause_rei_id',
                                'acause': 'acause_rei'}, inplace=True)
    # rename rei to match
    rei_info.rename(columns={'rei_id': 'cause_rei_id',
                             'rei': 'acause_rei'}, inplace=True)

    # concat rei and acause together
    folder_info = pd.concat([acause_info, rei_info])

    # drop rows that don't have bundle_ids
    folder_info = folder_info.dropna(subset=['bundle_id'])

    # drop cause_rei_id, because we don't need it for getting data into folders
    folder_info.drop("cause_rei_id", axis=1, inplace=True)

    # drop duplicates, just in case there are any
    folder_info.drop_duplicates(inplace=True)

    # MERGE ACAUSE/REI COMBO COLUMN ONTO DATA BY BUNDLE ID
    # there are NO null acause_rei entries!
    df = df.merge(folder_info, how="left", on="bundle_id")

    start = time.time()
    bundle_ids = df['bundle_id'].unique()

    # prevalence, indicence should be lower case
    df['measure'] = df['measure'].str.lower()

    # arrange columns
    columns_before = df.columns
    ordered = ['bundle_id', 'bundle_name', 'measure',
               'location_id', 'location_name', 'year_start', 'year_end',
               'age_start',
               'age_end', 'age_group_unit', 'sex', 'nid',
               'representative_name', 'cases_uncorrected',
               'cases',
               'sample_size',
               'mean', 'upper', 'lower',
               'source_type', 'urbanicity_type',
               'recall_type', 'unit_type', 'unit_value_as_published',
               'is_outlier', 'seq', 'underlying_nid',
               'sampling_type', 'recall_type_value', 'uncertainty_type',
               'uncertainty_type_value', 'input_type', 'standard_error',
               'effective_sample_size', 'design_effect', 'response_rate',
               'extractor', 'acause_rei']
    df = df[ordered]
    columns_after = df.columns
    print "are they equal?", set(columns_after) == set(columns_before)
    print "what's the difference?", set(columns_after).symmetric_difference(set(columns_after))
    print 'before minus after:', set(columns_before) - set(columns_after)
    print 'after minus before:', set(columns_after) - set(columns_before)
    assert set(columns_after) == set(columns_before), "you added/lost columns while reordering columns!!"
    failed_bundles = []  # initialize empty list to append to in this for loop
    print "WRITING FILES"
    for bundle in bundle_ids:
        # subset bundle data
        df_b = df[df['bundle_id'] == bundle].copy()

        acause_rei = str(df_b.acause_rei.unique()[0])
        df_b.drop('acause_rei', axis=1, inplace=True)

        writedir = r"FILEPATH"

        if not os.path.isdir(writedir):
            os.makedirs(writedir)  # make the directory if it does not exist

        # write for modelers
        # make path
        vers_id = "v5"
        date = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD
        bundle_path = "FILEPATH"

        # try to write to modelers' folders
        try:
            writer = pd.ExcelWriter(bundle_path, engine='xlsxwriter')
            df_b.to_excel(writer, sheet_name="extraction", index=False)
            writer.save()
        except:
            failed_bundles.append(bundle)  # if it fails for any reason
            # make note of it
    end = time.time()

    text = open("FILEPATH")
    text.write("function: write_bundles " + "\n"+
               "start time: " + str(start) + "\n" +
               " end time: " + str(end) + "\n" +
               " run time: " + str((end-start)/60.0) + " minutes")
    text.close()
    return(failed_bundles)

print "starting at {}".format(datetime.datetime.today().strftime("%X"))
failed_to_write = outpatient_write_bundles(done)
print "finished at {}".format(datetime.datetime.today().strftime("%X"))

failed_to_write

bids = [271, 367, 766, 484, 366, 765, 276]

pc_injuries[pc_injuries['Level1-Bundle ID'].isin(bids)]

get_ipython().magic(u'run ez_pz.py')

bundle_location([271, 367, 766, 484, 366, 765, 276])

# # Inspecting final data
done = pd.read_csv(r"FILEPATH")

done[['year_start', 'location_id']].drop_duplicates().shape

done[done.cases_uncorrected > done.sample_size].age_start.value_counts().sort_index()

done[done.cases_uncorrected > done.sample_size].location_name.value_counts()

sorted(done[done.cases_uncorrected > done.sample_size].bundle_name.unique())
