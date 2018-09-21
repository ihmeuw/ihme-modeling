# -*- coding: utf-8 -*-
"""
exract the nonfatal cause name, icd code, and Bundle id from
spreadsheet

"""
import platform
import pandas as pd
import numpy as np
import datetime
import re
import sys
import getpass
from db_tools.ezfuncs import query

# load our functions
hosp_path = "FILEPATH"
sys.path.append(hosp_path)

from hosp_prep import create_ms_durations, sanitize_diagnoses

if platform.system() == "Linux":
    root = "FILEPATH"

else:
    root = "FILEPATH"
# %%
####################################################################
# UPDATE DOWNSTREAM MAP SWITCHES
####################################################################
update_ms_durations = False
remove_period = True
manually_fix_bundles = True

# read in mapping data
map_9 = pd.read_excel(root + r"FILEPATH",
                      sheetname="ICD9-map", converters={'icd_code': str})
if map_9.drop_duplicates().shape < map_9.shape:
    print("There are duplicated rows in this icd9 map")

map_10 = pd.read_excel(root + r"FILEPATH",
                       sheetname="ICD10_map", converters={'icd_code': str})
if map_9.drop_duplicates().shape < map_9.shape:
    print("There are duplicated rows in this icd10 map")

# %%
# rename
# in ICD 9
# me name level 3 has measure
# there is NO level 3 name
map_9.rename(columns={'ME name level 3': 'level 3 measure',
                      "Baby Sequelae  (nonfatal_cause_name)":
                      "nonfatal_cause_name"}, inplace=True)

# In ICD 10
# ME name level 3 has measure
map_10.drop('level 3 measure', axis=1, inplace=True)  # this had MEIDSs
map_10.rename(columns={'ME name level 3': 'level 3 measure'}, inplace=True)

# %%
# select just the columns we need for mapping
map_9 = map_9[['icd_code', 'nonfatal_cause_name',
               'Level1-Bundel ID', 'level 1 measure',
               'Level2-Bundel ID', 'level 2 measure',
               'Level3-Bundel ID', 'level 3 measure']]

map_10 = map_10[['icd_code', 'nonfatal_cause_name',
                 'Level1-Bundel ID', 'level 1 measure',
                 'Level2-Bundel ID', 'level 2 measure',
                 'Level3-Bundel ID', 'level 3 measure']]
# %%

assert set(map_10.columns).symmetric_difference(set(map_9.columns)) == set(),\
    "columns are not matched"

# add code system id
map_9['code_system_id'] = 1
map_10['code_system_id'] = 2

# append and make icd col names match
maps = map_9.append(map_10)
maps = maps.rename(columns={'icd_code': 'cause_code'})

# fix the bad bundle ID
maps.loc[maps['nonfatal_cause_name'].str.contains("inj_homicide_sexual"),
         'Level1-Bundel ID'] = 765
# %%
col_names = ['cause_code', 'nonfatal_cause_name', 'bundle1', 'measure1',
             'bundle2', 'measure2', 'bundle3', 'measure3', 'code_system_id']
test = maps.copy()
test.columns = col_names
test.nonfatal_cause_name = test.nonfatal_cause_name.astype(str).str.lower()

def check_nfc_bundle_relat():
    for level in ["1", "2", "3"]:
        for nfc in test.nonfatal_cause_name.unique():
            if test.loc[test.nonfatal_cause_name == nfc, 'bundle'+level].unique().size > 1:
                print("This nfc has multiple level " + level + " bundles ", nfc)
# %%
#  %%
# new part that puts all the levels into one column
################################
# reshape method
# maps[1], maps[2], maps[3] = [maps['cause_code'], maps['cause_code'], maps['cause_code']]
# df = maps.set_index(['cause_code', 'nonfatal_cause_name', 'Level1-Bundel ID',
#       'level 1 measure', 'Level2-Bundel ID', 'level 2 measure',
#       'Level3-Bundel ID', 'level 3 measure', 'code_system_id']).stack().reset_index()
# then rename level_x and 0 to level and cause code
####################################

# select one level at a time
map_level_1 = maps.drop(['level 2 measure', 'Level2-Bundel ID',
                         'level 3 measure', 'Level3-Bundel ID'], axis=1)
map_level_2 = maps.drop(['level 1 measure', 'Level1-Bundel ID',
                         'level 3 measure', 'Level3-Bundel ID'], axis=1)
map_level_3 = maps.drop(['level 1 measure', 'Level1-Bundel ID',
                         'level 2 measure', 'Level2-Bundel ID'], axis=1)
# add an indicator var for the level of ME
map_level_1['level'] = 1
map_level_2['level'] = 2
map_level_3['level'] = 3
# %%
# rename to match
# rename level 1
map_level_1.rename(columns={'level 1 measure': 'bid_measure',
                            'Level1-Bundel ID': 'bundle_id'}, inplace=True)

# rename level 2
map_level_2.rename(columns={'level 2 measure': 'bid_measure',
                            'Level2-Bundel ID': 'bundle_id'}, inplace=True)

# rename level 3
map_level_3.rename(columns={'level 3 measure': 'bid_measure',
                            'Level3-Bundel ID': 'bundle_id'}, inplace=True)

# fix measure for bundle_id 1010
map_level_3.loc[map_level_3.bundle_id == 1010, 'bid_measure'] = 'prev'
# %%
maps = pd.concat([map_level_1, map_level_2, map_level_3])

# add the osteo arthritis mapping for ICD 9
osteo_arth_icd = ['715.0', '715.1', '715.2', '715.3', '715.4', '715.5',
                  '715.6', '715.7', '715.8', '715.9']
# it looks like .4, .5 .6 and .7 don't exist as icd 9 codes
maps.loc[(maps.cause_code.isin(osteo_arth_icd)) & (maps.level == 2) &
         (maps.code_system_id == 1),
         ['bundle_id', 'bid_measure']] = [215, 'prev']

# %%
# for every column, force the datatypes of the values inside to be what they
# are expected to be

# make icd codes strings
maps['cause_code'] = maps['cause_code'].astype(str)

# make nonfatal_cause_name strings
maps['nonfatal_cause_name'] = maps['nonfatal_cause_name'].astype(str)

# convert baby sequela to lower case to fix some mapping errors
maps['nonfatal_cause_name'] = maps['nonfatal_cause_name'].str.lower()

# convert TBD bundle_ids (or anything else that isn't a number) & 0 to np.nan
maps['bundle_id'] = pd.to_numeric(maps['bundle_id'], errors='coerce',
                                  downcast='integer')
maps.loc[maps['bundle_id'] == 0, 'bundle_id'] = np.nan

# assert we aren't mixing me_ids and bundle ids
if not (maps['bundle_id'] > 1010).sum() == 0:  # bundle ids so far tend to be
        # less than 1000 in magnitude
    print("You're mixing bundle IDs and ME IDs")

# make bid_measure strings
maps['bid_measure'] = maps['bid_measure'].astype(str)
# make lower case
maps['bid_measure'] = maps['bid_measure'].str.lower()

# make ints smaller
maps['code_system_id'] = pd.to_numeric(maps['code_system_id'],
                                       errors='raise', downcast='integer')
maps['level'] = pd.to_numeric(maps['level'],
                              errors='raise', downcast='integer')

# clean ICD codes
# remove non-alphanumeric characters
if remove_period:
    maps['cause_code'] = maps['cause_code'].str.replace("\W", "")
# make sure all letters are capitalized
maps['cause_code'] = maps['cause_code'].str.upper()

# remove durations from bid_measure and make bid_durations column
maps['bid_measure'], maps['bid_duration'] = maps.bid_measure.str.split("(").str
maps['bid_duration'] = maps['bid_duration'].str.replace(")", "")
maps['bid_duration'] = pd.to_numeric(maps['bid_duration'], downcast='integer', errors='raise')

# %%
# reorder columns
cols_before = maps.columns
maps = maps[['cause_code', 'code_system_id', 'level', 'nonfatal_cause_name',
             'bundle_id','bid_measure', 'bid_duration']]
cols_after = maps.columns
assert set(cols_before) == set(cols_after),\
    "you dropped columns while changing the column order"

# %%
# CHECK MEASURE IN BIDS

if (maps.loc[(maps.bundle_id.notnull())&(maps.cause_code == "Q91"), 'bundle_id'] == 646).all():
    maps.loc[maps.cause_code == "Q91", ['bundle_id', 'bid_measure']] = [638, 'prev']
# %%
# NEW
# add an assert to break script if a bundle has more
# than one measure
mapG = maps.groupby('bundle_id')
for measure in mapG.bid_measure.unique():
    assert len(measure) == 1, "A bundle ID has multiple measures"

# %%
# does every BID have a non null measure
assert maps[(maps.bid_measure!='prev')&(maps.bid_measure!='inc')&(maps.bundle_id.notnull())].shape[0] == 0,\
    ("There is a bundle ID with an incorrect measure. Run the code above "
    "without .shape to find it.")

# ADD IN MORE DURATIONS
# goal is to get all durations in one place

# this is a deravation of the old durations map that they used in GBD2015
# then it got mapped from MEID to BID.
bid_durations = pd.read_excel(root + r"FILEPATH")

# drop measure
bid_durations.drop("measure", axis=1, inplace=True)

# no need to drop duplicates

# want to merge on, maybe look and see if any are different

# merge
maps = maps.merge(bid_durations, how='left', on='bundle_id')

# compare durations
before = maps[['bid_measure', 'bid_duration', 'duration']].drop_duplicates()

maps.loc[maps.bid_duration.isnull(), 'bid_duration'] = maps.loc[maps.duration.notnull(), 'duration']

# make any prev measures have null duration
maps.loc[maps.bid_measure == 'prev', 'bid_duration'] = np.nan

after = maps[['bid_measure', 'bid_duration', 'duration']].drop_duplicates()

maps.drop("duration", axis=1, inplace=True)
# %%
# check durations
print("checking if all inc BIDs have at least one non null duration")
assert maps.loc[(maps.bid_measure == 'inc')&(maps.bundle_id.notnull()), 'bid_duration'].notnull().all(),\
    "We expect that 'inc' BIDs only have Non-Null durations"

print("checking if all prev BIDs only have null durations")
assert maps.loc[maps.bid_measure == 'prev', 'bid_duration'].isnull().all(),\
    "We expect that 'prev' BIDs only have null durations"

# looks like it worked.

# but check if everything has one duration
print("checking any inc BIDs have more than one duration")
for bid in maps['bundle_id'].unique():
    if len(maps[(maps['bundle_id'] == bid)&(maps.bid_measure == 'inc')].bid_duration.unique()) > 1:
        print(bid, maps[(maps['bundle_id'] == bid)&(maps.bid_measure == 'inc')].bid_duration.unique())

########################################
# Get nfc measures onto the map
########################################

# read in baby sequela measures
nfc_measures = pd.read_excel(root +
                            "FILEPATH")

# keep and rename useful cols
nfc_measures = nfc_measures[["Baby Sequelae  (nonfatal_cause_name)",
                           "level 1 measure"]]
nfc_measures.rename(columns={"Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name",
                            "level 1 measure": "measure"}, inplace=True)
# drop null baby sequela
nfc_measures = nfc_measures[nfc_measures.nonfatal_cause_name.notnull()]
# convert to lower case
nfc_measures['nonfatal_cause_name'] = nfc_measures['nonfatal_cause_name'].str.lower()

# str split the measure col to extract durations from measures
nfc_measures['nfc_measure'], nfc_measures['duration'] = nfc_measures.measure.str.split("(").str
# drop durations and mixed type measure column
nfc_measures.drop(['duration', 'measure'], axis=1, inplace=True)

# drop duplicated baby sequela
nfc_measures.drop_duplicates(['nonfatal_cause_name'], inplace=True)

# confirm row count remains the same after merge
pre = maps.shape[0]
maps = maps.merge(nfc_measures, how='left', on=['nonfatal_cause_name'])
assert pre == maps.shape[0]

#####################################
# check measure
#####################################
# BIDs should only have 2 unique measures
assert len(maps[maps['bundle_id'].notnull()].bid_measure.unique()) == 2

# BIDs should not have more than 1 measure
for bid in maps['bundle_id'].unique():
    assert len(maps[maps['bundle_id']==bid].bid_measure.unique()) <= 1
# %%
####################################
# create nfc key
####################################
nfc_key = maps[['nonfatal_cause_name', 'nfc_measure']].copy()
nfc_key.drop_duplicates('nonfatal_cause_name', inplace=True)
nfc_key['nfc_id'] = np.arange(1, nfc_key.shape[0]+1, 1)
nfc_key.drop('nfc_measure', axis=1, inplace=True)

pre_shape = maps.shape[0]
maps = maps.merge(nfc_key, how='left', on=['nonfatal_cause_name'])
assert maps.shape[0] == pre_shape

if manually_fix_bundles:
    # we were instructed to drop these baby sequela
    # from the bundles they were assigned to. We'll do this manual fix until
    # an updated map is provided
    # drop a baby sequela from 121-endocartitis
    maps.loc[maps.nonfatal_cause_name == "sti, syphilis, tertiary (endocarditis)", 'bundle_id'] = np.nan
    # drop 2 baby sequela from bundle 618
    maps.loc[maps.nonfatal_cause_name == "cong, gu, hypospadias", 'bundle_id'] = np.nan
    maps.loc[maps.nonfatal_cause_name == "cong, gu, male genitalia", 'bundle_id'] = np.nan
    # drop a baby sequela from 131 Cirrhosis
    maps.loc[maps.nonfatal_cause_name == "neonatal, digest, other (cirrhosis)", 'bundle_id'] = np.nan

# get cause_id so we can apply cause restrictions
cause_id_query = "QUERY"
cause_id_info = query(cause_id_query, conn_def='epi')

pre = maps.shape[0]
maps= maps.merge(cause_id_info, how='left', on='bundle_id')
assert maps.shape[0] == pre

# %%
# next version will be '10'
map_vers = 9
maps['map_version'] = map_vers


# drop duplicates across ALL columns right before saving
maps = maps.drop_duplicates()
maps.to_csv(root + "FILEPATH",
            index=False)

maps.to_csv(root + "FILEPATH",
            index=False)

nfc_key['nfc_id_version'] = map_vers

# %%
# additionally, prep list / txt with all the bundle_ids in a nice format
all_bids = sorted(list(maps.loc[maps.bundle_id.notnull(), 'bundle_id'].unique()))
all_bids = [int(i) for i in all_bids]

my_string = "{bids}".format(bids=', '.join(str(bid) for bid in all_bids))

text = open(root + r"FILEPATH", "w")
text.write(my_string)
text.close()

if update_ms_durations == True:
    # this function will write a new durations file
    create_ms_durations(maps)
