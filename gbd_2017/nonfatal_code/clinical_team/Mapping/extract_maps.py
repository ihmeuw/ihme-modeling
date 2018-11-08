# -*- coding: utf-8 -*-
"""

"""
import platform
import pandas as pd
import numpy as np
import datetime
import re
import sys
import getpass
from db_tools.ezfuncs import query

# updates
prep_path = r"FILEPATH/Functions"
sys.path.append(prep_path)

from hosp_prep import create_ms_durations, sanitize_diagnoses

if platform.system() == "Linux":
    root = r"FILEPATH/j"

else:
    root = "J:"


def dupe_icd_checker(df):
    """
    Make sure that every processed ICD code maps to only 1 baby sequelae

        df: Pandas DataFrame of map for a single ICD system
    """
    assert df.code_system_id.unique().size == 1,\
        "This function can only process one ICD system at a time"
    test = df[['icd_code', 'nonfatal_cause_name']].copy()
    test['icd_code'] = test['icd_code'].str.upper()
    test['icd_code'] = test['icd_code'].str.replace("\W", "")
    test = test.drop_duplicates()
    assert test.shape[0] == test.icd_code.unique().size,\
        "There are duplicated icd code/baby sequelae \n {}".\
        format(test[test.icd_code.duplicated(keep=False)])
    return

def extract_maps(update_ms_durations=False, remove_period=True,
    manually_fix_bundles=True,
    update_restrictions=False):

    maps_folder = (r"FILEPATH")

    # read in mapping data
    map_9 = pd.read_excel("{FILEPATH",
                      sheetname="ICD9-map", converters={'icd_code': str})
    if map_9.drop_duplicates().shape[0] < map_9.shape[0]:
        print("There are duplicated rows in this icd9 map")

    map_10_path = "FILEPATH".format(maps_folder)
    map_10 = pd.read_excel(map_10_path,
                           sheetname="ICD10_map", converters={'icd_code': str})
    if map_10.drop_duplicates().shape[0] < map_10.shape[0]:
        print("There are duplicated rows in this icd10 map")

    map_9.rename(columns={'ME name level 3': 'level 3 measure',
                          "Baby Sequelae  (nonfatal_cause_name)":
                          "nonfatal_cause_name"}, inplace=True)

    map_9 = map_9[['icd_code', 'nonfatal_cause_name',
                   'Level1-Bundel ID', 'level 1 measure',
                   'Level2-Bundel ID', 'level 2 measure',
                   'Level3-Bundel ID', 'level 3 measure']]

    map_10 = map_10[['icd_code', 'nonfatal_cause_name',
                     'Level1-Bundel ID', 'level 1 measure',
                     'Level2-Bundel ID', 'level 2 measure',
                     'Level3-Bundel ID', 'level 3 measure']]

    assert set(map_10.columns).symmetric_difference(set(map_9.columns)) == set(),\
        "columns are not matched"

    # add code system id
    map_9['code_system_id'] = 1
    map_10['code_system_id'] = 2

    # verify there aren't duped ICDs going to multiple baby seq
    dupe_icd_checker(map_9)
    dupe_icd_checker(map_10)

    # append and make icd col names match
    maps = map_9.append(map_10)
    maps = maps.rename(columns={'icd_code': 'cause_code'})

    # removing rows with null ICD codes
    null_rows = maps.cause_code.isnull().sum()
    if null_rows > 0:
        maps = maps[maps.cause_code.notnull()]
        print("Dropping {} row(s) because of NA / NAN cause code".format(null_rows))
        
    # fix the bad bundle ID
    maps.loc[maps['nonfatal_cause_name'].str.contains("inj_homicide_sexual"),
             'Level1-Bundel ID'] = 765
    
    maps.loc[maps['Level1-Bundel ID'] == 2872, 'Level1-Bundel ID'] = 2972


    def check_nfc_bundle_relat(maps):
        """
        loop over every nfc and bundle level to verify that each nfc has only
        1 unique bundle ID per level. If an nfc has more than 1 bundle for a
        single level then print that nfc name and break on an assert after the
        entire loop has run
        """
        col_names = ['cause_code', 'nonfatal_cause_name', 'bundle1', 'measure1',
             'bundle2', 'measure2', 'bundle3', 'measure3', 'code_system_id']
        
        
        test = maps.copy()
        test.columns = col_names
        test.nonfatal_cause_name = test.nonfatal_cause_name.astype(str).str.lower()

        bad_nfc = []  # ticker to count the number of bad NFC
        for level in ["1", "2", "3"]:
            for nfc in test.nonfatal_cause_name.unique():
                if test.loc[test.nonfatal_cause_name == nfc, 'bundle'+level].unique().size > 1:
                    print("This nfc has multiple level " + level + " bundles ", nfc)
                    bad_nfc.append(nfc)  # add to the list cause there's a bad one
        print("NFC - bundle ID check finished")
        return bad_nfc
    
    if  maps.loc[maps.nonfatal_cause_name.str.contains("Liver, chronic, etoh, cirrhosis"), 'Level1-Bundel ID'].\
            unique().tolist() == ["_none", 131]:
        maps.loc[maps.cause_code == "571.2", ['Level1-Bundel ID', 'level 1 measure']] = [131, "prev"]
   
    old_nfc = 'Cong, gu, kidney malformation'
    new_nfc = 'Cong, gu, kidney malformation (polycystic)'     
    maps.loc[(maps['Level1-Bundel ID'] == 3227) & (maps.nonfatal_cause_name == old_nfc), 
            'nonfatal_cause_name'] = new_nfc
   
    maps.loc[maps.nonfatal_cause_name == 'Hemog, thal (total)', ['Level2-Bundel ID', 'level 2 measure']] = [3008, 'prev']


    assert 3227 not in maps.loc[(maps.nonfatal_cause_name == old_nfc), 'Level1-Bundel ID'].\
            unique().tolist(), "There is a mismatch of bundle ids for nfc Cong, gu, kidney malformation"

    bad_ones = check_nfc_bundle_relat(maps)

    # return (maps, bad_ones)
    assert len(bad_ones) == 0, "There are baby seqeulae with more than 1 "\
        r"bundle id. This will lead to mapping differences. Review the var "\
        r"'bad_ones' for these baby sequelae"

    if update_restrictions:
        update_restrictions_file()

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
    map_level_3.loc[map_level_3.bundle_id == 1010, 'bid_measure'] = 'prev'  # cuz idk its duration

    maps = pd.concat([map_level_1, map_level_2, map_level_3])

    osteo_arth_icd = maps.loc[maps.cause_code.str.contains("^715.\d"), 'cause_code'].unique().tolist()

    assert pd.Series(osteo_arth_icd).apply(len).min() > 3,\
        "There seems to be a 3 digit icd code present in the data to map to all OA"

    to_overwrite = maps.loc[(maps.cause_code.isin(osteo_arth_icd)) & (maps.level == 2) &
             (maps.code_system_id == 1),
             'bundle_id']

    # zeros will be made into null bundle IDs
    to_overwrite.loc[to_overwrite == 0] = np.nan
    assert to_overwrite.notnull().sum() == 0,\
        "There is something that will be overwritten and it's this {}".format(to_overwrite.unique())

    # update the map to include dummy bundle 215 for ALL OA
    maps.loc[(maps.cause_code.isin(osteo_arth_icd)) & (maps.level == 2) &
             (maps.code_system_id == 1),
             ['bundle_id', 'bid_measure']] = [215, 'prev']

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
    if not (maps['bundle_id'] > 1010).sum() == 0:  
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


    # reorder columns
    cols_before = maps.columns
    maps = maps[['cause_code', 'code_system_id', 'level', 'nonfatal_cause_name',
                 'bundle_id','bid_measure', 'bid_duration']]
    cols_after = maps.columns
    assert set(cols_before) == set(cols_after),\
        "you dropped columns while changing the column order"
    
    if (maps.loc[(maps.bundle_id.notnull()) & (maps.cause_code == "Q91") &\
                (maps.level == 1), 'bundle_id'] == 646).all():
        
        maps.loc[(maps.cause_code == "Q91") & (maps.level == 1),
                 ['bundle_id', 'bid_measure']] = [638, 'prev']
        
    if '500' in maps.loc[maps.bundle_id == 502, 'bid_measure'].unique():
        maps.loc[(maps.bundle_id == 502) & (maps.level == 3), 'bid_measure'] = 'prev'
        assert maps.loc[maps.bundle_id == 502, 'bid_measure'].unique().size == 1

    mapG = maps.groupby('bundle_id')

    gi_prev = ['upper gi chronic', 'upper gi other']
    maps.loc[(maps.bundle_id == 3260) & (maps.bid_measure.isin(gi_prev)), 'bid_measure'] = 'prev'
    maps.loc[(maps.bundle_id == 3263) & (maps.bid_measure == 'upper gi acute'), 'bid_measure'] = 'inc'

    maps.loc[maps.bundle_id == 502, 'bid_measure'] = 'prev'

    measures = set(maps[maps.bundle_id == 3039].bid_measure)
    if 'prev' in measures and 'inc' not in measures:
        maps.loc[maps.bundle_id == 3039, 'bid_measure'] = 'prev'
    
    maps.loc[maps.bundle_id == 269, 'bundle_id'] = 3047

    for measure in mapG.bid_measure.unique():
        assert len(measure) == 1, "A bundle ID has multiple measures"
    
    assert maps[(maps.bid_measure!='prev')&(maps.bid_measure!='inc')&(maps.bundle_id.notnull())].shape[0] == 0,\
        (r"There is a bundle ID with an incorrect measure. Run the code above "
        "without .shape to find it.")

    bid_durations = pd.read_excel("FILEPATH")

    # drop measure
    bid_durations.drop("measure", axis=1, inplace=True)

    # no need to drop duplicates

    # merge
    maps = maps.merge(bid_durations, how='left', on='bundle_id')

    # compare durations
    before = maps[['bid_measure', 'bid_duration', 'duration']].drop_duplicates()

    maps.loc[maps.bid_duration.isnull(), 'bid_duration'] = maps.loc[maps.duration.notnull(), 'duration']

    # make any prev measures have null duration
    maps.loc[maps.bid_measure == 'prev', 'bid_duration'] = np.nan

    after = maps[['bid_measure', 'bid_duration', 'duration']].drop_duplicates()

    maps.drop("duration", axis=1, inplace=True)

    # check durations
    print("checking if all inc BIDs have at least one non null duration")

    maps.loc[maps.bundle_id == 2996, 'bid_duration'] = 28

    maps.loc[maps.bundle_id == 3263, 'bid_duration'] = 60

    assert maps.loc[(maps.bid_measure == 'inc')&(maps.bundle_id.notnull()), 'bid_duration'].notnull().all(),\
        "We expect that 'inc' BIDs only have Non-Null durations"

    assert len(maps.loc[(maps.bid_measure == 'inc')&(maps.bundle_id.notnull()), 'bid_duration'].unique()),\
       "We expect that 'inc' BIDs only have Non-Null durations"

    print("checking if all prev BIDs only have null durations")
    assert maps.loc[maps.bid_measure == 'prev', 'bid_duration'].isnull().all(),\
        "We expect that 'prev' BIDs only have null durations"

    # read in baby sequela measures
    bs_measures = pd.read_excel("FILEPATH")

    # keep and rename useful cols
    bs_measures = bs_measures[["Baby Sequelae  (nonfatal_cause_name)",
                               "level 1 measure"]]
    bs_measures.rename(columns={"Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name",
                                "level 1 measure": "measure"}, inplace=True)
    # drop null baby sequela
    bs_measures = bs_measures[bs_measures.nonfatal_cause_name.notnull()]
    # convert to lower case
    bs_measures['nonfatal_cause_name'] = bs_measures['nonfatal_cause_name'].str.lower()

    # str split the measure col to extract durations from measures
    bs_measures['bs_measure'], bs_measures['duration'] = bs_measures.measure.str.split("(").str
    # drop durations and mixed type measure column
    bs_measures.drop(['duration', 'measure'], axis=1, inplace=True)

    # drop duplicated baby sequela
    bs_measures.drop_duplicates(['nonfatal_cause_name'], inplace=True)

    # confirm row count remains the same after merge
    pre = maps.shape[0]
    maps = maps.merge(bs_measures, how='left', on=['nonfatal_cause_name'])
    assert pre == maps.shape[0]

    #####################################
    # check measure
    #####################################
    # BIDs should only have 2 unique measures
    assert len(maps[maps['bundle_id'].notnull()].bid_measure.unique()) == 2

    # BIDs should not have more than 1 measure
    for bid in maps['bundle_id'].unique():
        assert len(maps[maps['bundle_id']==bid].bid_measure.unique()) <= 1
    #
    ####################################
    # create bs key
    ####################################
    bs_key = maps[['nonfatal_cause_name', 'bs_measure']].copy()
    bs_key.drop_duplicates('nonfatal_cause_name', inplace=True)
    bs_key['bs_id'] = np.arange(1, bs_key.shape[0]+1, 1)
    bs_key.drop('bs_measure', axis=1, inplace=True)

    pre_shape = maps.shape[0]
    maps = maps.merge(bs_key, how='left', on=['nonfatal_cause_name'])
    assert maps.shape[0] == pre_shape

    if manually_fix_bundles:

        # drop a baby sequela from 121-endocartitis
        maps.loc[maps.nonfatal_cause_name == "sti, syphilis, tertiary (endocarditis)", 'bundle_id'] = np.nan
        # drop 2 baby sequela from bundle 618
        maps.loc[maps.nonfatal_cause_name == "cong, gu, hypospadias", 'bundle_id'] = np.nan
        maps.loc[maps.nonfatal_cause_name == "cong, gu, male genitalia", 'bundle_id'] = np.nan
        # drop a baby sequela from 131 Cirrhosis
        maps.loc[maps.nonfatal_cause_name == "neonatal, digest, other (cirrhosis)", 'bundle_id'] = np.nan

    # get cause_id so we can apply cause restrictions
    cause_id_query = "QUERY"
    cause_id_info = query(cause_id_query)

    pre = maps.shape[0]
    maps= maps.merge(cause_id_info, how='left', on='bundle_id')
    assert maps.shape[0] == pre

    maps = fix_deprecated_bundles(maps)

    # next version will be '18'
    map_vers = 17
    maps['map_version'] = map_vers

    # drop duplicates across ALL columns right before saving
    maps = maps.drop_duplicates()
    maps.to_csv("FILEPATH",index=False)

    maps.to_csv("FILEPATH", index=False)

    bs_key['bs_id_version'] = map_vers

    # additionally, prep list / txt with all the bundle_ids in a nice format
    all_bids = sorted(list(maps.loc[maps.bundle_id.notnull(), 'bundle_id'].unique()))
    all_bids = [int(i) for i in all_bids]

    my_string = "{bids}".format(bids=', '.join(str(bid) for bid in all_bids))

    text = open("FILEPATH", "w")
    text.write(my_string)
    text.close()

    if update_ms_durations == True:
        # this function will write a new durations file to J:WORK...
        create_ms_durations(maps)
    return

def update_restrictions_file(map_10_path, maps):
    """
    Update the age and sex restrictions for baby sequelae. This writes over the
    'FILEPATH' file and the 'FILEPATH' file
    
    Parameters:
        map_10_path(str): this contains the filepath to the icd 10 map which
        contains the restrictions on one of its sheets
        
        maps(Pandas DataFrame): Our finalized clean_maps, used to get map
        version and to merge restrictions onto bundle IDs
    """
    # get the map version
    map_vers = int(maps.map_version.unique())


    restrict = pd.read_excel(map_10_path,
                   sheetname="Lookup8", converters={'icd_code': str},
                   # select the exact columns with the restrictions
                   parse_cols = "BP, BT, BU, BV, BW")

    raw = "Baby Sequelae  (nonfatal_cause_name)"
    new = "nonfatal_cause_name"
    restrict.rename(columns={raw: new}, inplace=True)
    restrict.nonfatal_cause_name = restrict.nonfatal_cause_name.str.lower()
    restrict.sample(5)
    
    # drop all the duplicate null values
    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict.drop_duplicates(inplace=True)

    # if 0.1 or data below 1 is present in yld_age_start set that age to 0
    restrict.loc[restrict.yld_age_start < 1, 'yld_age_start'] = 0

    # there are some baby sequelae that are duplicated due to the new hierarchy
    # we'll duplicate rows if we keep these in, so just drop the more restrictive ones
    restrict = restrict[(restrict.nonfatal_cause_name != "cancer, liver") |
                        (restrict.yld_age_start < 15)]
    restrict = restrict[(restrict.nonfatal_cause_name != "other benign and in situ neoplasms") |
                        (restrict.yld_age_start < 15)]

    # the ICD codes that go into this baby are clearly for females
    restrict.loc[restrict.nonfatal_cause_name == "sti, pid, total", 'male'] = 0

    # check for duplicated babies
    if restrict.nonfatal_cause_name.unique().size != restrict.shape[0]:
        print("There are more rows than unique baby sequelae. "
              r"Look at this dataframe!{}{}"
              .format("\n\n\n", restrict[restrict.nonfatal_cause_name.
                                         duplicated(keep=False)]))
        assert False, "There are duplicated baby sequelae"
    # compare current restrictions to last version
    prior_restrict = pd.read_csv("FILEPATH")
    prior_restrict = prior_restrict[['Baby Sequelae  (nonfatal_cause_name)', 'male',
                         'female', 'yld_age_start', 'yld_age_end']].copy()
    prior_restrict = prior_restrict.reset_index(drop=True)
    prior_restrict.rename(columns={raw: new}, inplace=True)
    prior_restrict.nonfatal_cause_name =\
        prior_restrict.nonfatal_cause_name.str.lower()
    # compare current restricts to map
    print("These baby sequelae were removed {}".format(
          set(prior_restrict.nonfatal_cause_name.unique()) -
          set(restrict.nonfatal_cause_name.unique())))
    print("These baby sequelae were added {}".format(
          set(restrict.nonfatal_cause_name.unique()) -
          set(prior_restrict.nonfatal_cause_name.unique())))
    # not sure what to do with this programatically but let's print the
    # inner diff for now
    merged = restrict.merge(prior_restrict, how='inner',
                            on='nonfatal_cause_name')
    for col in ['male_', 'female_', 'yld_age_start_', 'yld_age_end_']:
        q = "{} != {}".format(col + 'x', col + 'y')
        print("The diff for column {} is \n {}".format(col, merged.query(q)))

    assert abs(restrict.shape[0] - prior_restrict.shape[0]) < 25,\
        "25 or more baby sequelae have different restrictions. "\
        "Confirm this is correct before updating"

    # run a few more tests
    assert set(restrict.male.unique()).symmetric_difference(set([0, 1])) == set()
    assert set(restrict.female.unique()).symmetric_difference(set([0, 1])) == set()

    for age in restrict.yld_age_start:
        assert age == int(age), "An age start is not type int"
    for age in restrict.yld_age_end:
        assert age == int(age), "An age end is not type int"

    # write the new restrictions following the original data structure
    all_restrict = restrict.rename(columns={new: raw})
    # write an archive file
    all_restrict.to_csv("FILEPATH",
                        index=False)
    all_restrict.to_csv("FILEPATH", index=False)

    # ########## now create bundle restrictions ############### #
    # prep to merge bundle ID onto the nfc restrictions
    to_merge = maps[['nonfatal_cause_name', 'bundle_id']].\
        copy().drop_duplicates()
    to_merge = to_merge[to_merge.bundle_id.notnull()]  # drop rows w/o bundles

    # do the actual merge
    bun_restrict = restrict.merge(to_merge, how='inner',
                                  on='nonfatal_cause_name')
    # we don't need nfc so drop it
    bun_restrict.drop("nonfatal_cause_name", axis=1, inplace=True)
    # now drop duplicate restriction values
    bun_restrict.drop_duplicates(inplace=True)

    res_list = []
    for bundle in bun_restrict.bundle_id.unique():
        tmp = bun_restrict.query("bundle_id == @bundle").copy()
        # if there are no dupe values just move on
        if tmp.shape[0] == 1:
            res_list.append(tmp)
            continue
        # set male and female to 1 if there's a 1 present in the restrictions
        if 1 in tmp.male.unique():
            tmp.male = 1
        if 1 in tmp.female.unique():
            tmp.female = 1
        # set age_end to the oldest age end
        tmp.yld_age_end = tmp.yld_age_end.max()
        # set age start to the youngest age start
        tmp.yld_age_start = tmp.yld_age_start.min()
        res_list.append(tmp)
    bun_restrict = pd.concat(res_list)
    # drop duplicates again to get unique bundle restrictions
    bun_restrict.drop_duplicates(inplace=True)
    assert bun_restrict.shape[0] == bun_restrict.bundle_id.unique().size,\
        "There are duplicated bundle restrictions"

    # compare new restrictions to old restrictions
    prior_restrict = pd.read_csv("FILEPATH")

    # compare current restricts to map with prints
    print("These bundles were removed {}".format(
          set(prior_restrict.bundle_id.unique()) -
          set(bun_restrict.bundle_id.unique())))
    print("These bundles were added {}".format(
          set(bun_restrict.bundle_id.unique()) -
          set(prior_restrict.bundle_id.unique())))
    merged = bun_restrict.merge(prior_restrict, how='inner',
                                on='bundle_id')
    for col in ['male_', 'female_', 'yld_age_start_', 'yld_age_end_']:
        q = "{} != {}".format(col + 'x', col + 'y')
        print("The diff for column {} is \n {}".format(col, merged.query(q)))

    assert abs(bun_restrict.shape[0] - prior_restrict.shape[0]) < 15,\
        "15 or more bundles have different restrictions. "\
        "Confirm this is correct before updating"
    # write an archive file
    bun_restrict.to_csv('FILEPATH',
                        index=False)
    bun_restrict.to_csv('FILEPATH', index=False)

    # now create restrictions for marketscan process
    ms = get_acause_bundle_map()
    # merge acauses onto restrictions
    ms = ms.merge(bun_restrict, how='right', on='bundle_id')
    ms.drop('bundle_id', axis=1, inplace=True)
    assert set(ms.columns).symmetric_difference(
        set(['acause', 'male', 'female', 'yld_age_start', 'yld_age_end'])) ==\
        set(), "wrong columns names"
    # many bundles go into a single cause, so drop the duplicated rows
    ms = ms.drop_duplicates()
    # this cause should definitely be 10-55
    ms.loc[ms.acause == "maternal_obstruct", 'yld_age_end'] = 55
    # there are some true duplicates
    true_dupes = ms[ms.acause.duplicated(keep=False)].acause
    # expand out the possible values like we do with bundles
    for dupe in true_dupes:
        for col in ['male', 'female', 'yld_age_start', 'yld_age_end']:
            if col == 'yld_age_start':
                ms.loc[ms['acause'] == dupe, col] =\
                    ms.loc[ms['acause'] == dupe, col].min()
            else:
                ms.loc[ms['acause'] == dupe, col] =\
                    ms.loc[ms['acause'] == dupe, col].max()

    assert ms.shape[0] == ms.acause.unique().size,\
        "duplicated acauses will break the M:1 merge"
    # write to archive then prod locations
    ms.to_stata('FILEPATH', write_index=False)
    ms.to_stata("FILEPATH", write_index=False)
    return

def fix_deprecated_bundles(df):

    updated_bundles = {808: 2978, 799: 2972, 803: 2975}
    for bundle in updated_bundles.keys():
        if bundle in df.bundle_id.unique():
            df.loc[df.bundle_id == bundle, 'bundle_id'] = updated_bundles[bundle]
    return df

def get_acause_bundle_map():
    """
    marketscan age/sex restrictions use acause rather than bundle id
    so create a map to get from acause/rei to bundle id
    """

    bundle_acause_query = "QUERY"

    df = query(bundle_acause_query)

    df.loc[df.acause.isnull(), 'acause'] = df.loc[df.acause.isnull(), 'rei']
    df.drop('rei', axis=1, inplace=True)
    return df
