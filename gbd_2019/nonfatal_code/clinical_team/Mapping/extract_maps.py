
"""
Created on Tue Nov 22 13:00:12 2016
exract the nonfatal cause name, icd code, and Bundle id from MAPMASTERs
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
import ipdb


user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)

from hosp_prep import create_ms_durations, sanitize_diagnoses

if platform.system() == "Linux":
    root = "FILENAME"

else:
    root = "FILEPATH"


def dupe_icd_checker(df):
    """
    Make sure that every processed ICD code maps to only 1 baby sequelae

        df: Pandas DataFrame of MAPMASTERs map for a single ICD system
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

    maps_folder = (r"FILEPATH"
                   "FILENAME".format(root))


    map_9 = pd.read_excel("FILEPATH".format(maps_folder),
                      sheet_name="ICD9-map", converters={'icd_code': str})
    if map_9.drop_duplicates().shape[0] < map_9.shape[0]:
        print("There are duplicated rows in this icd9 map")

    map_10_path = "FILEPATH".format(maps_folder)
    map_10 = pd.read_excel(map_10_path,
                           sheet_name="ICD10_map", converters={'icd_code': str})
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


    map_9['code_system_id'] = 1
    map_10['code_system_id'] = 2


    dupe_icd_checker(map_9)
    dupe_icd_checker(map_10)


    maps = map_9.append(map_10)
    maps = maps.rename(columns={'icd_code': 'cause_code'})


    null_rows = maps.cause_code.isnull().sum()
    if null_rows > 0:
        maps = maps[maps.cause_code.notnull()]
        print("Dropping {} row(s) because of NA / NAN cause code".format(null_rows))


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

        bad_nfc = []
        for level in ["1", "2", "3"]:
            for nfc in test.nonfatal_cause_name.unique():
                if test.loc[test.nonfatal_cause_name == nfc, 'bundle'+level].unique().size > 1:
                    print("This nfc has multiple level " + level + " bundles ", nfc)
                    bad_nfc.append(nfc)
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


    assert len(bad_ones) == 0, "There are baby seqeulae with more than 1 "\
        r"bundle id. This will lead to mapping differences. Review the var "\
        r"'bad_ones' for these baby sequelae"

    if update_restrictions:
        update_restrictions_file()











    map_level_1 = maps.drop(['level 2 measure', 'Level2-Bundel ID',
                             'level 3 measure', 'Level3-Bundel ID'], axis=1)
    map_level_2 = maps.drop(['level 1 measure', 'Level1-Bundel ID',
                             'level 3 measure', 'Level3-Bundel ID'], axis=1)
    map_level_3 = maps.drop(['level 1 measure', 'Level1-Bundel ID',
                             'level 2 measure', 'Level2-Bundel ID'], axis=1)

    map_level_1['level'] = 1
    map_level_2['level'] = 2
    map_level_3['level'] = 3



    map_level_1.rename(columns={'level 1 measure': 'bid_measure',
                                'Level1-Bundel ID': 'bundle_id'}, inplace=True)


    map_level_2.rename(columns={'level 2 measure': 'bid_measure',
                                'Level2-Bundel ID': 'bundle_id'}, inplace=True)


    map_level_3.rename(columns={'level 3 measure': 'bid_measure',
                                'Level3-Bundel ID': 'bundle_id'}, inplace=True)



    map_level_4 = map_level_3.copy()

    cols = ['bundle_id', 'bid_measure']
    map_level_4['bundle_id'] = np.nan
    map_level_4['bid_measure'] = '_none'
    map_level_4['level'] = 4


    map_level_3.loc[map_level_3.bundle_id == 1010, 'bid_measure'] = 'prev'

    maps = pd.concat([map_level_1, map_level_2, map_level_3, map_level_4])




    osteo_arth_icd = maps.loc[maps.cause_code.str.contains("^715.\d"), 'cause_code'].unique().tolist()

    assert pd.Series(osteo_arth_icd).apply(len).min() > 3,\
        "There seems to be a 3 digit icd code present in the data to map to all OA"


    to_overwrite = maps.loc[(maps.cause_code.isin(osteo_arth_icd)) & (maps.level == 2) &
             (maps.code_system_id == 1),
             'bundle_id']

    to_overwrite.loc[to_overwrite == 0] = np.nan
    assert to_overwrite.notnull().sum() == 0,\
        "There is something that will be overwritten and it's this {}".format(to_overwrite.unique())


    maps.loc[(maps.cause_code.isin(osteo_arth_icd)) & (maps.level == 2) &
             (maps.code_system_id == 1),
             ['bundle_id', 'bid_measure']] = [215, 'prev']





    maps['cause_code'] = maps['cause_code'].astype(str)


    maps['nonfatal_cause_name'] = maps['nonfatal_cause_name'].astype(str)


    maps['nonfatal_cause_name'] = maps['nonfatal_cause_name'].str.lower()


    maps['bundle_id'] = pd.to_numeric(maps['bundle_id'], errors='coerce',
                                      downcast='integer')
    maps.loc[maps['bundle_id'] == 0, 'bundle_id'] = np.nan


    if not (maps['bundle_id'] > 1010).sum() == 0:

        print("You're mixing bundle IDs and ME IDs")


    maps['bid_measure'] = maps['bid_measure'].astype(str)

    maps['bid_measure'] = maps['bid_measure'].str.lower()


    maps['code_system_id'] = pd.to_numeric(maps['code_system_id'],
                                           errors='raise', downcast='integer')
    maps['level'] = pd.to_numeric(maps['level'],
                                  errors='raise', downcast='integer')



    if remove_period:
        maps['cause_code'] = maps['cause_code'].str.replace("\W", "")

    maps['cause_code'] = maps['cause_code'].str.upper()








    maps['bid_duration'] = np.nan



    cols_before = maps.columns
    maps = maps[['cause_code', 'code_system_id', 'level', 'nonfatal_cause_name',
                 'bundle_id','bid_measure', 'bid_duration']]
    cols_after = maps.columns
    assert set(cols_before) == set(cols_after),\
        "you dropped columns while changing the column order"



    if (maps.loc[(maps.bundle_id.notnull()) & (maps.cause_code == "Q91") &\
                (maps.level == 1), 'bundle_id'] == 646).all():
        print("Yup, MAPMASTER still has Q91 going to 646. it should be 638. fixing...")
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


    maps.loc[maps.bundle_id == 1, 'bid_measure'] = 'prev'

    for measure in mapG.bid_measure.unique():
        assert len(measure) == 1, "A bundle ID has multiple measures"

    assert maps[(maps.bid_measure!='prev')&(maps.bid_measure!='inc')&(maps.bundle_id.notnull())].shape[0] == 0,\
        (r"There is a bundle ID with an incorrect measure. Run the code above "
        "without .shape to find it.")





























    bid_durations = pd.read_excel(root + r"FILENAME"
                                  r"FILEPATH")


    bid_durations.drop("measure", axis=1, inplace=True)




    maps = maps.merge(bid_durations, how='left', on='bundle_id')







    before = maps[['bid_measure', 'bid_duration', 'duration']].drop_duplicates()

    maps.loc[maps.bid_duration.isnull(), 'bid_duration'] = maps.loc[maps.duration.notnull(), 'duration']


    maps.loc[maps.bid_measure == 'prev', 'bid_duration'] = np.nan

    after = maps[['bid_measure', 'bid_duration', 'duration']].drop_duplicates()

    maps.drop("duration", axis=1, inplace=True)


    print("checking if all inc BIDs have at least one non null duration")




    maps.loc[maps.bundle_id == 2996, 'bid_duration'] = 28


    maps.loc[maps.bundle_id == 3263, 'bid_duration'] = 60



    temp = list(zip([3074, 3020, 4196], [90, 365, 27]))
    for e in temp:
        maps.loc[maps.bundle_id == e[0], 'bid_duration'] = e[1]

    assert maps.loc[(maps.bid_measure == 'inc')&(maps.bundle_id.notnull()), 'bid_duration'].notnull().all(),\
        "We expect that 'inc' BIDs only have Non-Null durations"

    assert len(maps.loc[(maps.bid_measure == 'inc')&(maps.bundle_id.notnull()), 'bid_duration'].unique()),\
       "We expect that 'inc' BIDs only have Non-Null durations"

    print("checking if all prev BIDs only have null durations")
    assert maps.loc[maps.bid_measure == 'prev', 'bid_duration'].isnull().all(),\
        "We expect that 'prev' BIDs only have null durations"



















    bs_measures = pd.read_excel(root +
                                r"FILEPATH")


    bs_measures = bs_measures[["Baby Sequelae  (nonfatal_cause_name)",
                               "level 1 measure"]]
    bs_measures.rename(columns={"Baby Sequelae  (nonfatal_cause_name)": "nonfatal_cause_name",
                                "level 1 measure": "measure"}, inplace=True)

    bs_measures = bs_measures[bs_measures.nonfatal_cause_name.notnull()]

    bs_measures['nonfatal_cause_name'] = bs_measures['nonfatal_cause_name'].str.lower()


    bs_measures['bs_measure'], bs_measures['duration'] = bs_measures.measure.str.split("(").str

    bs_measures.drop(['duration', 'measure'], axis=1, inplace=True)


    bs_measures.drop_duplicates(['nonfatal_cause_name'], inplace=True)


    pre = maps.shape[0]
    maps = maps.merge(bs_measures, how='left', on=['nonfatal_cause_name'])
    assert pre == maps.shape[0]



























    assert len(maps[maps['bundle_id'].notnull()].bid_measure.unique()) == 2


    for bid in maps['bundle_id'].unique():
        assert len(maps[maps['bundle_id']==bid].bid_measure.unique()) <= 1




    bs_key = maps[['nonfatal_cause_name', 'bs_measure']].copy()
    bs_key.drop_duplicates('nonfatal_cause_name', inplace=True)
    bs_key['bs_id'] = np.arange(1, bs_key.shape[0]+1, 1)
    bs_key.drop('bs_measure', axis=1, inplace=True)

    pre_shape = maps.shape[0]
    maps = maps.merge(bs_key, how='left', on=['nonfatal_cause_name'])
    assert maps.shape[0] == pre_shape

    if manually_fix_bundles:




        maps.loc[maps.nonfatal_cause_name == "sti, syphilis, tertiary (endocarditis)", 'bundle_id'] = np.nan

        maps.loc[maps.nonfatal_cause_name == "cong, gu, hypospadias", 'bundle_id'] = np.nan
        maps.loc[maps.nonfatal_cause_name == "cong, gu, male genitalia", 'bundle_id'] = np.nan

        maps.loc[maps.nonfatal_cause_name == "neonatal, digest, other (cirrhosis)", 'bundle_id'] = np.nan


    cause_id_query = "SQL"
    cause_id_info = query(cause_id_query, conn_def='epi')

    pre = maps.shape[0]
    maps= maps.merge(cause_id_info, how='left', on='bundle_id')
    assert maps.shape[0] == pre

    maps = fix_deprecated_bundles(maps)


    map_vers = 22
    maps['map_version'] = map_vers







    gastritis_10 = [
    'K29', 'K290', 'K2900', 'K291', 'K292',
    'K2920', 'K293', 'K2930', 'K294', 'K2940',
    'K295', 'K2950', 'K296', 'K2960', 'K297',
    'K2970', 'K2971', 'K298', 'K2980', 'K299',
    'K2990', 'K2991', 'K2901', 'K2921', 'K2931',
    'K2941', 'K2951', 'K2961', 'K2981']

    gastritis_09 = [
    '535', '5350', '53500', '5351', '53510',
    '5352', '53520', '5353', '53530', '5354',
    '53540', '5355', '53550', '5356', '53560',
    '5357', '53570', '5359', '53501', '53511',
    '53521', '53531', '53541', '53551', '53561',
    '53571 ']

    maps.loc[(maps.cause_code.isin(gastritis_09)) &
        (maps['level'] == 2) & (maps['code_system_id'] == 1), 'bundle_id'] = 545
    maps.loc[maps.cause_code.isin(gastritis_10) &
        (maps['level'] == 2) & (maps['code_system_id'] == 2), 'bundle_id'] = 545




    beta_thal = ['28244']
    maps.loc[(maps.cause_code.isin(beta_thal)) &
        (maps['level'] == 1), 'bundle_id'] = 206


    maps.loc[maps.bundle_id.isin(['79']), 'bundle_id'] = 3419




    keep = ['4329', 'I629']



    maps.loc[maps.bundle_id == 116, 'bundle_id'] = np.nan
    maps.loc[(maps.cause_code.isin(keep)) & (maps.level == 2), 'bundle_id'] = 116


    maps.loc[maps.cause_code.isin(keep), 'nonfatal_cause_name'] =\
    maps.loc[maps.cause_code.isin(keep), 'nonfatal_cause_name'] + '_special_to_split'



    maps.loc[maps.cause_code == '28244', 'nonfatal_cause_name'] = 'hemog, beta-thal major_icd9'





    newenceph = pd.read_excel(root + r"FILEPATH")
    new_codes = newenceph.loc[newenceph.new_mapping == 338,
                                'icd_code'].str.replace("\W", "").tolist()

    icgs = maps.loc[maps.cause_code.isin(new_codes), 'nonfatal_cause_name'].unique().tolist()


    maps.loc[maps.bundle_id == 338, 'bundle_id'] = np.nan


    maps.loc[(maps.nonfatal_cause_name.isin(icgs)) &
        (maps.code_system_id == 2) & (maps.level == 1), 'bundle_id'] = 338

    assert not set(new_codes).symmetric_difference(\
            set(maps.loc[maps.bundle_id == 338, 'cause_code']))





    m = maps.loc[maps.bs_measure.isnull(), 'nonfatal_cause_name'].unique()
    for e in m:
        x = maps.loc[(maps.nonfatal_cause_name == e) & (maps.bid_measure.isin(['prev', 'inc'])), 'bid_measure'].unique().tolist()

        if e == 'digest, upper gi others ':
            maps.loc[maps.nonfatal_cause_name == e, 'bs_measure'] = 'prev'
        elif len(x) > 1 and e.startswith('digest,'):
            maps.loc[maps.nonfatal_cause_name == e, 'bs_measure'] = 'inc'
        elif len(x) == 1:
            maps.loc[maps.nonfatal_cause_name == e, 'bs_measure'] = x[0]
        else:
            maps.loc[maps.nonfatal_cause_name == e, 'bs_measure'] = '_none'






    single_codes = ['74511', 'Q201']
    exclude_codes = ['7456', 'Q212']
    hip_codes =['7543','7543','75431', '75432', '75433','75435',
                'Q65','Q650','Q6500','Q6501','Q6502','Q651','Q652',
                'Q658','Q6581','Q6582','Q6589','Q659']
    club_codes = ['7545','7545','75451','75452','75453','75459',
                '7546','7546','75461','75462','75469','7547','7547',
                '75471','75479', 'Q66', 'Q6600', 'Q661']

    single_nfc = 'cong, heart, single ventricle and single ventricle pathway'
    exclude_nfc = 'cong, heart, severe congenital heart defects excluding single ventricle and single ventricle pathway'
    hip_nfc = 'hip dysplasia'
    club_nfc = 'club foot'

    maps.loc[maps.cause_code.isin(single_codes), 'nonfatal_cause_name'] = single_nfc
    maps.loc[maps.cause_code.isin(exclude_codes), 'nonfatal_cause_name'] = exclude_nfc
    maps.loc[maps.cause_code.isin(hip_codes), 'nonfatal_cause_name'] = hip_nfc
    maps.loc[maps.cause_code.isin(club_codes), 'nonfatal_cause_name'] = club_nfc



    maps.loc[(maps.nonfatal_cause_name == single_nfc) & (maps.level == 1), 'bundle_id'] = 630
    maps.loc[(maps.nonfatal_cause_name == exclude_nfc) & (maps.level == 1), 'bundle_id'] = 636
    maps.loc[(maps.nonfatal_cause_name == hip_nfc) & (maps.level == 1), 'bundle_id'] = 3779
    maps.loc[(maps.nonfatal_cause_name == club_nfc) & (maps.level == 1), 'bundle_id'] = 3776


    pah_codes = ['I270', 'I272', '4160', '4168']
    pah_icg = 'pulmonary arterial hypertension'
    maps.loc[maps.cause_code.isin(pah_codes), ['nonfatal_cause_name', 'bs_measure']] = pah_icg, 'prev'
    cond = "(maps.nonfatal_cause_name == pah_icg) & (maps.level == 1)"
    maps.loc[eval(cond), ['bundle_id', 'bid_measure']] = 6077, 'prev'


    other_codes = ['2385', '2386']
    cols = ['nonfatal_cause_name', 'bs_measure']
    maps.loc[maps.cause_code.isin(other_codes), cols] = 'cancer_other', 'prev'

    lymphoma = ['C838', 'C8380']
    cols = ['nonfatal_cause_name', 'bs_measure']
    maps.loc[maps.cause_code.isin(lymphoma), cols] = 'other non-hodgkin lymphoma', 'inc'


    pre = ['O11','O111','O112','O113','O119','O14','O140','O1400',
            'O1402','O1403','O141','O1410','O1412','O1413',
            'O142','O1420','O1422','O1423','O149','O1490',
            'O1492', 'O1493']

    maps.loc[maps.cause_code.isin(pre) & (maps.level == 2), 'bundle_id'] = 6107
    maps.loc[maps.bundle_id == 66107, ['bid_measure', 'cause_id', 'bid_duration']] = 'inc', np.nan, 180

    hellp = ['O142', 'O1420', 'O1422', 'O1423']
    maps.loc[maps.cause_code.isin(pre) & (maps.level == 4), 'bundle_id'] = 6110
    maps.loc[maps.bundle_id == 66110, ['bid_measure', 'bid_duration']] = 'inc', 180


    oa_all = ['msk, arthritis, osteo (other and unspecified)',
        'msk_osteoarthritis_hand_foot', 'msk_osteoarthritis_hip',
        'msk, arthritis, osteo (polyarticular)', 'msk_osteoarthritis_knee']
    oa_hand_foot = ['msk_osteoarthritis_hand_foot']
    ten_all = ['msk, arthritis, osteo (other and unspecified)']

    cond = "(maps.nonfatal_cause_name.isin(oa_all)) & (maps.code_system_id == 1) & (maps.level == 1)"
    maps.loc[eval(cond), 'bundle_id'] = 6083

    cond = "(maps.nonfatal_cause_name.isin(oa_hand_foot)) & (maps.code_system_id == 2) & (maps.level == 1)"
    maps.loc[eval(cond), 'bundle_id'] = 6086

    cond = "(maps.nonfatal_cause_name.isin(ten_all)) & (maps.code_system_id == 2) & (maps.level == 1)"
    maps.loc[eval(cond), 'bundle_id'] = 6089


    nfc = 'gastrointestinal bleeding'
    cond = "(maps.nonfatal_cause_name == '{}') & (maps.level == 2)".format(nfc)
    maps.loc[eval(cond), ['bundle_id', 'bid_duration']] = None, None


    inc = ['digest, vascular (other and unspecified)', 'digest, vascular (angiodysplasia)']
    prev = ['urinary, acute kidney injury']

    maps.loc[maps.nonfatal_cause_name.isin(inc), 'bs_measure'] = 'inc'
    maps.loc[maps.nonfatal_cause_name.isin(prev), 'bs_measure'] = 'prev'

    maps = maps.drop_duplicates()

    maps = maps.drop_duplicates()
    maps.to_csv(root + "FILENAME"
                r"FILEPATH",
                index=False)

    maps.to_csv(root + "FILENAME"\
                + re.sub("\W", "_", str(datetime.datetime.now()))[0:11] +\
                r"FILEPATH",
                index=False)


    bs_key['bs_id_version'] = map_vers


    all_bids = sorted(list(maps.loc[maps.bundle_id.notnull(), 'bundle_id'].unique()))
    all_bids = [int(i) for i in all_bids]

    my_string = "{bids}".format(bids=', '.join(str(bid) for bid in all_bids))

    text = open(root + r"FILEPATH", "w")
    text.write(my_string)
    text.close()

    if update_ms_durations == True:

        create_ms_durations(maps)
    return


def update_restrictions_file(map_10_path, maps):
    """
    Update the age and sex restrictions for baby sequelae. This writes over the
    "FILEPATH" file

    Parameters:
        map_10_path(str): this contains the filepath to the icd 10 map which
        contains the restrictions on one of its sheets
        maps(Pandas DataFrame): Our finalized clean_maps, used to get map
        version and to merge restrictions onto bundle IDs
    """

    map_vers = int(maps.map_version.unique())






    restrict = pd.read_excel(map_10_path,
                   sheet_name="Lookup9", converters={'icd_code': str},

                    usecols= "BD, BV, BW, BX, BY" )

    raw = "Baby Sequelae  (nonfatal_cause_name)"
    new = "nonfatal_cause_name"
    restrict.rename(columns={raw: new}, inplace=True)
    restrict.nonfatal_cause_name = restrict.nonfatal_cause_name.str.lower()
    restrict.sample(5)

    restrict = restrict[restrict.nonfatal_cause_name.notnull()]
    restrict.drop_duplicates(inplace=True)


    restrict.loc[restrict.yld_age_start < 1, 'yld_age_start'] = 0



    restrict = restrict[(restrict.nonfatal_cause_name != "cancer, liver") |
                        (restrict.yld_age_start < 15)]
    restrict = restrict[(restrict.nonfatal_cause_name != "other benign and in situ neoplasms") |
                        (restrict.yld_age_start < 15)]


    restrict.loc[restrict.nonfatal_cause_name == "sti, pid, total", 'male'] = 0


    restrict.loc[restrict.nonfatal_cause_name == "hiv", 'Level1-Bundel ID'] = '_none'
    restrict.loc[restrict.nonfatal_cause_name == "sti, other", 'Level1-Bundel ID'] = '_none'



    icgs = ['rubella ', 'exposure, pediculos', 'digest, gastritis and duodenitis acute without complication',
            'digest, gastritis and duodenitis chronic without complication',
            'digest, gastritis and duodenitis unspecified without complication']

    cols = ['male', 'female', 'yld_age_start', 'yld_age_end']
    restrict.loc[restrict.nonfatal_cause_name.isin(icgs), cols] = [1,1,0,95]

    restrict.drop_duplicates(keep='first', inplace=True)

    if restrict.nonfatal_cause_name.unique().size != restrict.shape[0]:
        print("There are more rows than unique baby sequelae. "
              r"Look at this dataframe!{}{}"
              .format("\n\n\n", restrict[restrict.nonfatal_cause_name.
                                         duplicated(keep=False)]))
        assert False, "There are duplicated baby sequelae"

    prior_restrict = pd.read_csv(root + r"FILENAME"
                             r"FILEPATH")
    prior_restrict = prior_restrict[['Baby Sequelae  (nonfatal_cause_name)', 'male',
                         'female', 'yld_age_start', 'yld_age_end']].copy()
    prior_restrict = prior_restrict.reset_index(drop=True)
    prior_restrict.rename(columns={raw: new}, inplace=True)
    prior_restrict.nonfatal_cause_name =\
        prior_restrict.nonfatal_cause_name.str.lower()

    print("These baby sequelae were removed {}".format(
          set(prior_restrict.nonfatal_cause_name.unique()) -
          set(restrict.nonfatal_cause_name.unique())))
    print("These baby sequelae were added {}".format(
          set(restrict.nonfatal_cause_name.unique()) -
          set(prior_restrict.nonfatal_cause_name.unique())))


    merged = restrict.merge(prior_restrict, how='inner',
                            on='nonfatal_cause_name')
    for col in ['male_', 'female_', 'yld_age_start_', 'yld_age_end_']:
        q = "{} != {}".format(col + 'x', col + 'y')
        print("The diff for column {} is \n {}".format(col, merged.query(q)))

    assert abs(restrict.shape[0] - prior_restrict.shape[0]) < 25,\
        "25 or more baby sequelae have different restrictions. "\
        "Confirm this is correct before updating"


    assert set(restrict.male.unique()).symmetric_difference(set([0, 1])) == set()
    assert set(restrict.female.unique()).symmetric_difference(set([0, 1])) == set()

    for age in restrict.yld_age_start:
        assert age == int(age), "An age start is not type int"
    for age in restrict.yld_age_end:
        assert age == int(age), "An age end is not type int"


    all_restrict = restrict.rename(columns={new: raw})

    all_restrict.to_csv(root + "FILENAME" +
                        re.sub("\W", "_",
                               str(datetime.datetime.now()))[0:11] +
                        r"FILEPATH",
                        index=False)
    all_restrict.to_csv(root +
                        r"FILEPATH",
                        index=False)



    to_merge = maps[['nonfatal_cause_name', 'bundle_id']].\
        copy().drop_duplicates()
    to_merge = to_merge[to_merge.bundle_id.notnull()]


    bun_restrict = restrict.merge(to_merge, how='inner',
                                  on='nonfatal_cause_name')

    bun_restrict.drop("nonfatal_cause_name", axis=1, inplace=True)

    bun_restrict.drop_duplicates(inplace=True)

    res_list = []
    for bundle in bun_restrict.bundle_id.unique():
        tmp = bun_restrict.query("bundle_id == @bundle").copy()

        if tmp.shape[0] == 1:
            res_list.append(tmp)
            continue

        if 1 in tmp.male.unique():
            tmp.male = 1
        if 1 in tmp.female.unique():
            tmp.female = 1

        tmp.yld_age_end = tmp.yld_age_end.max()

        tmp.yld_age_start = tmp.yld_age_start.min()
        res_list.append(tmp)
    bun_restrict = pd.concat(res_list)

    bun_restrict.drop_duplicates(inplace=True)
    assert bun_restrict.shape[0] == bun_restrict.bundle_id.unique().size,\
        "There are duplicated bundle restrictions"


    prior_restrict = pd.read_csv(root + r"FILENAME"
                                 r"FILEPATH")


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

    bun_restrict.to_csv(root + "FILENAME" +
                        re.sub("\W", "_", str(datetime.datetime.now()))[0:11] +
                        r"FILEPATH",
                        index=False)
    bun_restrict.to_csv(root +
                        r"FILEPATH",
                        index=False)


    ms = get_acause_bundle_map()

    ms = ms.merge(bun_restrict, how='right', on='bundle_id')
    ms.drop(['bundle_id', 'Level1-Bundel ID'], axis=1, inplace=True)
    assert set(ms.columns).symmetric_difference(
        set(['acause', 'male', 'female', 'yld_age_start', 'yld_age_end'])) ==\
        set(), "wrong columns names"

    ms = ms.drop_duplicates()

    ms.loc[ms.acause == "maternal_obstruct", 'yld_age_end'] = 55

    true_dupes = ms[ms.acause.duplicated(keep=False)].acause

    for dupe in true_dupes:
        for col in ['male', 'female', 'yld_age_start', 'yld_age_end']:
            if col == 'yld_age_start':
                ms.loc[ms['acause'] == dupe, col] =\
                    ms.loc[ms['acause'] == dupe, col].min()
            else:
                ms.loc[ms['acause'] == dupe, col] =\
                    ms.loc[ms['acause'] == dupe, col].max()
    ms.drop_duplicates(keep = 'first', inplace=True)
    assert ms.shape[0] == ms.acause.unique().size,\
        "duplicated acauses will break the M:1 merge"

    ms.to_stata(root + "FILENAME" +
                        re.sub("\W", "_", str(datetime.datetime.now()))[0:11] +
                        r"FILEPATH",
                        write_index=False)
    ms.to_stata(root + r"FILEPATH", write_index=False)
    return

def fix_deprecated_bundles(df):
    """
    hardcode remove deprecated bundles that find their way back into the map.

    """

    updated_bundles = {808: 2978, 799: 2972, 803: 2975}
    for bundle in list(updated_bundles.keys()):
        if bundle in df.bundle_id.unique():
            df.loc[df.bundle_id == bundle, 'bundle_id'] = updated_bundles[bundle]
    return df

def get_acause_bundle_map():
    """
    marketscan age/sex restrictions use acause rather than bundle id
    so create a map to get from acause/rei to bundle id
    """

    bundle_acause_query = "SQL"

    df = query(bundle_acause_query, conn_def='epi')


    df.loc[df.acause.isnull(), 'acause'] = df.loc[df.acause.isnull(), 'rei']
    df.drop('rei', axis=1, inplace=True)
    return df
