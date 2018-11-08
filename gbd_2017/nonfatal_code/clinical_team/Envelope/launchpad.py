
# coding: utf-8
import pandas as pd
import time
import sys
import getpass

user = getpass.getuser()
prep_path = r"FILEPATH/Functions"
sys.path.append(prep_path)

import hosp_prep
import gbd_hosp_prep

pd.set_option('display.float_format', lambda x: '%.8f' % x)
pd.options.display.max_columns=99

write_results=True

# ### make a new master data file
get_ipython().magic(u'run FILEPATH/standardize_format.py')

df = format_inpatient_main(write_hdf=write_results, downsize=False, verbose=True, head=False)

# ### run marketscan mapping process
run_ms_map = True
if run_ms_map:
    get_ipython().magic(u'run FILEPATH/01a_submit_MARKETSCAN.py')

# ### map data to baby sequelae
get_ipython().magic(u'run FILEPATH/submit_icd_mapping.py')

start = time.time()
df = icd_mapping(en_proportions=True, create_en_matrix_data=False,
                 save_results=write_results, write_log=True,
                deaths='non', extra_name="")
print((time.time()-start)/60)
# ##### verify that bundle 114 is being mapped to Georgia
bun114 = ['cvd, ischemic, infarction (initial unspecified, other and unspecified)',
 'cvd, ischemic, infarction (initial stemi and nstemi, total)',
 'cvd, ischemic, infarction (initial stemi, anterior)',
 'cvd, ischemic, infarction (initial stemi, inferior)',
 'cvd, ischemic, infarction (initial stemi, other and unspecified)',
 'cvd, ischemic, infarction (initial nstemi)',
 'cvd, ischemic, infarction (subsequent stemi and nstemi, total)',
 'cvd, ischemic, infarction (subsequent stemi, anterior)',
 'cvd, ischemic, infarction (subsequent stemi, inferior)',
 'cvd, ischemic, infarction (subsequent nstemi)',
 'cvd, ischemic, infarction (subsequent stemi, other and unspecified)',
 'cvd, ischemic, other acute']

g = df[(df.source == "GEO_COL_14") & (df.nonfatal_cause_name.isin(bun114))]
print(g.shape)
assert g.shape[0] > 200

# ### run age sex splitting
# read in mapped data
back = pd.read_hdf("FILEPATH/"
                   r"icd_mapping_v10_map.H5", key='df', format='table')

def check_merged_nids(df, break_if_missing=False):

    df = df[df.year_start > 1989].copy()
    df = df[(df.diagnosis_id == 1) & (df.facility_id.isin(['hospital', 'inpatient unknown']))]
    
    df = hosp_prep.apply_merged_nids(df, assert_no_nulls=True,
                          fillna=True)
    nulls = df['nid'].isnull().sum()
    missing = (df['nid'] <= 0).sum()
    null_msg = "There are null merged NIDs"
    missing_msg = "There are incorrectly coded NIDs"
    if break_if_missing:
        assert nulls == 0, "{}".format(null_msg)
        assert missing == 0, "{}".format(missing_msg)
    else:
        if nulls != 0:
            warnings.warn(null_msg)
        if missing != 0:
            warnings.warn(missing_msg)
    return

import time
s = time.time()
check_merged_nids(back.copy(), break_if_missing=False)
print((time.time()-s)/60)

df = hosp_prep.apply_merged_nids(back.copy(), assert_no_nulls=True,
                          fillna=True)

x = df[['source', 'year_start', 'nid']].drop_duplicates()
x[(x.nid.isnull()) | (x.nid < 1)]


if "GEO_COL_00_13" in back.source.unique():
   back = back[back.source != "GEO_COL_00_13"]

df = back.copy()

# ### make baby seq weights
get_ipython().magic(u'run FILEPATH/apply_env_only.py')
get_ipython().magic(u'run FILEPATH/compute_weights.py')
get_ipython().magic(u'run FILEPATH/run_age_sex_splitting.py')

source_round_df = pd.read_csv("FILEPATH/source_round_table.csv")
source_diff = set(back.source.unique()) - set(source_round_df.source.unique())
assert source_diff == set(),    "Please add {} to the list of sources and which round to use it in".format(source_diff)

og_start = time.time()
round_1_drops = ["KGZ_MHIF",
                 "QAT_AIDA", "NPL_HID", "KEN_IMMS",
                 "IDN_SIRS", "VNM_MOH",
                 "SWE_PATIENT_REGISTRY_98_12",
                 "GEO_COL_14", "JOR_ABHD",
                 "AUT_HDD", "UK_HOSPITAL_STATISTICS", "IRN_MOH"]

round_2_drops = ["KGZ_MHIF", "QAT_AIDA",
                 "NPL_HID", "KEN_IMMS",
                 "IDN_SIRS", "VNM_MOH",
                 "GEO_COL_14", "JOR_ABHD"]

round_1_newdrops = source_round_df.loc[source_round_df.use_in_round > 1, 'source']
round_2_newdrops = source_round_df.loc[source_round_df.use_in_round > 2, 'source']

make_weights = True
overwrite_weights=True
rounds = 2

# ##### make round 1 weights
if make_weights:
    start = time.time()
    df = back.copy()
    # drop sources you don't want to use for splitting
    df = df[~df.source.isin(round_1_drops)].copy()
    df = apply_envelope_only(df, env_path="FILEPATH/interp_hosp_env_stgpr_m39583_2018_03_08.csv",
                             apply_age_sex_restrictions=True,
                      want_to_drop_data=True, create_hosp_denom=False)
    compute_weights(df, round_id=1, fill_gaps=False, overwrite_weights=overwrite_weights)
    print((time.time() - start)/60)


# ##### make round 2 weights
if make_weights and rounds == 2:
    start = time.time()
    df = back.copy()  
    # drop sources you don't want to use for splitting
    df = df[~df.source.isin(round_2_drops)].copy()
    
    # split the data
    df = run_age_sex_splitting(df, verbose=True, round_id=2, write_viz_data=False)
    # go to rate space
    df = apply_envelope_only(df, env_path="FILEPATH/interp_hosp_env_stgpr_m39583_2018_03_08.csv",
                      apply_age_sex_restrictions=True,
                      want_to_drop_data=True, create_hosp_denom=False)
    # compute the actual weights
    compute_weights(df, round_id=2, fill_gaps=False, overwrite_weights=overwrite_weights)
    print((time.time() - start)/60)

# ### age/sex split the data
weights = pd.read_csv("FILEPATH/weights_nonfatal_cause_name.csv")
back[back.nonfatal_cause_name.str.contains("trans")].nonfatal_cause_name.unique()
back[back.nonfatal_cause_name.str.contains("poison")].nonfatal_cause_name.value_counts()
back.loc[back.nonfatal_cause_name == "e-code, (inj_trans)", 'source'].unique()

# these nfc from vietnam special map don't have weights
df.loc[df.nonfatal_cause_name == 'e-code, (inj_poisoning_other)',
       'nonfatal_cause_name'] = 'poisoning_other'
df.loc[df.nonfatal_cause_name == 'lower respiratory infection(all)',
       'nonfatal_cause_name'] = 'lower respiratory infection(unspecified)'
df.loc[df.nonfatal_cause_name == 'neoplasm non invasive other',
       'nonfatal_cause_name'] = 'other benign and in situ neoplasms'

df.loc[df.nonfatal_cause_name == 'e-code, (inj_trans)',
       'nonfatal_cause_name'] = 'z-code, (inj_trans)'


df.loc[df.nonfatal_cause_name == 'digest, gastritis and duodenitis',
      'nonfatal_cause_name'] = '_none'

df.loc[df.nonfatal_cause_name == 'resp, copd, bronchiectasis', 'nonfatal_cause_name'] = 'resp, other'

print(df.shape)
pre = df.val.sum()
df = df.groupby(df.columns.drop('val').tolist()).agg({'val': 'sum'}).reset_index()
print(df.shape)

# age/sex split the data using round `rounds` weights
start = time.time()
df = run_age_sex_splitting(df, verbose=True, round_id=rounds, write_viz_data=False)  # NOTE the round_id
print((time.time()-start)/60)

if write_results:
    # write the age sex split file
    wpath = "FILEPATH/age_sex_split_data.H5"
    hosp_prep.write_hosp_file(df, wpath, backup=write_results)

age_sex_time = (time.time()-og_start)/60
print("age/sex runtime in {}".format(age_sex_time))
df.source.unique()
df[df.location_id == 102].shape


# ### pull in the age/sex split data from file
df = pd.read_hdf("FILEPATH/"
                 r"age_sex_split_data.H5")
print(df.shape, df.nonfatal_cause_name.unique().size)


# ### ~set the envelope path~
env_path = "FILEPATH/interp_hosp_env_draws.H5"

# ### run prep env script
get_ipython().magic(u'run "FILEPATH/prep_for_env.py"')

df, full_coverage_df = prep_for_env_main(df, env_path=env_path, new_env_or_data=True,
                                         write=write_results, drop_data=False,
                                         create_hosp_denom=True,
                                         fix_norway_subnat=False)
print(df.shape, full_coverage_df.shape)
test = find_bad_env_locs2(break_if_bad=False)

for v in test.values():
    assert not v, "ok {}".format(v)

df = pd.read_hdf("FILEPATH/prep_for_env_df.H5", key="df")

full_coverage_df = pd.read_hdf("FILEPATH/"
                               r"/prep_for_env_full_coverage_df.H5", key="df")
print(df.shape, full_coverage_df.shape)

back = pd.concat([df, full_coverage_df])
print(back.source.unique(), "unique sources", back.source.unique().size)
back[back.bundle_id.isin([206, 176, 208, 766, 333])].bundle_id.value_counts()
