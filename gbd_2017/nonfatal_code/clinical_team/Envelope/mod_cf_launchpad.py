
# coding: utf-8
import pandas as pd
import time
import sys
import getpass
import warnings
import subprocess

prep_path = r"FILEPATH/Functions"
sys.path.append(prep_path)

import hosp_prep
import gbd_hosp_prep

pd.set_option('display.float_format', lambda x: '%.6f' % x)
pd.options.display.max_columns=99

write_results=True
get_ipython().magic(u'FILEPATH/compile_mod_cf3.py"')


# ### launch the marketscan jobs
df = pd.read_hdf("FILEPATH/prep_for_env_df.H5", key="df")
full_coverage_df = pd.read_hdf("FILEPATH/"
                               r"prep_for_env_full_coverage_df_v10_mapv20.H5", key="df")
print(df.shape, full_coverage_df.shape)

df = df[df.location_id != 8]
assert not "TWN_NHI" in df.source.unique()

# ### run apply env script
get_ipython().magic(u'run "FILEPATH/mod_apply_env.py"')
no_draws_env_path = "FILEPATH/interp_hosp_env_stgpr_m39583_2018_03_08.csv"

df_env = apply_env_main(df, full_coverage_df, env_path=no_draws_env_path,
                        run_tmp_unc=True, write=write_results)
to_int = ['age_group_id', 'bundle_id', 'location_id', 'nid', 
          'representative_id', 'sex_id', 'year_start', 'year_end']
for col in to_int:
    df_env[col] = df_env[col].astype(int)

df_env.info(memory_usage='deep')
for s in ["NOR_NIPH_08_12", "NZL_NMDS"]:
    assert df_env[df_env.source.isin([s])].location_id.unique().size > 1, "subnats missing"





# manually backup hosp data with env applied
write_path = r"FILEPATH/mod_cf_apply_env_v10_mapv20_fixed.H5"
hosp_prep.write_hosp_file(df_env, write_path)


# ### pull apply env from file
df_env = pd.read_hdf("FILEPATH/mod_cf_apply_env_v10_mapv20_fixed.H5")

print(df_env.shape)
df_env = df_env[df_env.source != 'ITA_IMCH']
print(df_env.shape)

df_env.query("year_start == 2012 & bundle_id == 686 & age_group_id == 19 & location_id == 35467")

df_env['lower_prevalence'] = df_env['lower_prevalence'].round(20)
df_env.loc[df_env['upper_prevalence'] > 1e5, 'upper_prevalence'] = 1e5
df_env.loc[df_env['mean_prevalence'] > 1000, 'mean_prevalence'] = 1000

df_env[df_env.upper_prevalence > 1e3].bundle_id.unique()

# ### run agg to five years script
get_ipython().magic(u'run "FILEPATH/agg_to_five_years.py"')

import time
start = time.time()
df_agg = agg_to_five_main(df_env, write=write_results, use_modified=True)
print("runtime is {} min".format((time.time()-start)/60))

'cf_mean_prevalence' in df_agg.columns


# ### pull agg to five years from file
d = df_agg[df_agg[['age_group_id', 'year_start', 'year_end',
               'location_id', 'sex_id', 'bundle_id']].duplicated(keep=False)]
to_int = ['age_group_id', 'bundle_id', 'location_id', 'nid', 
          'representative_id', 'sex_id', 'year_start', 'year_end']
for col in to_int:
    df_agg[col] = df_agg[col].astype(int)

# write agg data with modifed cf to drive
hosp_prep.write_hosp_file(df_agg, "FILEPATH/mod_CF_agg_to_five_years_v10_mapv20_fixed.H5")

# read agg data from drive
df_agg = pd.read_hdf("FILEPATH/mod_CF_agg_to_five_years_v10_mapv20_fixed.H5")

for col in ['super_region_id', 'model_prediction']:
    if col in df_agg.columns:
        df_agg.drop(col, axis=1, inplace=True)
df_agg.loc[(df_agg.lower_prevalence.notnull()) & (df_agg.lower_indvcf.isnull()),       
['mean_indvcf', 'lower_indvcf', 'upper_indvcf']] = 0

# ### run elmo formatting
get_ipython().magic(u'run FILEPATH/elmo_formatting.py')
df = elmo_formatting(df_agg, maternal_data=False, make_right_inclusive=True)

back = df.copy()

# ### run write bundles
get_ipython().magic(u'run FILEPATH/dev_write_bundles.py')

bun_id = [338]
neo = df[df.bundle_id.isin(bun_id)]
start = time.time()
failed = write_bundles(neo, write_location='work',
                       write_fixed_maternal=False, extra_filename="_modeled_CFs",
                       verbose=True,
                       write_final_data_switch=False)
print("run time is {} minutes".format((time.time()-start)/60))
df[df.bundle_id==264].bundle_name.unique()

# ## run the fixed maternal process
get_ipython().magic(u'run FILEPATH/elmo_formatting.py')
get_ipython().magic(u'run FILEPATH/dev_write_bundles.py')
get_ipython().magic(u'run FILEPATH/prep_maternal_data.py')

# get the data with the envelope applied
df_mat = pd.read_hdf("FILEPATH/mod_cf_apply_env_v9_mapv19_fixed.H5")

df_mat = df_mat[df_mat.source != 'ITA_IMCH']
assert df_mat.query("location_id == 8").shape[0] == 0
df_mat = prep_maternal_main(df_mat, write_denom=False, write=False, denom_type='ifd_asfr')
df_mat = elmo_formatting(df_mat, maternal_data=True, make_right_inclusive=True)


# ### write maternal data
start = time.time()
failed = write_bundles(df_mat, write_location='test',
                       write_fixed_maternal=True, extra_filename="_modeled_CFs",
                       write_final_data_switch=False, verbose=True)
print("run time is {} minutes".format((time.time()-start)/60))
