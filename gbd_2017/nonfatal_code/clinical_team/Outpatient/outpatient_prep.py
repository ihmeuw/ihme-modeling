
# coding: utf-8
import getpass
import sys

user = getpass.getuser()
prep_path = "FILEPATH"
sys.path.append(prep_path)

import os
import db_queries
import gbd_hosp_prep

# load functions
get_ipython().magic(u'FILEPATH/outpatient_funcs.py')


sources = ['SWE_PATIENT_REGISTRY_98_12', 'USA_NAMCS', 'USA_NHAMCS_92_10']
path = "FILEPATH"
files = []

for f in os.listdir(path):
    f = os.path.join(path, f)
    if os.path.isfile(f):
        if os.path.basename(f)[:-3] in sources:
            files.append(f)

df_list = []
for f in files:
    temp = pd.read_hdf(f)
    df_list.append(temp)
df_orig = pd.concat(df_list, ignore_index=True)
del df_list, temp

df = df_orig.copy()

df = drop_data_for_outpatient(df)
print "shape is {}".format(df.shape)
print "facility_id: {}".format(df.facility_id.value_counts())
print "source: {}".format(df.source.unique())
print "sex: {}".format(df.sex_id.unique())
df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=True)
df[['source', 'age_start', 'age_end']].drop_duplicates().sort_values(by=['source', 'age_start'])


# **fix age end**
df.loc[(df.age_start == 85)&(df.age_end == 90), ['age_start', 'age_end']] = [85, 125]
df[['source', 'age_start', 'age_end']].drop_duplicates().sort_values(['source', 'age_start'])

# ** mapping **
df = outpatient_mapping(df, which_map='current')

# ** parent inj **
checkpoint = df.copy()
df = checkpoint.copy()
df = get_parent_injuries(df.copy())

# ** correction factors **
df = apply_outpatient_correction(df)

# ** inj factors **
print df.shape
cols_before = df.columns
df = apply_inj_factor(df.copy(), fillna=True)
print df.shape
print set(df.columns).symmetric_difference(set(cols_before))

# ** age sex restrictions **
df = outpatient_restrictions(df)

# ## make square
checkpoint = df.copy()
df = checkpoint.copy()
df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=True)
df[['age_group_id', 'source']].drop_duplicates().sort_values(['source', 'age_group_id'])

# get all the ages we want in each source *before* making square
swe_ages = list(df[df.source == "SWE_PATIENT_REGISTRY_98_12"].age_group_id.unique())
nhamcs_ages = list(df[df.source == "USA_NHAMCS_92_10"].age_group_id.unique())
namcs_ages = list(df[df.source == "USA_NAMCS"].age_group_id.unique())

pre_check_val = df.loc[df['val'] > 0, 'val'].sort_values().reset_index(drop=True)
df = hosp_prep.make_zeros(df.copy(), etiology='bundle_id',
                                 cols_to_square=['val', 'val_corrected', 'val_inj_corrected'],
                                 icd_len=5)

# make a dictionary with source as keys and ages as values
source_age_dict = {
    "SWE_PATIENT_REGISTRY_98_12": swe_ages,
    "USA_NHAMCS_92_10": nhamcs_ages,
    "USA_NAMCS": namcs_ages,
}

# get rid of bad ages
print df.shape
df_list = []
for source in source_age_dict:
    
    print source
    
    temp = df[df.source == source].copy()
    temp = temp[temp.age_group_id.isin(source_age_dict[source])].copy()
    df_list.append(temp)
    
df = pd.concat(df_list, ignore_index=True)

# get ages back
df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=False)

df[['age_start', 'age_end', 'age_group_id', 'source']].drop_duplicates().sort_values(['source', 'age_start'])

post_check_val = df.loc[df['val'] > 0, 'val'].sort_values().reset_index(drop=True)
# here's a bunch of checks that haven't been formatted to be easy to read:

print df.shape
print len(post_check_val)
print len(pre_check_val)
print set(pre_check_val) - set(post_check_val)
print df[df.val == 0].shape
print (post_check_val == pre_check_val).all()
print (post_check_val != pre_check_val).any()
print (post_check_val == pre_check_val).sum()
print np.abs(post_check_val - pre_check_val).sum()

check = pd.DataFrame({"pre": pre_check_val, "post": post_check_val})

check['diff']  = check.pre - check.post

print check[check['diff'] !=0].shape
print df[df.val == 0].shape[0]


# ** fix nans **
df['age_group_unit'] = 1
df['metric_id'] = 1
df.drop('measure', axis=1, inplace=True)  # Drop measure

# ** recover measure **
maps = pd.read_csv(r"FILEPATH/clean_map.csv", dtype={'cause_code': object})

maps = maps[['bundle_id', 'bid_measure']].drop_duplicates()
maps.rename(columns={'bid_measure': 'measure'}, inplace=True)
print df.shape
df = df.merge(maps, how='left', on='bundle_id')
print df.shape

null_measure = df.loc[df.measure.isnull(), 'bundle_id'].unique()

# use your eyeballs and check these are all injuries
query(QUERY)  

# all injuries are incident causes
df.loc[df.measure.isnull(), 'measure'] = 'inc'

# we have to make up something, so make it fair
df.loc[df.factor.isnull(), 'factor'] = 1
df.loc[df.remove.isnull(), 'remove'] = 0

# now everything should be not null
assert df.isnull().sum().sum() == 0


# ** get sample size **
checkpoint = df.copy()
df = checkpoint.copy()
df[['age_start', 'age_end', 'age_group_id', 'source']].drop_duplicates().sort_values(['source', 'age_start'])

# any duplicated rows?
df[df[['age_start', 'age_end', 'year_start', 'year_end',
           'location_id', 'sex_id', 'bundle_id', 'nid']].duplicated(keep=False)]

df = get_sample_size(df.copy(), fix_top_age=True)

query(QUERY)   

df.isnull().sum()
sorted(df.columns)

checkpoint = df.copy()

(df.loc[df.age_end > 1, 'age_end'].values % 5 == 0).all()
source_bundles = df.loc[df.val != 0 ,['source', 'bundle_id']].drop_duplicates().sort_values(['source', 'bundle_id'])

source_bundles = source_bundles.merge(
    query(QUERY)

source_bundles.groupby("source").size()

# **NIDs**
test = df[['source', 'nid', 'year_start', 'year_end']].drop_duplicates()
nid_map = pd.read_excel("FILEPATH/NID_MAP.xlsx")

test = test.merge(nid_map, how='left', on='nid')
test.isnull().sum()
test[test.merged_nid.isnull()]
nids = list(df.nid.unique())

"{n}".format(n=", ".join(str(i) for i in nids))


# **ELMO**

done = outpatient_elmo(df.copy())

done[['age_start', 'age_end', 'age_group_id', 'age_demographer']].drop_duplicates()

sorted(done.columns)

done.drop("age_group_id", inplace=True, axis=1)

done[['age_start', 'age_end', 'age_demographer']].drop_duplicates().sort_values('age_start')


# Save a copy of the final data
# 
# ** change the date ** 

done.to_hdf(r"FILEPATH/2018_03_28_outpatient_final.H5", key='df', complib='blosc', complevel=5, mode='w')
done.to_csv(r"FILEPATH/2018_03_28_outpatient_final.csv", index=False, encoding='utf-8')
done = pd.read_hdf(r"FILEPATH/2018_03_28_outpatient_final.H5", key='df')

# ### Write data
print "starting at {}".format(datetime.datetime.today().strftime("%X"))
failed_to_write = outpatient_write_bundles(done, write_location='work')
print "finished at {}".format(datetime.datetime.today().strftime("%X"))