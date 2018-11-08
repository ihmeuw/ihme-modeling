
# coding: utf-8

# ### sample size in GBD 2015
# load functions
get_ipython().magic(u'FILEPATH/outpatient_funcs.py')
import os
from db_queries import get_location_metadata





# # Get the files for our three outpatient sources
df_orig = pd.read_hdf("FILEPATH/formatted_NOR_ICPC.H5")
df = df_orig.copy()

# the ICPC map contains only 3 digit codes, snip off the last space
df['diagnosis'] = df['diagnosis'].str[0:3]
assert df['diagnosis'].apply(len).unique() == np.array([3])

df.diagnosis.apply(len).value_counts(dropna=False)

# check val sums by year
df.groupby("year_start").agg({"contacts": "sum", "patients": "sum"}).reset_index()

# check the diagnosis col
(df.diagnosis.str.upper() == df.diagnosis).all()
assert (df.diagnosis.str.upper() == df.diagnosis).all()

# make sure nulls aren't introduced
county_nulls = df.county.isnull().sum()
# manually adjust the county names to fit the spelling in the IHME location table
df.loc[df.county == "Finnmark", 'county'] = "Finmark"
df.loc[df.county == "Hedmark", 'county'] = "Hedemark"
df.loc[df.county.isin(["Nord-Trondelag", "Sor-Trondelag"]), 'county'] = "Trondelag"

df[df.county.isnull()].patients.sum() / float(df.patients.sum())

locs = get_location_metadata(QUERY)
loc_subnats = locs.loc[locs.parent_id == 90, ['location_ascii_name', 'location_id']]
loc_subnats.head(2)
assert set(df.county.unique()) - set(loc_subnats.location_ascii_name.unique()) == set([np.nan])
assert set(loc_subnats.location_ascii_name.unique()) - set(df.county.unique()) == set()

# drop national location id 
df.drop('location_id', axis=1, inplace=True)
df.head(2)

pre = df.shape[0]
df = df.merge(loc_subnats, how='left', left_on='county', right_on='location_ascii_name')
assert pre == df.shape[0]
assert county_nulls == df.county.isnull().sum()

print "shape is {}".format(df.shape)
print "sex: {}".format(df.sex_id.unique())

# drop county and contacts
df.drop(['county', 'contacts', 'location_ascii_name'], axis=1, inplace=True)
df.rename(columns={'patients': 'val'}, inplace=True)

# throw on some additional cols
df['facility_id'] = "ICPC"
df['representative_id'] = 3
df['metric_id'] = 1
df['source'] = "NOR_ICPC"
df['age_group_unit'] = 1

# ** mapping **





imap = pd.read_excel(r"FILEPATH/icpc_map_to_bid.xlsx", sheetname="sunmmery1")
imap = imap[['ICPC code', 'Level1-Bundel ID', 'Level2-Bundel ID']]
imap.rename(columns={'ICPC code': 'diagnosis', 'Level1-Bundel ID': 'bid1',
                     'Level2-Bundel ID': 'bid2'}, inplace=True)

# manually fix GERD per USERNAME. These should be D84, D07, D03
print(imap[imap.bid1 == "FID3"])
imap.loc[imap.bid1 == "FID3", 'bid1'] = 3059
print(imap[imap.bid1 == 3059])

# don't forget to reshape long
imap = imap.set_index('diagnosis').stack().reset_index()
print(imap[imap[0] ==0].shape, imap[imap[0] == "_none"].shape)
imap = imap[imap[0] != 0]
imap = imap[imap[0] != "_none"]

imap.rename(columns={'level_1': 'bundle_level', 0: 'bundle_id'}, inplace=True)
print(imap.bundle_id.unique().size)
# drop rows that are duplicated
imap = imap.drop_duplicates()

imap[imap.diagnosis.duplicated(keep=False)].sort_values("diagnosis")
imap.to_csv("FILEPATH/clean_icpc_map.csv", index=False)

# then map on bundle
df = df.merge(imap, how='left', on='diagnosis')
df.bundle_id = pd.to_numeric(df.bundle_id, errors='raise')

# drop rows with missing bundles
df = df[df.bundle_id.notnull()]

# there are rows with missing county. explicitly drop them here
df = df[df.location_id.notnull()]

# drop bundle level and diagnosis cols
df.drop(['bundle_level', 'diagnosis'], axis=1, inplace=True)

# check val sums by year
df.groupby("year_start").agg({"val": "sum"}).reset_index()

# do a groupby and collapse on val
print(df.shape)
pre = df.val.sum()
print(pre)
df = df.groupby(df.columns.drop('val').tolist()).agg({'val': 'sum'}).reset_index()
print(df.shape)
df.head()
print(df.val.sum())
assert pre == df.val.sum()

df[df.bundle_id == 173].shape





# check val sums by year
df.groupby("year_start").agg({"val": "sum"}).reset_index()





maps = pd.read_csv(r"FILEPATH/clean_map.csv", dtype={'cause_code': object})

maps = maps[['bundle_id', 'bid_measure']].drop_duplicates()
maps.rename(columns={'bid_measure': 'measure'}, inplace=True)
pre_shape = df.shape[0]
df = df.merge(maps, how='left', on='bundle_id')
print df.shape
assert pre_shape == df.shape[0], "rows changed, not good"


# ** parent inj **
checkpoint = df.copy()
df = checkpoint.copy()
df = get_parent_injuries(df.copy())

# ** age sex restrictions **
def icpc_restrictions(df):
    """
   
    Args:
        df (Pandas DataFrame) Contains outpatient data at bundle level.

    Returns:
        Data with restrictions applied.
    """

    filepath = "FILEPATH/bundle_restrictions.csv"

    warnings.warn("""

                  Please ensure that the restrictions file is up to date.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(filepath)))))

    cause = pd.read_csv(filepath)
    cause = cause[['bundle_id', 'male', 'female', 'yld_age_start', 'yld_age_end']].copy()
    cause = cause.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.
    cause['yld_age_start'].loc[cause['yld_age_start'] < 1] = 0

    # merge get_cause_metadata onto hospital using cause_id map
    pre_cause = df.shape[0]
    df = df.merge(cause, how='left', on = 'bundle_id')
    assert pre_cause == df.shape[0],        "The merge duplicated rows unexpectedly"

    # set mean to zero where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'val'] = 0

    # set mean to zero where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'val'] = 0

    # set mean to zero where age end is smaller than yld age start
    df.loc[df['age_end'] < df['yld_age_start'], 'val'] = 0

    # set mean to zero where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], 'val'] = 0

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)
    print("\n")
    print("Done with Restrictions")

    return(df)

pre = df.val.sum()
print(pre)
df = icpc_restrictions(df)
print(df.val.sum())
print("{} cases were removed".format(pre - df.val.sum()))
df.sample(2)

# ## make square
checkpoint = df.copy()
df = checkpoint.copy()
df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=True)
pre_check_val = df.loc[df['val'] > 0, 'val'].sort_values().reset_index(drop=True)
df = hosp_prep.make_zeros(df.copy(), etiology='bundle_id',
                                 cols_to_square=['val'],
                                 icd_len=5)

# get ages back
df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=False)
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

df.loc[df.bundle_id == 144, 'measure'] = "inc"
df.loc[df.measure.isnull(), 'bundle_id'].unique()
df = df[df.bundle_id != 543]

# now everything should be not null
assert df.isnull().sum().sum() == 0

# ** get sample size **
checkpoint = df.copy()
df = checkpoint.copy()
df[df[['age_start', 'age_end', 'year_start', 'year_end',
           'location_id', 'sex_id', 'bundle_id', 'nid']].duplicated(keep=False)]

# merge population onto the data
df = get_sample_size(df.copy(), fix_top_age=True)
y = df[df.measure.isnull()]

df.isnull().sum()
def check_if_square(df):
    # test if data is square
    if "sex" in df.columns:
        sex_shape = df.sex.unique().size
    if "sex_id" in df.columns:
        sex_shape = df.sex_id.unique().size
    sq_shape = sex_shape * df.age_start.unique().size *    df.location_id.unique().size * df.year_start.unique().size *    df.bundle_id.unique().size
    if df.shape[0] == sq_shape:
        print("You're square")
    else:
        print("You're not square. df shape is {} and expected square shape is {}".format(df.shape[0], sq_shape))
        print("Diff is {}".format(df.shape[0] - sq_shape))
        assert False

check_if_square(df)

# check total population
df.groupby(['year_start', 'bundle_id']).agg({'population': 'sum'}).reset_index().loc[df.bundle_id == 25]
df[df[['sex_id', 'location_id', 'age_start', 'year_start', 'bundle_id']].duplicated(keep=False)].shape[0]
z = df[df.bundle_id == 354].copy()
z['est'] = z['val'] / z['population']

# # final stretch
# 
# check things
# 
# run elmo formatting
# 
# check more things

df[df['val'] > df['population']].shape
sorted(df.columns)
checkpoint = df.copy()

# verify which age end do we have
(df.loc[df.age_end > 1, 'age_end'].values % 5 == 0).all()
source_bundles = df.loc[df.val != 0 ,['source', 'bundle_id']].drop_duplicates().sort_values(['source', 'bundle_id'])

source_bundles = source_bundles.merge(
    query(QUERY)

source_bundles.groupby("source").size()

# **NIDs**
nids = list(df.nid.unique())
"{n}".format(n=", ".join(str(i) for i in nids))

# **ELMO**
done = outpatient_elmo(df.copy())
done[['age_start', 'age_end', 'age_group_id', 'age_demographer']].drop_duplicates().head()
done.isnull().sum()
done.shape

# any duplicated rows?
done[done[['age_start', 'age_end', 'year_start', 'year_end', 'location_id',
           'sex', 'bundle_id', 'nid']].duplicated(keep=False)].shape[0]

done[done[['age_start', 'age_end', 'year_start', 'year_end',
           'location_id', 'sex', 'bundle_id', 'nid']].duplicated(keep=False)].bundle_id.unique()   

check_if_square(done)
done.drop("age_group_id", inplace=True, axis=1)
done.rename(columns={"cases_uncorrected": "cases"}, inplace=True)

# Save a copy of the final data
# 
# ** change the date ** 
done_cases = done.cases.sum()
done_shape = done.shape[0]
done = done.merge(loc_subnats, how='left', on='location_id')
assert done_cases == done.cases.sum()
assert done_shape == done.shape[0]

done.drop('location_name', axis=1, inplace=True)
done.rename(columns={"location_ascii_name": "location_name"}, inplace=True)

done.bundle_name = done.bundle_name.astype(str)
done.location_name = done.location_name.astype(str)

for col in ['location_id', 'year_start', 'year_end', 'bundle_id']:
    done[col] = pd.to_numeric(done[col])

done.to_hdf(r"FILEPATH/2018_03_12_icpc_final.H5",
            key='df', format='table', mode='w')

done.to_csv(r"FILEPATH/2018_03_12_icpc_final.csv",
            index=False, encoding='utf-8')

done = pd.read_hdf(r"FILEPATH/2018_02_05_icpc_final.H5", key='df')
done[done.bundle_id == 195].cases.sum()

# ### Write data
def icpc_write_bundles(df, write_location="test"):
    """
    Function that writes data to modeler's folders.  Reorders columns. Once it
    starts writing, CTRL C usually won't stop it; a CTRL Z may be required.

    Args:
        df: (Pandas DataFrame)  Ootpatient data that has been prepared for
            upload to Epi DB.

    Returns:
        list of directories where it failed to write data.
    """
    # CAUSE INFORMATION
    # get cause_id so we can write to an acause
    # have to go through cause_id to get to a relationship between BID & acause
    cause_id_info = query(QUERY)
    # get acause
    acause_info = query(QUERY)
    # merge acause, bid, cause_id info together
    acause_info = acause_info.merge(cause_id_info, how="left", on="cause_id")

    # REI INFORMATION
    # get rei_id so we can write to a rei
    rei_id_info = query(QUERY)
    # get rei
    rei_info = query(QUERY)
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

    # get injuries bundle_ids so we can keep injury corrected data later
    pc_injuries = pd.read_csv('FILEPATH/'\
                              r"parent_child_injuries.csv")
    inj_bid_list = pc_injuries['Level1-Bundle ID'].unique()

    # arrange columns
    columns_before = df.columns
    ordered = ['bundle_id', 'bundle_name', 'measure',
               'location_id', 'location_name', 'year_start', 'year_end',
               'age_start',
               'age_end', 'age_group_unit', 'age_demographer', 'sex', 'nid',
               'representative_name', 'cases',
               # 'cases_corrected', "cases_inj_corrected", "factor", "remove",
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
    assert set(columns_after) == set(columns_before),        "you added/lost columns while reordering columns!!"
    failed_bundles = []  # initialize empty list to append to in this for loop
    print "WRITING FILES"
    for bundle in bundle_ids:
        # subset bundle data
        df_b = df[df['bundle_id'] == bundle].copy()

        acause_rei = str(df_b.acause_rei.unique()[0])
        df_b.drop('acause_rei', axis=1, inplace=True)

        if write_location == "work":
            writedir = r"FILEPATH"
        if write_location == "test":
            writedir = r"FILEPATH"
        print "{}".format(writedir)

        if not os.path.isdir(writedir):
            os.makedirs(writedir)  # make the directory if it does not exist

        # write for modelers
        # make path
        vers_id = "GBD2017_v3" 
        date = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD
        
        bundle_path = "{}ICPC_{}_{}_{}.xlsx".format(writedir, int(bundle), vers_id, date)
        
        # try to write to modelers' folders
        try:
        
            writer = pd.ExcelWriter(bundle_path, engine='xlsxwriter')
            df_b.to_excel(writer, sheet_name="extraction", index=False)
            writer.save()
        except:
            failed_bundles.append(bundle)  
            
    end = time.time()
    text = open(root + re.sub("\W", "_", str(datetime.datetime.now())) +
                "_OUTPATIENT_write_bundles_logs.txt", "w")
    text.write("function: write_bundles " + "\n"+
               "start time: " + str(start) + "\n" +
               " end time: " + str(end) + "\n" +
               " run time: " + str((end-start)/60.0) + " minutes")
    text.close()
    return(failed_bundles)


print "starting at {}".format(datetime.datetime.today().strftime("%X"))
failed_to_write = icpc_write_bundles(done, write_location='work')
print "finished at {}".format(datetime.datetime.today().strftime("%X"))
