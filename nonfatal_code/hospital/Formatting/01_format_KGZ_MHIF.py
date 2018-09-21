"""
Formatting  KGZ data
"""
import pandas as pd
import numpy as np
import os.path

# Get Data
df = pd.read_table("FILEPATH", sep = ",")

# Identify which features to retain
keep = ['Year','Sex','Age', 'Main_Diagnosis','Related_Diagnosis_1',
        'Related_Diagnosis_2','Treatment_Outcome', 'Hospitalization_Type']

# drop everything else
df = df[keep]

######################
###Rename Features###
######################

# create dicitonary with all of our raw features
# Change the appropriate keys in this dictionary to rename features
hosp_wide_feat = {
    'nid' : 'nid',
    'location_id' : 'location_id',
    'representative_id' : 'representative_id',
    'Year' : 'year',
    'year_start' : 'year_start',
    'year_end' : 'year_end',
    'Sex' : 'sex_id',
    'Age': 'age',
    'age_start' : 'age_start',
    'age_end' : 'age_end',
    'age_group_unit' : 'age_group_unit',
    'Hospitalization_Type' : 'facility_id',
    'code_system_id' : 'code_system_id',
    'Treatment_Outcome': 'outcome_id',
    'Main_Diagnosis' : 'dx_1',
    'Related_Diagnosis_1' : 'dx_2',
    'Related_Diagnosis_2' : 'dx_3',
    'dx_4' : 'dx_4',
    'dx_5' : 'dx_5',
    'dx_6' : 'dx_6'}

# rename
df.rename(columns= hosp_wide_feat, inplace = True)

# Add columns that will be filled later
new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

# - Fill columns that can be hard coded
#     - E.g., NID
# - These will change depending on how many source-years you are formatting
df['nid'] = 96714
df['representative_id'] = 3  # Not representative
df['location_id'] = 37
df['age_group_unit'] = 1
df['source'] = 'KGZ_MHIF'

def fill_nid(df, nid_dict):
    """
    Function that assigns the NID to each row based on the year.
    Accepts a dictionary that contains years as keys and nids as values, and a DataFrame.
    Returns DataFrame with a filled "nid" column. If the DataFrame didn't already have a "nid" column it makes one.
    Assumes that the DataFrame has a column named "year"
    """
    assert 'year' in df.columns, "DataFrame doesn't have a 'year' column"
    df['nid'] = df['year'].map(nid_dict)
    return df

####################
###Clean Features###
####################

#  - Make the values contained in the data match the shared tables
def age_binning(df):
    """
    function accepts a pandas DataFrame that contains age-detail and bins them into age ranges.
    Returns the DataFrame with two age_group columns and drops age-detail. Assumes that the DataFrame passed
    in contains a column named 'age'

    Example: 32 would become 30, 34

    Example call: df = age_binning(df)

    The bins are [0   1   4  10  15  20  25  30  35  40  45  50  55  60  65  70  75  80
  85  90  95 100 105 110 115]

    This function assumes no one will be older than 115 years old.
    """

    # bins that age is sorted into
    age_bins = np.append(np.array([0,1,4]), np.arange(10, 116, 5)) # 0, 1, 4 do not follow the 5 year bin pattern.

    # labels for age columns are the lower and upper ages of bin
    age_start_list = np.append(np.array([0,1]), np.arange(5, 111, 5))
    age_end_list = np.append(np.array([1]), np.arange(4, 116, 5))

    # Create 2 new age columns
    df['age_start'] = pd.cut(df['age'], age_bins, labels = age_start_list, right=False)
    df['age_end'] = pd.cut(df['age'], age_bins, labels = age_end_list, right = False)

    # Drop age variable
    df.drop('age', 1, inplace=True)

    # return dataframe with age_start,age_end features
    return (df)

def year_range(df):
    """
    Accepts a pandas DataFrame. Returns the lowest and highest values. Assumes that the DataFrame contains a column named 'years'
    Assumes that there is a column that contains all the years.
    Meant to be used to form year_start and year_end columns in a DataFrame.
    """

    if not isinstance(df, pd.DataFrame):
        print("year_range was not passed a pandas DataFrame.")
        return

    df['year_start'] = df['year'].min()
    df['year_end'] = df['year'].max()
    df.drop('year' , axis = 1, inplace = True)
    return df

def sanitize_diagnoses(df):
    """
    This function accepts one column of a DataFrame (a Series) and removes any non-alphanumeric character
    """
    df = df.str.replace("\W", "") # "\W" regex represents ANY non-alphanumeric character
#     assert (df.str.contains("\W")).any(), "At least one diagnosis has a non-alphanumeric character in it"
    return df

# apply age binning and year range functions
df = age_binning(df)
df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col
df = year_range(df)

# ### Replace values manually
# E.g. make sure men are denoted with a 1 and women denoted with a 2.
df['sex_id'].replace(['2 - MEN','1 - FEMALE'],[1,2], inplace = True)
df['outcome_id'].replace(['3 - DIED (LA)','2 - Translated (A) TO ANOTHER HOSPITAL','1 - Out (A) HOME'],['death', 'discharge', 'discharge'], inplace=True)
df['facility_id'].replace(['1 - ROUTINE', '2 - EMERGENCY TO 24 HOURS', '3 - EMERGENCY AFTER 24 HOURS'], ['hospital', 'emergency', 'hospital'], inplace=True)

assert len(df['sex_id'].unique()) == 2, "sex_id has too many feature levels"
assert len(df['outcome_id'].unique()) == 2, "outcome_id has too many feature levels"
assert len(df['facility_id'].unique()) == 2, "facility_id has too many feature levels"

######################
###Restructure Data###
######################
#      - After stacking, `df` will have a column with all the diagnoses in it. An adjacent column will contain the entries of `diagnosis_vars`
#  - Rename the columns that `.stack()` auto-generates to something intelligible
#  - `patient_index` column contains the original index. That is, the unique identifier for each patient.
#      - This allows us to merge the stacked DataFrame with the rest of the data.

def stack_merger(df):
    """
    Takes a dataframe
    Selects only the columns with diagnosis information
    Pivots along the index of the original dataframe to create a longer form
    Outer joins this long df with the original wide df
    Returns a stacked and merged data frame

    Assumes that the primary diagnosis column in diagnosis_vars is named dx_1

    Also compares values before and after the stack to verify that the stack worked.
    """

    # pick out all columns that may contain diagnoses
    diagnosis_vars = ['dx_1', 'dx_2', 'dx_3','dx_4','dx_5','dx_6']
    # select vars with just diagnosis info
    diag_df = df[diagnosis_vars]
    #stack diagnosis var subset using index of original df
    stacked_df = diag_df.set_index([diag_df.index]).stack().reset_index()
    # rename stacked vars
    stacked_df = stacked_df.rename(columns = {"level_0" : "patient_index", 'level_1' : 'diagnosis_id', 0 : 'cause_code'})
    # replace diagnosis_id with codes, 1 for primary, 2 for secondary and beyond
    stacked_df['diagnosis_id'] = np.where(stacked_df['diagnosis_id'] == 'dx_1', 1, 2)
    # merge stacked_df with the original df
    merged_df = stacked_df.merge(df, left_on = 'patient_index', right_index = True, how = 'outer')

    # verify that no data was lost
    # Check if primary diagnosis value counts are the same
    assert (diag_df[diagnosis_vars[0]].value_counts()
        == merged_df['cause_code'].loc[merged_df['diagnosis_id'] == 1].value_counts()).all(),\
        "Primary Diagnoses counts are not the same before and after the Stack-Merge"
    # check if all primary diagnosis are present before and after
    assert (diag_df[diagnosis_vars[0]].sort_values().values
        == merged_df[merged_df['diagnosis_id'] == 1]['cause_code'].dropna().sort_values().values).all(),\
        "Not all Primary Diagnoses are present before and after the Stack-Merge"

    # check if counts of all secondary diagnoses are the same before and after
    old_second_total = diag_df[diagnosis_vars[1:]].apply(pd.Series.value_counts).sum(axis = 1)
    new_second_total = merged_df['cause_code'].loc[merged_df['diagnosis_id'] == 2].value_counts()
    assert (old_second_total.sort_index().values == new_second_total.sort_index().values).all(),\
        "The counts of Secondary Diagnoses were not the same before and after the Stack-Merge"
    #check if all secondary diagnoses are present before and after
    assert (old_second_total.sort_index().index == new_second_total.sort_index().index).all(),\
        "Not all Secondary Diagnoses are present before and after the Stack-Merge"

    # drop all the diagnosis features, we don't need them anymore
    merged_df.drop(diagnosis_vars, axis = 1, inplace = True)
    print("All tests passed")
    return merged_df

# apply stack merger function
merged = stack_merger(df)
# add the val feature
# every row counts as one instance of primary or secondary death or discharge
merged['val'] = 1

# remove non-alphanumeric content from icd code vector
merged['cause_code'] = sanitize_diagnoses(merged['cause_code'])

group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start','age_end',
            'year_start','year_end','location_id','nid','age_group_unit',
            'outcome_id', 'facility_id', 'representative_id', 'source']

# apply melt and merge function
merged_agg = merged.groupby(group_vars).agg({'val' : 'sum'}).reset_index()

# add features needed for compiled format
merged_agg['code_system_id'] = 2
merged_agg['metric_id'] = 1

hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source','nid',
                   'facility_id',
                   'code_system_id', 'cause_code']

# re-order columns
merged_agg = merged_agg[hosp_frmat_feat]

assert len(hosp_frmat_feat) == len(merged_agg.columns), "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in merged_agg.columns, "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

# check data types
for i in merged_agg.drop(['cause_code','source','outcome_id', 'facility_id'], axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now) are NOT object
    assert merged_agg[i].dtype != object, "%s should not be of type object" % (i)

# ideas for checks from problems we've come across recently-
# Check if the cause code is ever blank
assert merged_agg['cause_code'].isnull().sum() == 0, "There are null values present in the cause code feature"
assert sum(merged_agg['cause_code'] == "") == 0, "There are blank strings present in the cause code feature"
    # Check if val ever equals 0
assert sum(merged_agg['val'] == 0) == 0, "There are zero values in the val feature"

# check number of unique feature levels
assert len(merged_agg['year_start'].unique()) == len(merged_agg['nid'].unique()), "number of feature levels of years should match number of feature levels of nid"
assert len(merged_agg['age_start'].unique()) == len(merged_agg['age_end'].unique()), "number of feature levels age start should match number of feature levels age end"
assert len(merged_agg['diagnosis_id'].unique()) <= 2, "diagnosis_id should have 2 or less feature levels"
assert len(merged_agg['sex_id'].unique()) == 2, "There should only be two feature levels to sex_id"
assert len(merged_agg['code_system_id'].unique()) <= 2, "code_system_id should have 2 or less feature levels"
assert len(merged_agg['source'].unique()) == 1, "source should only have one feature level"

# write formatted, collapsed hospital data to csv
filepath = r"FILEPATH"
#merged_agg.to_csv(filepath, index = False)
merged_agg.to_hdf(filepath, key='df', format='table',
                  complib='blosc', complevel=5, mode='w')

# verify file was written and print results
if os.path.exists(filepath):
    print("\n\n#######\n##########\nFormatted data successfully written to ", filepath, "\n##########\n#######")
else:
    print("Formatted data was not written to ", filepath)
