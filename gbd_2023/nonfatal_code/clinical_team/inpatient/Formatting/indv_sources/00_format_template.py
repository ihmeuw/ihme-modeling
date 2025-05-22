"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

PLEASE put link to GHDx entry for the source here

When you are finished:
1) run the new source through FILEPATH
2) add the data to the tableau template
3) publish the tableau
4) if everything looks good in your tableau, open a Pull Request and include
    the link to your tableau
"""
import getpass
import platform
import re
import sys
import warnings

import pandas as pd

from crosscutting_functions.nid_tables.new_source import InpatientNewSource

# load our functions
from clinical_info.Functions import hosp_prep

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
filepath = root + r"FILEPATH"
df = pd.read_csv(filepath)

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert_msg = f"""index is not unique, the index has a length of
{str(len(df.index.unique()))} while the DataFrame has
{str(df.shape[0])} rows. Try this: df = df.reset_index(drop=True)"""
assert_msg = " ".join(assert_msg.split())
assert df.shape[0] == len(df.index.unique()), assert_msg

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    # 'year': 'year',
    "year_start": "year_start",
    "year_end": "year_end",
    "sex_id": "sex_id",
    # 'age': 'age',
    "age_start": "age_start",
    "age_end": "age_end",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "facility_id": "facility_id",
    # diagnosis varibles
    "dx_1": "dx_1",
    "dx_2": "dx_2",
    "dx_3": "dx_3",
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# Select features from raw data to keep
# Bare minimum columns have been filled, add more as needed
keep = ["age", "sex_id", "year", "location", "dx_1", "facility_id", "outcome_id"]
df = df[keep]

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################

# These are completely dependent on data source

# Ryan said that we only have 1 and 3 kinds of data
# -1: "Not Set",
# 0: "Unknown",
# 1: "Nationally representative only",
# 2: "Representative for subnational location only",
# 3: "Not representative",
# 4: "Nationally and subnationally representative",
# 5: "Nationally and urban/rural representative",
# 6: "Nationally, subnationally and urban/rural representative",
# 7: "Representative for subnational location and below",
# 8: "Representative for subnational location and urban/rural",
# 9: "Representative for subnational location, urban/rural and below",
# 10: "Representative of urban areas only",
# 11: "Representative of rural areas only"

df["representative_id"] = 1  # Do not take this as gospel, it's guesswork
df["location_id"] = -1

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "data_source"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# Check codes for code system

msg = "There is a mismatch between codes and code_system_id"
dx_list = [e for e in df.columns if e.startswith("dx_")]

for code_sys in df["code_system_id"].unique().tolist():
    if code_sys == 1:
        regex = re.compile("^[A-DF-UW-Z]")
        for e in dx_list:
            codes = df[df.code_system_id == code_sys][e].unique().tolist()
            icd10 = list(filter(regex.match, codes))
            assert len(icd10) == 0, msg
    else:
        regex = re.compile("^[0-9]")
        for e in dx_list:
            codes = df[df.code_system_id == code_sys][e].unique().tolist()
            icd9 = list(filter(regex.match, codes))
            assert len(icd9) == 0, msg

# case is the sum of live discharges and deaths
# pick one
df["outcome_id"] = "case/discharge/death"

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {"example_year": "example_nid"}
df = hosp_prep.fill_nid(df, nid_dictionary)

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################


# Replace feature levels manually
# Make the values contained in the data match the shared tables
#   - E.g.: "MALE" should become 1, "FEMALE" should become 2
# These are merely examples
# df['sex_id'].replace(['2 - MEN', '1 - FEMALE'], [1, 2], inplace=True)
# df['outcome_id'].replace(['3 - DIED (LA)',
#                           '2 - Translated (A) TO ANOTHER HOSPITAL',
#                           '1 - Out (A) HOME'],
#                          ["death", "discharge", "discharge"], inplace=True)
# df['facility_id'].replace(['3 - EMERGENCY AFTER 24 HOURS',
#                            '1 - ROUTINE',
#                            '2 - EMERGENCY TO 24 HOURS'],
#                           ['hospital', 'hospital', 'emergency'],
#                           inplace=True)

# Manually verify the replacements
# assert len(df['sex_id'].unique()
#            ) == 2, "df['sex_id'] should have 2 feature levels"
# assert len(df['outcome_id'].unique()
#            ) == 2, "df['outcome_id'] should have 2 feature levels"
# assert len(df['facility_id'].unique() ==
#            2), "df['facility_id] should have 2 feature levels"

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",
    # 'age',
    "age_start",
    "age_end",
    "sex_id",
    "nid",
    "representative_id",
    "metric_id",
]
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ["source", "facility_id", "outcome_id"]

# use this to infer data types
# df.apply(lambda x: pd.lib.infer_dtype(x.values))

if df[str_cols].isnull().any().any():
    warnings.warn(
        "\n\n There are NaNs in the column(s) {}".format(
            df[str_cols].columns[df[str_cols].isnull().any()]
        )
        + "\n These NaNs will be converted to the string 'nan' \n"
    )

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = hosp_prep.age_binning(df, clinical_age_group_set_id=-1)

# assign unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

# Create year range if the data covers multiple years
# df = year_range(df)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
# If tablulated, then DO NOT do this
df["val"] = 1

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

# Group by all features we want to keep and sums 'val'
group_vars = [
    "cause_code",
    "diagnosis_id",
    "sex_id",
    "age_start",
    "age_end",
    "year_start",
    "year_end",
    "location_id",
    "nid",
    "age_group_unit",
    "source",
    "facility_id",
    "code_system_id",
    "outcome_id",
    "representative_id",
    "metric_id",
]
df_agg = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
columns_before = df_agg.columns
hosp_frmat_feat = [
    "age_group_unit",
    "age_start",
    "age_end",
    "year_start",
    "year_end",
    "location_id",
    "representative_id",
    "sex_id",
    "diagnosis_id",
    "metric_id",
    "outcome_id",
    "val",
    "source",
    "nid",
    "facility_id",
    "code_system_id",
    "cause_code",
]
df_agg = df_agg[hosp_frmat_feat]
columns_after = df_agg.columns

# check if all columns are there
assert set(columns_before) == set(columns_after), "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert (
        hosp_frmat_feat[i] in df_agg.columns
    ), "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(
    ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg["year_start"].unique()) == len(
    df_agg["nid"].unique()
), "number of feature levels of years and nid should match number"
assert len(df_agg["age_start"].unique()) == len(df_agg["age_end"].unique()), (
    "number of feature levels age start should match number of feature " + r"levels age end"
)
assert (
    len(df_agg["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"
assert len(df_agg["sex_id"].unique()) == 2, "There should only be two feature levels to sex_id"
assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (df.val >= 0).all(), "for some reason there are negative case counts"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + r"FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)

#####################################################
# UPDATE SOURCE_TABLE
#####################################################

table.to_csv(file_path, index=False)

# Update nid tables.
ns = InpatientNewSource(df_agg)

# update the variables uses_env and age_sex.
# Almost all of inp sources will use the envelope, but this is
# not 100%.
#
# To determine age_sex round check to see that there are
# 25 age groups and both sexes in each age group. If so
# then it is a gold standard. If only a one or two age groups
# are missing than consider using it in the 2nd round.
# Else the age_sex round should be 3.
#
# merged_dict is a dictionary where k is the merged_nid and v
# is a list of nids
#
# Add nids to each list value that matches the correct assignment
src_metadata = {
    "pipeline": {"inpatient": [], "claims": [], "outpatient": []},
    "uses_env": {1: [], 0: []},
    "age_sex": {1: [], 2: [], 3: []},
    "merged_dict": {},
}

ns.process(**src_metadata)
