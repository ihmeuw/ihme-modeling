# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME

New data was provided towards the end of GBD2017, but not incorporated.
Looks like the formatting script wasn't entirely complete. The data is in two
excel sheets

"""
import getpass
import platform
import sys
import warnings

import numpy as np
import pandas as pd

# load our functions
user = getpass.getuser()
prep_path = FILEPATH
sys.path.append(prep_path)


from crosscutting_functions import *

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
# filepath = FILEPATH
# this file is gone, may cause an issue df_raw = pd.read_excel(FILEPATH)
sheetnames = ["Stockholm", "Rest of Sweden"]
filepath = (
    root
    + r"FILEPATH"
)
tmp_list = [pd.read_excel(filepath, sheet_name=sheet) for sheet in sheetnames]
df_raw = pd.concat(tmp_list, sort=False, ignore_index=True)

df = df_raw.copy()
# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df.index.unique()))
    + " while the DataFrame has "
    + str(df.shape[0])
    + " rows"
    + "try this: df = df.reset_index(drop=True)"
)

warnings.warn(
    "\n\n\nWe're about to drop the 'Unique' column which has admissions "
    "adjusted for re-admission. it doesn't fit our current inpatient "
    "process but might be useful in the future"
)
# Select features from raw data to keep
keep = ["Country", "Region", "Year", "Diagnos", "Sex", "Age", "Encounters", "Reg"]
df = df[keep]

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    "Year": "year",
    "year_start": "year_start",
    "year_end": "year_end",
    "Sex": "sex_id",
    "Age": "age",
    "age_start": "age_start",
    "age_end": "age_end",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "Reg": "facility_id",
    # diagnosis varibles
    "Diagnos": "dx_1",
    # why? 'Unique': 'val'
    "Encounters": "val",
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)
# df['year_end'] = df['year_start']

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

# #####################################################
# # FILL COLUMNS THAT SHOULD BE HARD CODED
# # this is where you fill in the blanks with the easy
# # stuff, like what version of ICD is in the data.
# #####################################################

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
# df['location_id'] = 4944  # this is just stockholm
df["location_id"] = 93  # this is sweden

pre = df.age.value_counts().reset_index()
# fill the proper neonatal year start values
replace_dict = {
    "0-6day": "0-.01917808",
    "07-27day": ".01917808-.07671233",
    "28-364day": ".07671233-1",
}
for key in list(replace_dict.keys()):
    df.loc[df["age"] == key, "age"] = replace_dict[key]
assert (
    df.age.value_counts().reset_index().age == pre.age
).all(), "renaming age values changed the counts"

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "SWE_Patient_Register_15_16"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# case is the sum of live discharges and deaths
df["outcome_id"] = "case"

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1


df["year"].replace([16, 15], [2016, 2015], inplace=True)

df["year_start"] = df["year"]
df["year_end"] = df["year"]

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2015: 333649, 2016: 333650}
# df = fill_nid(df, nid_dictionary)
df["nid"] = df["year_start"].map(nid_dictionary)
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
# df['sex_id'].replace(['2 - MEN','1 - FEMALE'],[1,2], inplace = True)
# df['outcome_id'].replace(['3 - DIED (LA)','2 - Translated (A) TO ANOTHER HOSPITAL','1 - Out (A) HOME'],
#                          ["death","discharge","discharge"], inplace = True)
# df['facility_id'].replace(['3 - EMERGENCY AFTER 24 HOURS','1 - ROUTINE','2 - EMERGENCY TO 24 HOURS'],
#                           ['hospital','hospital','emergency'], inplace=True)

# Manually verify the replacements
# assert len(df['sex_id'].unique()) == 2, "df['sex_id'] should have 2 feature levels"
# assert len(df['outcome_id'].unique()) == 2, "df['outcome_id'] should have 2 feature levels"
# assert len(df['facility_id'].unique() == 2), "df['facility_id] should have 2 feature levels"

# update sex_id
df["sex_id"].replace(["Male", "Female"], [1, 2], inplace=True)

# update facility_id
df["facility_id"].replace(
    ["In", "Out"], ["inpatient unknown", "outpatient unknown"], inplace=True
)

# #####################################################
# # CLEAN VARIABLES
# #####################################################

# Columns contain only 1 optimized data type
int_cols = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",  # 'age',
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

# df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df["age"] = df["age"].apply(lambda x: x.split("-")[0] if "-" in x else x)
df.loc[df["age"] == "95+", "age"] = 95
df["age"] = df["age"].apply(pd.to_numeric)

df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)

# drop unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

# Create year range if the data covers multiple years
# df = year_range(df)

# #####################################################
# # IF MULTIPLE DX EXIST:
#     # TRANSFORM FROM WIDE TO LONG
# #####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

# # If individual record: add one case for every diagnosis
# df['val'] = 1

# #####################################################
# # GROUPBY AND AGGREGATE
# #####################################################

# # Check for missing values
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

# #####################################################
# # ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
# #####################################################

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

# # check data types
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

# probably don't need for sweden 15/16 cause the data not in prod
# NEW- test the newly prepped data against the last formatted version
# This is manually pulled in, and doesn't break if the test results are an issue, so carefully
# run this portion of the formatting script and review the output for warnings
# compare_df = pd.read_hdf("FILEPATH")
# test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
# if test_results == "test_case_counts has passed!!":
#     pass
# else:
#     warnings.warn(" --- ".join(test_results))
# #####################################################
# # WRITE TO FILE
# #####################################################

# some weird unicode errors
df_agg["cause_code"] = df_agg["cause_code"].apply(
    lambda x: x.decode("unicode_escape").encode("ascii", "ignore").strip()
)

# Saving the file
write_path = (
    root + FILEPATH
)

write_hosp_file(df_agg, write_path, backup=True)
