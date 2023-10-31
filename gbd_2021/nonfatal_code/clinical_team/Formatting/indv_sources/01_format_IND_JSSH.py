# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017
@author: 

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
from getpass import getuser

# load our functions
from clinical_info.Functions import hosp_prep, stage_hosp_prep
from clinical_info.Functions.live_births import live_births

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
# data exists in multiple sheets of an excel file
fpath = r"FILEPATH"
sheetnames = ["2014", "2015", "2016", "2017"]

sheetlist = [pd.read_excel(fpath, sheet_name=s) for s in sheetnames]

df = pd.concat(sheetlist, ignore_index=True)

start_cases = df.shape[0]

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

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    # 'year': 'year',
    "Year": "year_start",
    # 'year_end': 'year_end',
    "Gender": "sex_id",
    "Age_Unit": "age",
    # 'age_start': 'age_start',
    # 'age_end': 'age_end',
    "Age_Value": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "Outcome": "outcome_id",
    "Facility Type": "facility_id",
    # diagnosis varibles
    "Diagnosis_1": "dx_1",
    "Diagnosis_2": "dx_2",
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)


cond = "(df['age_group_unit'] == 'Days') & (df['age'] < 0)"
df.loc[eval(cond), "age"] = abs(df.loc[eval(cond), "age"])

df["age_group_name"] = df["age_group_unit"]
df["age_group_unit"] = 1
df.loc[df["age_group_name"] == "Days", "age_group_unit"] = 2
assert not set(["Days", "Year"]).symmetric_difference(
    set(df["age_group_name"].unique())
), "There are other ages groups that need review"
df.drop("age_group_name", axis=1, inplace=True)
unit_dict = {"days": 2, "years": 1}

# 365 day olds (1 year) were being set to the 0-1exlusive age group
# this should've been set to 365
# set age to zero
# df.loc[df.age_group_unit == "Days", 'age'] = 0
# propogate the issue for decomp 1
warnings.warn(
    "We're setting the 365 day olds to 364 days to "
    "continue an error from decomp1 and gbd2017"
)
df.loc[(df["age_group_unit"] == unit_dict["days"]) & (df["age"] == 365), "age"] = 364

# ensure that age is always less then 366 days when age value is days
assert df.loc[df.age_group_unit == 2, "age"].max() <= 366

df = stage_hosp_prep.convert_age_units(df, unit_dict)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################

# These are completely dependent on data source

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

df["representative_id"] = 3  # This is just a single hospital
df["location_id"] = 4856

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "IND_JSSH"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# case is the sum of live discharges and deaths
# one outcome is clearly for deaths
df.loc[df.outcome_id != "EXPIRED", "outcome_id"] = "discharge"
df.loc[df.outcome_id == "EXPIRED", "outcome_id"] = "death"
# df['outcome_id'] = "case/discharge/death"

assert (df.facility_id.unique() == np.array("Inpatient")).all()
df["facility_id"] = "hospital"

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

df["year_end"] = df["year_start"]

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2014: 333358, 2015: 333359, 2016: 333360, 2017: 333361}
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
df["sex_id"].replace(["M", "F"], [1, 2], inplace=True)
df.loc[~df.sex_id.isin([1, 2]), "sex_id"] = 3
# df['outcome_id'].replace(['3 - DIED (LA)','2 - Translated (A) TO ANOTHER HOSPITAL','1 - Out (A) HOME'],
#                          ["death","discharge","discharge"], inplace = True)
# df['facility_id'].replace(['3 - EMERGENCY AFTER 24 HOURS','1 - ROUTINE','2 - EMERGENCY TO 24 HOURS'],
#                           ['hospital','hospital','emergency'], inplace=True)

# Manually verify the replacements
# assert len(df['sex_id'].unique()) == 2, "df['sex_id'] should have 2 feature levels"
# assert len(df['outcome_id'].unique()) == 2, "df['outcome_id'] should have 2 feature levels"
# assert len(df['facility_id'].unique() == 2), "df['facility_id] should have 2 feature levels"

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",
    "age",
    "sex_id",
    "nid",
    "representative_id",
    "metric_id",
]
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ["source", "facility_id", "outcome_id", "dx_1"]

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
df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    df[feat] = df[feat].str.upper()

preswap = df.copy()
df = live_births.swap_live_births(df, user=getuser(), drop_if_primary_still_live=False)


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    stack_idx = [n for n in df.columns if "dx_" not in n]
    # print(stack_idx)
    len_idx = len(stack_idx)

    df = df.set_index(stack_idx).stack().reset_index()

    # drop the empty strings
    pre_dx1 = df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
    df = df[df[0] != "none"]
    diff = pre_dx1 - df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
    print("{} dx1 cases/rows were lost after dropping blanks".format(diff))

    df = df.rename(
        columns={"level_{}".format(len_idx): "diagnosis_id", 0: "cause_code"}
    )

    df.loc[df["diagnosis_id"] != "dx_1", "diagnosis_id"] = 2
    df.loc[df.diagnosis_id == "dx_1", "diagnosis_id"] = 1

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
df["val"] = 1

assert (
    start_cases == df[df.diagnosis_id == 1].val.sum()
), "some cases were added or lost"
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
assert set(columns_before) == set(
    columns_after
), "You lost or added a column when reordering"
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
    "number of feature levels age start should match number of feature "
    + r"levels age end"
)
assert (
    len(df_agg["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"

assert not [
    s for s in df_agg.sex_id.unique() if s not in [1, 2, 3]
], "There should only be 3 unique sex id values (1, 2, 3)"

assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (df.val >= 0).all(), "for some reason there are negative case counts"

assert (
    start_cases == df_agg[df_agg.diagnosis_id == 1].val.sum()
), "some cases were added or lost"

# NEW- test the newly prepped data against the last formatted version
# This is manually pulled in, and doesn't break if the test results are an issue, so carefully
# run this portion of the formatting script and review the output for warnings
compare_df = pd.read_hdf(root + "FILEPATH")
test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = r"FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
