# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 17:32:28 2018
@author: 

Formatting raw hospital data from GEO Hospital Discharges
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass
import getpass

# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH"
sys.path.append(prep_path)
from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
filepath = root + r"FILEPATH"
files = glob.glob(filepath + "*.xlsx")

df_list = []
for each_file in files:
    df = pd.read_excel(each_file)
    df["year_start"] = df["year_end"] = int(each_file.split(".")[0].split("_")[5])
    df_list.append(df)
df = pd.concat(df_list)

# If this assert fails uncomment this line:
df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df.index.unique()))
    + " while the DataFrame has "
    + str(df.shape[0])
    + " rows"
    + "try this: df = df.reset_index(drop=True)"
)

df.rename(
    {
        "Age (years)": "age_years",
        "Age (months)": "age_months",
        "Age (days)": "age_days",
    },
    axis=1,
    inplace=True,
)

# avoid na products and sums
values = {"age_years": 0, "age_months": 0, "age_days": 0}
df.fillna(value=values, inplace=True)

# converting months to days using 30.5 as the denom
# this is in-line with CoD's process and was signed of by USER
df["age_months"] = df["age_months"] * 30.5
# df['age_years'] = (df['age_days'] + df['age_months']) / 365
df.loc[df.age_years == 0, "age_years"] = (df["age_days"] + df["age_months"]) / 365

keep = [
    "year_start",
    "year_end",
    "Sex",
    "age_years",
    "Disharge status",
    "Main Diagnosis (ICD10)",
    "External causes (ICD10)",
    "Complication (ICD 10)",
    "Comorbidity (ICD 10)",
]
df = df[keep]

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    "year_start": "year_start",
    "year_end": "year_end",
    "Sex": "sex_id",
    "age_years": "age",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "Disharge status": "outcome_id",
    "facility_id": "facility_id",
    # diagnosis varibles
    "Main Diagnosis (ICD10)": "dx_1",
    "External causes (ICD10)": "dx_2",
    "Complication (ICD 10)": "dx_3",
    "Comorbidity (ICD 10)": "dx_4",
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

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

df["representative_id"] = 3  # Do not take this as gospel, it's guesswork

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["location_id"] = 35
df["source"] = "GEO_Discharge_16_17"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1
df["facility_id"] = "inpatient unknown"

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2016: 319414, 2017: 354240}  # 2016 NID PLEASE
df["nid"] = df["year_start"].map(nid_dictionary)

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.
# For example, repalce "Male" with the number 1
#####################################################
# Replace discharge status code with outcome id
df["outcome_id"].replace(
    [1, 2, 3, 4], ["discharge", "discharge", "discharge", "death"], inplace=True
)

# Replace some typos in sex_id as well as missing data to equal 3
df.loc[~df["sex_id"].isin([1, 2]), "sex_id"] = 3

# Case cast the cause_codes
df["dx_1"] = df["dx_1"].str.upper()
df["dx_2"] = df["dx_2"].str.upper()
df["dx_3"] = df["dx_3"].str.upper()
df["dx_4"] = df["dx_4"].str.upper()

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

str_cols = ["source", "facility_id", "outcome_id", "dx_1", "dx_2", "dx_3", "dx_4"]

# Remove non-alphanumeric characters from dx feats
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
# df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df, new_age_detail=True)
df = df.drop("age", 1)

# Swap dx_1 and dx_2
df = swap_ecode_with_ncode(df, icd_vers=10)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

# Saving the wide file before changing to long
write_path = r"FILEPATH"
write_hosp_file(df, write_path, backup=True)

write_path = r"FILEPATH"
df.to_stata(write_path)

# Transform from wide to long when necessary
if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    stack_idx = [n for n in df.columns if "dx_" not in n]
    len_idx = len(stack_idx)
    df = df.set_index(stack_idx).stack().reset_index()
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

# replace nans with cc_code
df.loc[(df["cause_code"] == "nan"), "cause_code"] = "cc_code"

# If individual record: add one case for every diagnosis
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

# check sex_id col for acceptable values
good_ids = [1, 2, 3]
good_in_data = [n for n in good_ids if n in df_agg.sex_id.unique()]
assert (
    set(df_agg["sex_id"].unique()).symmetric_difference(set(good_in_data)) == set()
), "There are unexpected sex_id values {} in the df".format(df_agg.sex_id.unique())

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

write_hosp_file(df_agg, write_path, backup=True)

#####################################################
# UPDATE SOURCE_TABLE
#####################################################
# read the existing source_table
file_path = "FILEPATH"
table = pd.read_csv(file_path)

# read the merged nids table
mnid = pd.read_excel("FILEPATH")
mnid = mnid[["nid", "merged_nid"]]

# only one source per formatting script
source_name = df_agg.source.iloc[0]

# get unique (year_id, nid) pairs
year_list = df_agg.year_start.unique()
pairs = [
    (x, y)
    for x in year_list
    for y in df_agg.loc[df_agg["year_start"] == x]["nid"].unique()
]

# update the table row by row (source_name, year_id, nid)
for (year_id, nid) in pairs:
    table = update_source_table(
        table=table,
        mnid=mnid,
        source_name=source_name,
        year_id=year_id,
        nid=nid,
        is_active=1,
        active_type="inpatient",
    )

table.to_csv(file_path, index=False)
