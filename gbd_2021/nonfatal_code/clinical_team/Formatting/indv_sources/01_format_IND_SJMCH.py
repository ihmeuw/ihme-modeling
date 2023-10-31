# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 17:32:28 2018
@author: 

Formatting raw hospital data from ST_JOHNS_MEDICAL_COLLEGE_HOSPITAL_INPATIENT_DATA
in Bangalore, IND
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
from getpass import getuser

# load our functions
from clinical_info.Functions import hosp_prep
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
filepath = root + r"FILEPATH"
df = pd.read_excel(filepath)

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

# Select features from raw data to keep
keep = ["Year", "sex", "age_y", "diagnosis_type", "icdcode", "dischstatus"]
df = df[keep]

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    "Year": "year_start",
    "year_end": "year_end",
    "sex": "sex_id",
    "age_y": "age",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "dischstatus": "outcome_id",
    "facility_id": "facility_id",
    # diagnosis varibles
    "icdcode": "cause_code",
    "diagnosis_type": "diagnosis_id",
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
df["location_id"] = 43887  # Urban Karnataka
df["source"] = "IND_SJMCH"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1
df["facility_id"] = "hospital"
df["nid"] = 354896

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.
# For example, repalce "Male" with the number 1
#####################################################
# Replace discharge status with outcome id
df["outcome_id"] = np.where(df["outcome_id"] == "Expired", "death", "discharge")

# Translate sex_id
df["sex_id"].replace(["M", "F"], [1, 2], inplace=True)

# Case cast the cause_code
df["cause_code"] = df["cause_code"].str.upper()

# Translate diagnosis_id
df["diagnosis_id"].replace(["Primary", "Secondary"], [1, 0], inplace=True)

# Create pseudo patient identifier
pat_id = 0
df["patid"] = np.nan

# Iterate while checking secondary diagnoses for if they belong to same patient
for i in range(0, len(df)):
    if df.loc[i, "diagnosis_id"] == 0:  # Increase diagnosis_id
        if (df.loc[i, "sex_id"] == df.loc[i - 1, "sex_id"]) and (
            df.loc[i, "age"] == df.loc[i - 1, "age"]
        ):
            df.loc[i, "diagnosis_id"] = df.loc[i - 1, "diagnosis_id"] + 1
        else:
            df.loc[i, "diagnosis_id"] = 1  # Typo; should be primary
            df.loc[i, "patid"] = pat_id
            pat_id += 1
    else:  # New patient
        df.loc[i, "patid"] = pat_id
        pat_id += 1

# Forward fill patient IDs
df["patid"] = df["patid"].ffill()

# Set year_end
df["year_end"] = df["year_start"]

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

str_cols = ["source", "facility_id", "outcome_id"]

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
# df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = hosp_prep.age_binning(df, clinical_age_group_set_id=3)
df = df.drop("age", 1)

#####################################################
# DIAGNOSIS DATA IS LONG
# TRANSFORM FROM LONG TO WIDE AND SAVE
#####################################################
index = [col for col in df.columns if "cause_code" not in col]
index = [col for col in index if "diagnosis_id" not in col]
df = pd.pivot_table(
    df,
    values="cause_code",
    index=index,
    columns="diagnosis_id",
    aggfunc=lambda x: " ".join(str(v) for v in x),
)
df.reset_index(inplace=True)

# Rename diagnoses columns
dx_cols = dict(
    list(zip(np.arange(1, 15, 1), ["dx_{}".format(i) for i in np.arange(1, 15, 1)]))
)
df.rename(columns=dx_cols, inplace=True)
df = df.drop("patid", 1)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])

preswap = df.copy()
df = live_births.swap_live_births(df, user=getuser(), drop_if_primary_still_live=False)

# Saving the wide file
write_path = r"FILEPATH"
hosp_prep.write_hosp_file(df, write_path, backup=True)

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
df.loc[df["cause_code"] == "nan", "cause_code"] = "cc_code"

#####################################################
# GROUPBY AND AGGREGATE
#####################################################
# If individual record: add one case for every diagnosis
df["val"] = 1

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

# Removing procedure codes
dat_1 = df_agg[df_agg.cause_code.str.contains("^[A-Z]")]
dat_2 = df_agg[df_agg.cause_code == "cc_code"]
df_agg = pd.concat([dat_1, dat_2])

#####################################################
# REVIEW AGAINST ARCHIVED DATA
#####################################################
base = root + r"FILEPATH"
compare_df = pd.read_hdf((f"FILEPATH"), key="df")
compare_df.loc[compare_df["age_start"] == 95, "age_end"] = 125

groups = [
    "age_start",
    "age_end",
    "sex_id",
    "location_id",
    "year_start",
    "diagnosis_id",
]  # , 'cause_code']
a = compare_df.groupby(groups).val.sum().reset_index()
b = df.groupby(groups).val.sum().reset_index()

m = a.merge(b, how="outer", validate="1:1", suffixes=("_old", "_new"), on=groups)
m = m[m.diagnosis_id == 1]
m["new_minus_old"] = m["val_new"] - m["val_old"]
assert (
    abs(m.loc[m["new_minus_old"].notnull(), "new_minus_old"]) < 1
).all(), "large diffs"


# Saving the file
write_path = root + r"FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
