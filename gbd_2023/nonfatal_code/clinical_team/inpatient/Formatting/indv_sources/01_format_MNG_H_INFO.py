# -*- coding: utf-8 -*-
"""
Created 03/01/2021
@author: USERNAME

Formatting raw inpatient and outpatient hospital data from Mongolia , 2018(outpatient only), 2019-2020
"""
import glob
import platform
import warnings

import numpy as np
import pandas as pd
from crosscutting_functions import demographic, general_purpose
from crosscutting_functions.formatting-functions import formatting


AGE_GROUP_SET_ID = 3

#############################
## MNG DICTIONARY
############################
mng_data_inpatient = {
    "Хүйс": "sex_id",
    "Нас/Жил": "age_years",
    "Сар": "age_months",
    "Өдөр": "age_days",
    "Өвчний төгсгөл": "outcome_id",
    "Үндсэн онош": "dx_1",
    "Шалтгаан": "dx_2",
    "Хүндрэл": "dx_3",
}


mng_data_outpatient = {
    "Хүйс": "sex_id",
    "Нас": "age_years",
    "Нас.1": "age_months",
    "Нас.2": "age_days",
    "Мэдээлсэн огноо": "date",
    "Өвчтөний тавилан": "outcome_id",
    "Үндсэн онош": "dx_1",
    "Шалтгаан": "dx_2",
}


#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# First read in Inpatient data
# Second read in Outpatient data
# Combine both
#####################################################
# Inpatient file with 2019-2020 data
file = (FILEPATH
)
# Outpatient 2018, 2019, 2020 data
filepath = FILEPATH
all_files = glob.glob(FILEPATH)

# Start to read in and format dataframe, consistent DATE column was not in inpatient file, so we had to create one from the sheet name.
in_p = pd.read_excel(file, sheet_name=None)
sheets = in_p.keys()
sheet_names = list(sheets)
years = [x[-4:] for x in sheet_names]

df1 = in_p["ГАРАГЧ2020"]
df1.rename(columns=mng_data_inpatient, inplace=True)
df1 = df1[mng_data_inpatient.values()]
df1["year_start"] = df1["year_end"] = years[0]

df2 = in_p["Гарагч2019"]
df2.rename(columns=mng_data_inpatient, inplace=True)
df2 = df2[mng_data_inpatient.values()]
df2["year_start"] = df2["year_end"] = years[1]
df = pd.concat([df1, df2], ignore_index=True)
# adding classification column early
df["facility_id"] = "inpatient unknown"

# outpatient data
out_df = []
for each_file in all_files:
    op = pd.read_excel(each_file, sheet_name=None)
    op = pd.concat(op, ignore_index=True)
    op.rename(columns=mng_data_outpatient, inplace=True)
    op = op[mng_data_outpatient.values()]
    op["year_end"] = op["year_start"] = op.date.astype(str).str[:4]
    out_df.append(op)
op = pd.concat(out_df, ignore_index=True)
op.drop("date", axis=1, inplace=True)
# adding classification and renaming 2021 value in
op["facility_id"] = "outpatient unknown"
op["year_start"] = op["year_start"].replace(["2021"], "2020")
op["year_end"] = op["year_end"].replace(["2021"], "2020")
# join both inpatient and outpatient data
df = pd.concat([op, df], ignore_index=True)


#####################################################
# CLEAN AGE COLUMNS
#####################################################

# remove rows were data quality is inconsistent with the rest of the dataset, Wil confirmed this.
mng_text = df[pd.to_numeric(df.age_years, errors="coerce").isnull()]
df = df.drop(mng_text.index)

# dropping rows for age_years where there are date-years(YYYY)
df.query(
    "age_years != 1934 & age_years != 1965 & age_years != 1974 & age_years != 2008",
    inplace=True,
)

# dropping rows in age_months and age_days that are outliers, not 1-31 days in a month and 12 months in a year
# also, instances of year 1900-x-x-x has good year data so we ae keeping those
mng_datetime = df[pd.to_numeric(df.age_months, errors="coerce").isnull()]
mng_datetime.age_months = np.nan
df = df.drop(mng_datetime.index)
df = df.append(mng_datetime).reset_index(drop=True)

mng_datetime = df[pd.to_numeric(df.age_days, errors="coerce").isnull()]
mng_datetime.age_days = np.nan
df = df.drop(mng_datetime.index)
df = df.append(mng_datetime).reset_index(drop=True)

df.query("age_months >= 0", inplace=True)
df.query("age_months <= 12", inplace=True)
df.query("age_days >= 0", inplace=True)
df.query("age_days <= 31", inplace=True)

# reset to unique index
df = df.reset_index(drop=True)

# avoid na products and sums
values = {"age_years": 0, "age_months": 0, "age_days": 0}
df.fillna(value=values, inplace=True)

# converting months to days using 30.5 as the denom
# this is in-line with CoD's process and was signed of by Steve
df["age_months"] = df["age_months"].astype(int) * 30.5
df.loc[df.age_years == 0, "age_years"] = (
    df["age_days"].astype(int) + df["age_months"].astype(int)
) / 365

keep = [
    "year_start",
    "year_end",
    "sex_id",
    "age_years",
    "outcome_id",
    "dx_1",
    "dx_2",
    "dx_3",
    "facility_id",
]
df = df[keep]

# Replace feature names on the left with those found in data where appropriate,
# other columns were modified in first dictionary
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    "age_years": "age",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
}


# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)


# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

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

df["representative_id"] = 1  # This MNG file is nationally representative

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["location_id"] = 38
df["source"] = "MNG_H_INFO"
# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2
# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

####################################################
# Manual Processing and Renaming Values
###################################################

# convert strings to true null for all columns(if there are any)
df = df.replace({"nan": np.nan})

# male, female, and "male,female"
df.replace(
    {"sex_id": {"Эрэгтэй": 1, "Эмэгтэй": 2, "Эрэгтэй, эмэгтэй": 3, "Амбулаторийн": 3}},
    inplace=True,
)
# there is one nan
df.dropna(subset=["sex_id"], inplace=True)

# Rename outcome ID for inpatient and outpatient, outpatient numeric values = discharge
df["outcome_id"] = df["outcome_id"].replace(
    ["Сайжирсан", "Хэвэндээ", "Эдгэрсэн", "Дордсон", np.nan, 0.0, 1.0, 2.0, 3.0],
    "discharge",
)
df["outcome_id"] = df["outcome_id"].replace(["Нас барсан"], "death")

# Case cast the cause_codes
df["dx_1"] = df["dx_1"].str.upper()
df["dx_2"] = df["dx_2"].str.upper()
df["dx_3"] = df["dx_3"].str.upper()

####################################################
# Clean variables
###################################################

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

# took dx_1, dx_2, dx_3 out because we'll want those as trull nulls during transformation
str_cols = ["source", "facility_id", "outcome_id", "dx_1", "dx_2", "dx_3"]

if df[str_cols].isnull().any().any():
    format(df[str_cols].columns[df[str_cols].isnull().any()])
    print("\n These NaNs will be converted to the string 'nan' \n")

# Remove non-alphanumeric characters from dx feats
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
for feat in diagnosis_feats:
    df[feat] = formatting.sanitize_diagnoses(df[feat])

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age_years' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df = demographic.age_binning(
    df, terminal_age_in_data=True, clinical_age_group_set_id=AGE_GROUP_SET_ID
)
df = df.drop("age", axis=1)

# Swap dx_1 and dx_2
df = formatting.swap_ecode_with_ncode(df, icd_vers=10)

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2019: 469234, 2020: 469235, 2018: 470741}
df["nid"] = df["year_start"].map(nid_dictionary)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################

# Saving the wide file before changing to long ##need to figure out filepath
write_path = FILEPATH
general_purpose.write_hosp_file(df, write_path, backup=True)

write_path = FILEPATH
df.to_stata(write_path)

# convert string nans and blanks to true null for cause_code column
df["dx_1"] = df["dx_1"].replace("nan", np.nan)
df["dx_2"] = df["dx_2"].replace("nan", np.nan)
df["dx_2"] = df["dx_2"].replace("", np.nan)
df["dx_3"] = df["dx_3"].replace("nan", np.nan)
df["dx_3"] = df["dx_3"].replace("", np.nan)

# Transform from wide to long when necessary
if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    stack_idx = [n for n in df.columns if "dx_" not in n]
    len_idx = len(stack_idx)
    df = df.set_index(stack_idx).stack().reset_index()
    df = df.rename(columns={"level_{}".format(len_idx): "diagnosis_id", 0: "cause_code"})
    df.loc[df["diagnosis_id"] != "dx_1", "diagnosis_id"] = 2
    df.loc[df.diagnosis_id == "dx_1", "diagnosis_id"] = 1
elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1
else:
    print("Something went wrong, there are no ICD code features")


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
agg_df = df.groupby(group_vars).agg({"val": "sum"}).reset_index()


#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################
# Arrange columns in our standardized feature order
columns_before = agg_df.columns
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
agg_df = agg_df[hosp_frmat_feat]
columns_after = agg_df.columns


# check if all columns are there
assert set(columns_before) == set(columns_after), "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert (
        hosp_frmat_feat[i] in agg_df.columns
    ), "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

# check data types
for i in agg_df.drop(
    ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert agg_df[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(agg_df["year_start"].unique()) == len(
    agg_df["nid"].unique()
), "number of feature levels of years and nid should match number"
assert len(agg_df["age_start"].unique()) == len(agg_df["age_end"].unique()), (
    "number of feature levels age start should match number of feature " + r"levels age end"
)
assert (
    len(agg_df["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"

# check sex_id col for acceptable values
good_ids = [1, 2, 3]
good_in_data = [n for n in good_ids if n in agg_df.sex_id.unique()]
assert (
    set(agg_df["sex_id"].unique()).symmetric_difference(set(good_in_data)) == set()
), "There are unexpected sex_id values {} in the df".format(agg_df.sex_id.unique())

assert (
    len(agg_df["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
# assert len(agg_df['source'].unique()) == 1,\
# "source should only have one feature level"

assert (agg_df.val >= 0).all(), "for some reason there are negative case counts"

# create csv for plots
agg_df.to_csv(FILEPATH)


#################################################################
# WRITE TO FILE
#################################################################
# Saving the file

write_path = (FILEPATH
)

general_purpose.write_hosp_file(agg_df, write_path, backup=True)

########################################################
# UPDATING MERGED NIDS AND SOURCE TABLE
########################################################

from clinical_db_tools.nid_tables.new_source import InpatientNewSource

ins = InpatientNewSource(df)
nids = [469234, 469235]
src_metadata = {
    "pipeline": {"inpatient": nids, "claims": [], "outpatient": nids + [470741]},
    "uses_env": {1: [], 0: nids + [470741]},
    "age_sex": {1: nids, 2: [], 3: [], 0: [470741]},
    "merged_dict": {474055: nids},
}
ins.process(**src_metadata)
