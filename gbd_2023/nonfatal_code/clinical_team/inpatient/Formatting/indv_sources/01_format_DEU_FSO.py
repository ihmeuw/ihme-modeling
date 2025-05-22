"""
Last Updated: May 2023
@author: USERNAME

Template for formatting Germany inpatient source
from the Federal Statistics Office
Years of data captured: 2000-2021

GHDx series:
ADDRESS

"""
import glob
import os
import platform
import re
import warnings

import pandas as pd
from db_queries import get_location_metadata

# load our functions
from crosscutting_functions.nid_tables.new_source import InpatientNewSource
from crosscutting_functions import *

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
release_id = 10
# DEU location id from lookup hierarchy
all_locs = get_location_metadata(location_set_id=35, release_id=release_id)
deu_locid = all_locs[all_locs.location_name == "Germany"]["location_id"].values[0]

# glob & read all files
path = FILEPATH
path_2 = (FILEPATH
)  # had to convert xls files to xlsx here due to outdated xlrd ver

all_files = glob.glob(os.path.join(path, "*.XLSX")) + glob.glob(os.path.join(path_2, "*.xlsx"))

df_list = []
except_yrs = list(range(2005, 2010))  # years where sheet position changed

# loop for reading and parsing year from all files
for f in all_files:
    # get year from filenames
    fn = os.path.splitext(os.path.basename(f))[0]
    year = int(FILEPATH)

    if year in except_yrs:
        temp_df = pd.read_excel(f, sheet_name=5)
    else:
        temp_df = pd.read_excel(f, sheet_name=4)

    temp_df["year"] = year
    df_list.append(temp_df)

#####################################################
# MANUAL PROCESSING & RECODE
#####################################################
new_col_names = [
    "dx_1",
    "sex_id",
    "total",
    "0-1",
    "1-5",
    "5-10",
    "10-15",
    "15-20",
    "20-25",
    "25-30",
    "30-35",
    "35-40",
    "40-45",
    "45-50",
    "50-55",
    "55-60",
    "60-65",
    "65-70",
    "70-75",
    "75-80",
    "80-85",
    "85-90",
    "90-95",
    "95-125",
    "0-125",
    "year",
]
new_col_names_alt = new_col_names.copy()
new_col_names_alt.remove("15-20")
new_col_names_alt[7:7] = ["15-17", "18-19"]

# aligning age group between years
for d in df_list:
    # future runs should make sure the iloc stays consistent with age groups
    if len(d.columns) == 26:
        d.columns = new_col_names

    elif len(d.columns) == 27:
        d.columns = new_col_names_alt
        d["15-20"] = d["15-17"] + d["18-19"]
        d.drop(columns=["15-17", "18-19"], inplace=True)

    # clean up rows
    d.drop(
        d.loc[
            (d["dx_1"] == "ICD-10-4") | (d["dx_1"] == "Unbekannt") | (d["dx_1"].isnull())
        ].index,
        inplace=True,
    )

# combine all dfs and convert from wide to long
df = pd.concat(df_list).drop(columns=["total"])
df = pd.melt(df, id_vars=["dx_1", "sex_id", "year"])

# further clean-up and recodes
df[["age_start", "age_end"]] = df["variable"].str.split("-", n=1, expand=True)
df.rename(columns={"year": "year_start", "value": "val"}, inplace=True)
df["year_end"] = df["year_start"]
df["sex_id"].replace(["m", "w"], [1, 2], inplace=True)
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3  # assign unknown sex_id
df["val"] = pd.to_numeric(df["val"])
df.drop(columns={"variable"}, inplace=True)

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert_msg = f"""index is not unique, the index has a length of
{str(len(df.index.unique()))} while the DataFrame has
{str(df.shape[0])} rows. Try this: df = df.reset_index(drop=True)"""
assert_msg = " ".join(assert_msg.split())
assert df.shape[0] == len(df.index.unique()), assert_msg

#####################################################
# ADD HOSPITAL FEATURE COLUMNS
#####################################################
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
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
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


df["representative_id"] = 1  # nationally rep. only
df["outcome_id"] = "case"  # unknown/no outcome-related column
df["location_id"] = deu_locid  # DEU location from hierarchy
df["source"] = "DEU_FSO"
df["code_system_id"] = 2  # ICD 10
df["metric_id"] = 1  # data in count space, default
df["facility_id"] = "inpatient unknown"
df["age_group_unit"] = 1  # data is in years, default

# Check codes for code system
dx_list = [e for e in df.columns if e.startswith("dx_")]
df["dx_1"] = df["dx_1"].astype(str)

# Create a dictionary with year-nid as key-value pairs
nid_dict = {
    2000: 520075,
    2001: 520078,
    2002: 520080,
    2003: 520082,
    2004: 520084,
    2005: 520086,
    2006: 520096,
    2007: 520098,
    2008: 520100,
    2009: 520102,
    2010: 520104,
    2011: 520106,
    2012: 520108,
    2013: 520110,
    2014: 520112,
    2015: 520114,
    2016: 520117,
    2017: 520119,
    2018: 520125,
    2019: 520127,
    2020: 520129,
    2021: 520131,
}
df = hosp_prep.fill_nid(df, nid_dict)

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

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])

df = hosp_prep.swap_ecode_with_ncode(df, icd_vers=10)

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

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

s = [1, 2, 3]
check_sex = [n for n in df.sex_id.unique() if n not in s]
assert len(check_sex) == 0, "There is an unexpected sex_id value"

assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"

assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert len(df_agg.loc[df_agg.val < 0]) == 0, "for some reason there are negative case counts"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = (FILEPATH
)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)

#####################################################
# UPDATE SOURCE_TABLE
#####################################################

# Update nid tables.
ins = InpatientNewSource(df_agg)
nids = list(nid_dict.values())

src_metadata = {
    "pipeline": {"inpatient": nids, "claims": [], "outpatient": []},
    "uses_env": {1: [], 0: nids},
    "age_sex": {1: [], 2: nids, 3: []},
    "merged_dict": {},
}
ins.process(**src_metadata)
