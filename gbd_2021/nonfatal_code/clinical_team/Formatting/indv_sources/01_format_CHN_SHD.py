# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017
@author: 

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

PLEASE put link to GHDx entry for the source here
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import glob
import getpass


user = getpass.getuser()
# load our functions
prep_path = r"FILEPATH".format(user)
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
inp_files = glob.glob(r"FILEPATH")


warnings.warn(
    "need to continue prepping inp main diagnosis data. it appears to be an extension, not a subset"
    " of total diagnosis. b/c the most common dx1 codes don't make sense, ie hypertension"
)
inp_main_files = glob.glob(r"FILEPATH")

otp_files = glob.glob(r"FILEPATH")

# filepath = root + r"FILEPATH"
inp = pd.concat(
    [pd.read_csv(f, sep="\t", encoding="GBK") for f in inp_files], ignore_index=True
)

inp_main = pd.concat(
    [pd.read_csv(f, sep="\t", encoding="GBK") for f in inp_main_files],
    ignore_index=True,
)


pre_cases = inp.jzcnt.sum()
inp["facility_id"] = "hospital"
# keep select columns
inp = inp[
    ["sex", "age", "zdbm", "flg_sh", "jzcnt", "jzpeople", "facility_id", "zyts_mean"]
]
# drop migrant population
inp = inp[inp.flg_sh != "非沪籍"]
inp.drop("flg_sh", axis=1, inplace=True)

pre_main_cases = inp_main.jzcnt.sum()
inp_main["facility_id"] = "hospital"
# keep select columns
inp_main = inp_main[
    ["sex", "age", "zdbm", "flg_sh", "jzcnt", "jzpeople", "facility_id", "zyts_mean"]
]
inp_main.rename(columns={"zdbm": "dx_1"}, inplace=True)
# drop migrant population
inp_main = inp_main[inp_main.flg_sh != "非沪籍"]
inp_main.drop("flg_sh", axis=1, inplace=True)

# cast cols to numeric
num_cols = inp.columns.drop(["zdbm", "facility_id"])
for col in num_cols:
    inp[col] = pd.to_numeric(inp[col], errors="coerce")
inp.isnull().sum() / inp.shape[0]

num_cols = inp_main.columns.drop(["dx_1", "facility_id"])
for col in num_cols:
    inp_main[col] = pd.to_numeric(inp_main[col], errors="coerce")
inp_main.isnull().sum() / inp_main.shape[0]

# drop the very very few null age and sex rows
# inp[(inp['age'].notnull()) & (inp['sex'].notnull())]

assert inp.zyts_mean.min() >= 1
inp.drop("zyts_mean", axis=1, inplace=True)
inp_cases = inp.jzcnt.sum()

assert inp_main.zyts_mean.min() >= 1
inp_main.drop("zyts_mean", axis=1, inplace=True)
inp_main_cases = inp_main.jzcnt.sum()

otp = []
for f in otp_files:
    if "79.csv" in f:
        print("fixing the bad one")
        # for some reason there's a quote at the beginning and end of every
        # line of data for this one file
        tmp = pd.read_csv(f, sep="\t", encoding="GBK", quoting=3)
    else:
        tmp = pd.read_csv(f, sep="\t", encoding="GBK")
    # print(f, tmp.regname2.unique().size)
    otp.append(tmp)

back = otp.copy()

otp = pd.concat(otp, ignore_index=True)
otp["facility_id"] = "outpatient unknown"

assert otp.regname2.unique().size <= 34

otp = otp[["sex", "age", "zdbm", "flg_sh", "jzcnt", "jzpeople", "facility_id"]]
# drop migrant population
otp = otp[otp.flg_sh != "非沪籍"]
otp.drop("flg_sh", axis=1, inplace=True)
# cast cols to numeric
num_cols = otp.columns.drop(["zdbm", "facility_id"])
for col in num_cols:
    otp[col] = pd.to_numeric(otp[col], errors="coerce")
otp.isnull().sum() / otp.shape[0]

# bring inp and outpatient together
df = pd.concat([inp, inp_main, otp], ignore_index=True)

# split out the dx cols
col_names = ["dx_{}".format(i) for i in np.arange(2, 13, 1)]
dx_df = df.zdbm.str.split(";", expand=True)
dx_df.columns = col_names

# concat the dx dataframe onto the hosp dataframe
assert df.shape[0] == dx_df.shape[0]
df = pd.concat([df, dx_df], axis=1)
# drop the old dx columns
df.drop("zdbm", axis=1, inplace=True)

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
    "year_start": "year_start",
    "year_end": "year_end",
    "sex": "sex_id",
    "age": "age",
    # 'age_start': 'age_start',
    # 'age_end': 'age_end',
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "facility_id": "facility_id",
    "jzcnt": "val",
}
#    # diagnosis varibles
#    'dx_1': 'dx_1',
#    'dx_2': 'dx_2',
#    'dx_3': 'dx_3'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

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
df["location_id"] = 514

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "CHN_SHD"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# case is the sum of live discharges and deaths
df["outcome_id"] = "case"

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

df["year_start"] = 2016
df["year_end"] = 2016

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2016: 336860}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)

# drop unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

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

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
df.fillna("none", inplace=True)
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])
    # remove the trailing X from the data
    df.loc[df[feat].str.contains("x$"), feat] = df.loc[
        df[feat].str.contains("x$"), feat
    ].str[:-1]
    # sometimes there's a second trailing x, remove it as well
    df.loc[df[feat].str.contains("x$"), feat] = df.loc[
        df[feat].str.contains("x$"), feat
    ].str[:-1]

# get the none values back to na
df[diagnosis_feats] = df[diagnosis_feats].replace("none", np.nan)

# before_reshape = df.copy()

# df = before_reshape.copy()

pre = df.val.sum()
diff = df[(df.facility_id == "outpatient unknown") & (df.dx_2.isnull())].val.sum()
# there are a ton of outpatient cases with no dx
df = df[(df.facility_id != "outpatient unknown") | (df.dx_2.notnull())]
assert pre - df.val.sum() == diff

# shift outpatient dx_2 into dx_1 position so checks can pass
# df.loc[df.facility_id == 'outpatient unknown', 'dx_1'] =\
#    df.loc[df.facility_id == 'outpatient unknown', 'dx_2']
# df.loc[df.facility_id == 'outpatient unknown', 'dx_2'] = np.nan
pre = df.val.sum()


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
# df['val'] = 1

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

# remove things that aren't letters or numbers, specifically chinese characters
df_agg["cause_code"] = df_agg["cause_code"].str.upper()
df_agg["cause_code"] = df_agg.cause_code.str.replace("[^a-zA-Z0-9]", "")

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
assert (
    len(df_agg["sex_id"].unique()) == 3
), "There should only be three feature levels to sex_id"
assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (df.val >= 0).all(), "for some reason there are negative case counts"

assert (
    inp_cases == df[(df.facility_id == "hospital") & (df.diagnosis_id == 1)].val.sum()
), "Inpatient primary case counts have changed"
assert (
    inp_cases
    == df_agg[(df_agg.facility_id == "hospital") & (df_agg.diagnosis_id == 1)].val.sum()
), "Inpatient primary case counts have changed"
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
