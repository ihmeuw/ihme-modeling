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
import glob
import os
import re

# load our functions
USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
sys.path.append(USER_path)
sys.path.append(USER_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

pd.options.display.max_columns = 99


def fix_col_names(n):
    """
    clean up these column names
    """
    # split into a list along the "_" string
    n = n.split("_")

    # set correct start/end for youngs
    if "<" in "_".join(n):
        n[2] = "0_1"
    # correct s/e for terminal age
    if "70+" in "_".join(n):
        n[2] = "70_124"
    # correct s/e for missing ages
    if "N/Av" in "_".join(n):
        n[2] = "0_124"
    # clean up the rest, remove spaces
    if "-" in "_".join(n):
        n[2] = n[2].split("-")
        n[2] = "_".join(n[2])
        n[2] = n[2].replace(" ", "")

    # bring the list back together
    n = "_".join(n)
    return n


# This data needs to be heavily pre-processed before use

data_dir = "FILEPATH"

files1 = glob.glob(data_dir + "*.xlsx")
files2 = glob.glob(data_dir + "*.xls")
files = files1 + files2
assert len(files) == 12

df = pd.read_excel(files[10], header=None)
for i in np.arange(0, 3, 1):
    # forward fill missing values where possible
    df.loc[i] = df.loc[i].fillna(method="ffill")
    # fill the remaining NAs with random symbol
    df.loc[i] = df.loc[i].fillna("^")

# now we have a rough set of column names
col_names = df.loc[0].str.cat(df.loc[1], sep="_").str.cat(df.loc[2], sep="_")
col_names[0:3] = ["number", "immr", "disease_name"]

col_names = [fix_col_names(n) for n in col_names]


df_list = []  # create a list to append each year to
exp_col_len = 35
for i in np.arange(0, len(files), 1):
    # get the year of the data from the filename
    year = int(os.path.basename(files[i]).split("_")[1].split(".")[0])
    tmp = pd.read_excel(files[i], header=None)
    assert tmp.shape[1] == exp_col_len
    tmp.columns = col_names
    tmp["year_start"], tmp["year_end"] = [year, year]

    # drop the first 3 rows. They contain the vals we used to make "col_names"
    tmp = tmp.loc[3:]
    df_list.append(tmp)
    print(tmp.shape, files[i])

# every year brought together
df = pd.concat(df_list, ignore_index=True)
# drop the rows where years were summed up
df = df[df.number != "Grand Total"]
# drop rows with null disease names, some formatting issues and they can't be
# mapped to anything
df = df[df.disease_name.notnull()]

# lost disease number and immr code cols
df.drop(["number", "immr"], inplace=True, axis=1)

# prep to check if cases will be lost during reshaping
# also convert count cols to numeric
run_sum = 0
count_cols = [x for x in col_names if "LIVE" in x or "DEATHS" in x]
for col in count_cols:
    df[col] = df[col].replace(".", "0")
    df[col] = pd.to_numeric(df[col], errors="raise")
    run_sum = run_sum + df[col].sum()

# Check the Total columns provided in the data to the totals we manually
# calculate
to_calc = [n for n in count_cols if "Total" not in n]
sum_cols = [n for n in count_cols if "Total" in n]

df["calc_total"] = round(df[to_calc].sum(axis=1))
df["data_total"] = round(df[sum_cols].sum(axis=1))

assert df[df.calc_total != df.data_total].shape[0] == 0

# remvoe the totals from our running sum
run_sum = run_sum - df[[n for n in df.columns if "Total" in n]].sum().sum()

# drop all the total columns
to_drop = [n for n in df.columns if "Total" in n or "total" in n]
df.drop(to_drop, axis=1, inplace=True)

long_df = df.set_index(["disease_name", "year_start", "year_end"]).stack().reset_index()
long_df.rename(columns={"level_3": "out_sex_age", 0: "val"}, inplace=True)

# check if cases were lost after reshaping
assert run_sum == long_df.val.sum()

# split out the wide columns
long_df["outcome_id"], long_df["sex_id"], long_df["age_start"], long_df["age_end"] = (
    long_df["out_sex_age"].str.split("_", 3).str
)

assert (long_df.outcome_id.unique() == ["LIVE DISCHARGES", "DEATHS"]).all()
long_df["outcome_id"] = long_df["outcome_id"].replace(
    ["DEATHS", "LIVE DISCHARGES"], ["death", "discharge"]
)

assert (long_df.sex_id.unique() == ["Male", "Female"]).all()
long_df["sex_id"] = long_df["sex_id"].replace(["Male", "Female"], [1, 2])

# write some data for USER to make a special map for the grouped ICD codes
find_unmappable_names_for_USER = False
if find_unmappable_names_for_USER:
    df["disease_name"] = df["disease_name"].astype(str)
    df["raw_disease_name"] = df["disease_name"]

    df["disease_name"] = df["disease_name"].str.replace("\W", "")

    # split and capture on capital letters followed by digits like so
    df["cc_list"] = df.disease_name.map(lambda x: re.split("([A-Z]\d+)", x))

    # now you've got a col of lists which need to be split out into new columns
    # remove blank elements in list
    if sys.version[0] == "3":
        df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  # py 3
    if sys.version[0] == "2":
        df.cc_list = df.cc_list.map(lambda x: [_f for _f in x if _f])  # py 2

    # length of cc_list
    df["cc_list_len"] = df.cc_list.apply(len)

    to_m = df[(df.cc_list_len > 2) | (df.cc_list_len == 1)]
    to_m = to_m[["disease_name", "raw_disease_name", "cc_list"]].drop_duplicates(
        "disease_name"
    )
    to_m.sort_values("raw_disease_name", inplace=True)
    to_m.to_csv(r"FILEPATH", index=False)

    # expand each element in cc_list into it's own column of a new df
    dat = df["cc_list"].apply(pd.Series)

    # these are the rows with just 1 icd code
    dat2 = dat[dat[2].isnull()][[0, 1]]
    dat2.columns = ["disease_name", "cause_code"]

    # map them to nfc
    # read in clean maps
    maps = pd.read_csv(r"FILEPATH")
    maps = maps[maps.code_system_id == 2]
    maps["cause_code"] = maps["cause_code"].str.upper()
    maps = maps[["cause_code", "nonfatal_cause_name"]].drop_duplicates("cause_code")
    dat2 = dat2.merge(maps, how="left", on="cause_code")

    # find unmapped rows, there shouldn't be any
    assert (
        0
        == dat2[
            (dat2.nonfatal_cause_name.isnull()) & (dat2.cause_code.notnull())
        ].shape[0]
    )


# prep disease name into dx_1
df = long_df.copy()
df["disease_name"] = df["disease_name"].astype(str)
df["raw_disease_name"] = df["disease_name"]

df["disease_name"] = df["disease_name"].str.replace("\W", "")

# split and capture on capital letters followed by digits like so
df["cc_list"] = df.disease_name.map(lambda x: re.split("([A-Z]\d+)", x))

# now you've got a col of lists which need to be split out into new columns
# remove blank elements in list
if sys.version[0] == "3":
    df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  # py 3
if sys.version[0] == "2":
    df.cc_list = df.cc_list.map(lambda x: [_f for _f in x if _f])  # py 2

# length of cc_list
df["cc_list_len"] = df.cc_list.apply(len)
# expand each element in cc_list into it's own column of a new df
dat = df["cc_list"].apply(pd.Series)

df = pd.concat([df, dat[1]], axis=1)
df["dx_1"] = np.nan
df.loc[df.cc_list_len == 2, "dx_1"] = df.loc[df.cc_list_len == 2, 1]

df.loc[df.cc_list_len != 2, "dx_1"] = df.loc[df.cc_list_len != 2, "raw_disease_name"]
assert df.dx_1.isnull().sum() == 0, "There are nulls in dx 1"
df.drop(
    ["cc_list", "raw_disease_name", "disease_name", "out_sex_age", "cc_list_len", 1],
    axis=1,
    inplace=True,
)

#####################################################
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

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
df["location_id"] = 17

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "LKA_IMMR"

# hospital data
df["facility_id"] = "hospital"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {
    2015: 328684,
    2014: 328682,
    2013: 259989,
    2012: 259990,
    2011: 328685,
    2010: 259991,
    2009: 259992,
    2008: 259993,
    2007: 259994,
    2006: 259995,
    2005: 259996,
    2004: 259997,
}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

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

## raw data is already age binned
# df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
# df = age_binning(df)

# drop unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

# Create year range if the data covers multiple years
# df = year_range(df)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = stack_merger(df)

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
    len(df_agg["sex_id"].unique()) == 2
), "There should only be two feature levels to sex_id"
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
write_path = root + r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
