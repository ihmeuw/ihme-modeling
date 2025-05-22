# coding: utf-8

"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME
Edited by: USERNAME

Format PHL health claims data as if it were hospital data
Updated May 2020 to swap live birth codes out of primary dx
"""
import multiprocessing
import platform
import re
import time
import warnings
from itertools import zip_longest

import numpy as np
import pandas as pd

# load our functions
from crosscutting_functions import demographic, general_purpose
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions.live_births import swap_live_births

start = time.time()

#####################################################
# helper functions
#####################################################


def reshape_long_mp(each_df):
    each_df = each_df.set_index(list(int_cols + str_cols)).stack().reset_index()

    each_df[0] = each_df[0].str.upper()
    # drop blank diagnoses
    each_df = each_df[~each_df[0].isin(["NONE", "NAN", "", np.nan])]
    # rename cols
    each_df = each_df.rename(columns={"level_14": "diagnosis_id", 0: "cause_code"})
    # replace diagnosis_id with codes, 1 for primary, 2 for secondary
    # and beyond
    each_df["diagnosis_id"] = np.where(each_df["diagnosis_id"] == "dx_1", 1, 2)
    print("One subset df is done")
    return each_df


def fill_cols(df):
    df["location_id"] = 16

    # group_unit 1 signifies age data is in years
    df["source"] = "PHL_HICC"

    # code 1 for ICD-9, code 2 for ICD-10
    df["code_system_id"] = 2  # codebook says icd 10

    # replacing missing sexes with the code 3
    df.loc[(df.sex_id != "M") & (df.sex_id != "F"), "sex_id"] = 3
    df["sex_id"].replace(["F", "M"], [2, 1], inplace=True)

    df["year_end"] = df["year_start"]

    return df


def create_dxdf(df):
    """convert column of cause code lists to wide dataframe"""
    dat = pd.DataFrame.from_records(zip_longest(*df["cc_list"].values)).T
    dat.index = df.index
    dat = dat.fillna(value=np.nan)
    return dat


def fix_cause_codes(df):
    # clean the cause code col to remove non alpha numeric symbols
    df["cause_code"] = df["cause_code"].str.replace(r"\W", "")
    # make all ICD codes upper case
    df["cause_code"] = df["cause_code"].str.upper()

    # split and capture on capital letters followed by digits like so
    df["cc_list"] = df.cause_code.map(lambda x: re.split(r"([A-Z]\d+)", x))

    # now you've got a col of lists which need to be split out into new columns
    # remove blank elements in list
    df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  # py 3

    n_cores = 6
    warnings.warn(
        (
            f"This script uses multiprocessing with {n_cores} pools. If ran on "
            f"fair cluster then it needs to be ran with at least {n_cores} threads."
        )
    )

    # runtime on 3 cores is 15 minutes
    # runtime on 6 cores is 8 minutes
    print("Start multiprocessing", (time.time() - start) / 60)

    # todo pool here
    p = multiprocessing.Pool(n_cores)
    tasks = 50
    df_l = list(p.map(create_dxdf, np.array_split(df, tasks)))
    dx_df = pd.concat(df_l)
    print("Done multiprocessing", (time.time() - start) / 60)

    # then name cols
    nums = np.arange(1, dx_df.shape[1] + 1, 1)
    names = ["dx_" + str(num) for num in nums]
    dx_df.columns = names

    df.drop(["cause_code", "cc_list"], axis=1, inplace=True)
    # data was split out into two DFs so make sure the indices are the same
    assert (df.index == dx_df.index).all()
    df = pd.concat([df, dx_df], axis=1)

    # fill null and missing primary icd codes with np.nan
    placeholders = ["nan", "NAN", "NaN", "NONE", "NULL", ""]
    df.dx_1.fillna(np.nan, inplace=True)
    df.loc[df.dx_1.isin(placeholders), "dx_1"] = np.nan

    # Find all columns with dx_ at the start
    diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
    # Remove non-alphanumeric characters from dx feats
    for feat in diagnosis_feats:
        df[feat] = formatting.sanitize_diagnoses(df[feat])
        df[feat] = df[feat].astype(str)

    # swap live birth codes
    for col in list(df.filter(regex="^(dx_)").columns.drop("dx_1")):
        df.loc[df[col].isin(placeholders), col] = np.nan

    df = swap_live_births(df, drop_if_primary_still_live=False)
    return df


def run_format_program(df, split_int, clinical_age_group_set_id, extra_cols_wide):
    # If this assert fails uncomment this line:
    # df = df.reset_index(drop=True)
    assert df.shape[0] == len(df.index.unique()), (
        "index is not unique, "
        + "the index has a length of "
        + str(len(df.index.unique()))
        + " while the DataFrame ("
        + str(split_int)
        + ") has "
        + str(df.shape[0])
        + " rows try this: df = df.reset_index(drop=True)"
    )

    df.drop(["facility_id"], axis=1, inplace=True)

    # Rename features using dictionary created above
    df.rename(columns=hosp_wide_feat, inplace=True)

    # set difference of the columns you have and the columns you want,
    # yielding the columns you don't have yet
    new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
    df = df.join(new_col_df)

    print("Done renaming columns", (time.time() - start) / 60)
    #####################################################
    # FILL COLUMNS THAT SHOULD BE HARD CODED
    # this is where you fill in the blanks with the easy
    # stuff, like what version of ICD is in the data.
    #####################################################
    # These are completely dependent on data source
    df["representative_id"] = 1

    # group_unit 1 signifies age data is in years
    df["age_group_unit"] = 1

    # metric_id == 1 signifies that the 'val' column consists of counts
    df["metric_id"] = 1

    df.loc[df.facility_id == "T", "facility_id"] = "outpatient unknown"
    df.loc[df.facility_id == "F", "facility_id"] = "inpatient unknown"

    # Create a dictionary with year-nid as key-value pairs
    df = formatting.fill_nid(df, nid_dictionary)

    #######################
    # SWAP E AND N CODES
    #######################
    print("Start swapping", (time.time() - start) / 60)
    # use handy hosp prep function
    df = formatting.swap_ecode_with_ncode(df, 10)

    print("Ecodes swapped", (time.time() - start) / 60)

    #####################################################
    # CLEAN VARIABLES
    #####################################################

    # Columns contain only 1 optimized data type
    for col in list(int_cols + ["age"]):
        df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
    for col in str_cols:
        df[col] = df[col].astype(str)

    # Turn 'age' into 'age_start' and 'age_end'
    #   - bin into year age ranges
    #   - under 1, 1-4, 5-9, 10-14 ...
    # age pattern becomes very strange above age 112
    df = df[df["age"] < 112]

    total_obs = df.shape[0]

    # age_binning
    df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
    # terminal age start will be lumped into the terminal age group.
    df = demographic.age_binning(df, clinical_age_group_set_id=clinical_age_group_set_id)

    # convert unknown sexes to 3
    df.loc[(df.sex_id != 1) & (df.sex_id != 2), "sex_id"] = 3

    #####################################################
    # MANUAL PROCESSING
    # this is where fix the quirks of the data, like making values in the
    # data match the values we use.

    # For example, repalce "Male" with the number 1
    #####################################################
    # Find all columns with dx_ at the start
    diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

    wide = df.copy()
    # drop deaths
    wide = wide[wide.outcome_id != "death"]
    # drop 100% null columns
    for col in wide.columns:
        if wide[col].isnull().all():
            wide.drop(col, axis=1, inplace=True)
    # drop date/time columns
    if extra_cols_wide:
        wide.drop(
            ["adm_date", "dis_date", "date_of_death", "los", "enrollee_id"],
            axis=1,
            inplace=True,
        )
    wide.to_stata(FILEPATH
    )
    del wide

    #####################################################
    # IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
    #####################################################
    df = df[list(int_cols + str_cols) + list(diagnosis_feats)]

    df.dx_1 = df.dx_1.str.upper()
    df.loc[df.dx_1.isin(["NONE", "NAN", ""]), "dx_1"] = np.nan
    dx_1_val_counts = df.dx_1.value_counts()

    print("Start reshaping long", (time.time() - start) / 60)

    if platform.system == "Linux":
        p = multiprocessing.Pool(3)
        df_l = list(p.map(reshape_long_mp, np.array_split(df, 15)))
        df = pd.concat(df_l)
    else:
        df = reshape_long_mp(df)

    assert (
        dx_1_val_counts == df[df.diagnosis_id == 1].cause_code.value_counts()
    ).all, "Reshaping long changed the value count of primary ICD codes"

    print("Done reshaping long", (time.time() - start) / 60)
    # If individual record: add one case for every diagnosis
    df["val"] = 1

    row_diff = total_obs - df[df.diagnosis_id == 1].val.sum()
    assert row_diff == 0, "{} rows seem to have been lost".format(row_diff)

    #####################################################
    # GROUPBY AND AGGREGATE
    #####################################################
    # Check for missing values
    print("Are there missing values in any row?")
    null_condition = df.isnull().values.any()
    if null_condition:
        print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY\n")
    else:
        print(">> No.\n")

    # Group by all features we want to keep and sums 'val'
    group_vars = list(int_cols + str_cols + ["cause_code", "diagnosis_id"])
    df = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

    #####################################################
    # ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
    #####################################################
    # Arrange columns in our standardized feature order
    columns_before = df.columns

    hosp_frmat_feat = list(group_vars + ["val"])
    df = df[hosp_frmat_feat]
    columns_after = df.columns

    # check if all columns are there
    assert set(columns_before) == set(
        columns_after
    ), "You lost or added a column when reordering"
    for i in range(len(hosp_frmat_feat)):
        assert (
            hosp_frmat_feat[i] in df.columns
        ), "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

    # check data types
    for i in df.drop(
        ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
    ).columns:
        # assert that everything but cause_code, source, measure_id (for now)
        # are NOT object
        assert df[i].dtype != object, "%s should not be of type object" % (i)

    # check number of unique feature levels
    assert len(df["year_start"].unique()) == len(
        df["nid"].unique()
    ), "number of feature levels of years and nid should match number"
    assert len(df["age_start"].unique()) == len(df["age_end"].unique()), (
        "number of feature levels age start should match number of "
        + r"feature levels age end"
    )
    assert (
        len(df["diagnosis_id"].unique()) <= 2
    ), "diagnosis_id should have 2 or less feature levels"
    assert (
        len(df["sex_id"].unique()) <= 3
    ), "There should only be three feature levels to sex_id"
    assert (
        len(df["code_system_id"].unique()) <= 2
    ), "code_system_id should have 2 or less feature levels"
    assert len(df["source"].unique()) == 1, "source should only have one feature level"

    assert total_obs == df[df.diagnosis_id == 1].val.sum(), "{} cases were lost".format(
        total_obs - df[df.diagnosis_id == 1].val.sum()
    )
    return df


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
    "age": "age",
    "age_start": "age_start",
    "age_end": "age_end",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "is_otp": "facility_id",
}

nid_dictionary = {2013: 222560, 2014: 222563, 2019: 472563, 2020: 472564}

int_cols = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",
    "age_start",
    "age_end",
    "sex_id",
    "nid",
    "representative_id",
    "metric_id",
    "code_system_id",
]

prefrmt_int_cols = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",
    "age",
    "sex_id",
]

str_cols = ["source", "facility_id", "outcome_id"]


# ====================================================
# new code to extract ICD codes in a diff way
# ====================================================
file_2013_2014 = (FILEPATH
)
file_2019 = (FILEPATH
)
file_2020 = (FILEPATH
)


#####################################################
# 2013 & 2014
#####################################################
df_1314 = pd.read_hdf(file_2013_2014, key="df")
print("Data read in")

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert df_1314.shape[0] == len(df_1314.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df_1314.index.unique()))
    + " while the DataFrame has "
    + str(df_1314.shape[0])
    + " rows"
    + "try this: df = df.reset_index(drop=True)"
)

# rename claims cols
df_1314.rename(
    columns={
        "NEWPIN": "enrollee_id",
        "ICDCODES": "cause_code",
        "PATAGE": "age",
        "PATSEX": "sex_id",
        "OPD_TST": "is_otp",
        "DATE_ADM": "adm_date",
        "DATE_DIS": "dis_date",
        "CLASS_DEF": "facility_id",
        "DATE_OF_DEATH": "date_of_death",
    },
    inplace=True,
)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################
# These are completely dependent on data source
# df['representative_id'] = 0

# group_unit 1 signifies age data is in years
df_1314["age_group_unit"] = 1

df_1314["year_start"] = 0
# 16 rows dropped out of ~12.5m
df_1314.dropna(subset=["adm_date"], inplace=True)
typ = pd._libs.tslibs.timestamps.Timestamp
assert sum([isinstance(x, typ) for x in df_1314.adm_date]) == len(
    df_1314.adm_date
), "there are admin dates that are not Timestamp"

df_1314.loc[df_1314.adm_date < "2014-01-01", "year_start"] = 2013
df_1314.loc[df_1314.adm_date >= "2014-01-01", "year_start"] = 2014

# duplicate date of death col to turn it into outcome ID
df_1314["outcome_id"] = df_1314["date_of_death"]
df_1314.loc[df_1314.outcome_id.notnull(), "outcome_id"] = "death"
df_1314.loc[df_1314.outcome_id.isnull(), "outcome_id"] = "discharge"

df_1314 = fill_cols(df_1314)

print("Done filling columns")
#####################################################
# CLEAN VARIABLES
#####################################################

# remove age values below 0 and above 150
df_1314 = df_1314[(df_1314.age >= 0) & (df_1314.age < 150)]
total_obs = df_1314.shape[0]  # dropped some rows here

# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)

for col in prefrmt_int_cols:
    df_1314[col] = pd.to_numeric(df_1314[col], errors="raise", downcast="integer")
for col in str_cols:
    df_1314[col] = df_1314[col].astype(str)

############################################
# fix cause codes
############################################
df_1314 = fix_cause_codes(df_1314)

str_feats = [
    "enrollee_id",
    "is_otp",
    "facility_id",
    "date_of_death",
    "source",
    "outcome_id",
]
for feat in str_feats:
    df_1314[feat] = df_1314[feat].astype(str)
print("Done cleaning columns", (time.time() - start) / 60)

# remove day cases
# create a length of stay variable
df_1314["los"] = df_1314["dis_date"] - df_1314["adm_date"]

# drop day cases unless the patient died
# get pre drop count of non death day cases
tmp = df_1314[df_1314.los == "0 days"].copy()
tmp = tmp[tmp.outcome_id != "death"]
non_death_day_cases = tmp.shape[0]
del tmp

# get the pre shape of data, the diff between now and dropping should equal
# the number of non-death day cases
pre = df_1314.shape[0]
df_1314 = df_1314[(df_1314["los"] != "0 days") | (df_1314.outcome_id != "discharge")]

total_obs = df_1314.shape[0]

assert pre - df_1314.shape[0] == non_death_day_cases, "The wrong number of cases were dropped"
assert (
    df_1314[df_1314.los == "0 days"].outcome_id.unique()[0] == "death"
), "There are day cases that didn't result in death present"
assert (
    df_1314[df_1314.los == "0 days"].outcome_id.unique().size == 1
), "Too many unique outcome id values for day cases"

print("Done removing day cases", (time.time() - start) / 60)


#####################################################
# 2019 & 2020
#####################################################
df1 = pd.read_csv(file_2019)
df1["DATE_ADM"] = 2019

df2 = pd.read_csv(file_2020)
df2["DATE_ADM"] = 2020

print("Data read in")

df_new = df1.append(df2, ignore_index=True)

df_new.drop("Unnamed: 0", axis=1, inplace=True)

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert df_new.shape[0] == len(df_new.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df_new.index.unique()))
    + " while the DataFrame has "
    + str(df_new.shape[0])
    + " rows"
    + "try this: df = df.reset_index(drop=True)"
)

# rename claims cols
df_new.rename(
    columns={
        "SERIES": "series",
        "CODE": "cause_code",
        "AGE": "age",
        "SEX": "sex_id",
        "DATE_ADM": "year_start",
        "TYP": "type",
        "CLASS": "facility_id",
        "IS_OUT_PATIENT": "outpatient",
    },
    inplace=True,
)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################
# These are completely dependent on data source
# df['representative_id'] = 0

# group_unit 1 signifies age data is in years
df_new["age_group_unit"] = 1  # ages are in years

df_new["outcome_id"] = "case"

df_new = fill_cols(df_new)

print("Done filling columns")
#####################################################
# CLEAN VARIABLES
#####################################################

# remove age values below 0 and above 150
df_new = df_new[(df_new.age >= 0) & (df_new.age < 150)]

# remove outpatient and procedural records
df_new.loc[df_new.outpatient, "facility_id"] = "T"
df_new.loc[~df_new.outpatient, "facility_id"] = "F"
df_new["is_otp"] = df_new["facility_id"]

# drop procedural and nulls
df_new = df_new[df_new.type == "MEDICAL"]

total_obs = df_new.shape[0]  # dropped some rows here

for col in prefrmt_int_cols:
    df_new[col] = pd.to_numeric(df_new[col], errors="raise", downcast="integer")
for col in str_cols:
    df_new[col] = df_new[col].astype(str)

############################################
# fix cause codes
############################################
df_new = fix_cause_codes(df_new)

str_feats = ["series", "is_otp", "facility_id", "source", "outcome_id"]
for feat in str_feats:
    df_new[feat] = df_new[feat].astype(str)
print("Done cleaning columns", (time.time() - start) / 60)


# ====================================================
# ====================================================
# run format program
# ====================================================
# ====================================================
n = 100
df_list = np.array_split(df_1314, n)
del df_1314

final_list = []
split_int = 0
for df in df_list:
    split_int += 1
    df = run_format_program(df, split_int, clinical_age_group_set_id=3, extra_cols_wide=True)
    final_list.append(df)

df = pd.concat(final_list)
# give new data index 0 easy to see in error message if any
df_new = run_format_program(df_new, 0, clinical_age_group_set_id=2, extra_cols_wide=False)
df = df.append(df_new, ignore_index=True)


#####################################################
# REVIEW AGAINST ARCHIVED DATA
#####################################################
base = FILEPATH
compare_df = pd.read_hdf((f"{base}/_archive/2017_09_14_formatted_PHL_HICC.H5"))
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
assert (abs(m.loc[m["new_minus_old"].notnull(), "new_minus_old"]) < 1).all(), "large diffs"

#####################################################
# WRITE TO FILE
#####################################################
# Saving the file
write_path_final = FILEPATH
general_purpose.write_hosp_file(df, write_path_final, backup=True)
