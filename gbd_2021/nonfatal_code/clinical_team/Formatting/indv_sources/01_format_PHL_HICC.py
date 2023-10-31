# coding: utf-8

"""
Created on Mon Jan 23 17:32:28 2017
@author: 

Format PHL health claims data as if it were hospital data
Updated May 2020 to swap live birth codes out of primary dx
"""
import pandas as pd
import platform
import numpy as np
import sys
import re
import time
import multiprocessing
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

start = time.time()


# ====================================================
# new code to extract ICD codes in a diff way
# ====================================================

df = pd.read_hdf("FILEPATH", key="df",)
print("Data read in")

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

# rename claims cols
df.rename(
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
df["location_id"] = 16

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "PHIL_HEALTH_CLAIMS"

df["year_start"] = 0
df.loc[df.adm_date < "2014-01-01", "year_start"] = 2013
df.loc[df.adm_date >= "2014-01-01", "year_start"] = 2014
df["year_end"] = df["year_start"]

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2  # codebook says icd 10

# duplicate date of death col to turn it into outcome ID
df["outcome_id"] = df["date_of_death"]
df.loc[df.outcome_id.notnull(), "outcome_id"] = "death"
df.loc[df.outcome_id.isnull(), "outcome_id"] = "discharge"

# replacing missing sexes with the code 3
df.loc[(df.sex_id != "M") & (df.sex_id != "F"), "sex_id"] = 3
df["sex_id"].replace(["F", "M"], [2, 1], inplace=True)

print("Done filling columns")
#####################################################
# CLEAN VARIABLES
#####################################################

# remove age values below 0 and above 150
df = df[(df.age >= 0) & (df.age < 150)]
total_obs = df.shape[0]  # dropped some rows here

# Columns contain only 1 optimized data type
int_cols = ["location_id", "year_start", "year_end", "age_group_unit", "age", "sex_id"]
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ["source", "facility_id", "outcome_id"]

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)

print("Done cleaning cols")
#################
# fix cause codes
############################################
df["og_cause_code"] = df["cause_code"]  # for dev testing

# clean the cause code col to remove non alpha numeric symbols
df["cause_code"] = df["cause_code"].str.replace("\W", "")
# make all ICD codes upper case
df["cause_code"] = df["cause_code"].str.upper()

# split and capture on capital letters followed by digits like so
df["cc_list"] = df.cause_code.map(lambda x: re.split("([A-Z]\d+)", x))

# now you've got a col of lists which need to be split out into new columns
# remove blank elements in list
if sys.version[0] == "3":
    df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  # py 3
if sys.version[0] == "2":
    df.cc_list = df.cc_list.map(lambda x: [_f for _f in x if _f])  # py 2

# turn the column of lists into a dataframe where each element in the list is
# its own column
# dx_df = df['cc_list'].apply(pd.Series)  # this is super slow


def create_dxdf(df):
    dat = df["cc_list"].apply(pd.Series)
    return dat


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
l = list(p.map(create_dxdf, np.array_split(df, 45)))
dx_df = pd.concat(l)
print("Done multiprocessing", (time.time() - start) / 60)

# then name cols
nums = np.arange(1, dx_df.shape[1] + 1, 1)
names = ["dx_" + str(num) for num in nums]
dx_df.columns = names

# cbind back together
df.drop(["cause_code", "cc_list", "og_cause_code"], axis=1, inplace=True)
# data was split out into two DFs so make sure the indices are the same
assert (df.index == dx_df.index).all()
df = pd.concat([df, dx_df], axis=1)

# fill null and missing primary icd codes with "cc_code"
df.dx_1.fillna("CC_CODE", inplace=True)
df.loc[(df.dx_1 == "NAN") | (df.dx_1 == "") | (df.dx_1 == "NONE"), "dx_1"] = "CC_CODE"

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    df[feat] = df[feat].astype(str)

# swap live birth codes
placeholders = ["nan", "NAN", "NaN", "NONE", "NULL", ""]
for col in list(df.filter(regex="^(dx_)").columns.drop("dx_1")):
    df.loc[df[col].isin(placeholders), col] = np.nan

df = live_births.swap_live_births(df, user=getuser(), drop_if_primary_still_live=False)

str_feats = [
    "enrollee_id",
    "is_otp",
    "facility_id",
    "date_of_death",
    "source",
    "outcome_id",
]
for feat in str_feats:
    df[feat] = df[feat].astype(str)
print("Done cleaning columns", (time.time() - start) / 60)

# remove day cases
# create a length of stay variable
df["los"] = df["dis_date"] - df["adm_date"]

# drop day cases unless the patient died
# get pre drop count of non death day cases
tmp = df[df.los == "0 days"].copy()
tmp = tmp[tmp.outcome_id != "death"]
non_death_day_cases = tmp.shape[0]
del tmp

# get the pre shape of data, the diff between now and dropping should equal
# the number of non-death day cases
pre = df.shape[0]
df = df[(df["los"] != "0 days") | (df.outcome_id != "discharge")]

total_obs = df.shape[0]

assert (
    pre - df.shape[0] == non_death_day_cases
), "The wrong number of cases were dropped"
assert (
    df[df.los == "0 days"].outcome_id.unique()[0] == "death"
), "There are day cases that didn't result in death present"
assert (
    df[df.los == "0 days"].outcome_id.unique().size == 1
), "Too many unique outcome id values for day cases"

print("Done removing day cases", (time.time() - start) / 60)

# ====================================================
# run format program
# ====================================================
print("Beginning formatting", (time.time() - start) / 60)

df_list = np.array_split(df, 100)
del df

final_list = []

split_int = 0
for df in df_list:
    split_int += 1
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

    df.drop(["facility_id"], axis=1, inplace=True)

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

    # Rename features using dictionary created above
    df.rename(columns=hosp_wide_feat, inplace=True)

    # set difference of the columns you have and the columns you want,
    # yielding the columns you don't have yet
    new_col_df = pd.DataFrame(
        columns=list(set(hosp_wide_feat.values()) - set(df.columns))
    )
    df = df.join(new_col_df)

    print("Done renaming columns", (time.time() - start) / 60)
    #####################################################
    # FILL COLUMNS THAT SHOULD BE HARD CODED
    # this is where you fill in the blanks with the easy
    # stuff, like what version of ICD is in the data.
    #####################################################

    # These are completely dependent on data source
    df["representative_id"] = 1
    # df['location_id'] = -1

    # group_unit 1 signifies age data is in years
    df["age_group_unit"] = 1
    df["source"] = "PHL_HICC"

    # code 1 for ICD-9, code 2 for ICD-10
    # df['code_system_id'] = 2

    # metric_id == 1 signifies that the 'val' column consists of counts
    df["metric_id"] = 1

    df.loc[df.facility_id == "T", "facility_id"] = "outpatient unknown"
    df.loc[df.facility_id == "F", "facility_id"] = "inpatient unknown"

    # Create a dictionary with year-nid as key-value pairs
    nid_dictionary = {2013: 222560, 2014: 222563}
    df = hosp_prep.fill_nid(df, nid_dictionary)

    #######################
    # SWAP E AND N CODES
    #######################
    print("Start swapping", (time.time() - start) / 60)
    # use handy hosp prep function
    df = hosp_prep.swap_ecode_with_ncode(df, 10)

    print("Ecodes swapped", (time.time() - start) / 60)

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

    for col in int_cols:
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
    df = hosp_prep.age_binning(df, clinical_age_group_set_id=3)

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
    # Remove non-alphanumeric characters from dx feats
    # for feat in diagnosis_feats:
    #    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])

    wide = df.copy()
    # drop deaths
    wide = wide[wide.outcome_id != "death"]
    # drop 100% null columns
    for col in wide.columns:
        if wide[col].isnull().all():
            wide.drop(col, axis=1, inplace=True)
    # drop date/time columns
    wide.drop(
        ["adm_date", "dis_date", "date_of_death", "los", "enrollee_id"],
        axis=1,
        inplace=True,
    )
    wide.to_stata(r"FILEPATH")
    del wide

    #####################################################
    # IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
    #####################################################
    df = df[
        [
            "location_id",
            "year_start",
            "year_end",
            "age_start",
            "age_end",
            "sex_id",
            "outcome_id",
            "nid",
            "facility_id",
            "metric_id",
            "code_system_id",
            "representative_id",
            "source",
            "age_group_unit",
        ]
        + list(diagnosis_feats)
    ]

    def reshape_long_mp(each_df):
        each_df = (
            each_df.set_index(
                [
                    "location_id",
                    "year_start",
                    "year_end",
                    "age_start",
                    "age_end",
                    "sex_id",
                    "outcome_id",
                    "nid",
                    "facility_id",
                    "metric_id",
                    "code_system_id",
                    "representative_id",
                    "source",
                    "age_group_unit",
                ]
            )
            .stack()
            .reset_index()
        )

        each_df[0] = each_df[0].str.upper()
        # drop blank diagnoses
        each_df = each_df[each_df[0] != "NONE"]
        each_df = each_df[each_df[0] != "NAN"]
        each_df = each_df[each_df[0] != ""]
        # rename cols
        each_df = each_df.rename(columns={"level_14": "diagnosis_id", 0: "cause_code"})
        # replace diagnosis_id with codes, 1 for primary, 2 for secondary
        # and beyond
        each_df["diagnosis_id"] = np.where(each_df["diagnosis_id"] == "dx_1", 1, 2)
        print("One subset df is done")
        return each_df

    df.dx_1 = df.dx_1.str.upper()
    df.loc[(df.dx_1 == "NONE") | (df.dx_1 == ""), "dx_1"] = "CC_CODE"
    dx_1_val_counts = df.dx_1.value_counts()

    print("Start reshaping long", (time.time() - start) / 60)

    if platform.system == "Linux":
        p = multiprocessing.Pool(3)
        l = list(p.map(reshape_long_mp, np.array_split(df, 15)))
        df = pd.concat(l)
    else:
        df = reshape_long_mp(df)

    assert (
        dx_1_val_counts == df[df.diagnosis_id == 1].cause_code.value_counts()
    ).all(), "Reshaping long changed the value count of primary ICD codes"

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
    df = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

    #####################################################
    # ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
    #####################################################

    # Arrange columns in our standardized feature order
    columns_before = df.columns
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
    final_list.append(df)

df = pd.concat(final_list)

#####################################################
# REVIEW AGAINST ARCHIVED DATA
#####################################################
base = root + r"FILEPATH"
compare_df = pd.read_hdf((f"FILEPATH"))
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

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = f"FILEPATH"
hosp_prep.write_hosp_file(df, write_path, backup=True)
