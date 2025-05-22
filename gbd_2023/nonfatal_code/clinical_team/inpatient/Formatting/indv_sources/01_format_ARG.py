"""
Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

ADDRESS

When you are finished:
1) run the new source through clinical_info/Plotting/vet_new_source.py
2) add the data to the tableau template
3) publish the tableau
4) if everything looks good in your tableau, open a Pull Request and include
    the link to your tableau
"""
import getpass
import platform
import re
import sys
import warnings

import numpy as np
import pandas as pd

# load our functions
from crosscutting_functions import *


#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
filepath = FILEPATH

date_year = {
    "DIS": {"Y2014M05D27": [2010], "Y2016M04D18": [2011]},
    "INJ": {"Y2014M03D18": [2010], "Y2014M04D16": [2011]},
}
dis_df = []
inj_df = []


def read_helper(file_type, date, year):
    prefix = f"ARG_HOSPITAL_DISCHARGES_{year}"

    if file_type == "DIS":
        abs_path = FILEPATH

        temp = pd.read_stata(abs_path)
        temp["anioinfor"] = year
        dis_df.append(temp)
    else:
        inj_file = FILEPATH
        abs_path = FILEPATH

        inj_df.append(pd.read_stata(abs_path))


def read_files(d, file_type=None):
    """
    Iterate thru the nested dict via recursion
    """
    for k, v in d.items():
        try:
            if v.items():
                read_files(d=v, file_type=k)
        except:
            for e in v:
                read_helper(file_type=file_type, date=k, year=e)


read_files(date_year)

inj = pd.concat(inj_df, sort=False)
dis = pd.concat(dis_df, sort=False)

# drop day cases
inj = inj[inj.diastotest > 1]

# Only two years of data includes this col.
# Dropping for consistency
dis.drop("codprov", axis=1, inplace=True)

# dX and cX are shorter codes of coddiagpr and codcauext.
inj.drop(
    [
        "procquir",
        "externa",
        "c19",
        "c194",  # provience, and additional codes
        "co4",
        "co3",
        "do3",
        "do4",  # different code len
        "code1",
        "code2",
        "nat",  # codes for codebook map
        "fecingreso",
        "fecegreso",
        "diastotest",  # admin / discharge dates. los
        "grupedad",  # age group
    ],
    axis=1,
    inplace=True,
)

# conver inj df from individual to tabulation
cols = inj.columns.tolist()
inj["cnt"] = 1

inj_temp = inj.groupby(cols).agg({"cnt": "sum"}).reset_index()

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
# assert_msg = f"""index is not unique, the index has a length of
# {str(len(df.index.unique()))} while the DataFrame has
# {str(df.shape[0])} rows. Try this: df = df.reset_index(drop=True)"""
# assert_msg = " ".join(assert_msg.split())
# assert df.shape[0] == len(df.index.unique()), assert_msg

# # Replace feature names on the left with those found in data where appropriate
# # ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# # want
hosp_wide_feat = {
    "nid": "nid",
    "representative_id": "representative_id",
    # demographics
    "anioinfor": "year_start",
    "codsexo": "sex_id",
    "edading": "age",
    "coduniedad": "age_group_unit",
    "code_system_id": "code_system_id",
    "location_id": "location_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "facility_id": "facility_id",
    "muer": "death",
    "cnt": "val",
    # diagnosis varibles
    "coddiagpr": "dx_1",
    "codcauext": "dx_2",  # only present for inj
}

df = pd.concat([dis, inj], sort=False)
# # Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)
df["year_end"] = df["year_start"]

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

# #####################################################
# # FILL COLUMNS THAT SHOULD BE HARD CODED
# # this is where you fill in the blanks with the easy
# # stuff, like what version of ICD is in the data.
# #####################################################

# # These are completely dependent on data source

# # Ryan said that we only have 1 and 3 kinds of data
# # -1: "Not Set",
# # 0: "Unknown",
# # 1: "Nationally representative only",
# # 2: "Representative for subnational location only",
# # 3: "Not representative",
# # 4: "Nationally and subnationally representative",
# # 5: "Nationally and urban/rural representative",
# # 6: "Nationally, subnationally and urban/rural representative",
# # 7: "Representative for subnational location and below",
# # 8: "Representative for subnational location and urban/rural",
# # 9: "Representative for subnational location, urban/rural and below",
# # 10: "Representative of urban areas only",
# # 11: "Representative of rural areas only"

df["representative_id"] = 3  # Do not take this as gospel, it's guesswork
df["location_id"] = 97

df["source"] = "ARG_HSR"

# # code 1 for ICD-9, code 2 for ICD-10
code_sys = [e for e in df.dx_1.unique().tolist() if re.match(e, "^[0-9]")]
if len(code_sys) > 0:
    print("there are ICD9 codes")
df["code_system_id"] = 2

# # case is the sum of live discharges and deaths
# # pick one
df["outcome_id"] = np.where(df.death == 1, "death", "discharge")
df.drop("death", axis=1, inplace=True)

# # metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Type to numeric and cast unknown sexes
# to CI standards
df["sex_id"] = pd.to_numeric(df.sex_id)
df.loc[df.sex_id == 9, "sex_id"] = 3

# # Create a dictionary with year-nid as key-value pairs
nid_dictionary = {
    2010: 433042,  # merged nid of dis and inj
    2011: 433045,  # merged nid of dis and inj
}
df = hosp_prep.fill_nid(df, nid_dictionary)

# #####################################################
# # MANUAL PROCESSING
# # this is where fix the quirks of the data, like making values in the
# # data match the values we use.

# # For example, repalce "Male" with the number 1
# #####################################################

# Convert NN ages into year space
# age_group_unit:
#   1 : year
#   2 : month
#   3 : day
#   4 : hour
df["age_group_unit"] = pd.to_numeric(df.age_group_unit)

# This could abstracted a bit.
# Works for now
df.loc[df.age_group_unit == 2, "age"] = (
    df.loc[df.age_group_unit == 2, "age"] * 30.5
) / 365
df.loc[df.age_group_unit == 3, "age"] = df.loc[df.age_group_unit == 3, "age"] / 365
df.loc[df.age_group_unit == 4, "age"] = df.loc[df.age_group_unit == 4, "age"] / (
    365 * 24
)

# re-assign var now that everything is in year space
df.loc[df.age_group_unit != 9, "age_group_unit"] = 1

# drop unknown ages
df = df[df.age_group_unit != 9]

# floor year ages that are float type as a result of
# converting non year ages into year space
cond = "(df.age_group_unit == 1) & (df.age >= 1)"
df.loc[eval(cond), "age"] = np.floor(df.loc[eval(cond), "age"])

# # Replace feature levels manually

# drop unknown sex ids
df = df[df.sex_id != 3]
df["facility_id"] = "hospital"
# # df['outcome_id'].replace(['3 - DIED (LA)',
# #                           '2 - Translated (A) TO ANOTHER HOSPITAL',
# #                           '1 - Out (A) HOME'],
# #                          ["death", "discharge", "discharge"], inplace=True)
# # df['facility_id'].replace(['3 - EMERGENCY AFTER 24 HOURS',
# #                            '1 - ROUTINE',
# #                            '2 - EMERGENCY TO 24 HOURS'],
# #                           ['hospital', 'hospital', 'emergency'],
# #                           inplace=True)

# Manually verify the replacements
assert len(df["sex_id"].unique()) == 2, "df['sex_id'] should have 2 feature levels"
assert (
    len(df["outcome_id"].unique()) == 2
), "df['outcome_id'] should have 2 feature levels"
assert len(
    df["facility_id"].unique() == 2
), "df['facility_id] should have 2 feature levels"

# #####################################################
# # INJ FORMATTING
# #####################################################
# remove white space
df["dx_1"] = df["dx_1"].str.strip()
df["dx_2"] = df["dx_2"].str.strip()

# To avoid making assumptions about what is supposed to be in dx_2 column,
# make rules that follow how ICD is set up. Nature of injury starts with S or T
# cause of injury starts with V, W, X, or Y.  Based on Mohsen 2/24/17
nature_condition = df["dx_1"].str.startswith("S") | (df["dx_1"].str.startswith("T"))

cause_condition = (
    (df["dx_2"].str.startswith("V"))
    | (df["dx_2"].str.startswith("W"))
    | (df["dx_2"].str.startswith("X"))
    | (df["dx_2"].str.startswith("Y"))
)

df.loc[nature_condition & cause_condition, ["dx_1", "dx_2"]] = df.loc[
    nature_condition & cause_condition, ["dx_2", "dx_1"]
].values

# #####################################################
# # CLEAN VARIABLES
# #####################################################

# # Columns contain only 1 optimized data type
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


# # BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# # to the string "nan"
# # fast way to cast to str while preserving Nan:
# # df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ["source", "facility_id", "outcome_id"]

# # use this to infer data types
# # df.apply(lambda x: pd.lib.infer_dtype(x.values))

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

# # Turn 'age' into 'age_start' and 'age_end'
# #   - bin into year age ranges
# #   - under 1, 1-4, 5-9, 10-14 ...

df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
# # terminal age start will be lumped into the terminal age group.
df = hosp_prep.age_binning(df, terminal_age_in_data=True, under1_age_detail=True)

# # drop unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

# #####################################################
# # IF MULTIPLE DX EXIST:
# # TRANSFORM FROM WIDE TO LONG
# #####################################################
# # Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# # Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df.reset_index(drop=True))
    df.drop("patient_index", axis=1, inplace=True)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

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

# # check if all columns are there
assert set(columns_before) == set(
    columns_after
), "You lost or added a column when reordering"
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
write_path = (
FILEPATH
)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)

# #####################################################
# # UPDATE SOURCE_TABLE
# #####################################################

# # TODO import Database.NID_Tables
