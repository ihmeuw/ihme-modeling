import pandas as pd
import platform
import numpy as np
import sys
import glob
import re


# load our functions
hosp_path = r"FILEPATH"
sys.path.append(hosp_path)

# when running on the cluster:
USER_path = r"FILEPATH"
sys.path.append(USER_path)

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

# get list of data files NOT including the code book
file_dir = glob.glob(root + r"FILEPATH")

df_list = []
for f in file_dir:
    adf = pd.read_excel(f)
    adf["year_start"] = int(re.search(r"[0-9]+", f).group(0))
    df_list.append(adf)
df = pd.concat(df_list, ignore_index=True)
df["year_end"] = df["year_start"]

# Select features from raw data to keep
keep = [
    "ANA_SESSO",
    "DATA_DECESSO",
    "ETA_DATA_INGRESSO",
    "DIA1",
    "DIA2",
    "DIA3",
    "DIA4",
    "DIA5",
    "DIA6",
    "year_start",
    "year_end",
    "RICSDO_GIORNI_DEGENZA",
]
df = df[keep]

hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    #    'year': 'year',
    "year_start": "year_start",
    "year_end": "year_end",
    "ANA_SESSO": "sex_id",
    #    'age': 'age',
    #    'age_start': 'age_start',
    #    'age_end': 'age_end',
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "facility_id": "facility_id",
    # diagnosis varibles
    "DIA1": "dx_1",
    "DIA2": "dx_2",
    "DIA3": "dx_3",
    "DIA4": "dx_4",
    "DIA5": "dx_5",
    "DIA6": "dx_6",
    # source specific
    "ETA_DATA_INGRESSO": "age",
    #     'DATA_NASCITA': 'DOB',
    "DATA_DECESSO": "DOD",
    "RICSDO_GIORNI_DEGENZA": "bed_days"
    #     'DATA_USCITA': 'DOE',
    #     'ATTRIBSDO_DESC': 'type_of_admission'  # don't know if this is useful yet
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
df["representative_id"] = 3  # Not representative
df["location_id"] = 86  # this is national level data for Italy

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "ITA_IMCH"  # Institute for Maternal and Child Health

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 1  # icd 9

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
# Year: NID
# 2010: 285460
# 2011: 285464
# 2012: 285465
# 2013: 285467
# 2014: 285468
# 2015: 285471

nid_dictionary = {
    2010: 285460,
    2011: 285464,
    2012: 285465,
    2013: 285467,
    2014: 285468,
    2015: 285471,
}

# nid_dictionary = {'example_year': 'example_nid'}
df = fill_nid(df, nid_dictionary)

df["facility_id"] = "inpatient unknown"


# infer outcome_id from the Date of Death information. If data marks them as alive,
# then we will mark them as a discharge, else, mark as death

# first, make sure we're dealing with strings and not datetime dtypes:
df["DOD"] = df["DOD"].astype(str)

# the data doesn't explicitly say "this person died" or not. they mark
# date of deaths. The documentation states that "If 31/12/9999,
# then person is till[sic] alive". Note pandas interprets
# 31/12/9999 as 9999-12-31 00:00:00

df.loc[df["DOD"] == "9999-12-31 00:00:00", "outcome_id"] = "discharge"
df.loc[df["DOD"] != "9999-12-31 00:00:00", "outcome_id"] = "death"

# drop day cases unless the patient died
# get pre drop count of non death day cases
tmp = df[df.bed_days == 0].copy()
tmp = tmp[tmp.outcome_id != "death"]
non_death_day_cases = tmp.shape[0]
del tmp

# get the pre shape of data, the diff between now and dropping should equal
# the number of non-death day cases
pre = df.shape[0]
df = df[(df.bed_days != 0) | (df.outcome_id != "discharge")]  # drop day cases
assert (
    pre - df.shape[0] == non_death_day_cases
), "The wrong number of cases were dropped"
assert (
    df[df.bed_days == 0].outcome_id.unique()[0] == "death"
), "There are day cases that didn't result in death present"
assert (
    df[df.bed_days == 0].outcome_id.unique().size == 1
), "Too many unique outcome id values for day cases"
df.drop(["DOD", "bed_days"], axis=1, inplace=True)


#####################################################
# CLEAN VARIABLES
#####################################################

# replace string values in the sex_id with ints
df["sex_id"] = np.where(df["sex_id"] == "M", 1, 2)

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
str_cols = ["source", "facility_id", "outcome_id"]

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)


df = df[df["age"] < 125]  # toss out data where age is greater than GBD terminal age
df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)
df["age_start"] = pd.to_numeric(df["age_start"], downcast="integer", errors="raise")
df["age_end"] = pd.to_numeric(df["age_end"], downcast="integer", errors="raise")
df.drop("age", axis=1, inplace=True)  # changed age_binning to not drop age col


# keep columns we want, because it might be messing up stack_merger,
# this line shouldn't be dropping anything
df = df[
    [
        "sex_id",
        "dx_1",
        "dx_2",
        "dx_3",
        "dx_4",
        "dx_5",
        "dx_6",
        "year_start",
        "year_end",
        "representative_id",
        "nid",
        "code_system_id",
        "facility_id",
        "age_group_unit",
        "outcome_id",
        "location_id",
        "source",
        "metric_id",
        "age_start",
        "age_end",
    ]
]


#####################################################
# WIDE TO LONG DX
#####################################################

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
df["val"] = 1


# after reshaping, make sure ICD codes are strings
df["cause_code"] = df["cause_code"].astype(str)


#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
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
    "number of feature levels age start should match number of feature "
    + r"levels age end"
)
assert (
    len(df["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"
assert (
    len(df["sex_id"].unique()) == 2
), "There should only be two feature levels to sex_id"
assert (
    len(df["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df["source"].unique()) == 1, "source should only have one feature level"


# df.apply(lambda x: pd.lib.infer_dtype(x.values))


#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + r"FILEPATH"
write_hosp_file(df, write_path, backup=True)
