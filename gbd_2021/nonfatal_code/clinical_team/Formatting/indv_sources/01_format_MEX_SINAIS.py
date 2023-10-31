import pandas as pd
import platform
import numpy as np
import sys
import warnings
from getpass import getuser

# load our functions
prep_path = "FILEPATH"
sys.path.append(prep_path)

import hosp_prep

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

df_list = []

files = {
    2013: "FILEPATH",
    2014: "FILEPATH",
    2015: "FILEPATH",
}

for year, file in files.items():
    tmp = pd.read_csv(file)
    tmp["year_start"] = year
    tmp["year_end"] = year
    # standardize age cols
    if year in [2014, 2015]:
        tmp.drop("EDAD", axis=1, inplace=True)
        tmp.rename(columns={"EDAD1": "EDAD"}, inplace=True)
    # make sure there are exactly 32 subnational locations
    assert tmp.CEDOCVE.unique().size == 32, "Wrong number of locations"
    df_list.append(tmp)
    del tmp

df = pd.concat(df_list, sort=False, ignore_index=True)

mex_locs = pd.read_csv("FILEPATH")
mex_locs.rename(columns={"IDEDO": "CEDOCVE"}, inplace=True)
# mex_locs.drop('DESCRIP', axis=1, inplace=True)
mex_locs = mex_locs[mex_locs.location_id.notnull()]

pre = df.shape
df = df.merge(mex_locs[["CEDOCVE", "location_id", "DESCRIP"]], how="left", on="CEDOCVE")
assert pre[0] == df.shape[0], "the merge changed the df shape"
assert df.location_id.isnull().sum() == 0, "missing loc IDs not expected"

# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    "nid": "nid",
    "representative_id": "representative_id",
    "year_start": "year_start",
    "year_end": "year_end",
    "SEXO": "sex_id",
    "EDAD": "age",
    "CVEEDAD": "age_units",
    "DIAS_ESTA": "los",
    # measure_id variables
    "age_start": "age_start",
    "age_end": "age_end",
    "age_group_unit": "age_group_unit",
    # 'facility_id': 'facility_id',
    "code_system_id": "code_system_id",
    "MOTEGRE": "outcome_id",
    "CLUES": "facility_id",
    # diagnosis varibles
    "AFECPRIN4": "dx_1",
}  # 4 digit ICD code primary dx
#'CAUSABAS4': 'dx_2'}
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df["representative_id"] = 3

"""  Mapping from MotEgreso to Spanish description (accents removed)
1   CURACIAN (Cured)
2   MEJORAA (Improved)
3   VOLUNTARIO (Voluntary discharge)
4   PASE A OTRO HOSPITAL (discharged to other hospital)
5   DEFUNCIAN  (Death)
6   OTRO MOTIVO  (Other Reason)
9   (N.E.) (NA)"""
# every row that isn't a death is a discharge
df["outcome_id"] = np.where(df["outcome_id"] == 5, "death", "discharge")

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "MEX_SINAIS"

# code 1 for ICD-9, code 2 for ICD-10
# MEX is ICD-10
df["code_system_id"] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dict = {
    2004: 86953,
    2005: 86954,
    2006: 86955,
    2007: 86956,
    2008: 86957,
    2009: 86958,
    2010: 94170,
    2011: 94171,
    2012: 121282,
    2013: 150449,
    2014: 220205,
    2015: 281773,
}
df = hosp_prep.fill_nid(df, nid_dict)


#####################################################
# CLEAN VARIABLES
#####################################################
# review and drop day cases
df.los.value_counts(dropna=False).head()  # about 2 million day cases of 18 million
df.los.isnull().sum()  # no missing values

# drop em
df = df[df["los"] > 0]
final_admits = len(df)

# This col contains more detailed info but we don't use it (yet?)
df["facility_id"] = "hospital"

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df = hosp_prep.age_binning(df, drop_age=True)


# Columns contain only 1 optimized data type
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
]
str_cols = ["source", "facility_id", "outcome_id"]

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer")
for col in str_cols:
    df[col] = df[col].astype(str)

# replace sex_id with 3(missing) when it's not identified
df.loc[~df["sex_id"].isin([1, 2]), "sex_id"] = 3

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# the null value for CAUSABAS seems to be '8888'
# df.loc[df['dx_2'] == '8888', 'dx_2'] = np.nan
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    # convert to all upper case
    df[feat] = df[feat].str.upper()

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional function documentation
    df = hosp_prep.stack_merger(df)
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

# Check for missing values in cols to group
print(
    "Are there missing values in any row?\n"
    + str(not (df[group_vars].isnull().sum().sum() == 0))
)
df_agg = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

del df  # mainly so I don't accidentally call it
#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
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

# check if all columns are there
assert len(hosp_frmat_feat) == len(
    df_agg.columns
), "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert (
        hosp_frmat_feat[i] in df_agg.columns
    ), "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(
    ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
).columns:
    # assert that everything but cause_code, source, measure_id (for now) are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg["year_start"].unique()) == len(
    df_agg["nid"].unique()
), "number of feature levels of years should match number of feature levels of nid"
assert len(df_agg["age_start"].unique()) == len(
    df_agg["age_end"].unique()
), "number of feature levels age start should match number of feature levels age end"
assert (
    len(df_agg["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"

assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (
    df_agg[df_agg.diagnosis_id == 1].val.sum() == final_admits
), "The count of admits from the middle of formating should match primary admits in final data"
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + r"FILEPATH"
hosp_prep.write_hosp_file(df=df_agg, write_path=write_path, backup=True)
