# -*- coding: utf-8 -*-
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass

# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH".format(user)

sys.path.append(prep_path)
import hosp_prep

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

src_name = "BWA_HMDS"

paths = {
    "IG_2007_MORB": "FILEPATH",
    "IG_2007_MORT": "FILEPATH",
    "IG_2008_MORB": "FILEPATH",
    "IG_2008_MORT": "FILEPATH",
    "IG_2009_MORB": "FILEPATH",
    "IG_2009_MORT": "FILEPATH",
}

# Read in Data
data = {k: pd.read_csv(paths[k], dtype=str) for k in paths}


def rename_and_drop(df, key, proc_year):
    df = df[key.keys()]
    df["year"] = proc_year
    return df.rename(columns=key)


# Format Morbidity 2007
# id_cols = ["YEARPRO", "MONTPRO", "DISTRT", "HFAC", "MEDNUM"]
# data["IG_2007_MORB"].index = data["IG_2007_MORB"][id_cols].apply(lambda row: hash(tuple(row)), axis=1)
data["IG_2007_MORB"] = rename_and_drop(
    data["IG_2007_MORB"],
    {
        "AGE": "age",
        "DISCHCON": "outcome_id",
        "HFACTY": "facility_id",
        "MORBDIAG": "dx_1",
        "MORTYPE": "diagnosis_id",
        "SEXOFPAT": "sex_id",
        "YYOFADM": "adm_year",
        "MMOFADM": "adm_month",
        "DDOFADM": "adm_day",
        "DISCHADD": "dis_day",
        "DISCHAMM": "dis_month",
        "DISCHAYY": "dis_year",
    },
    proc_year=2007,
)
data["IG_2007_MORB"]["nid"] = 126516

# Format Mortality 2007
# id_cols = ["YEARPRO", "MONTPRO", "DISTRT", "HFAC", "MEDNUM"]
# data["IG_2007_MORT"].index = data["IG_2007_MORT"][id_cols].apply(lambda row: hash(tuple(row)), axis=1)
data["IG_2007_MORT"] = rename_and_drop(
    data["IG_2007_MORT"],
    {
        "AGE": "age",
        "DISCHCON": "outcome_id",
        "HFACTY": "facility_id",
        "MORTDIAG": "dx_1",
        "MORTTYPE": "diagnosis_id",
        "SEXOFPAT": "sex_id",
        "YYOFADM": "adm_year",
        "MMOFADM": "adm_month",
        "DDOFADM": "adm_day",
        "DISCHADD": "dis_day",
        "DISCHAMM": "dis_month",
        "DISCHAYY": "dis_year",
    },
    proc_year=2007,
)
data["IG_2007_MORT"]["nid"] = 126516

# Format Morbidity 2008
# id_cols = ["YEARPRO", "MONTPRO", "D_DIS", "H_FAC", "MED_NUM"]
# data["IG_2008_MORB"].index = data["IG_2008_MORB"][id_cols].apply(lambda row: hash(tuple(row)), axis=1)
data["IG_2008_MORB"] = rename_and_drop(
    data["IG_2008_MORB"],
    {
        "AGE": "age",
        "DIS_CON": "outcome_id",
        "H_FAC_TY": "facility_id",
        "MORBDIAG": "dx_1",
        "MORB_TY": "diagnosis_id",
        "P_SEX": "sex_id",
        "Y_ADM": "adm_year",
        "M_ADM": "adm_month",
        "D_ADM": "adm_day",
        "D_DIS": "dis_day",
        "M_DIS": "dis_month",
        "Y_DIS": "dis_year",
    },
    proc_year=2008,
)
data["IG_2008_MORB"]["nid"] = 126517

# Format Mortality 2008
# id_cols = ["YEARPRO", "MONTPRO", "D_DIS", "H_FAC", "MED_NUM"]
# data["IG_2008_MORT"].index = data["IG_2008_MORT"][id_cols].apply(lambda row: hash(tuple(row)), axis=1)
data["IG_2008_MORT"] = rename_and_drop(
    data["IG_2008_MORT"],
    {
        "AGE": "age",
        "DIS_CON": "outcome_id",
        "H_FAC_TY": "facility_id",
        "MORTDIAG": "dx_1",
        "MORT_TY": "diagnosis_id",
        "P_SEX": "sex_id",
        "Y_ADM": "adm_year",
        "M_ADM": "adm_month",
        "D_ADM": "adm_day",
        "D_DIS": "dis_day",
        "M_DIS": "dis_month",
        "Y_DIS": "dis_year",
    },
    proc_year=2008,
)
data["IG_2008_MORT"]["nid"] = 126517

# Format Morbidity 2009
# id_cols = ["YEARPRO", "MONTPRO", "HFAC", "MEDRENO"]
# data["IG_2009_MORB"].index = data["IG_2009_MORB"][id_cols].apply(lambda row: hash(tuple(row)), axis=1)
data["IG_2009_MORB"] = rename_and_drop(
    data["IG_2009_MORB"],
    {
        "AGE": "age",
        "DISCHCON": "outcome_id",
        "FACI_TY": "facility_id",
        "MORBDIAG": "dx_1",
        "TYPE_MOR": "diagnosis_id",
        "SEX": "sex_id",
        "YYOFADM": "adm_year",
        "MMOFADM": "adm_month",
        "DDOFADM": "adm_day",
        "DISCHADD": "dis_day",
        "DISCHAMM": "dis_month",
        "DISCHAYY": "dis_year",
    },
    proc_year=2009,
)
data["IG_2009_MORB"]["nid"] = 126518

# Format Mortality 2009
# id_cols = ["YEARPRO", "MONTPRO", "HFAC", "MEDRENO"]
# data["IG_2009_MORT"].index = data["IG_2009_MORT"][id_cols].apply(lambda row: hash(tuple(row)), axis=1)
data["IG_2009_MORT"] = rename_and_drop(
    data["IG_2009_MORT"],
    {
        "AGE": "age",
        "DISCHCON": "outcome_id",
        "FACI_TY": "facility_id",
        "MORTDIAG": "dx_1",
        "TYP_MORT": "diagnosis_id",
        "SEX": "sex_id",
        "YYOFADM": "adm_year",
        "MMOFADM": "adm_month",
        "DDOFADM": "adm_day",
        "DISCHADD": "dis_day",
        "DISCHAMM": "dis_month",
        "DISCHAYY": "dis_year",
    },
    proc_year=2009,
)
data["IG_2009_MORT"]["nid"] = 126518

df = pd.concat(data.values(), sort=False)

begin_admits = df[df.diagnosis_id == 1].shape[0]
# Filter out non primary diagnosis
# 1=main
# 2=other
# 3=external (ecodes)
# df = df.loc[df.diagnosis_id != '2']
df = df[df.dx_1.notnull()]  # can't use data without diagnoses
# swap primary dx n codes out and ecodes in
warnings.warn(
    "We're not swapping ecodes yet, waiting on inj team to see if it's useful"
)
# overwrite inj dx value
df.loc[
    (df["diagnosis_id"].isin(["3", "9"])) | (df.diagnosis_id.isnull()), "diagnosis_id"
] = "2"

"""
Facility values ['hospital','hospital','emergency']
"""
df["facility_id"] = "hospital"

# outcome values: ["death","discharge","discharge"]
df.loc[df.outcome_id.isnull(), "outcome_id"] = "9"
# discharge codes
# 1=alive
# 2=death
# 3=Referral
# 9=Unknown
df.outcome_id.replace(
    {"1": "discharge", "2": "death", "3": "discharge", "9": "discharge"}, inplace=True
)


# 1=Male
# 2=Female
# 9=Missing
df.sex_id.replace("9", "3", inplace=True)
df.loc[df.sex_id.isnull(), "sex_id"] = "3"

# set discharge year to reporting year
df.loc[df["dis_year"].isnull(), "dis_year"] = df.loc[
    df["dis_year"].isnull(), "year"
].astype(str)

df["raw_dis"] = df["dis_year"] + "-" + df["dis_month"] + "-" + df["dis_day"]
df["raw_admit"] = df["adm_year"] + "-" + df["adm_month"] + "-" + df["adm_day"]

# fix admission days that are breaking datetime,
# basically these aren't real end of month days
for col in ["raw_dis", "raw_admit"]:
    df.loc[df[col] == "2007-11-31", col] = "2007-11-30"
    df.loc[df[col] == "2007-4-31", col] = "2007-4-30"
    df.loc[df[col] == "2007-9-31", col] = "2007-9-30"
    df.loc[df[col] == "2007-6-31", col] = "2007-6-30"
    df.loc[
        (df[col] == "2007-2-30") | (df[col] == "2007-2-31") | (df[col] == "2007-2-29"),
        col,
    ] = "2007-2-28"
    df.loc[df[col] == "2006-2-31", col] = "2006-2-28"

pre = len(df)
df = df[df["raw_admit"].notnull()]
df = df[df["raw_dis"].notnull()]
print("{} rows lost".format(pre - len(df)))

# manually adjust a missing admission
df.loc[df.adm_year == "9999", "raw_admit"] = "2008-1-1"

df["dis_date"] = pd.to_datetime(df["raw_dis"])
df["adm_date"] = pd.to_datetime(df["raw_admit"])
df["los"] = df["dis_date"] - df["adm_date"]

# finally remove day cases!!
df = df[df.los != "0 days"]

# remove rows with admissions before 2004. Some long term
# care is fine, especially if it's just a few months but
# the envelope probably doesn't include admissions from the 1970s or 1930s!
n_rows = len(df)
warnings.warn("dropping data before 2004 and after 2009")
df["adm_year"] = df["adm_year"].astype(int)
df = df[df.adm_year >= 2004]  # drop anything before 2004

print("Dropping {} rows of very long term care".format(n_rows - len(df)))
# Fill in year start and end
df["year_start"] = df.year
df["year_end"] = df.year

final_admits_check = len(df)

# Select features from raw data to keep
keep = [
    "sex_id",
    "age",
    "dx_1",
    "year_start",
    "year_end",
    "outcome_id",
    "diagnosis_id",
    "facility_id",
    "nid",
]
df = df[keep]

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    "year": "year",
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
    "facility_id": "facility_id",
    # diagnosis varibles
    "dx_1": "dx_1",
    #'dx_2': 'dx_2',
    #'dx_3': 'dx_3'
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

df["representative_id"] = 0  # Do not take this as gospel, it's guesswork
df["location_id"] = 193

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = src_name

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# case is the sum of live discharges and deaths
# df['outcome_id'] = "case/discharge/death"

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1


#####################################################
# CLEAN VARIABLES
#####################################################

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

df["age"] = pd.to_numeric(df["age"])
df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
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
    "diagnosis_id",
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

# assign unknown sex_id to 3 for splitting
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    # can't run this! df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
df["val"] = 1

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

assert df.val.sum() == final_admits_check, "There are {} missing admissions".format(
    final_admits_check - df.val.sum()
)

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

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df[group_vars].isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

df_agg = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

del df
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
    len(df_agg["sex_id"].unique()) == 3
), "There should only be two feature levels to sex_id"
assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (df_agg.val >= 0).all(), "for some reason there are negative case counts"

assert df_agg.val.sum() == final_admits_check, "There are {} missing admissions".format(
    final_admits_check - df_agg.val.sum()
)

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = root + r"FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
