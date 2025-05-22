"""
Worker script to format Mexico Sectorial hospital data
@author: USERNAME

Main argument: filepath for each year of MEX data

"""
import argparse
import os
import platform
import warnings

import numpy as np
import pandas as pd
from crosscutting_functions import demographic
from crosscutting_functions.formatting-functions import formatting
from db_queries import get_location_metadata

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "J:"

base = f"{root}FILEPATH"

#####################################################
# PARSE ARG
#####################################################

parser = argparse.ArgumentParser()
parser.add_argument(
    "--arg",
    type=str,
    action="store",
    required=True,
    help="Filepath to each year's data for Mexico",
)

arg_input = parser.parse_args()

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
#####################################################

release_id = 10
# subnat location ids for MEX
all_locs = get_location_metadata(location_set_id=35, release_id=release_id)
sub_locs = all_locs[all_locs.parent_id == 130].shape[0]

# filepath from arg
fp = arg_input.arg

# parse year from filename
# first sequence of digits in fn is year
fn = os.path.splitext(os.path.basename(fp))[0]
year = [int(x) for x in fn.split("_") if x.isdigit()][0]

# dict for raw files' delimiters
# yr 2018+ require different separator value
delim_dict = {"before_2018": ",", "after_2018": "|"}

# main data read
if year == 2015:
    # currently 2015 is an edge case where
    # small amt of rows have diff amt of cols from rest
    df = pd.read_csv(fp, sep=delim_dict["before_2018"], on_bad_lines="skip")
elif year < 2018:
    df = pd.read_csv(fp, sep=delim_dict["before_2018"])
else:
    df = pd.read_csv(fp, sep=delim_dict["after_2018"])

df["year_start"] = year
df["year_end"] = year

# standardize age column name
if all(x in df.columns for x in ["EDAD", "EDAD1"]):
    df.drop(columns=["EDAD"], inplace=True)
if "EDAD1" in df.columns:
    df.rename(columns={"EDAD1": "EDAD"}, inplace=True)

# make sure there are exactly 32 subnational locations
# the non-subnational locations below are from the codebook
non_sub_locs = [33, 34, 35, 37, 38, 39, 88, 99, 0]
df = df.loc[~df.CEDOCVE.isin(non_sub_locs)]
df = df.dropna(subset=["CEDOCVE"])
if df.CEDOCVE.unique().size != sub_locs:
    raise ValueError("Wrong number of locations")

# later data years have more granular age units
# which needs standardization to 1-year count space
if "CLAVE_EDAD" in df.columns:
    """Age unit specification coding:
    2   HORAS (hours)
    3   DÍAS (days)
    4   MESES (months)
    5   AÑOS (years)
    9   NO ESPECIFICADO (NA)
    """
    df.CLAVE_EDAD = df.CLAVE_EDAD.astype(str).str.strip()
    df.CLAVE_EDAD.replace("", np.nan, inplace=True)
    df.dropna(subset=["CLAVE_EDAD"], inplace=True)
    df.CLAVE_EDAD = df.CLAVE_EDAD.astype(float)

    if set(df.CLAVE_EDAD.unique()) != set([2, 3, 4, 5, 9]):
        raise ValueError("There are unexpected age unit values")

    df.loc[df.CLAVE_EDAD == 2, "EDAD"] = 1 / 365
    df.loc[df.CLAVE_EDAD == 3, "EDAD"] = df.loc[df.CLAVE_EDAD == 3, "EDAD"] / 365
    df.loc[df.CLAVE_EDAD == 4, "EDAD"] = df.loc[df.CLAVE_EDAD == 4, "EDAD"] / 12
    df.loc[df.CLAVE_EDAD == 9, "EDAD"] = np.nan

# account for year(s) where EDAD does not exist,
# using instead the aggregate age in col GPO_EDAD
binned_age = False
if "EDAD" not in df.columns:
    # currently only year 2004 is like this
    binned_age = True

    # age group based on code book
    conds = [
        df["GPO_EDAD"] == 1,
        df["GPO_EDAD"] == 2,
        df["GPO_EDAD"] == 3,
        df["GPO_EDAD"] == 4,
        df["GPO_EDAD"] == 5,
        df["GPO_EDAD"] == 6,
        df["GPO_EDAD"].isin([0, 7]),
    ]
    start_brac = [0, 1, 5, 15, 45, 65, 0]
    end_brac = [1, 4, 14, 44, 64, 125, 125]

    df["age_start"] = np.select(conds, start_brac)
    df["age_end"] = np.select(conds, end_brac)

#####################################################
# MAP SUBNATIONALS
#####################################################
# use the loc map used in GBD2017 and earlier.
# double checked the location id to name mapping
# and it matches what we currently have (Apr 2023)
mex_locs = pd.read_csv(f"{base}FILEPATH/mex_hospital_data_location_key.csv")
mex_locs.rename(columns={"IDEDO": "CEDOCVE"}, inplace=True)
# mex_locs.drop('DESCRIP', axis=1, inplace=True)
mex_locs = mex_locs[mex_locs.location_id.notnull()]

df = df.merge(
    mex_locs[["CEDOCVE", "location_id", "DESCRIP"]], how="left", on="CEDOCVE", validate="m:1"
)
if df.location_id.isnull().sum() != 0:
    raise ValueError("Missing loc IDs not expected")

del mex_locs

#####################################################
# ADD HOSPITAL FEATURE COLUMNS
#####################################################

# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    "nid": "nid",
    "representative_id": "representative_id",
    "location_id": "location_id",
    "year_start": "year_start",
    "year_end": "year_end",
    "SEXO": "sex_id",
    "EDAD": "age",
    "DIAS_ESTA": "los",
    # measure_id variables
    "age_start": "age_start",
    "age_end": "age_end",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    "MOTEGRE": "outcome_id",
    # diagnosis varibles
    "AFECPRIN4": "dx_1",  # 4 digit ICD code primary dx
    "CAUSABAS4": "dx_2",  # External cause of injury
    # other metadata
    "metric_id": "metric_id",
    "source": "source",
    "facility_id": "facility_id",
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

# We only have type 1 (natl rep only) or 3 (not representative) for this var
df["representative_id"] = 3  # not representative

""" Mapping from MotEgreso to Spanish description (accents removed)
1   CURACIAN (Cured)
2   MEJORAA (Improved)
3   VOLUNTARIO (Voluntary discharge)
4   PASE A OTRO HOSPITAL (discharged to other hospital)
5   DEFUNCIAN  (Death)
6   OTRO MOTIVO  (Other Reason)
9   (N.E.) (NA)"""

# every row that isn't a death is a discharge
df["outcome_id"] = np.where(df["outcome_id"] == 5, "death", "discharge")

df = formatting.swap_ecode_with_ncode(df, icd_vers=10)

df["age_group_unit"] = 1  # default, age data is in years
df["metric_id"] = 1  # default, data in count space
df["code_system_id"] = 2  # MEX is ICD-10
df["source"] = "MEX_SECT_SINAIS"  # source name
df["facility_id"] = "hospital"

# keep only relevant cols
df = df[df.columns.intersection(list(hosp_wide_feat.values()))]

# Create a dictionary with year-nid as key-value pairs
nid_dict = {
    2004: 524073,
    2005: 524075,
    2006: 306565,
    2007: 306527,
    2008: 306523,
    2009: 306519,
    2010: 306518,
    2011: 296630,
    2012: 299242,
    2013: 299245,
    2014: 296639,
    2015: 521801,
    2016: 521883,
    2017: 521894,
    2018: 521910,
    2019: 521912,
    2020: 521914,
}

if year not in list(nid_dict.keys()):
    raise ValueError(
        """
    Year not found in nid dictionary;
    >> Consider updating nid_dict
    """
    )
else:
    df["nid"] = nid_dict[year]

#####################################################
# CLEAN VARIABLES
#####################################################
# 09/2023
# no need to drop los since Rafael believes there's no
# true day cases in this source
if df.los.isnull().sum() != 0:
    # check for no missing values
    warnings.warn(
        """Missing row value(s) found for length of stay col.
                  Keeping as default."""
    )

final_admits = len(df)

if binned_age:
    # drop age if year with no individual age
    # no binning function needed
    df.drop(columns=["age"], inplace=True)
else:
    # everything older than GBD terminal age start
    # will be lumped into the terminal age group.
    df.loc[df["age"] > 95, "age"] = 95

    # no <1 age group before 2018; 0 -> 0-1 bin
    if year <= 2017:
        df = demographic.age_binning(df, 3, drop_age=True)
    else:
        df = demographic.age_binning(df, 2, drop_age=True)

# columns contain only 1 optimized data type
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

# find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

# remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = formatting.sanitize_diagnoses(df[feat])
    # convert to all upper case
    df[feat] = df[feat].str.upper()

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    df = formatting.stack_merger(df)
elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1
else:
    print("Something went wrong, there are no ICD code features")

# individual-level record, 1 case for each row
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

del df

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
if len(hosp_frmat_feat) != len(df_agg.columns):
    raise ValueError("The DataFrame has the wrong number of columns")

for i in range(len(hosp_frmat_feat)):
    if hosp_frmat_feat[i] not in df_agg.columns:
        raise ValueError(
            "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])
        )

# check data types
for i in df_agg.drop(
    ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
).columns:
    # assert that everything but cause_code, source, measure_id (for now) are NOT object
    if df_agg[i].dtype == object:
        raise ValueError("%s should not be of type object" % (i))

# check number of unique feature levels

if len(df_agg["year_start"].unique()) != len(df_agg["nid"].unique()):
    raise ValueError(
        "Number of feature levels of years should match number of feature levels of nid"
    )

print(len(df_agg["age_start"].unique()))
print(len(df_agg["age_end"].unique()))

if len(df_agg["age_start"].unique()) != len(df_agg["age_end"].unique()):
    raise ValueError(
        "Number of feature levels age start should match number of feature levels age end"
    )

if len(df_agg["diagnosis_id"].unique()) > 2:
    raise ValueError("diagnosis_id should have 2 or less feature levels")

if len(df_agg["code_system_id"].unique()) > 2:
    raise ValueError("code_system_id should have 2 or less feature levels")

if len(df_agg["source"].unique()) != 1:
    raise ValueError("Source should only have one feature level")

if df_agg[df_agg.diagnosis_id == 1].val.sum() != final_admits:
    raise ValueError(
        """The count of admits from the middle of formatting should match
        primary admits in final data"""
    )

#####################################################
# WRITE OUT FILE OF THE PROCESSING INSTANCE
#####################################################
df_agg.to_parquet(
    f"{base}FILEPATH{year}.parquet",
    index=False,
)
print(f"Data for year {year} successfully processed")
