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
import getpass
import warnings

# load our functions
# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH"
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
filepath = root + "FILEPATH"
files = glob.glob(filepath + "*(FINAL).csv")

list_df = []
for file in files:
    df = pd.read_csv(file)
    list_df.append(df)


df = pd.concat(list_df)
df_og = df.copy()

# If this assert fails uncomment this line:
df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df.index.unique()))
    + " while the DataFrame has "
    + str(df.shape[0])
    + " rows "
    + "try this: df = df.reset_index(drop=True)"
)

# Select features from raw data to keep
drop_cols = ["Unnamed: 0", "household.id", "province"]
df = df.drop(drop_cols, 1)
keep = df.columns.tolist()
df = df[keep]


df["year"] = df.date_adm.apply(lambda x: x.split("-")[0])
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
    "sex": "sex_id",
    "age_value": "age",
    "age_start": "age_start",
    "age_end": "age_end",
    "age_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "facility_id": "facility_id",
    # diagnosis varibles
    "icdcode1": "dx_1",
    "icdcode2": "dx_2",
    "icdcode3": "dx_3",
    "icdcode4": "dx_4",
    "icdcode5": "dx_5",
    # procedure codes
    "rvscode1": "procx_1",
    "rvscode2": "procx_2",
    "rvscode3": "procx_3",
    "rvscode4": "procx_4",
    "rvscode5": "procx_5",
    # annoying column name convention
    "household.id": "household_id",
    "patient.id": "patient_id",
    "patient.visit.id": "patient_visit_id",
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

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

df["representative_id"] = 5  # Do not take this as gospel, it's guesswork

# While the data has subnational region, these subnationals are not yet stored in
# central comp
df["location_id"] = 16

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "PHL_PHI"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# case is the sum of live discharges and deaths
df["outcome_id"] = "case/discharge/death"

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {"example_year": "example_nid"}
df = fill_nid(df, nid_dictionary)

# #####################################################
# # MANUAL PROCESSING
# # this is where fix the quirks of the data, like making values in the
# # data match the values we use.

# # For example, repalce "Male" with the number 1
# #####################################################


# Removing negative LoS observations
df = df[df.los >= 0]

# Rename sex_id
df["sex_id"].replace(["M", "F"], [1, 2], inplace=True)


# Cast ages
df["age"] = df["age"].astype(int)
# #####################################################
# # CLEAN VARIABLES
# #####################################################
