# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017
@author: 

Formatting Georgia

"""

import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass


if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# load our functions
user = getpass.getuser()
prep_path = "FILEPATH"
sys.path.append(prep_path)

import hosp_prep

input_folder = "FILEPATH"

# remove data that has grouped ICD codes
remove_grouped_icds = False  # we got a map from USER 12/15/17

# helper functions
def check_subset_columns(df, parent_column, child_column):
    """
    Check two pandas series where one should be larger than the other
    i.e., parent_column < parent_column.
    e.g., the number of hospital discharges under 1 years old should
    be a subset of hospital discharges under 15 years old.  Returns a boolean
    index (idk if that's what to call it) so you can select rows where bad
    stuff is happening

    Args:
        df (DataFrame): Data you want to check
        parent_column (str): What should be the larger column
        child_column (str): Column that should be a subset of the
            parent column

    Returns:
        bad_condition (boolean series?): conditional mask where child > parent
    """
    # check all the subset age groups to see if they make sense
    check_list = []
    # E.g., deaths under 1 greater than deaths under 15
    # this is the opposite of what we want:
    bad_condition = df[child_column] > df[parent_column]

    return bad_condition


def prep_2000_2011():
    """
    Funtion to read in and prepare data from GEO for the years 2000-2011.

    Returns:
        Pandas DataFrame with data for the years 2000-2011 in a format
        consistent with the other years of data.
    """
  
    input_columns = ["b", "g", "d", "1", "2", "3", "4", "5", "6", "7", "8", "year"]

    # data from 2005 with translated column names. has a multi index,
    # but I just kept the main columns
    # FILEPATH

    # columns from the translated columns, renamed (manually) to separate
    # 0-15 and 15+ years old.
    template_columns = [
        "number_in_group",
        "dx_1",
        "line_number",
        "15_125_discharge",
        "15_125_beddays",
        "15_125_death",
        "0_15_discharge",
        "0_1_discharge",
        "0_15_beddays",
        "0_15_death",
        "0_1_death",
        "year",
    ]

    # make a dictionary out of the two sets of columns for later
    column_dict = dict(list(zip(input_columns, template_columns)))

    df_list = []
    # skip 2012, 2013, 2014, they have a differnt format
    for year in range(2000, 2012):
        df = pd.read_csv("{}/{}.csv".format(input_folder, year))

        assert_msg = """Columns don't match for year {}, here's the set
        difference:
        {}""".format(
            year, set(df.columns).symmetric_difference(set(input_columns))
        )
        assert set(df.columns) == set(input_columns), assert_msg

        df_list.append(df)

    # stick 2000-2011 together
    df = pd.concat(df_list, ignore_index=True)

    # rename columns
    df = df.rename(columns=column_dict)

    # there are some rows in 2000 and 2001 that are all null because they were
    # exported to excel (csv) from tables in word documents
    df = df[df.dx_1.notnull()]

    # drop these columns we didn't need them
    df = df.drop(["number_in_group", "line_number"], axis=1)

    # fill NaNs.  They're all in value columns
    df.update(
        df[
            [
                "15_125_discharge",
                "15_125_beddays",
                "15_125_death",
                "0_15_discharge",
                "0_1_discharge",
                "0_15_beddays",
                "0_15_death",
                "0_1_death",
            ]
        ].fillna(0)
    )

    # proof all Nans were in value columns
    assert df.notnull().all().all(), "There are nulls"

    # There are several relationships that we hope are true (want to impose)
    # within the columns
    # 0_1_died < 0_15_died
    # 0_1_discharged < 0_15_discharged

    # check for 0-1 discharges > 0-14 discharges
    nonsense = check_subset_columns(
        df, parent_column="0_15_discharge", child_column="0_1_discharge"
    )

    # drop rows where 0_14_discharge > 0_1_discharge
    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    # check for 0-1 deaths > 0-14 deaths
    nonsense = check_subset_columns(
        df, parent_column="0_15_death", child_column="0_1_death"
    )

    # drop rows where 0_14_death > 0_1_death
    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy

    # This only dropped one row.

    # currently we have data for age groups 15-125, 0-15, and 0-1.
    # We want to make non overlapping age groups

    # create true 1-15 age group
    df["1_15_discharge"] = df["0_15_discharge"] - df["0_1_discharge"]
    df["1_15_death"] = df["0_15_death"] - df["0_1_death"]
    # so the age groups are all left-inclusive right-exclusive: 0-1,1-15,15-125

    # The one row where child > parent was removed already, but to prove it:
    assert (df["1_15_discharge"] >= 0).all()
    assert (df["1_15_death"] >= 0).all()

    # Can now drop the overlapping parent columns
    df = df.drop(["0_15_discharge", "0_15_death"], axis=1)

    # and drop the beddays columns
    df = df.drop(["15_125_beddays", "0_15_beddays"], axis=1)

    # reshape so ages are long
    df = df.set_index(["year", "dx_1"]).stack().reset_index()

    # split out the ages and the outcome type
    df["age_start"], df["age_end"], df["outcome_id"] = (
        df["level_2"].str.split("_", 2).str
    )

    # makes ages numeric
    df["age_start"] = pd.to_numeric(df["age_start"], errors="raise")
    df["age_end"] = pd.to_numeric(df["age_end"], errors="raise")

    # drop the column used to make these
    df = df.drop("level_2", axis=1)

    # rename
    df = df.rename(columns={"year": "year_start", 0: "val"})

    # add sex and year_start and year_end
    df["year_end"] = df.year_start
    df["sex_id"] = 3

    # We're done with 2000-2011, save for later

    return df


def prep_2012_2013():
    """
    Funtion to read in and prepare data from GEO for the years 2012-2013.

    Returns:
        Pandas DataFrame with data for the years 2012-2013-2011 in a format
        consistent with the other years of data.
    """

    # these ones have more columns
    input_columns = [
        "b",
        "g",
        "d",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "year",
    ]
    df_list = []
    # skip 2012, 2013, 2014, they have a differnt format
    for year in [2012, 2013]:
        df = pd.read_csv("{}/{}.csv".format(input_folder, year))

        assert_msg = """Columns don't match for year {}, here's the set
        difference:
        {}""".format(
            year, set(df.columns).symmetric_difference(set(input_columns))
        )
        assert set(df.columns) == set(input_columns), assert_msg

        df_list.append(df)
    df = pd.concat(df_list, ignore_index=True)

    df.columns = [
        "group_number",
        "dx_1",
        "line_number",
        "18_125_discharge",
        "18_125_beddays",
        "18_125_death",
        "15_18_discharge",
        "15_18_beddays",
        "15_18_death",
        "0_15_discharge",
        "0_5_discharge",
        "0_1_discharge",
        "0_15_beddays",
        "0_15_death",
        "0_5_death",
        "0_1_death",
        "year_start",
    ]

    # check for 0-5 discharges > 0-15 discharges
    nonsense = check_subset_columns(
        df, parent_column="0_15_discharge", child_column="0_5_discharge"
    )

    # drop rows where 0_14_discharged > 0_1_discharge
    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    # check for 0-1 discharges > 0-5 discharges
    nonsense = check_subset_columns(
        df, parent_column="0_5_discharge", child_column="0_1_discharge"
    )

    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    # check for 0-5 death > 0-15 death
    nonsense = check_subset_columns(
        df, parent_column="0_15_death", child_column="0_5_death"
    )

    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    # check for 0-1 death > 0-5 death
    nonsense = check_subset_columns(
        df, parent_column="0_5_death", child_column="0_1_death"
    )

    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()
    # none of that dropped any rows, everything was a proper subset

    # create non overlapping age groups
    df["5_15_discharge"] = df["0_15_discharge"] - df["0_5_discharge"]
    df["5_15_death"] = df["0_15_death"] - df["0_5_death"]

    df["1_5_discharge"] = df["0_5_discharge"] - df["0_1_discharge"]
    df["1_5_death"] = df["0_5_death"] - df["0_1_death"]

    # drop unnecessary columns
    df.drop(
        [
            "group_number",
            "line_number",
            "0_15_discharge",
            "0_5_discharge",
            "0_15_death",
            "0_5_death",
        ],
        axis=1,
        inplace=True,
    )

    # drop beddays columns
    df = df.drop(["18_125_beddays", "15_18_beddays", "0_15_beddays"], axis=1)

    # reshape long on age and outcome
    df = df.set_index(["dx_1", "year_start"]).stack().reset_index()

    # split out the ages and the outcome type
    df["age_start"], df["age_end"], df["outcome_id"] = (
        df["level_2"].str.split("_", 2).str
    )

    # make ages numeric
    df["age_start"] = pd.to_numeric(df["age_start"], errors="raise")
    df["age_end"] = pd.to_numeric(df["age_end"], errors="raise")

    # rename column
    df = df.rename(columns={0: "val"})

    # drop column that was used to make these
    df = df.drop("level_2", axis=1)

    # make some columns
    df["year_end"] = df.year_start
    df["sex_id"] = 3

    # done with 2012-2013, save for later

    return df


def prep_2014():
    """
    Funtion to read in and prepare data from GEO for the years 2012-2013.

    Returns:
        Pandas DataFrame with data for the years 2012-2013-2011 in a format
        consistent with the other years of data.
    """

    df = pd.read_excel(root + r"FILEPATH")

    # Select features from raw data to keep
    keep = [
        "Sex",
        "Age (years)",
        "Main Diagnosis (ICD10)",
        "External causes (ICD10)",
        "Disharge status",  # typo: Discharge
        "Complication (ICD 10)",
        "Comorbidity (ICD 10)",
        "Beddays",
    ]
    df = df[keep].copy()

    # rename the diagnosis columns
    df = df.rename(
        columns={
            "Main Diagnosis (ICD10)": "dx_1",
            "External causes (ICD10)": "dx_2",
            "Complication (ICD 10)": "dx_3",
            "Comorbidity (ICD 10)": "dx_4",
        }
    )

    # make outcome id
    # this is from the codebook on the second sheet of the file
    outcome_dict = {1: "discharge", 2: "discharge", 3: "discharge", 4: "death"}
    df["outcome_id"] = df["Disharge status"].map(outcome_dict)

    # now can drop "Disharge status"
    df.drop("Disharge status", axis=1, inplace=True)

    # Need to get rid of bed days. before that, need to use bed days
    # can't have bed days of 0, unless they died.
    df = df[(df.outcome_id != "discharge") | (df.Beddays >= 1)]  # stuff to keep

    # now can drop Beddays
    df = df.drop("Beddays", axis=1)

    # rename and make columns
    df = df.rename(columns={"Sex": "sex_id", "Age (years)": "age"})
    df["year_start"] = 2014
    df["year_end"] = 2014

    # make a val column. every row is an admission
    df["val"] = 1

    # there are some ages that can't be cast... georgian script
    df["age"] = pd.to_numeric(df["age"], errors="coerce")

    # bin the ages
    df.loc[df.age > 99, "age"] = 99  # 100 and up were not binning
    df = hosp_prep.age_binning(df)

    # don't need age after binning.
    df = df.drop("age", axis=1)

    # take care of some nulls
    df.loc[df.sex_id.isnull(), "sex_id"] = 3  # for unknown
    df = df[df.dx_1.notnull()]  # can't recover any null diagnoses
    df = df[df.outcome_id.notnull()]  # just 4 rows

    return df


df_00_11 = prep_2000_2011()
df_12_13 = prep_2012_2013()
df = prep_2014()

# Ready to combine all years of data
df = pd.concat([df_00_11, df_12_13, df], ignore_index=True)


# On to normal formatting
df["representative_id"] = 3  # Do not take this as gospel, it's guesswork
df["location_id"] = 35

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1

# source
df["source"] = np.nan
df.loc[df.year_start == 2014, "source"] = "GEO_COL_14"
df.loc[df.year_start < 2014, "source"] = "GEO_COL_00_13"

# code 1 for ICD-9, code 2 for ICD-10
# for 2000-2005 it's ICD 9, 2006-2014 it's ICD10
df.loc[df.year_start <= 2005, "code_system_id"] = 1
df.loc[df.year_start >= 2006, "code_system_id"] = 2
# could check this with the following code:
# for year in df.year_start.unique():
#     print year, df.loc[df.year_start == year, 'dx_1'].str[0].unique()

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

df["facility_id"] = "inpatient unknown"

# nid
df["nid"] = df.year_start.map(
    {
        2000: 212469,
        2001: 212479,
        2002: 212480,
        2003: 212481,
        2004: 212482,
        2005: 212483,
        2006: 212484,
        2007: 212486,
        2008: 212487,
        2009: 212488,
        2010: 212489,
        2011: 212490,
        2012: 212491,
        2013: 212492,
        2014: 212493,
    }
)


# check and clean values
# make sure dx columns are strings:
for col in ["dx_1", "dx_2", "dx_3", "dx_4"]:
    df[col] = df.loc[df[col].notnull(), col].apply(lambda x: x.encode("utf-8"))
    #     df[col] = df.loc[df[col].notnull(), col].astype(str)
    df[col] = df.loc[df[col].notnull(), col].str.upper()


# sanitize diagnoses
for col in ["dx_1", "dx_2", "dx_3", "dx_4"]:
    # [^A-Z0-9.\-] selects anything that isn't a capital letter, number, - or .
    df[col] = df[col].str.replace("[^A-Z0-9.\-]", "")


# move injuries.  want external cause as the main dx, with nature as 2ndary
df.loc[df["dx_2"].notnull(), ["dx_1", "dx_2"]] = df.loc[
    df["dx_2"].notnull(), ["dx_2", "dx_1"]
].values

###########################################################
# save wide for EN Matrix
###########################################################

# only 2014 has dual coding.
df.loc[df.year_start == 2014].drop("val", axis=1).to_stata(
    "FILEPATH", write_index=False
)


# reshape wide to long
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)
    df.drop("patient_index", axis=1, inplace=True)
elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

# 2014 data needs to have the diagnoses cleaned b/c they're good ICD codes.
df.loc[df.source == "GEO_COL_14", "cause_code"] = hosp_prep.sanitize_diagnoses(
    df.loc[df.source == "GEO_COL_14", "cause_code"]
)

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

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
assert (
    len(df_agg["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"
assert (
    len(df_agg["sex_id"].unique()) == 3
), "There should only be two feature levels to sex_id"
assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"

if remove_grouped_icds:
    # remove the data with ICD groupings
    df_agg = df_agg[df_agg.year_start == 2014]
    # remove decimals places from the formatted data
    df_agg.cause_code = df_agg.cause_code.str.replace("\W", "")

# save!
write_path = root + r"FILEPATH"
hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
