# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME
Years of data captured: 2001-2021

Template for formatting raw hospital data. Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

The GHDx entry is at
ADDRESS

INFO ABOUT THE SOURCE AND DATA:
The data in the Chile Hospital Discharge Information System (Sistema de
Información de Egresos Hospitalarios) come from the Statistical Reporting
of Hospital Discharges (Informe Estadístico de Egreso Hospitalario),
compulsory for all health facilities in the country.

Database contains multiple diagnoses per patient, including both E and
N-codes for injuries.

Contributors: Ministry of Health (Chile)
Publisher: Ministry of Health (Chile)
Citation: Ministry of Health (Chile). Chile Hospital Discharges.
Santiago, Chile: Ministry of Health (Chile).
Publication status: Published

Updated January 2020 to include additional years (2013-2018)
Updated May 2023 to include additional years (2019 & 2021)

"""
import glob
import platform
import warnings

import numpy as np
import pandas as pd
from crosscutting_functions.nid_tables.new_source import InpatientNewSource
from crosscutting_functions import demographic, general_purpose
from crosscutting_functions.formatting-functions import formatting
from crosscutting_functions.formatting-functions.formatting import test_case_counts


#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

# list of filepaths to chile data
filepath_list = glob.glob(FILEPATH)
filepath_list = sorted(filepath_list)
filepath_list = [i.replace("\\", "/") for i in filepath_list]  
del filepath_list[4]  # the variable names for 2005 are different so we want to
# treat it separately

df_list = []  # initialize empty list to append dfs to as we loop
years = np.arange(2001, 2013, 1)
years = np.delete(years, 4)  # can't use del with np arrarys

year_path_dict = dict(list(zip(years, filepath_list)))  # want to associate a year
# with a file path (saves the manual labor of checking each filepath)

# read in data for every year
for year in years:
    df = pd.read_stata(
        year_path_dict[year],
        columns=["edad", "sexo", "DIAG1", "DIAG2", "COND_EGR", "estab"],
    )
    df["year_start"] = year
    df["year_end"] = year
    df_list.append(df)

# append (almost) everything together
df = pd.concat(df_list)

# now do 2005:
df_2005 = pd.read_stata(FILEPATH,
    columns=["edad", "sexo", "DIAG1", "CAUSA_EXT", "COND_EGR", "estab"],
)
df_2005["year_start"] = 2005
df_2005["year_end"] = 2005
df_2005.rename(columns={"CAUSA_EXT": "DIAG2"}, inplace=True)

# append 2005
df = pd.concat([df, df_2005])
df = df.reset_index(drop=True)  # ALWAYS DO THIS AFTER CONCAT
del df_2005  # don't need
df.columns = df.columns.str.lower()

# Updating to include new years after 2012
ypd = dict(
    zip(
        [i for i in (list(range(2013, 2020)) + [2021])],
        sorted(glob.glob(FILEPATH)),
    )
)
new_data = []
for year, filepath in ypd.items():

    if year == 2017:  # 2017 has irregular quotes
        tmp = pd.read_csv(filepath, sep=";", quoting=3)
        tmp.columns = [col.replace('"', "") for col in tmp.columns]
        for col in tmp.columns:
            tmp[col] = tmp[col].str.replace('"', "")
    else:
        tmp = pd.read_csv(
            filepath, sep=";", encoding=str(np.where(year >= 2019, "latin-1", "utf-8"))
        )
        if year == 2019:  # 2019 with changed col names
            tmp.rename(
                columns={
                    "CONDICION_EGRESO": "COND_EGR",
                    "EDAD_A_OS": "EDAD",
                    "ESTABLECIMIENTO_SALUD": "ESTAB",
                },
                inplace=True,
            )
        if year == 2021:  # 2021 had aggr only data, not indv
            tmp.rename(columns={"EDAD_AÑOS": "EDAD"}, inplace=True)
            tmp["COND_EGR"] = 0
            tmp["ESTAB"] = "inpatient unknown"

    tmp.columns = tmp.columns.str.lower()
    assert tmp.shape[1] > 4, "this broke"
    tmp["year_start"] = year
    tmp["year_end"] = year
    tmp = tmp[df.columns.tolist()]
    new_data.append(tmp)


df = pd.concat([df] + new_data, sort=False, ignore_index=True)

total_val = df.shape[0]

assert df.shape[0] == len(df.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df.index.unique()))
    + " while the DataFrame has "
    + str(df.shape[0])
    + " rows"
)
# we're keeping all the columns, we selected the ones we wanted earlier.

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    #    'year': 'year',  # already have year_start and end made so i dont want
    "year_start": "year_start",
    "year_end": "year_end",
    "sexo": "sex_id",
    "edad": "age",
    "age_start": "age_start",  # age_binning will fill these two columns
    "age_end": "age_end",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "cond_egr": "outcome_id",
    "estab": "facility_id",
    # diagnosis varibles
    "diag1": "dx_1",
    "diag2": "dx_2",
    #    'dx_3': 'dx_3'  # don't want to make
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# everything to lowecase
df.columns = list(map(str.lower, df.columns))

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)

assert df.shape[0] == total_val, "some cases were lost"

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you do the manual data processing, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################

# These are completely dependent on data source

df["representative_id"] = 3  # not representative: we don't know the coverage
df["location_id"] = 98

# group_unit 1 signifies age data is in years
df["age_group_unit"] = 1  # 1 =  years
df["source"] = "CHL_MOH"  # MOH for "Ministry of Health"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2  # this is ICD10

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {
    2001: 121444,
    2002: 121445,
    2003: 121446,
    2004: 121447,
    2005: 121448,
    2006: 121449,
    2007: 121450,
    2008: 121451,
    2009: 121452,
    2010: 121453,
    2011: 121454,
    2012: 193857,
    2013: 336853,
    2014: 336854,
    2015: 336855,
    2016: 336856,
    2017: 369475,
    2018: 422874,
    2019: 473534,
    2021: 515979,
}
df = formatting.fill_nid(df, nid_dictionary)  # fill in the NID column

# make all the ICD codes upper case
df["dx_1"] = df["dx_1"].str.upper()
df["dx_2"] = df["dx_2"].str.upper()

df = formatting.swap_ecode_with_ncode(df, icd_vers=10)
# %%
#####################################################
# CLEAN VARIABLES
#####################################################
# %%
# distinguish age processing between
# year(s) with already-binned age vs.
# those w/ individual age in years
df_age_agg = df.loc[df.year_start == 2021]
df_age_ind = df.loc[df.year_start != 2021]

# for year with aggr age (2021)
df_age_agg[["age_start", "age_end"]] = df_age_agg["age"].str.split(", ", n=1, expand=True)
df_age_agg.age_start = df_age_agg.age_start.str.replace("[", "", regex=False)
df_age_agg.age_end = df_age_agg.age_end.str.replace("[", "", regex=False)
df_age_agg["age_start"].replace("*", 0, inplace=True, regex=False)
df_age_agg["age_end"].replace("None", 125, inplace=True)

# rest of years with indv age in year
# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df_age_ind["age"] = pd.to_numeric(df_age_ind["age"], errors="raise")
df_age_ind = demographic.age_binning(df_age_ind, 3)

df = pd.concat([df_age_agg, df_age_ind])
df.drop("age", axis=1, inplace=True)

del df_age_agg
del df_age_ind

# account for year (2021 only currently) where sex_id are coded in str
df["sex_id"].replace(["HOMBRE", "MUJER"], [1, 2], inplace=True)
# replace non male/female sex id with 3
df.loc[~df["sex_id"].isin([1, 2, "1", "2"]), "sex_id"] = 3

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
# str_cols = ['source', 'facility_id', 'outcome_id']  # commented out cuz still
# need to do some formatting on these columns

for col in int_cols:
    df[col] = pd.to_numeric(
        df[col], errors="raise"
    )  # cluster doesn't recognize downcast, downcast='integer')
# for col in str_cols:
#    df[col] = df[col].astype(str)
assert df.shape[0] == total_val, "some cases were lost"

# fix outcome_id levels
# codebook says that 1 = improved, 2 = died.
# need to clean values
df["outcome_id"] = df["outcome_id"].fillna("unknown")

# set 9 and 0 to unknown cuz the codebook doesn't say what they mean
df["outcome_id"].replace(9, "unknown", inplace=True)
df["outcome_id"].replace(0, "unknown", inplace=True)

# replace code for "improved" with string "discharge"
df["outcome_id"].replace(1, "discharge", inplace=True)
df["outcome_id"].replace("1", "discharge", inplace=True)
# replace code for "died" with string "death"
df["outcome_id"].replace(2, "death", inplace=True)
df["outcome_id"].replace("2", "death", inplace=True)

# %%
# fix facility_id

# read in a map that gives names to the coded values in "estab"
# if you need to find this map, use the codebook in the 2011 folder, and click
# the hyperlink in the definition for "estab"
facility_id_map = pd.read_excel(
    FILEPATH,
    sheet_name="Hoja1",
    header=2,
)
facility_id_map.dropna(inplace=True)
facility_id_map = facility_id_map[
    ["Nombre Tipo de Establecimiento", "Codigo Establecimiento"]
].drop_duplicates()
facility_id_map["Nombre Tipo de Establecimiento"] = facility_id_map[
    "Nombre Tipo de Establecimiento"
].str.strip()

# unfortunately, the vast majority of values in the data don't appear in any
# of the codebooks so we have to label them as unknown

# we have these values for facility_id: hospital: day clinic, emergency,
# clinic in hospital, outpatient clinic, inpatient unknown, outpatient unknown

# data has these (translated): Health reference center, clinic / health center,
# Family health center, clinic, Clinic or hospital of mutuality,
# Clinic or penitentiary hospital, greater complexity establishment,
# Medium complexity establishment, hospital, Field hospital,
# delegate hospital, Military field hospital

# make dictionary for replacement:
facility_id_dict = {
    "Establecimiento Mayor Complejidad": "hospital",
    "Clínica": "day clinic",
    "Hospital": "hospital",
    "Centro de Salud": "day clinic",
    "Establecimiento Mediana Complejidad": "hospital",
    "Establecimiento Menor Complejidad": "hospital",
    "Clínica u Hospital de Mutualidad": "hospital",
    "Centro de Referencia de Salud": "day clinic",
    "Hospital Delegado": "hospital",
    "Hospital Militar de Campaña": "hospital",
    "Hospital de Campaña": "hospital",
    "Centro de Salud Familiar": "hospital",
    "Clínica u Hospital Penitenciario": "hospital",
}
facility_id_map["Nombre Tipo de Establecimiento"].replace(facility_id_dict, inplace=True)

# rename facility_id to temp name:
df.rename(columns={"facility_id": "facility_code"}, inplace=True)
facility_id_map.rename(columns={"Codigo Establecimiento": "facility_code"}, inplace=True)

# merge
df = df.merge(facility_id_map, how="left", on="facility_code", validate="m:1")

df["Nombre Tipo de Establecimiento"] = df["Nombre Tipo de Establecimiento"].fillna(
    "inpatient unknown"
)

df.rename(columns={"Nombre Tipo de Establecimiento": "facility_id"}, inplace=True)

df.drop("facility_code", axis=1, inplace=True)
# %%
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = formatting.sanitize_diagnoses(df[feat])

# store the data wide for the EN matrix
df.to_csv(FILEPATH)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################

# TODO drop patient index in stack_merger
if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    df = formatting.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")
df.drop("patient_index", axis=1, inplace=True)
# If individual record: add one case for every diagnosis
df["val"] = 1
assert df[df.diagnosis_id == 1].val.sum() == total_val, "some cases were lost"

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# HOT FIX
# can't have more than one facility_id in one year
df["facility_id"] = df["facility_id"].replace("hospital", "inpatient unknown")

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.")
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
assert set(columns_before) == set(columns_after), "You lost or added a column when reordering"
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

assert (
    len(df_agg["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"

unexp_sex = list(set(df_agg["sex_id"].unique()) - set([1, 2, 3]))
assert len(unexp_sex) == 0, "There are unexpected value(s) in sex_id"

for yr in list(df_agg.year_start.unique()):
    df_temp = df_agg.loc[df_agg.year_start == yr]

    assert len(df_temp["age_start"].unique()) == len(
        df_temp["age_end"].unique()
    ), f"Year {yr} has umatched number of feature levels for age start & age end"

assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"

assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

# NEW- test the newly prepped data against the last formatted version
# This is manually pulled in, and doesn't break if the test results are an issue, so carefully
# run this portion of the formatting script and review the output for warnings
compare_df = pd.read_hdf(FILEPATH
)

# drop the new data- can't compare it
test_df = df_agg.query("year_start < 2019").copy()
test_results = test_case_counts(test_df, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    warnings.warn(
        (
            f"Tests failed. old had cases {compare_df.val.sum()} while "
            f"new had {test_df.val.sum()} and here are test "
            f"results {test_results}"
        )
    )

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = (FILEPATH
)
general_purpose.write_hosp_file(df_agg, write_path, backup=True)

#####################################################
# UPDATE SOURCE_TABLE
#####################################################

# Update nid tables
ins = InpatientNewSource(df_agg)
nids = [n for k, n in nid_dictionary.items() if k >= 2019]
# merged nid for 2018-2021
merged_nid = 526348

src_metadata = {
    "pipeline": {"inpatient": nids, "claims": [], "outpatient": []},
    "uses_env": {1: [], 0: nids},
    "age_sex": {1: [], 2: nids, 3: []},
    "merged_dict": {merged_nid: nids + [422874]},
}
ins.process(**src_metadata)
