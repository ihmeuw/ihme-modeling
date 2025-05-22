"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

ADDRESS

When you are finished:
1) run the new source through FILEPATH
2) add the data to the tableau template
3) publish the tableau
4) if everything looks good in your tableau, open a Pull Request and include
    the link to your tableau
"""
import platform
import re
import warnings

import numpy as np
import pandas as pd
from crosscutting_functions import demographic, general_purpose
from crosscutting_functions.formatting-functions import formatting


#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
filepath = FILEPATH

df_list = []
for e in [2015, 2017]:
    in_file = FILEPATH
    df = pd.read_csv(in_file, sep="\t")
    df["year_start"] = e
    df_list.append(df)

df = pd.concat(df_list, sort=False)

## If this assert fails uncomment this line:
df = df.reset_index(drop=True)
# assert_msg = f"""index is not unique, the index has a length of
# {str(len(df.index.unique()))} while the DataFrame has
# {str(df.shape[0])} rows. Try this: df = df.reset_index(drop=True)"""
# assert_msg = " ".join(assert_msg.split())
# assert df.shape[0] == len(df.index.unique()), assert_msg

# # Replace feature names on the left with those found in data where appropriate
# # ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# # want

# #     df = df.rename(columns={
# #         "sexo": "sex",
# #         "diagentr": "diagnosis_id",
# #         "diagprin": "cause_code",
# #         "edadanios": "age_years",
# #         "edadmes": "age_months",
# #         "edaddias": "age_days",
# #         "diasestancia": "length_of_stay"
# #     })
hosp_wide_feat = {
    "nid": "nid",
    # demographics
    "ProvResi": "location",
    "representative_id": "representative_id",
    "Sexo": "sex_id",
    "age_start": "age_start",
    "age_end": "age_end",
    "EdadAnios": "age_years",
    "EdadMes": "age_months",
    "EdadDias": "age_days",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "MotivoAlta": "outcome_id",
    "facility_id": "facility_id",
    "DiasEstancia": "los",
    # diagnosis varibles
    "DiagPrin": "dx_1",
    "diagnosis_id": "diagnosis_id",
}

# # Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)
df["year_end"] = df["year_start"]

# Drop vars
# Vars are related to the hospital facility
drop = ["Factor", "Blanco", "Norden", "ProvHosp", "FxAlta", "DiagEntr"]
keep = [e for e in df.columns if e not in drop]
df = df[keep]

# # set difference of the columns you have and the columns you want,
# # yielding the columns you don't have yet
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

df["representative_id"] = 2  # Do not take this as gospel, it's guesswork
df["location_id"] = 92  # dropping subnat locations

# # group_unit 1 signifies age data is in years
df["age_group_unit"] = 1
df["source"] = "ESP_NSI"

# # code 1 for ICD-9, code 2 for ICD-10
df.loc[df.year_start == 2015, "code_system_id"] = 1
df.loc[df.year_start == 2017, "code_system_id"] = 2

msg = "There is a mismatch between codes and code_system_id"
dx_list = [e for e in df.columns if e.startswith("dx_")]

for code_sys in df["code_system_id"].unique().tolist():
    if code_sys == 1:
        regex = re.compile("^[A-DF-UW-Z]")
        for e in dx_list:
            codes = df[df.code_system_id == code_sys][e].unique().tolist()
            icd10 = list(filter(regex.match, codes))
            assert len(icd10) == 0, msg
    else:
        regex = re.compile("^[0-9]")
        for e in dx_list:
            codes = df[df.code_system_id == code_sys][e].unique().tolist()
            icd9 = list(filter(regex.match, codes))
            # assert len(icd9) == 0, msg

df.loc[(df.dx_1.isin(icd9)) & (df.code_system_id == 2), "code_system_id"] = 1

# # metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Primary dx only
df["diagnosis_id"] = 1
df.rename(columns={"dx_1": "cause_code"}, inplace=True)

# # Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2015: 284235, 2017: 428363}
df = formatting.fill_nid(df, nid_dictionary)

# #####################################################
# # MANUAL PROCESSING
# # this is where fix the quirks of the data, like making values in the
# # data match the values we use.

# # For example, repalce "Male" with the number 1
# #####################################################

assert df.age_days.max() < 365
assert df.age_months.max() < 12

cond = "(df.age_years == 0)"
df["age"] = np.nan

# # Convert to days
df.loc[eval(cond), "age_days"] += df.loc[eval(cond), "age_months"] * 30.5
# Convert to year space
df.loc[eval(cond), "age"] = df.loc[eval(cond), "age_days"] / 365

df.loc[df.age.isnull(), "age"] = df.loc[df.age.isnull(), "age_years"]
assert df[df.age.isnull()].shape[0] == 0
df.drop(["age_months", "age_days", "age_years"], axis=1, inplace=True)

# # Replace feature levels manually
df["outcome_id"].replace(
    [3, 1, 2, 4], ["death", "discharge", "discharge", "discharge"], inplace=True
)
df["facility_id"] = "hospital"

# Drop day cases
df = df[df.los > 1]
df.drop("los", axis=1, inplace=True)

# Manually verify the replacements
assert len(df["sex_id"].unique()) == 2, "df['sex_id'] should have 2 feature levels"
assert len(df["outcome_id"].unique()) == 2, "df['outcome_id'] should have 2 feature levels"
assert len(df["facility_id"].unique() == 2), "df['facility_id] should have 2 feature levels"

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

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

df.loc[df["age"] > 95, "age"] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = demographic.age_binning(
    df, drop_age=True, terminal_age_in_data=True, under1_age_detail=True
)

# # drop unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

# # Create year range if the data covers multiple years
# # df = year_range(df)

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# # Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

df["val"] = 1
# # Group by all features we want to keep and sums 'val'
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

# # Arrange columns in our standardized feature order
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
assert len(df_agg["age_start"].unique()) == len(df_agg["age_end"].unique()), (
    "number of feature levels age start should match number of feature " + r"levels age end"
)
assert (
    len(df_agg["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"
assert len(df_agg["sex_id"].unique()) == 2, "There should only be two feature levels to sex_id"
assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (df.val >= 0).all(), "for some reason there are negative case counts"

# # #####################################################
# # # WRITE TO FILE
# # #####################################################

# # # Saving the file
write_path = (FILEPATH
)

general_purpose.write_hosp_file(df_agg, write_path, backup=True)

# # #####################################################
# # # UPDATE SOURCE_TABLE
# # #####################################################
