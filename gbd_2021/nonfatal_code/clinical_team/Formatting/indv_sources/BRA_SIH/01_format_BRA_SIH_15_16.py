# coding: utf-8

"""
Created on Mon Jan 23 17:32:28 2017
@author:

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

Just the years 2015 and 2016
"""
import pandas as pd
import platform
import warnings
import time

# load our functions

from clinical_info.Functions import hosp_prep
from clinical_info.Functions import stage_hosp_prep


# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
start = time.time()
filepath = "FILENAME"
df = pd.read_hdf(filepath, key="df")
print((time.time() - start) / 60)

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert_msg = f"""index is not unique, the index has a length of
{str(len(df.index.unique()))} while the DataFrame has
{str(df.shape[0])} rows. Try this: df = df.reset_index(drop=True)"""
assert_msg = " ".join(assert_msg.split())
assert df.shape[0] == len(df.index.unique()), assert_msg
# drop a column full of '0000' values
assert df["DIAG_SECUN"].unique().size == 1, "there are actualy diagnoses here"
df.drop("DIAG_SECUN", axis=1, inplace=True)

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "MUNIC_RES": "location_id",
    "representative_id": "representative_id",
    "ANO_CMPT": "year_start",
    "SEXO": "sex_id",
    "IDADE": "age",
    "COD_IDADE": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "MORTE": "outcome_id",
    "facility_id": "facility_id",
    # dates
    "DT_SAIDA": "dis_date",
    "DT_INTER": "adm_date",
    # diagnosis varibles
    "DIAG_PRINC": "dx_1",
    "DIAGSEC1": "dx_2",
    "DIAGSEC2": "dx_3",
    "DIAGSEC3": "dx_4",
    "DIAGSEC4": "dx_5",
    "DIAGSEC5": "dx_6",
    "DIAGSEC6": "dx_7",
    "DIAGSEC7": "dx_8",
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

df["representative_id"] = 1  # Do not take this as gospel, it's guesswork

# make a dictionary from munic res values to location IDs using a method
# from the GBD 2015 Stata scripts
loc_dict = {
    "12": 4750,  # Acre
    "27": 4751,  # Alagoas
    "13": 4752,  # Amazonas
    "16": 4753,  # Amapa
    "29": 4754,  # Bahia
    "23": 4755,  # Ceara
    "53": 4756,  # Distrito Federal
    "32": 4757,  # Espirito Santo
    "52": 4758,  # Goias
    "21": 4759,  # Maranhao
    "31": 4760,  # Minas Gerais
    "50": 4761,  # Mato Grosso do Sul
    "51": 4762,  # Mato Grosso
    "15": 4763,  # Para
    "25": 4764,  # Paraiba
    "41": 4765,  # Parana
    "26": 4766,  # Pernambuco
    "20": 4766,  # Pernambuco
    "22": 4767,  # Piaui
    "33": 4768,  # Rio de Janeiro
    "24": 4769,  # Rio de Janeiro do Norte
    "11": 4770,  # Rondonia
    "14": 4771,  # Roraima
    "43": 4772,  # Rio Grande do Sul
    "42": 4773,  # Santa Catarina
    "28": 4774,  # Sergipe
    "35": 4775,  # Sao Paulo
    "17": 4776,
}  # Tocantins

df["to_map_id"] = df["location_id"].astype(str).str[0:2]


df["location_id"] = df["to_map_id"].map(loc_dict)
# drop the var we used to map
df.drop("to_map_id", axis=1, inplace=True)

# match year end to year start
df["year_end"] = df["year_start"]

# group_unit 1 signifies age data is in years
# df['age_group_unit'] = 1  # this exists in BRA SIH
df["source"] = "BRA_SIH"

# code 1 for ICD-9, code 2 for ICD-10
df["code_system_id"] = 2

# set facility_id, needs to match what's in the former BRA SIH data
df["facility_id"] = "inpatient unknown"

# case is the sum of live discharges and deaths
df["outcome_id"] = df["outcome_id"].replace([0, 1], ["discharge", "death"])
# males are coded correctly to 1 but females are coded to 3
df["sex_id"] = df["sex_id"].replace([3], [2])

# metric_id == 1 signifies that the 'val' column consists of counts
df["metric_id"] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2015: 237832, 2016: 281543}
df = hosp_prep.fill_nid(df, nid_dictionary)


# retain the under1 neonatal age groups
unit_dict = {"days": 2, "months": 3, "years": 4, "centuries": 5}

# AGE IS LONG
# convert everything to years

# When age in days, the oldest age is 30 days
df.loc[df.age_group_unit == 2, "age"] = df[df.age_group_unit == 2].age / 365

# When age in months, the oldest age is 11 months
df.loc[df.age_group_unit == 3, "age"] = df[df.age_group_unit == 3].age / 12

# When age in years, don't need to do anything

# when age_group_unit is 5 for centuries, it's denoting years past 100 years old
# still need to convert to years
df.loc[df.age_group_unit == 5, "age"] = df[df.age_group_unit == 5].age + 100

# Now set all age units to years (our number id for years, not theirs)
df["age_group_unit"] = 1

# this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df.loc[df["age"] > 95, "age"] = 95

df = hosp_prep.age_binning(df, new_age_detail=True)
assert df.age_start.notnull().all()
assert df.age_end.notnull().all()


def drop_day_cases(df, drop_date_cols=True):
    """
    drop individuals who we know stayed for less than 24 hours using the
    admission and discharge dates. This isn't perfect (it keeps someone who
    stays for 8 hours overnight) but it does remove the cases we know aren't
    full days
    """
    pre_drop = df.shape[0]

    df["dis_date"] = pd.to_datetime(df["dis_date"])
    df["adm_date"] = pd.to_datetime(df["adm_date"])

    # drop day cases
    df = df[df.dis_date != df.adm_date].copy()
    dropped = round(1 - (float(df.shape[0]) / pre_drop), 4) * 100
    print("{} percent of rows were dropped".format(dropped))

    # check for negative lengths of stay
    los = df["dis_date"].subtract(df["adm_date"])
    assert los.min() == pd.Timedelta(
        "1 days 00:00:00"
    ), "Minimum length of stay is less than a day, something wrong"
    if drop_date_cols:
        df.drop(["dis_date", "adm_date"], axis=1, inplace=True)
    return df


df = drop_day_cases(df, drop_date_cols=True)

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
    df[col] = pd.to_numeric(df[col], errors="raise")
for col in str_cols:
    df[col] = df[col].astype(str)


# use the staged hosp prep function that allows for neonatal groups
# df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)

# re-code unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

start = time.time()

# store the data wide for the EN matrix
df.to_stata(r"FILEPATH")
write_path = r"FILEPATH"
hosp_prep.write_hosp_file(df, write_path, backup=False)

#####################################################
# SWAP ECODES INTO DX_1
#####################################################

# NOTE: order of DX doesn't matter for E-N matrix code.
df = hosp_prep.move_ecodes_into_primary_dx(df, 10)

#####################################################
# IF MULTIPLE DX EXIST:
# TRANSFORM FROM WIDE TO LONG
#####################################################

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith("dx_")]

# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    df.loc[df[feat] == "nan", feat] = pd.np.nan

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={"dx_1": "cause_code"}, inplace=True)
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

print("checking nulls after before dropping them")
print(df.isnull().sum())


# There are some rows with null cause_code and diagnosis_id = 2
condition = (df.diagnosis_id == 2) & (df.cause_code.isnull())
print(
    f"There are {df[condition].shape} rows where diagnosis_id is 2 and cause_code is null. Dropping them."
)
df = df[~condition].copy()

print("checking nulls after dropping them")
print(df.isnull().sum())

# If individual record: add one case for every diagnosis
df["val"] = 1

# takes about 45 minutes
print("This ran in {} min".format((time.time() - start) / 60))

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
print(df.isnull().sum())
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

print("Are there nulls after groupby?")

print(df_agg.isnull().sum())


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
    len(df_agg["sex_id"].unique()) == 2
), "There should only be two feature levels to sex_id"
assert (
    len(df_agg["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df_agg["source"].unique()) == 1, "source should only have one feature level"

assert (df.val >= 0).all(), "for some reason there are negative case counts"

# NEW- test the newly prepped data against the last formatted version
# This is manually pulled in, and doesn't break if the test results are an issue, so carefully
# run this portion of the formatting script and review the output for warnings

# Removing location ids that we know will cause the below assert to fail
updated_locs = [4761, 4750, 4760]
test_df = df_agg[~df_agg.location_id.isin(updated_locs)]

compare_df = pd.read_hdf("FILEPATH")
compare_df = compare_df[~compare_df.location_id.isin(updated_locs)]

test_results = stage_hosp_prep.test_case_counts(test_df, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    warnings.warn(
        f"Tests failed. old had cases {compare_df.val.sum()} while new had {test_df.val.sum()} and here are test results {test_results}"
    )

#####################################################
# WRITE TO FILE
#####################################################

print("Are there nulls right before saving?")

print(df_agg.isnull().sum())

# Saving the file
write_path = "FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
