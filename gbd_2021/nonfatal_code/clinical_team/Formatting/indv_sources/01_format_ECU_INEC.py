import pandas as pd
import numpy as np
import warnings

from clinical_info.Functions import hosp_prep, stage_hosp_prep


def file_reader(year, filepath):
    """Data is stored in both csv and Stata .dta files"""
    print(f"Reading in year {year}")
    fend = filepath[-3:]
    if fend == "DTA" or fend == "dta":
        tmp = pd.read_stata(filepath)
    elif fend == "csv":
        tmp = pd.read_csv(filepath, encoding="latin-1", sep=";")
    else:
        print("could not read the file {}".format(filepath))

    tmp["year"] = year
    return tmp


##############################################
# READ IN DATA
##############################################

# NOTE THIS DICTIONARY! IT CONTROLS THE YEARS AVAILABLE IN HARDCODING
max_year = 2019
hardcoding_dict = {
    "years": list(range(1997, max_year)),  # 2018 is most recent year
    "single_date_col": list(range(2015, max_year)),
    "icd_9_years": list(range(1990, 1999)),
    "icd_10_years": list(range(1999, max_year)),
}

# many of the years of data were extracted from some weird file format and
# put into the following dir:
base_dir = "FILEPATH"
file_name = "ECUADOR_INEC_HOSPITAL_DISCHARGE_"
years = hardcoding_dict["years"]
years_dict = dict(list(zip(years, list(range(len(years))))))

# the rest of the data is in these dirs.  This is the original loc for all data
new_year_files = {
    2012: "FILEPATH",
    2013: "FILEPATH",
    2014: "FILEPATH",
    2015: "FILEPATH",
    2016: "FILEPATH",
    2017: "FILEPATH",
    2018: "FILEPATH",
}

# The names and spellings of the variables change inbetween years.  below is
# reading in the data with the appropriate names for each year

# The old method hard-coded a dict of col names for each year. Let's put all possible
# values together in this 1 dict, and then pull out what we need for each year in
# order to re-use as much of this as possible
rename_dict = {
    "eda_pacien": "age",
    "edapacie": "age",
    "edad_pac": "age",
    "edad": "age",
    "con_edadpa": "age_units",
    "conedadp": "age_units",
    "cond_edad": "age_units",
    "cod_edad": "age_units",
    "dia_estada": "bed_days",
    "diaestad": "bed_days",
    "dia_estad": "bed_days",
    "dia_ingpac": "day_adm",
    "diaingpa": "day_adm",
    "dia_ingr": "day_adm",
    "dia_egrpac": "day_dis",
    "diaegrpa": "day_dis",
    "dia_egr": "day_dis",
    "cau_9narev": "dx_1",
    "cau_10arev": "dx_1",
    "cau10are": "dx_1",
    "cau_cie10": "dx_1",
    "mes_ingpac": "mon_adm",
    "mesingpa": "mon_adm",
    "mes_ingr": "mon_adm",
    "mes_egrpac": "mon_dis",
    "mesegrpa": "mon_dis",
    "mes_egr": "mon_dis",
    "con_egrpac": "outcome_id",
    "conegrpa": "outcome_id",
    "con_egrpa": "outcome_id",
    "sex_pacien": "sex",
    "sexpacie": "sex",
    "sexo_pac": "sex",
    "sexo": "sex",
    "ani_ingpac": "year_adm",
    "aniingpa": "year_adm",
    "anio_ingr": "year_adm",
    "fecha_ingr": "date_adm",
    "fecha_egr": "date_dis",
}
assert len(rename_dict) == len(
    set(rename_dict.keys())
), "There are duplicated dictionary keys"

keep_cols = [
    "year",
    "code_system_id",
    "age_units",
    "age",
    "sex",
    "dx_1",
    "outcome_id",
    "day_adm",
    "mon_adm",
    "year_adm",
    "day_dis",
    "mon_dis",
    "bed_days",
    "date_adm",
    "date_dis",
]

df_list = []

for year in years:
    if year < 2012:
        myfile = "{}{}{}.dta".format(base_dir, file_name, year)
    else:
        myfile = new_year_files[year]

    df = file_reader(year=year, filepath=myfile)

    df.columns = [x.lower() for x in df.columns]

    df_list.append(df)
    del df

    print(f"Processing year {year}")
    if year in hardcoding_dict["icd_9_years"]:
        df_list[years_dict[year]]["code_system_id"] = 1
    elif year in hardcoding_dict["icd_10_years"]:
        df_list[years_dict[year]]["code_system_id"] = 2
    else:
        raise ValueError("Did you add a new year? what is the ICD coding system?")

    # just to be safe make a subset dictionary containing only columns present
    # in a given year
    cols = df_list[years_dict[year]].columns.tolist()
    df_rename_dict = {k: v for k, v in rename_dict.items() if k in cols}
    # rename the columns
    df_list[years_dict[year]].rename(columns=df_rename_dict, inplace=True)

    # drop cols we won't keep keeping around
    renamed_cols = df_list[years_dict[year]].columns.tolist()
    drops = [d for d in renamed_cols if d not in keep_cols]
    print(f"Dropping {len(drops)} columns")
    df_list[years_dict[year]].drop(drops, axis=1, inplace=True)

    # year 2016 is a csv with a different structure, and the age unit codes are different
    # 1:Hour, 2:Day, 3:Month, 4:Year, while everything else is 1:Day, 2:Month, 3:Year
    if year == 2016:
        print("Standardizing age unit codes for 2016 data")
        df_list[years_dict[year]].age_units = df_list[years_dict[year]].age_units - 1

    # final check of columns present
    for i in [
        "year",
        "code_system_id",
        "age_units",
        "age",
        "sex",
        "dx_1",
        "outcome_id",
        "day_adm",
        "mon_adm",
        "year_adm",
        "day_dis",
        "mon_dis",
        "bed_days",
    ]:
        assert i in df_list[years_dict[year]], "{} not here for df {}".format(i, year)

#####################################################
# CONCAT ALL THE DATAFRAMES TOGETHER
#####################################################

df = pd.concat(df_list, ignore_index=True, sort=False)
del df_list

# keep needed columns
df = df[keep_cols].copy()

#####################################################
# KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

# If this assert fails uncomment this line:
# df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), (
    "index is not unique, "
    + "the index has a length of "
    + str(len(df.index.unique()))
    + " while the DataFrame has "
    + str(df.shape[0])
    + " rows"
    + "try this: df = df.reset_index(drop=True)"
)


# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    "nid": "nid",
    "location_id": "location_id",
    "representative_id": "representative_id",
    "year": "year",
    "sex": "sex_id",
    "age": "age",
    "age_units": "age_group_unit",
    "code_system_id": "code_system_id",
    # measure_id variables
    "outcome_id": "outcome_id",
    "facility_id": "facility_id",
    # diagnosis variable
    "dx_1": "dx_1",
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

df["metric_id"] = 1

df["year_start"] = df["year"]
df["year_end"] = df["year"]

df["representative_id"] = 1
df["location_id"] = 122

# group_unit 1 signifies age data is in years
df["source"] = pd.Series(pd.Categorical(["ECU_INEC"] * len(df)))

df["facility_id"] = pd.Series(pd.Categorical(["inpatient unknown"] * len(df)))

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {
    1997: 86997,
    1998: 86998,
    1999: 86999,
    2000: 87000,
    2001: 87001,
    2002: 87002,
    2003: 87003,
    2004: 87004,
    2005: 87005,
    2006: 87006,
    2007: 87007,
    2008: 87008,
    2009: 87009,
    2010: 87010,
    2011: 87011,
    2012: 114876,
    2013: 160484,
    2014: 237756,
    2015: 283865,
    2016: 369564,
    2017: 369579,
    2018: 422318,
}
df = hosp_prep.fill_nid(df, nid_dictionary)
assert (
    df.drop(["date_adm", "date_dis"], axis=1, inplace=False).isnull().sum().sum() == 0
), "We should not have any null values"
#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, replace "Male" with the number 1
#####################################################

# FIX SEXES
fix_sex_dict = {"Mujeres": 2, "Mujer": 2, "Hombres": 1, "Hombre": 1}
df["sex_id"].replace(fix_sex_dict, inplace=True)
df["sex_id"] = pd.to_numeric(df["sex_id"], downcast="integer", errors="raise")

# make unknown sex_id
df.loc[(df["sex_id"] != 1) & (df["sex_id"] != 2), "sex_id"] = 3

assert set(df.sex_id.unique()).issubset({1, 2, 3})

print("FIX AGES")
age_unit_translation_dict = {
    "A\xf1os (1 a 98 a\xf1os de edad)": "Years",
    "Meses (1 a 11 meses de edad)": "Months",
    "D\xedas (1 a 29 d\xedas de edad)": "Days",
    "Ignorado": "Unknown",
    "Anios (1 a 115 anios de edad)": "Years",
    "Dias (1 a 29 dias de edad)": "Days",
    "A\xf1os (1 a 115 a\xf1os de edad)": "Years",
    "D\xedas (1 a 28 d\xedas de edad)": "Days",
    "Horas (1 a 23 horas de edad)": "Hours",
    "0": "Hours",
    "01": "Days",
    "1": "Days",
    "02": "Months",
    "2": "Months",
    "03": "Years",
    "3": "Years",
    "9": "Unknown",
    "09": "Unknown",
}
df["age_group_unit"] = df["age_group_unit"].astype(str)
df["age_group_unit"].replace(age_unit_translation_dict, inplace=True)

# detail down the the level of hours isn't needed
df.loc[df["age_group_unit"] == "Hours", "age"] = 0
df.loc[df["age_group_unit"] == "Hours", "age_group_unit"] = "Days"

# set unknown ages to NaN, so that age_binner will set it to all ages
df.loc[df.age_group_unit == "Unknown", "age"] = np.nan

# get rid of strings
df["age"] = pd.to_numeric(df["age"], downcast="integer", errors="raise")

# check that everything labeled as days or months don't amount to a year
assert df.loc[df["age_group_unit"] == "Days", "age"].max() < 365
assert df.loc[df["age_group_unit"] == "Months", "age"].max() < 12

df["age_in_years"] = np.nan
df.loc[df["age_group_unit"] == "Years", "age_in_years"] = df.loc[
    df["age_group_unit"] == "Years", "age"
]
df.loc[df["age_group_unit"] == "Months", "age_in_years"] = (
    df.loc[df["age_group_unit"] == "Months", "age"] * 30.5
) / 365
df.loc[df["age_group_unit"] == "Days", "age_in_years"] = (
    df.loc[df["age_group_unit"] == "Days", "age"] / 365
)
assert (
    df.loc[df["age_group_unit"] != "Unknown", "age_in_years"].isnull().sum() == 0
), "Not expecting nulls"

df["old_age"] = df["age"].astype(str) + df["age_group_unit"]
df["age"] = df["age_in_years"]
df["age_group_unit"] = 1
df.drop(["age_in_years", "old_age"], axis=1, inplace=True)

print("AGES FIXED")

# FIX OUTCOME ID
outcome_dict = {
    "Alta": "discharge",
    "Vivo": "discharge",
    # py2 doesn't like reading scripts that aren't ascii Fallecido 48H y mas': 'death',
    "Fallecido 48H y m\xc3s": "death",
    "Fallecido 48H y m\xe1s": "death",
    "Fallecido < 48H": "death",
    "Fallecido en menos de 48H": "death",
    "Fallecido en 48 horas y más": "death",
    "Fallecido menos de 48 horas": "death",
    "1": "discharge",
    "01": "discharge",
    "2": "death",
    "02": "death",
    "3": "death",
    "03": "death",
}
df["outcome_id"] = df["outcome_id"].astype(str)
df["outcome_id"].replace(outcome_dict, inplace=True)

assert set(df.outcome_id.unique()) == {"discharge", "death"}

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",
    "sex_id",
    "nid",
    "representative_id",
    "metric_id",
]
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ["source", "facility_id", "outcome_id", "dx_1"]

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

# Turn 'age' into 'age_start' and 'age_end'
df = hosp_prep.age_binning(df, under1_age_detail=True)

####################################################
# CLEAN DATES AND REMOVE BED DAYS OF 0
####################################################

# New data already has date in a convertable format, split the data
print("Begin converting admission and discharge dates")
date_years = hardcoding_dict["single_date_col"]
includes_date = df[df["year_start"].isin(date_years)].copy()
df = df[~df["year_start"].isin(date_years)].copy()

if df.date_adm.notnull().any():
    print("There are some non-null dates bfefore 2012!!")
    print(df[df.date_adm.notnull()].year.unique())
if includes_date.date_adm.isnull().any():
    print("There are some missing dates after 2012!!")
    print(len(df.loc[df.date_adm.isnull()]))

month_replace_dict = {
    "Abril": 4,
    "Agosto": 8,
    "Diciembre": 12,
    "Enero": 1,
    "Febrero": 2,
    "Julio": 7,
    "Junio": 6,
    "Marzo": 3,
    "Mayo": 5,
    "Noviembre": 11,
    "Octubre": 11,
    "Septiembre": 9,
}

# rename months to numbers
df["mon_adm"].replace(month_replace_dict, inplace=True)
df["mon_dis"].replace(month_replace_dict, inplace=True)

# convert to numbers (some where still strings)
df["mon_adm"] = pd.to_numeric(df["mon_adm"], downcast="integer", errors="raise")
df["mon_dis"] = pd.to_numeric(df["mon_dis"], downcast="integer", errors="raise")

# drop nonsense months
pre = len(df)
df = df[df.mon_dis.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]
df = df[df.mon_adm.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]
print(f"{pre-len(df)} rows were dropped due to nonsense months")

# convert days to numbers
df["day_adm"] = pd.to_numeric(df["day_adm"], downcast="integer", errors="raise")
df["day_dis"] = pd.to_numeric(df["day_dis"], downcast="integer", errors="raise")

# drop nonsense days
pre = len(df)
df = df[df.day_dis.isin(list(range(1, 32)))]
df = df[df.day_adm.isin(list(range(1, 32)))]
print(f"{pre-len(df)} rows were dropped due to nonsense days")

# change to strings so it's easy to concat
df[["day_adm", "mon_adm", "year_adm", "day_dis", "mon_dis"]] = df[
    ["day_adm", "mon_adm", "year_adm", "day_dis", "mon_dis"]
].astype(str)

# make dates of admission and discharge
df["date_adm"] = df["year_adm"] + "-" + df["mon_adm"] + "-" + df["day_adm"]
df["date_dis"] = (
    df["year_start"].astype(str) + "-" + df["mon_dis"] + "-" + df["day_dis"]
)

# convert to datetime dtype.  Invalid dates will be set to NaT
df["date_adm"] = pd.to_datetime(df["date_adm"], format="%Y-%m-%d", errors="coerce")
df["date_dis"] = pd.to_datetime(df["date_dis"], format="%Y-%m-%d", errors="coerce")

includes_date["date_adm"] = pd.to_datetime(includes_date["date_adm"], errors="coerce")
includes_date["date_dis"] = pd.to_datetime(includes_date["date_dis"], errors="coerce")

# Bring the data back together
print("Conversion finished, concatting back together")
df = pd.concat([df, includes_date], sort=False, ignore_index=True)

# there are some NaT date_adm, and NaT date_dis that we want to drop
# these are caused by impossible dates e.g. Feb 29 on a non leap year
# with some work and some assumptions we could move these to the nearest valid
pre = len(df)
df = df[df.date_adm.notnull()]
df = df[df.date_dis.notnull()]
print(f"{pre-len(df)} rows were dropped due to missing dates")

# compute bed days
df["days_diff"] = df.date_dis - df.date_adm

# Now we can drop bed days of zero.
# Drop the negative bed days from dates that were improperly recorded.
df = df[df.days_diff >= pd.to_timedelta(0, unit="D")]

# drop bed days of zero that were not deaths!
pre = len(df)
df = df[(df.days_diff > pd.to_timedelta(0, unit="D")) | (df.outcome_id == "death")]
print(f"{pre-len(df)} rows of {pre} total rows were dropped due to 0 day stays")
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
    df["diagnosis_id"] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
df["val"] = 1


#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
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
assert (
    len(df["diagnosis_id"].unique()) <= 2
), "diagnosis_id should have 2 or less feature levels"
assert not set(df["sex_id"].unique()).symmetric_difference(
    [1, 2, 3]
), "There should only be three feature levels to sex_id"
assert (
    len(df["code_system_id"].unique()) <= 2
), "code_system_id should have 2 or less feature levels"
assert len(df["source"].unique()) == 1, "source should only have one feature level"

assert (df.val >= 0).all(), "for some reason there are negative case counts"

# NEW- test the newly prepped data against the last formatted version
# This is manually pulled in, and doesn't break if the test results are an issue, so carefully
# run this portion of the formatting script and review the output for warnings
files = ["FILEPATH"]
compare_df = pd.concat([pd.read_hdf(f) for f in files], sort=False, ignore_index=True)

# can't compare 2018 *new data* to the older results!
test_results = stage_hosp_prep.test_case_counts(
    df.query("year_start != 2018"), compare_df
)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = "\n --- \n".join(test_results)
    assert False, msg
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH"

hosp_prep.write_hosp_file(df, write_path, backup=True)
