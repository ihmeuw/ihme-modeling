
"""


NOTE: there are nearly DOUBLE the amount of females in this data compared to
Males.

Ecuador's National Institute of Statistics and Censuses (INEC) collects
inpatient discharge information from all health facilities operating in the
country, both public and private. Hospitals report discharge information on a
monthly basis. Data on healthy newborns are not collected as part of the
discharge statistics. INEC also collects information on the number,
department, and use of hospital beds. Data are released on an annual basis,
and can be downloaded either in the form of patient-level data files
(discharges and beds are provided separately), or as a hospital statistics
yearbook containing tabulated data. This is an ad hoc series name rather
than a system name.
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass

if getpass.getuser() == 'USERNAME':
    USERNAME_path = r"FILEPATH/Functions"
    sys.path.append(USERNAME_path)
if getpass.getuser() == 'USERNAME':
    USERNAME_path = r"FILEPATH/Functions"
    sys.path.append(USERNAME_path)
import hosp_prep

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

##############################################
# READ IN DATA
##############################################

# many of the years of data were extracted from some weird file format and
# put into the following dir:
base_dir = "FILEPATH/"
file_name = "ECUADOR_INEC_HOSPITAL_DISCHARGE_"
years = range(1997, 2015)
years_dict = dict(zip(years, range(len(years))))

# the rest of the data is in these dirs.  This is the original loc for all data
new_year_files = [
    "FILEPATH/ECU_HOSPITAL_DISCHARGES_2012_Y2013M10D22.DTA",
    "FILEPATH/ECU_HOSPITAL_DISCHARGES_2013_Y2014M12D22.DTA",
    "FILEPATH/ECU_HOSPITAL_DISCHARGES_2014_Y2016M02D02.DTA"]

# The names and spellings of the variables change inbetween years.  below is
# reading in the data with the appropriate names for each year
df_list = []
for year in range(1997, 2012):
    myfile = "{}{}{}.dta".format(base_dir, file_name, year)
    try:
        df = pd.read_stata(myfile)
        df['year'] = year
        df_list.append(df)
    except:
        print "could not read the file {}".format(myfile)

for f in new_year_files:
#    print f
    df = pd.read_stata(f)
    df_list.append(df)

for i in range(len(df_list)):
    df_list[i].columns = [x.lower() for x in df_list[i].columns]

for year in [1997, 1998]:
    df_list[years_dict[year]].rename(columns={"con_edadpa": "age_units",
                                               "eda_pacien": "age",
                                               "sex_pacien": "sex",
                                               "cau_9narev": "dx_1",
                                               "con_egrpac": "outcome_id",
                                               "dia_ingpac": "day_adm",
                                               "mes_ingpac": "mon_adm",
                                               "ani_ingpac": "year_adm",
                                               "dia_egrpac": "day_dis",
                                               "mes_egrpac": "mon_dis",
                                               "dia_estada": "bed_days"}, inplace=True)
    assert {"age_units", "age", "sex", "dx_1", "outcome_id", "day_adm",
            "mon_adm", "year_adm", "day_dis", "mon_dis", "bed_days"}.\
            issubset(df_list[years_dict[year]])
    df_list[years_dict[year]]['code_system_id'] = 1

for year in range(1999, 2002):
    df_list[years_dict[year]].rename(columns={"con_edadpa": "age_units",
                                               "eda_pacien": "age",
                                               "sex_pacien": "sex",
                                               "cau_10arev": "dx_1",
                                               "con_egrpac": "outcome_id",
                                               "dia_ingpac": "day_adm",
                                               "mes_ingpac": "mon_adm",
                                               "ani_ingpac": "year_adm",
                                               "dia_egrpac": "day_dis",
                                               "mes_egrpac": "mon_dis",
                                               "dia_estada": "bed_days"}, inplace=True)
    assert {"age_units", "age", "sex", "dx_1", "outcome_id", "day_adm",
            "mon_adm", "year_adm", "day_dis", "mon_dis", "bed_days"}\
            .issubset(df_list[years_dict[year]])
    df_list[years_dict[year]]['code_system_id'] = 2

for year in range(2002, 2011):
    df_list[years_dict[year]].rename(columns={"conedadp": "age_units",
                                              "edapacie": "age",
                                              "sexpacie": "sex",
                                              "cau10are": "dx_1",
                                              "conegrpa": "outcome_id",
                                              "diaingpa":"day_adm",
                                              "mesingpa":"mon_adm",
                                              "aniingpa":"year_adm",
                                              "diaegrpa":"day_dis",
                                              "mesegrpa":"mon_dis",
                                              "diaestad":"bed_days"}, inplace=True)
    assert {"age_units", "age", "sex", "dx_1", "outcome_id", "day_adm",
            "mon_adm", "year_adm", "day_dis", "mon_dis", "bed_days"}\
            .issubset(df_list[years_dict[year]])
    df_list[years_dict[year]]['code_system_id'] = 2

for year in [2011, 2012]:
    df_list[years_dict[year]].rename(columns={"cond_edad": "age_units",
                                              "edad_pac": "age",
                                              "sexo_pac": "sex",
                                              "cau_cie10": "dx_1",
                                              "con_egrpa": "outcome_id",
                                              "dia_ingr":"day_adm",
                                              "mes_ingr":"mon_adm",
                                              "anio_ingr":"year_adm",
                                              "dia_egr":"day_dis",
                                              "mes_egr":"mon_dis",
                                              "dia_estad":"bed_days"}, inplace=True)
    for i in ["age_units", "age", "sex", "dx_1", "outcome_id", "day_adm",
              "mon_adm", "year_adm", "day_dis", "mon_dis" ,"bed_days"]:
        assert i in df_list[years_dict[year]], "{} not here for year {}".\
            format(i, year)
    df_list[years_dict[year]]['code_system_id'] = 2
    df_list[years_dict[year]]['year'] = year

for year in [2013, 2014]:
    df_list[years_dict[year]].rename(columns={"cod_edad": "age_units",
                                              "edad": "age",
                                              "sexo": "sex",
                                              "cau_cie10": "dx_1",
                                              "con_egrpa": "outcome_id",
                                              "dia_ingr":"day_adm",
                                              "mes_ingr":"mon_adm",
                                              "anio_ingr":"year_adm",
                                              "dia_egr":"day_dis",
                                              "mes_egr":"mon_dis",
                                              "dia_estad":"bed_days"}, inplace=True)
    for i in ["age_units", "age", "sex", "dx_1", "outcome_id", "day_adm",
              "mon_adm", "year_adm", "day_dis", "mon_dis", "bed_days"]:
        assert i in df_list[years_dict[year]], "{} not here".format(i)
    df_list[years_dict[year]]['code_system_id'] = 2
    df_list[years_dict[year]]['year'] = year

for j in range(len(df_list)):
    for i in ["year", "code_system_id", "age_units", "age", "sex", "dx_1",
              "outcome_id", "day_adm", "mon_adm", "year_adm", "day_dis",
              "mon_dis", "bed_days"]:
        assert i in df_list[j], "{} not here for df {}".format(i, j)


#####################################################
# CONCAT ALL THE DATAFRAMES TOGETHER
#####################################################

df = pd.concat(df_list, ignore_index=True)

# keep needed columns
df = df[["year", "code_system_id", "age_units", "age", "sex", "dx_1",
         "outcome_id" ,"day_adm", "mon_adm", "year_adm", "day_dis", "mon_dis",
         "bed_days"]].copy()


#####################################################
# KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

# If this assert fails uncomment this line:
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")


# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',

    'year': 'year',
#     'year_start': 'year_start',
#     'year_end': 'year_end',
    'sex': 'sex_id',
    'age': 'age',
#     'age_start': 'age_start',
#     'age_end': 'age_end',
    'age_units': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'dx_1': 'dx_1'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################

# These are completely dependent on data source
df['metric_id'] = 1

df['year_start'] = df['year']
df['year_end'] = df['year']

df['representative_id'] = 1
df['location_id'] = 122

# group_unit 1 signifies age data is in years
df['source'] = 'ECU_INEC_97_14'

df['facility_id'] = 'inpatient unknown'

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {1997: 86997,
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
                  2014: 237756}
df = hosp_prep.fill_nid(df, nid_dictionary)

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################

# FIX SEXES
fix_sex_dict = {'Mujeres': 2,
                'Mujer': 2,
                'Hombres': 1,
                'Hombre': 1}
df['sex_id'].replace(fix_sex_dict, inplace=True)
df['sex_id'] = pd.to_numeric(df['sex_id'], downcast='integer', errors='raise')

# make unknown sex_id
df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

assert set(df.sex_id.unique()).issubset({1, 2, 3})

# FIX AGES
age_unit_translation_dict = {
    "A\xf1os (1 a 98 a\xf1os de edad)": "Years",
    "Meses (1 a 11 meses de edad)": "Months",
    'D\xedas (1 a 29 d\xedas de edad)': 'Days',
    'Ignorado': 'Unknown',
    'Anios (1 a 115 anios de edad)': "Years",
    'Dias (1 a 29 dias de edad)': 'Days',
    'A\xf1os (1 a 115 a\xf1os de edad)': "Years",
    'D\xedas (1 a 28 d\xedas de edad)': 'Days',
    '01': 'Days',
    '1': 'Days',
    '02': 'Months',
    '2': 'Months',
    '03': 'Years',
    '3': 'Years',
    '9': 'Unknown',
    '09': 'Unknown'}
df['age_group_unit'].replace(age_unit_translation_dict, inplace=True)

# set unknown ages to NaN, so that age_binner will set it to all ages
df.loc[df.age_group_unit == 'Unknown', 'age'] = np.nan

# get rid of strings
df['age'] = pd.to_numeric(df['age'], downcast='integer', errors='raise')

# check that everything labeled as days or months don't amount to a year
assert df.loc[df['age_group_unit'] == "Days", 'age'].max() < 365
assert df.loc[df['age_group_unit'] == "Months", 'age'].max() < 12

# Now we know that we can set units to years and set age to zero
# convert months
df.loc[df['age_group_unit'] == "Days", 'age'] = 0
df.loc[df['age_group_unit'] == "Days", 'age_group_unit'] = "Years"

# convert months to years
df.loc[df['age_group_unit'] == "Months", 'age'] = 0
df.loc[df['age_group_unit'] == "Months", 'age_group_unit'] = "Years"

assert set(df.age_group_unit.unique()) ==\
    {"Years", "Unknown"}, "All units should be year or unknown"

# now everything is in years or unknown.  We will set the unknown units to
# all ages. So we can change 'age_group_unit' to say that this is all years
df['age_group_unit'] = 1

# FIX OUTCOME ID
outcome_dict = {
    'Alta': 'discharge',
    'Fallecido 48H y más': 'death',
    'Fallecido 48H y m\xe1s': 'death',
    'Fallecido < 48H': 'death',
    'Fallecido en menos de 48H': 'death',
    '1': 'discharge', '01': 'discharge',
    '2': 'death', '02': 'death',
    '3': 'death', '03': 'death'
}

df['outcome_id'].replace(outcome_dict, inplace=True)

assert set(df.outcome_id.unique()) == {'discharge', 'death'}

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
             'sex_id', 'nid', 'representative_id', 'metric_id']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
str_cols = ['source', 'facility_id', 'outcome_id']

if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

df = hosp_prep.age_binning(df)

####################################################
# CLEAN DATES AND REMOVE BED DAYS OF 0
####################################################

month_replace_dict = {
    'Abril': 4,
    'Agosto': 8,
    'Diciembre': 12,
    'Enero': 1,
    'Febrero': 2,
    'Julio': 7,
    'Junio': 6,
    'Marzo': 3,
    'Mayo': 5,
    'Noviembre': 11,
    'Octubre': 11,
    'Septiembre': 9
}

# rename months to numbers
df['mon_adm'].replace(month_replace_dict, inplace=True)
df['mon_dis'].replace(month_replace_dict, inplace=True)

# convert to numbers (some where still strings)
df['mon_adm'] = pd.to_numeric(df['mon_adm'], downcast='integer', errors='raise')
df['mon_dis'] = pd.to_numeric(df['mon_dis'], downcast='integer', errors='raise')

# drop nonsense months
df = df[df.mon_dis.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]
df = df[df.mon_adm.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]

# convert days to numbers
df['day_adm'] = pd.to_numeric(df['day_adm'], downcast='integer', errors='raise')
df['day_dis'] = pd.to_numeric(df['day_dis'], downcast='integer', errors='raise')

# drop nonsense days
df = df[df.day_dis.isin(range(1, 32))]
df = df[df.day_adm.isin(range(1, 32))]

# change to strings so it's easy to concat
df[['day_adm', 'mon_adm', 'year_adm', 'day_dis', 'mon_dis']] =\
    df[['day_adm', 'mon_adm', 'year_adm', 'day_dis', 'mon_dis']].astype(str)

# make dates of admission and discharge
df['date_adm'] = df['year_adm'] + "-" + df['mon_adm'] + "-" + df['day_adm']
df['date_dis'] = df['year_start'].astype(str) + "-" + df['mon_dis'] + "-" + df['day_dis']

# convert to datetime dtype.  Invalid dates will be set to NaT
df['date_adm'] = pd.to_datetime(df['date_adm'], format="%Y-%m-%d", errors='coerce')
df['date_dis'] = pd.to_datetime(df['date_dis'], format="%Y-%m-%d", errors='coerce')

df = df[df.date_adm.notnull()]
df = df[df.date_dis.notnull()]

# compute bed days
df['days_diff'] = df.date_dis - df.date_adm

df = df[df.days_diff >= pd.to_timedelta(0, unit="D")]

# drop bed days of zero that were not deaths!
df = df[(df.days_diff > pd.to_timedelta(0, unit="D"))|
        (df.outcome_id == "death")]

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
df['val'] = 1


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
group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()


#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
columns_before = df_agg.columns
hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source', 'nid',
                   'facility_id',
                   'code_system_id', 'cause_code']
df_agg = df_agg[hosp_frmat_feat]
columns_after = df_agg.columns

# check if all columns are there
assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
    "number of feature levels of years and nid should match number"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

assert (df.val >= 0).all(), ("for some reason there are negative case counts")


#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/formatted_ECU_INEC_97_14.H5"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
