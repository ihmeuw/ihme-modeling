
"""
Created on 2017/08/03
@author: USER

containing folder: FILEPATH

GHDx link: URL

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

user = getpass.getuser()
prep_path = "FILEPATH".format(user)
sys.path.append(prep_path)
import hosp_prep
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"







base_dir = "FILEPATH"
file_name = "ECUADOR_INEC_HOSPITAL_DISCHARGE_"
years = list(range(1997, 2015))
years_dict = dict(list(zip(years, list(range(len(years))))))


new_year_files = [
    "FILEPATH",
    "FILEPATH",
    "FILEPATH"]



df_list = []
for year in range(1997, 2012):
    myfile = "FILEPATH".format(base_dir, file_name, year)
    try:
        df = pd.read_stata(myfile)
        df['year'] = year
        df_list.append(df)
    except:
        print("could not read the file {}".format(myfile))

for f in new_year_files:

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






df = pd.concat(df_list, ignore_index=True, sort=False)


df = df[["year", "code_system_id", "age_units", "age", "sex", "dx_1",
         "outcome_id" ,"day_adm", "mon_adm", "year_adm", "day_dis", "mon_dis",
         "bed_days"]].copy()









assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")





hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',

    'year': 'year',


    'sex': 'sex_id',
    'age': 'age',


    'age_units': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'dx_1': 'dx_1'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['metric_id'] = 1

df['year_start'] = df['year']
df['year_end'] = df['year']

df['representative_id'] = 1
df['location_id'] = 122


df['source'] = 'ECU_INEC_97_14'

df['facility_id'] = 'inpatient unknown'


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










fix_sex_dict = {'Mujeres': 2,
                'Mujer': 2,
                'Hombres': 1,
                'Hombre': 1}
df['sex_id'].replace(fix_sex_dict, inplace=True)
df['sex_id'] = pd.to_numeric(df['sex_id'], downcast='integer', errors='raise')


df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

assert set(df.sex_id.unique()).issubset({1, 2, 3})


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


df.loc[df.age_group_unit == 'Unknown', 'age'] = np.nan


df['age'] = pd.to_numeric(df['age'], downcast='integer', errors='raise')


assert df.loc[df['age_group_unit'] == "Days", 'age'].max() < 365
assert df.loc[df['age_group_unit'] == "Months", 'age'].max() < 12

unit_dict = {'days': 2, 'months': 3, 'years': 1}
df['age_group_name'] = df['age_group_unit']
df['age_group_unit'] = 1
df.loc[df['age_group_name'] == 'Days', 'age_group_unit'] = 2
df.loc[df['age_group_name'] == 'Months', 'age_group_unit'] = 3
assert not set(unit_dict.values()).\
    symmetric_difference(set(df['age_group_unit'].unique())),\
    "There are other ages groups that need review"
df.drop('age_group_name', axis=1, inplace=True)

df = stage_hosp_prep.convert_age_units(df, unit_dict)
















outcome_dict = {
    'Alta': 'discharge',
    
    'Fallecido 48H y m\xc3s': 'death',
    'Fallecido 48H y m\xe1s': 'death',
    'Fallecido < 48H': 'death',
    'Fallecido en menos de 48H': 'death',
    '1': 'discharge', '01': 'discharge',
    '2': 'death', '02': 'death',
    '3': 'death', '03': 'death'
}

df['outcome_id'].replace(outcome_dict, inplace=True)

assert set(df.outcome_id.unique()) == {'discharge', 'death'}

















int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
             'sex_id', 'nid', 'representative_id', 'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)





df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)


















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


df['mon_adm'].replace(month_replace_dict, inplace=True)
df['mon_dis'].replace(month_replace_dict, inplace=True)


df['mon_adm'] = pd.to_numeric(df['mon_adm'], downcast='integer', errors='raise')
df['mon_dis'] = pd.to_numeric(df['mon_dis'], downcast='integer', errors='raise')


df = df[df.mon_dis.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]
df = df[df.mon_adm.isin([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])]


df['day_adm'] = pd.to_numeric(df['day_adm'], downcast='integer', errors='raise')
df['day_dis'] = pd.to_numeric(df['day_dis'], downcast='integer', errors='raise')


df = df[df.day_dis.isin(list(range(1, 32)))]
df = df[df.day_adm.isin(list(range(1, 32)))]


df[['day_adm', 'mon_adm', 'year_adm', 'day_dis', 'mon_dis']] =\
    df[['day_adm', 'mon_adm', 'year_adm', 'day_dis', 'mon_dis']].astype(str)


df['date_adm'] = df['year_adm'] + "-" + df['mon_adm'] + "-" + df['day_adm']
df['date_dis'] = df['year_start'].astype(str) + "-" + df['mon_dis'] + "-" + df['day_dis']


df['date_adm'] = pd.to_datetime(df['date_adm'], format="%Y-%m-%d", errors='coerce')
df['date_dis'] = pd.to_datetime(df['date_dis'], format="%Y-%m-%d", errors='coerce')





df = df[df.date_adm.notnull()]
df = df[df.date_dis.notnull()]


df['days_diff'] = df.date_dis - df.date_adm



df = df[df.days_diff >= pd.to_timedelta(0, unit="D")]


df = df[(df.days_diff > pd.to_timedelta(0, unit="D"))|
        (df.outcome_id == "death")]



    


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])


if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1







print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")


group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()







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


assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])


for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    
    
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)


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





compare_df = pd.read_hdf("FILEPATH")
test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg





write_path = root + r"FILEPATH"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
