
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

This script formats the data at FILEPATH
covers years 2001-2012.


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


"""
import pandas as pd
import platform
import numpy as np
import sys
import glob



hosp_path = r"FILEPATH"
clust_path = r"FILENAME"
sys.path.append(hosp_path)
sys.path.append(clust_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"






















filepath_list = glob.glob(root + r"FILEPATH")
filepath_list = sorted(filepath_list)
filepath_list = [i.replace("\\","FILENAME") for i in filepath_list]  

del filepath_list[4]  


df_list = []  
years = np.arange(2001, 2013, 1)
years = np.delete(years, 4)  

year_path_dict = dict(list(zip(years, filepath_list)))  



for year in years:
    df = pd.read_stata(year_path_dict[year],
                       columns=["edad", "sexo", "DIAG1", "DIAG2",
                                "COND_EGR", "estab"])
    df['year_start'] = year
    df['year_end'] = year
    df_list.append(df)


df = pd.concat(df_list)


df_2005 = pd.read_stata(root + r"FILENAME"
                        r"FILEPATH",
                        columns=["edad", "sexo", "DIAG1", "CAUSA_EXT",
                                 "COND_EGR", "estab"])
df_2005['year_start'] = 2005
df_2005['year_end'] = 2005
df_2005.rename(columns={"CAUSA_EXT": "DIAG2"}, inplace=True)


df = pd.concat([df, df_2005])
df = df.reset_index(drop=True)  
del df_2005  

total_val = df.shape[0]

assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows")







hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',

    'year_start': 'year_start',
    'year_end': 'year_end',
    'sexo': 'sex_id',
    'edad': 'age',
    'age_start': 'age_start',  
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'COND_EGR': 'outcome_id',
    'estab': 'facility_id',
    
    'DIAG1': 'dx_1',
    'DIAG2': 'dx_2',

    }


df.rename(columns=hosp_wide_feat, inplace=True)


df.columns = list(map(str.lower, df.columns))



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

assert df.shape[0] == total_val, "some cases were lost"











df['representative_id'] = 3  
df['location_id'] = 98


df['age_group_unit'] = 1  
df['source'] = 'CHL_MOH'  


df['code_system_id'] = 2  


df['metric_id'] = 1


nid_dictionary = {2001: 121444,
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
                  2012: 193857}
df = fill_nid(df, nid_dictionary)  



df['dx_1'] = df['dx_1'].str.upper()
df['dx_2'] = df['dx_2'].str.upper()




nature_condition = (df['dx_1'].str.startswith("S") |
                    (df['dx_1'].str.startswith("T")))

cause_condition = ((df['dx_2'].str.startswith("V")) |
                   (df['dx_2'].str.startswith("W")) |
                   (df['dx_2'].str.startswith("X")) |
                   (df['dx_2'].str.startswith("Y")))

















df.loc[nature_condition & cause_condition, ['dx_1', 'dx_2']] =\
    df.loc[nature_condition & cause_condition, ['dx_2', 'dx_1']].values






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']



for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')  


assert df.shape[0] == total_val, "some cases were lost"




df.loc[df['age'] > 95, 'age'] = 95  

df = age_binning(df)
df.drop('age', axis=1, inplace=True)  











df['outcome_id'] = df['outcome_id'].fillna("unknown")


df['outcome_id'].replace(9, "unknown", inplace=True)
df['outcome_id'].replace(0, "unknown", inplace=True)


df['outcome_id'].replace(1, "discharge", inplace=True)
df['outcome_id'].replace("1", "discharge", inplace=True)

df['outcome_id'].replace(2, "death", inplace=True)
df['outcome_id'].replace("2", "death", inplace=True)







facility_id_map = pd.read_excel(root + r"FILENAME"
                                r"facility_id_map_CHL_MOH.xls",
                                sheetname="Hoja1", header=2)
facility_id_map.dropna(inplace=True)
facility_id_map = facility_id_map[['Nombre Tipo de Establecimiento',
                                   'Codigo Establecimiento']].drop_duplicates()
facility_id_map['Nombre Tipo de Establecimiento'] =\
    facility_id_map['Nombre Tipo de Establecimiento'].str.strip()














facility_id_dict = {'Establecimiento Mayor Complejidad': "hospital",
                    'Clínica': "day clinic",
                    'Hospital': "hospital",
                    'Centro de Salud': "day clinic",
                    'Establecimiento Mediana Complejidad': "hospital",
                    'Establecimiento Menor Complejidad': "hospital",
                    'Clínica u Hospital de Mutualidad': "hospital",
                    'Centro de Referencia de Salud': "day clinic",
                    'Hospital Delegado': "hospital",
                    'Hospital Militar de Campaña': "hospital",
                    'Hospital de Campaña': "hospital",
                    'Centro de Salud Familiar': "hospital",
                    'Clínica u Hospital Penitenciario': "hospital"}
facility_id_map['Nombre Tipo de Establecimiento'].replace(facility_id_dict,
                                                          inplace=True)


df.rename(columns={'facility_id': 'facility_code'}, inplace=True)
facility_id_map.rename(columns={"Codigo Establecimiento": "facility_code"},
                       inplace=True)


df = df.merge(facility_id_map, how="left", on="facility_code")

df['Nombre Tipo de Establecimiento'] = df['Nombre Tipo de Establecimiento'].\
    fillna("inpatient unknown")

df.rename(columns={"Nombre Tipo de Establecimiento": "facility_id"},
                   inplace=True)

df.drop("facility_code", axis=1, inplace=True)


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


df.to_stata(r"FILEPATH")



    



if len(diagnosis_feats) > 1:
    
    
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")
df.drop("patient_index", axis=1, inplace=True)

df['val'] = 1
assert df[df.diagnosis_id==1].val.sum() == total_val, "some cases were lost"






df['facility_id'] = df['facility_id'].replace("hospital", "inpatient unknown")


print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.")
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

assert df_agg[df_agg.diagnosis_id == 1].val.sum() == total_val, "some cases were lost"

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
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"







write_path = root + r"FILENAME" +\
    r"FILEPATH"
write_hosp_file(df_agg, write_path, backup=True)
