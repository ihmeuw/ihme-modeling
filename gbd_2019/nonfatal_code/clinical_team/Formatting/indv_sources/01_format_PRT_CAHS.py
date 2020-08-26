
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Formatting Portugal

Data is located in FILEPATH

GHDx entry: URL

There are 7 files, and they seem to be an sample of a relational database
There are patient ID numbers, which persumably act as keys.  For example, the
file "Episodios" has sex, age, outcome, ect ..., but doesn't have any diagnoses
those are in different files.  In fact the file
Variaveis_BD_MH_2015_Ficheiros_v2.xlsx acts as a code book and talks about
variables that are common and unique to all the files.



file Episodios Codebook in English:

- [var name, our name for it, definition]
- ano (year): year
- sexo (sex_id): sex
- idade (age): Age of the user, in years, at the date of entry
- dsp (outcome_id): Code of destination of the user after the discharge of a
hospital
service:
    0 - Unknown
    1 - For the household
    2 - To another institution with hospitalization
    6 - Home service
    7 - Exit against medical opinion
    13 - Specialized aftercare (tertiary) (collected as of 2011)
    20 - Deceased
    51 - Palliative care - medical center (collected as of 2011)
    61 - Post-hospital care (CMS 19-22, AP21) (collected as of 2011)
    63 - Long-term hospital care (CMS 19-22, AP 21) (collected as of 2011)
- adm_tip (facility_id): Nature or mode of admission of a patient to a
health facility:
    1 - Scheduled
    2 - Urgent
    3 - Access
    4 - Peclec
    5 - Private Medicine
    6 - SIGIC
    7 - PACO
    12 - External SIGIC (since 2012)
    It can be grouped as Programmed (codes 1,3,4,6 and 12), Urgent (code 2),
    Private Medicine (code 5) and Access Plan for Ophthalmic Surgery (code 7)



CODEBOOK for file "diagnoses"

-cod_diagnostico: ICD-9-CM Code ("International Classification of Diseases,
9th Revision, Clinical
- tipo_p_s:
    P- Main Diagnosis: defined as one who, after the study of the patient, is
    considered responsible for admission of the patient to the hospital for
    treatment.
    S- Additional diagnosis is any diagnosis attributed to a patient, in a
    particular episode of care, in addition to the main diagnosis.
- ordem: Coding order where:
    0 - main diagnosis
    Other orders (1,2,3 ...) - additional diagnostics
- pna: It identifies if the diagnosis was present when the patient was
admitted, being:
    S-Yes
    N - No
    D - Unknown
    I - Undetermined clinically
    NA - Exempt PNA registration



CODEBOOK for the file "external causes"

- cod_diagnostico: ICD-9-CM Code ("International Classification of Diseases,
9th Revision, Clinical Modification"), which identifies the Diagnosis.
- cod_causa_ext: ICD-9-CM (International Classification of Diseases, 9th
Revision, Clinical Modification) Code of External Cause that led the patient
to the health institution. External cause codes allow you to codify the
circumstances in which a certain injury or intoxication occurred. See Section
3 for designations.
- tipo_p_s:
    P- Main Diagnosis: defined as one who, after the study of the patient, is
    considered responsible for admission of the patient to the hospital for
    treatment.
    S- Additional diagnosis is any diagnosis attributed to a patient, in a
    particular episode of care, in addition to the main diagnosis.
- ordem: Coding order where:
    0 - main diagnosis
    Other orders (1,2,3 ...) - additional diagnostics
- ordem_assoc: Order of association of the external cause to the diagnosis,
where:
    1 - First external cause associated to the diagnosis in reference;
    2 - Second external cause associated to the diagnosis in reference;
    3 - Third external cause associated to the diagnosis in reference;
    4 - ....
- pna: It identifies if the diagnosis was present when the patient was
admitted, being:
    S-Yes
    N - No
    D - Unknown
    I - Undetermined clinically
    NA - Exempt PNA registration

NOTES
- "ordem" variable appears to be zero indexed.
- "ordem_assoc" in external causes tells you which diagnoses the external cause
corresponds to

"""
import pandas as pd
import platform
import numpy as np
import sys















USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
clust_path = r"FILENAME"
sys.path.append(clust_path)
sys.path.append(USER_path)
sys.path.append(USER_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"







df_episodes = pd.read_excel(root + r"FILENAME"
                            r"PRT_HOSP_INPATIENT_DISCHARGES_2015"
                            r"FILEPATH")
df_episodes = df_episodes[['ano', 'seq_number', 'sexo', 'idade', 'dsp']]


df_diagnoses = pd.read_excel(root + r"FILENAME"
                                    r"PRT_HOSP_INPATIENT_DISCHARGES_2015_"
                                    r"FILEPATH")
df_diagnoses = df_diagnoses[['seq_episodio_int', 'cod_diagnostico',
                             'tipo_p_s', 'ordem']]



df_diagnoses = df_diagnoses.pivot(index='seq_episodio_int', columns='ordem',
                                  values='cod_diagnostico').reset_index()


raw_names = df_diagnoses.filter(regex="^[\d]").columns
col_names = ['seq_episodio_int']
for col in raw_names:
    col_names.append("dx_" + str(col+ 1))
df_diagnoses.columns = col_names


df_external_causes = pd.read_excel(root + r"FILENAME"
                                          r"PRT_HOSP_INPATIENT_DISCHARGES_2015_CAUSAS"
                                          r"FILEPATH")
df_external_causes = df_external_causes[['seq_episodio_int',
                                         'cod_diagnostico', 'cod_causa_ext',
                                         'tipo_p_s', 'ordem', 'ordem_assoc']]

df_external_causes['col_name'] = 'broke'
for seq in df_external_causes.seq_episodio_int.unique():
    seq_len = df_external_causes[df_external_causes.seq_episodio_int == seq].shape[0]
    ints = np.arange(1, seq_len+1, 1)
    to_name = []
    for int in ints:
        to_name.append("ecode_" + str(int))
    df_external_causes.loc[df_external_causes.seq_episodio_int == seq,
                           'col_name'] = to_name
assert 'broke' not in df_external_causes.col_name.unique(), \
    "The code above broke"
df_external_causes = df_external_causes.pivot(index='seq_episodio_int',
                                              columns='col_name',
                                              values='cod_causa_ext').\
                                              reset_index()


raw_names = df_external_causes.filter(regex="^[\d]").columns
col_names = ['seq_episodio_int']




len(df_episodes.seq_number.unique())  
len(df_diagnoses.seq_episodio_int.unique())  
len(df_external_causes.seq_episodio_int.unique())  



assert set.difference(set(df_episodes.seq_number.unique()),
                      set(df_diagnoses.seq_episodio_int.unique())) == set(),\
    "ids don't match between sets"




assert set(df_external_causes.seq_episodio_int.unique()) -\
    set(df_episodes.seq_number.unique()) == set(), "result should be empty set"

























pre_eps = df_diagnoses.seq_episodio_int.unique().size
merged_ex_dx = df_diagnoses.merge(df_external_causes, how='left',
                                  on='seq_episodio_int',
                                  
                                  suffixes=['_diag', '_inj'])
assert pre_eps == merged_ex_dx.seq_episodio_int.unique().size, "cases lost"
merged_ex_dx.rename(columns={'seq_episodio_int': 'seq_number'}, inplace=True)
assert (merged_ex_dx.seq_number.value_counts(dropna=False) ==
        df_diagnoses.seq_episodio_int.value_counts(dropna=False)).all(), \
    "Cases were lost"
df = df_episodes.merge(merged_ex_dx, how='left', on='seq_number')
assert (merged_ex_dx.seq_number.value_counts(dropna=False) ==
        df.seq_number.value_counts(dropna=False)).all(), \
    "Cases were lost"



assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows")


df.drop('seq_number', axis=1, inplace=True)  




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'ano': 'year',


    'sexo': 'sex_id',
    'idade': 'age',


    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'dsp': 'outcome_id',
    'adm_tip': 'facility_id',
    
    




    
    
    }


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)








df['representative_id'] = 3  
df['location_id'] = 91  


df['age_group_unit'] = 1
df['source'] = 'PRT_CAHS'  


df['code_system_id'] = 1  


df['metric_id'] = 1




df['nid'] = 285520

df['facility_id'] = 'inpatient unknown'










df = df[df['age'] < 125]  
df.loc[df['age'] > 95, 'age'] = 95  

df = age_binning(df)
df.drop('age', axis=1, inplace=True)  































outcome_dict = {
    0: 'unknown',
    1: 'discharge',
    2: 'discharge',
    6: 'discharge',
    7: 'discharge',
    13: 'discharge',
    20: 'death',
    51: 'discharge',
    61: 'discharge',
    63: 'discharge'}
df['outcome_id'].replace(outcome_dict, inplace=True)







df['dx_40'] = np.nan
df.loc[df.ecode_1.notnull(), 'dx_40'] = df.loc[df.ecode_1.notnull(), 'dx_1']
df.loc[df.ecode_1.notnull(), 'dx_1'] = df.loc[df.ecode_1.notnull(), 'ecode_1']




int_cols = ['year', 'sex_id', 'representative_id',
            'code_system_id', 'age_group_unit', 'age_group_unit', 'nid',
            'metric_id', 'age_start', 'age_end']







for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')

































diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])






df.drop('ecode_1', axis=1, inplace=True)
df.to_stata(r"FILEPATH")




    



e_cols = df.filter(regex="^ecode").columns
df.drop(e_cols, axis=1, inplace=True)
if len(diagnosis_feats) > 1:
    
    
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1






df.drop("patient_index", axis=1, inplace=True)  






print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")


df['year_start'] = df['year']
df['year_end'] = df['year']
df.drop('year', axis=1, inplace=True)


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


df_agg.cause_code = df_agg.cause_code.astype(str)


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






write_path = root + r"FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')

