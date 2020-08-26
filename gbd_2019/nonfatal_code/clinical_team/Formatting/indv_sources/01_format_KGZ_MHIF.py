"""

Authors: USER and USER
"""
import pandas as pd
import numpy as np
import os.path
import platform
import sys
import warnings
import getpass

user = getpass.getuser()


hosp_path = r"FILEPATH".format(user)
sys.path.append(hosp_path)
import hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    warnings.warn("must run on the cluster")
    exit
    


df = pd.read_table(root + r"FILEPATH", sep = ",")


keep = ['Year','Sex','Age','Main_Diagnosis','Related_Diagnosis_1',
        'Related_Diagnosis_2','Treatment_Outcome', 'Hospitalization_Type']


df = df[keep]







hosp_wide_feat = {
    'nid' : 'nid',
    'location_id' : 'location_id',
    'representative_id' : 'representative_id',
    'Year' : 'year',
    'year_start' : 'year_start',
    'year_end' : 'year_end',
    'Sex' : 'sex_id',
    'Age': 'age',
    'age_start' : 'age_start',
    'age_end' : 'age_end',
    'age_group_unit' : 'age_group_unit',
    'Hospitalization_Type' : 'facility_id',
    'code_system_id' : 'code_system_id',
    'Treatment_Outcome': 'outcome_id',
    'Main_Diagnosis' : 'dx_1',
    'Related_Diagnosis_1' : 'dx_2',
    'Related_Diagnosis_2' : 'dx_3',
    'dx_4' : 'dx_4',
    'dx_5' : 'dx_5',
    'dx_6' : 'dx_6'}


df.rename(columns= hosp_wide_feat, inplace = True)


new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values()) - set(df.columns)))
df = df.join(new_col_df)




df['nid'] = 96714
df['representative_id'] = 3  
df['location_id'] = 37
df['age_group_unit'] = 1 
df['source'] = 'KGZ_MHIF'

def fill_nid(df, nid_dict):
    """
    Function that assigns the NID to each row based on the year.
    Accepts a dictionary that contains years as keys and nids as values, and a DataFrame.
    Returns DataFrame with a filled "nid" column. If the DataFrame didn't already have a "nid" column it makes one.
    Assumes that the DataFrame has a column named "year"
    """
    assert 'year' in df.columns, "DataFrame doesn't have a 'year' column"
    df['nid'] = df['year'].map(nid_dict)
    return df







































def year_range(df):
    """
    Accepts a pandas DataFrame. Returns the lowest and highest values. Assumes that the DataFrame contains a column named 'years'
    Assumes that there is a column that contains all the years.
    Meant to be used to form year_start and year_end columns in a DataFrame.
    """

    if not isinstance(df, pd.DataFrame):
        print("year_range was not passed a pandas DataFrame.")
        return

    df['year_start'] = df['year'].min()
    df['year_end'] = df['year'].max()
    df.drop('year' , axis = 1, inplace = True)
    return df

def sanitize_diagnoses(df):
    """
    This function accepts one column of a DataFrame (a Series) and removes any non-alphanumeric character
    """
    df = df.str.replace("\W", "") 

    return df


df = hosp_prep.age_binning(df)

df = year_range(df)



df['sex_id'].replace(['2 - MEN','1 - FEMALE'],[1,2], inplace = True)
df['outcome_id'].replace(['3 - DIED (LA)','2 - Translated (A) TO ANOTHER HOSPITAL','1 - Out (A) HOME'],['death', 'discharge', 'discharge'], inplace=True)
df['facility_id'].replace(['1 - ROUTINE', '2 - EMERGENCY TO 24 HOURS', '3 - EMERGENCY AFTER 24 HOURS'], ['hospital', 'emergency', 'hospital'], inplace=True)

assert len(df['sex_id'].unique()) == 2, "sex_id has too many feature levels"
assert len(df['outcome_id'].unique()) == 2, "outcome_id has too many feature levels"
assert len(df['facility_id'].unique()) == 2, "facility_id has too many feature levels"

diagnosis_feats = df.filter(regex="^dx_").columns
for feat in diagnosis_feats:
    df[feat] = df[feat].str.replace("\W", "")
    df[feat] = df[feat].astype(str)


df_wide = df.copy()

str_cols = ['outcome_id', 'facility_id', 'source']
for strc in str_cols:
    df_wide[strc] = df_wide[strc].astype(str)

df_wide['code_system_id'] = 2
df_wide['metric_id'] = 1
df_wide['metric_discharges'] = 1


df_wide.to_stata("FILEPATH", write_index=False)
wide_path = "FILEPATH"
hosp_prep.write_hosp_file(df_wide, wide_path, backup=True)









def stack_merger(df):
    """
    Takes a dataframe
    Selects only the columns with diagnosis information
    Pivots along the index of the original dataframe to create a longer form
    Outer joins this long df with the original wide df
    Returns a stacked and merged data frame

    Assumes that the primary diagnosis column in diagnosis_vars is named dx_1

    Also compaires values before and after the stack to verify that the stack worked.
    """

    
    diagnosis_vars = ['dx_1', 'dx_2', 'dx_3','dx_4','dx_5','dx_6']
    
    diag_df = df[diagnosis_vars]
    
    stacked_df = diag_df.set_index([diag_df.index]).stack().reset_index()
    
    stacked_df = stacked_df.rename(columns = {"level_0" : "patient_index", 'level_1' : 'diagnosis_id', 0 : 'cause_code'})
    
    stacked_df['diagnosis_id'] = np.where(stacked_df['diagnosis_id'] == 'dx_1', 1, 2)
    
    merged_df = stacked_df.merge(df, left_on = 'patient_index', right_index = True, how = 'outer')

    
    
    assert (diag_df[diagnosis_vars[0]].value_counts()
        == merged_df['cause_code'].loc[merged_df['diagnosis_id'] == 1].value_counts()).all(),\
        "Primary Diagnoses counts are not the same before and after the Stack-Merge"
    
    assert (diag_df[diagnosis_vars[0]].sort_values().values
        == merged_df[merged_df['diagnosis_id'] == 1]['cause_code'].dropna().sort_values().values).all(),\
        "Not all Primary Diagnoses are present before and after the Stack-Merge"

    
    old_second_total = diag_df[diagnosis_vars[1:]].apply(pd.Series.value_counts).sum(axis = 1)
    new_second_total = merged_df['cause_code'].loc[merged_df['diagnosis_id'] == 2].value_counts()
    assert (old_second_total.sort_index().values == new_second_total.sort_index().values).all(),\
        "The counts of Secondary Diagnoses were not the same before and after the Stack-Merge"
    
    assert (old_second_total.sort_index().index == new_second_total.sort_index().index).all(),\
        "Not all Secondary Diagnoses are present before and after the Stack-Merge"

    
    merged_df.drop(diagnosis_vars, axis = 1, inplace = True)
    print("All tests passed")
    return merged_df


merged = stack_merger(df)


merged['val'] = 1


merged['cause_code'] = sanitize_diagnoses(merged['cause_code'])

group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start','age_end',
            'year_start','year_end','location_id','nid','age_group_unit',
            'outcome_id', 'facility_id', 'representative_id', 'source']



merged_agg = merged.groupby(group_vars).agg({'val' : 'sum'}).reset_index()


merged_agg['code_system_id'] = 2
merged_agg['metric_id'] = 1

hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source','nid',
                   'facility_id',
                   'code_system_id', 'cause_code']


merged_agg = merged_agg[hosp_frmat_feat]


merged_agg = merged_agg[merged_agg.cause_code != "nan"]

assert len(hosp_frmat_feat) == len(merged_agg.columns), "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in merged_agg.columns, "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])


for i in merged_agg.drop(['cause_code','source','outcome_id', 'facility_id'], axis=1, inplace=False).columns:
    
    assert merged_agg[i].dtype != object, "%s should not be of type object" % (i)



assert merged_agg['cause_code'].isnull().sum() == 0, "There are null values present in the cause code feature"
assert sum(merged_agg['cause_code'] == "") == 0, "There are blank strings present in the cause code feature"
    
assert sum(merged_agg['val'] == 0) == 0, "There are zero values in the val feature"


assert len(merged_agg['year_start'].unique()) == len(merged_agg['nid'].unique()), "number of feature levels of years should match number of feature levels of nid"
assert len(merged_agg['age_start'].unique()) == len(merged_agg['age_end'].unique()), "number of feature levels age start should match number of feature levels age end"
assert len(merged_agg['diagnosis_id'].unique()) <= 2, "diagnosis_id should have 2 or less feature levels"
assert len(merged_agg['sex_id'].unique()) == 2, "There should only be two feature levels to sex_id"
assert len(merged_agg['code_system_id'].unique()) <= 2, "code_system_id should have 2 or less feature levels"
assert len(merged_agg['source'].unique()) == 1, "source should only have one feature level"


filepath = root + r"FILEPATH"
hosp_prep.write_hosp_file(merged_agg, filepath, backup=True)



if os.path.exists(filepath):
    print("\n\n
else:
    print("Formatted data was not written to ", filepath)
