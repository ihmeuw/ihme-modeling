
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Formatting Georgia

URL
"""

import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass



if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"


user = getpass.getuser()
prep_path = "FILEPATH".format(user)
sys.path.append(prep_path)






import hosp_prep


input_folder = ("FILEPATH")


remove_grouped_icds = False  


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
    
    check_list = []
    
    
    bad_condition = df[child_column] > df[parent_column]

    return bad_condition






def prep_2000_2011():
    """
    Funtion to read in and prepare data from GEO for the years 2000-2011.

    Returns:
        Pandas DataFrame with data for the years 2000-2011 in a format
        consistent with the other years of data.
    """
    
    input_columns = ['b', 'g', 'd', '1', '2', '3', '4',
                     '5', '6', '7', '8', 'year']

    
    
    
    


    
    
    template_columns =['number_in_group', 'dx_1', 'line_number',
                       '15_125_discharge', '15_125_beddays', '15_125_death',
                       '0_15_discharge', '0_1_discharge',
                       '0_15_beddays', '0_15_death',
                       '0_1_death', "year"]

    
    column_dict = dict(list(zip(input_columns, template_columns)))


    df_list =[]
    
    for year in range(2000, 2012):
        df = pd.read_csv("FILEPATH".format(input_folder, year))

        assert_msg = """Columns don't match for year {}, here's the set
        difference:
        {}""".format(year,
                     set(df.columns).symmetric_difference(set(input_columns)))
        assert set(df.columns) == set(input_columns),\
            assert_msg

        df_list.append(df)

    
    df = pd.concat(df_list, ignore_index=True)

    
    df = df.rename(columns=column_dict)

    
    
    df = df[df.dx_1.notnull()]

    
    df = df.drop(["number_in_group", "line_number"], axis=1)


    
    df.update(df[['15_125_discharge', '15_125_beddays',
                  '15_125_death', '0_15_discharge', '0_1_discharge',
                  '0_15_beddays', '0_15_death', '0_1_death']].fillna(0))

    
    assert df.notnull().all().all(), "There are nulls"


    
    
    
    

    
    nonsense = check_subset_columns(df, parent_column="0_15_discharge",
                                    child_column="0_1_discharge")

    
    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    
    nonsense = check_subset_columns(df, parent_column="0_15_death",
                                    child_column="0_1_death")

    
    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy

    


    
    

    
    df['1_15_discharge'] = df["0_15_discharge"] - df["0_1_discharge"]
    df['1_15_death'] = df["0_15_death"] - df["0_1_death"]
    

    
    assert (df["1_15_discharge"] >= 0).all()
    assert (df["1_15_death"] >= 0).all()

    
    df = df.drop(["0_15_discharge", "0_15_death"], axis=1)

    
    df = df.drop(["15_125_beddays", "0_15_beddays"], axis=1)


    
    df = df.set_index(['year', 'dx_1']).stack().reset_index()

    
    df['age_start'], df['age_end'], df['outcome_id'] =\
        df['level_2'].str.split("_", 2).str

    
    df['age_start'] = pd.to_numeric(df["age_start"], errors="raise")
    df['age_end'] = pd.to_numeric(df["age_end"], errors="raise")

    
    df = df.drop("level_2", axis=1)

    
    df = df.rename(columns={"year": "year_start", 0: "val"})


    
    df["year_end"] = df.year_start
    df["sex_id"] = 3


    

    return df


def prep_2012_2013():
    """
    Funtion to read in and prepare data from GEO for the years 2012-2013.

    Returns:
        Pandas DataFrame with data for the years 2012-2013-2011 in a format
        consistent with the other years of data.
    """

    
    input_columns = ['b', 'g', 'd', '1', '2', '3', '4', '5', '6',
                     '7', '8', "9", "10", "11", "12", "13", 'year']
    df_list =[]
    
    for year in [2012, 2013]:
        df = pd.read_csv("FILEPATH".format(input_folder, year))

        assert_msg = """Columns don't match for year {}, here's the set
        difference:
        {}""".format(year,
                     set(df.columns).symmetric_difference(set(input_columns)))
        assert set(df.columns) == set(input_columns),\
            assert_msg

        df_list.append(df)
    df = pd.concat(df_list, ignore_index=True)


    df.columns = ['group_number', 'dx_1', 'line_number',
                  '18_125_discharge', '18_125_beddays', '18_125_death',
                  '15_18_discharge', '15_18_beddays', '15_18_death',
                  '0_15_discharge', '0_5_discharge', '0_1_discharge',
                  '0_15_beddays',
                  '0_15_death', '0_5_death', '0_1_death',
                  'year_start']


    
    nonsense = check_subset_columns(df, parent_column="0_15_discharge",
                                    child_column="0_5_discharge")

    
    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    
    nonsense = check_subset_columns(df, parent_column="0_5_discharge",
                                    child_column="0_1_discharge")

    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    
    nonsense = check_subset_columns(df, parent_column="0_15_death",
                                    child_column="0_5_death")

    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()

    
    nonsense = check_subset_columns(df, parent_column="0_5_death",
                                    child_column="0_1_death")

    if df[nonsense].shape[0] > 0:
        df = df[~nonsense].copy()
    

    
    df['5_15_discharge'] = df["0_15_discharge"] - df["0_5_discharge"]
    df['5_15_death'] = df["0_15_death"] - df["0_5_death"]

    df['1_5_discharge'] = df["0_5_discharge"] - df["0_1_discharge"]
    df['1_5_death'] = df["0_5_death"] - df["0_1_death"]


    
    df.drop(['group_number', 'line_number',
             '0_15_discharge', '0_5_discharge',
             '0_15_death', '0_5_death'], axis=1, inplace=True)


    
    df = df.drop(["18_125_beddays", "15_18_beddays", "0_15_beddays"], axis=1)


    
    df = df.set_index(["dx_1", "year_start"]).stack().reset_index()

    
    df['age_start'], df['age_end'], df['outcome_id'] =\
        df['level_2'].str.split("_", 2).str

    
    df['age_start'] = pd.to_numeric(df["age_start"], errors="raise")
    df['age_end'] = pd.to_numeric(df["age_end"], errors="raise")

    
    df = df.rename(columns={0: "val"})

    
    df = df.drop("level_2", axis=1)


    
    df["year_end"] = df.year_start
    df["sex_id"] = 3

    

    return df

def prep_2014():
    """
    Funtion to read in and prepare data from GEO for the years 2012-2013.

    Returns:
        Pandas DataFrame with data for the years 2012-2013-2011 in a format
        consistent with the other years of data.
    """

    df = pd.read_excel(root + r"FILENAME"
                       r"FILEPATH")


    
    keep = ['Sex', 'Age (years)', 'Main Diagnosis (ICD10)',
            'External causes (ICD10)', 'Disharge status',  
            'Complication (ICD 10)', 'Comorbidity (ICD 10)', 'Beddays']
    df = df[keep].copy()

    
    
    
    


    
    df = df.rename(columns={'Main Diagnosis (ICD10)': 'dx_1',
                            'External causes (ICD10)': 'dx_2',
                            'Complication (ICD 10)': 'dx_3',
                            'Comorbidity (ICD 10)': 'dx_4'})


    
    
    outcome_dict = {1: 'discharge',
                    2: 'discharge',
                    3: 'discharge',
                    4: 'death'}
    df['outcome_id'] = df["Disharge status"].map(outcome_dict)


    
    df.drop("Disharge status", axis=1, inplace=True)


    
    
    df = df[(df.outcome_id != "discharge")|(df.Beddays >= 1)]  

    
    df = df.drop("Beddays", axis=1)


    
    df = df.rename(columns={"Sex": "sex_id", "Age (years)": "age"})
    df['year_start'] = 2014
    df['year_end'] = 2014

    
    df["val"] = 1


    
    df['age'] = pd.to_numeric(df['age'], errors="coerce")

    
    df.loc[df.age > 99, 'age'] = 99  
    df = hosp_prep.age_binning(df)

    
    df = df.drop("age", axis=1)


    
    df.loc[df.sex_id.isnull(), "sex_id"] = 3  
    df = df[df.dx_1.notnull()]  
    df = df[df.outcome_id.notnull()]  

    return df


df_00_11 = prep_2000_2011()
df_12_13 = prep_2012_2013()
df = prep_2014()


df = pd.concat([df_00_11, df_12_13, df], ignore_index=True)



df['representative_id'] = 3  
df['location_id'] = 35


df['age_group_unit'] = 1


df["source"] = np.nan
df.loc[df.year_start == 2014, "source"] = "GEO_COL_14"
df.loc[df.year_start < 2014, "source"] = "GEO_COL_00_13"



df.loc[df.year_start <= 2005, 'code_system_id'] = 1
df.loc[df.year_start >= 2006, 'code_system_id'] = 2





df['metric_id'] = 1

df['facility_id'] = 'inpatient unknown'


df["nid"] = df.year_start.map({2000: 212469,
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
                               2014: 212493})




for col in ["dx_1", "dx_2", "dx_3", "dx_4"]:
    df[col] = df.loc[df[col].notnull(), col].apply(lambda x: x.encode('utf-8'))

    df[col] = df.loc[df[col].notnull(), col].str.upper()



for col in ["dx_1", "dx_2", "dx_3", "dx_4"]:
    
    df[col] = df[col].str.replace("[^A-Z0-9.\-]", "")



df.loc[df["dx_2"].notnull(), ["dx_1", "dx_2"]] = df.loc[df["dx_2"].notnull(),
                                                        ["dx_2", "dx_1"]].values






df.loc[df.year_start == 2014].drop("val", axis=1).\
    to_stata("FILEPATH",
             write_index=False)




diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)
    df.drop('patient_index', axis=1, inplace=True)
elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")


df.loc[df.source == "GEO_COL_14", 'cause_code'] =\
    hosp_prep.sanitize_diagnoses(df.loc[df.source == "GEO_COL_14", 'cause_code'])






print("Are there missing values in any row?")
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


assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 3,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"

if remove_grouped_icds:
    
    df_agg = df_agg[df_agg.year_start == 2014]
    
    df_agg.cause_code = df_agg.cause_code.str.replace("\W", "")


write_path = root + r"FILENAME" + \
    r"FILEPATH"
hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
