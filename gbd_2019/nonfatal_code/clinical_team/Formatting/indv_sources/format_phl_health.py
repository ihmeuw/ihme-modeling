
"""
Created on Tue Jan 17 13:24:34 2017

@author: USERNAME

prepping and inspecting Phillipines claims data

FILEPATH
FILEPATH
FILEPATH

"""
import pandas as pd
import platform




if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"



df_list = []
years = [2010, 2011, 2012]


for year in years:
    df = pd.read_csv(root + r"FILENAME"
                     r"FILENAME"
                     r"PHL_HEALTH_INSURANCE_CLAIMS_" + str(year) +
                     "FILEPATH", encoding='latin-1', sep='|')
    df['year_start'] = year
    df['year_end'] = year
    df_list.append(df)
df = pd.concat(df_list)




df.drop(['REG', 'PROVINCE_NAME', 'ICD_DESCRIPTION', 'RVS', 'RVS_DESCRIPTION',
         'AMT_ACTUAL', 'TOTAL_AMOUNT', 'YR'], axis=1, inplace=True)


counts = df.groupby("PATIENT_ID").size()
