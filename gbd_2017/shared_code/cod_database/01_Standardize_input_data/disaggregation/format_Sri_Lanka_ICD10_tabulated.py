
import pandas as pd
import numpy as np

import os
import sys
import re

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../../..'))
sys.path.append(repo_dir)

from cod_prep.downloaders.ages import get_ages
from cod_prep.downloaders import get_cause_map
from cod_prep.downloaders.locations import get_current_location_hierarchy
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import get_adult_age_codebook
from cod_prep.utils import get_infant_age_codebook
from cod_prep.utils import report_if_merge_fail
from cod_prep.utils import print_log_message
CONF = Configurator('standard')

path = 'FILEPATH'
rdp_path = 'FILEPATH'

ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id', 'site'
]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL

def clean_df(df):
    #cleaning of unnecessary rows/columns due to formatting of imported excel file
    df.iloc[4,0] = df.iloc[3,0]
    df.drop(df.index[[0, 1, 2]], inplace = True)
    df.reset_index(inplace = True, drop = True)

    df.columns = df.iloc[1].tolist()
    df.drop(df.index[[0, 1]], inplace = True)
    df.drop(['Male', 'Female'], axis = 1, inplace = True)

    #removing rows that are sums of other rows
    df.columns.values[0] = 'cause_name'
    #aggregates is a list of rows which contain sums of the most detailed rows
    aggregates = ['1-001', '1-026', '1-048', '1-051', '1-055', '1-058',
                 '1-064', '1-072', '1-078', '1-084', '1-087', '1-095', 'Total']
    df['cause_name'] = df['cause_name'].str.lstrip()
    df['value'] = df['cause_name'].str[0:5]
    df = df.loc[~(df.value.isin(aggregates))]
    
    return df

def split_sexes(df):
    #getting total deaths before splitting, to check against split df at end
    df['Total'] = df['Total'].replace('-', 0)
    initial_total = df.Total.sum()
    
    #splitting df into male and female dfs, which will be stacked back on top of each other
    males = df[['M']]
    females = df[['F']]
    males.replace({'-':0}, inplace = True)
    females.replace({'-':0}, inplace = True)

    #assigning age_group headers ('drop' refers to columns that are aggregate sum columns)
    headers = ['Early Neonatal', 'Late Neonatal', 'drop', 'drop', 'drop', 'drop', 'drop',
              'Post Neonatal', 'drop', 'drop', 'drop', 'drop', 'drop', '1-4',
              '5-9', '10-14', 'drop', '15-19', '20-24', 'drop', '25-29', '30-34', '35-39',
              '40-44', 'drop', '45-49', '50-54', '55-59', '60-64', 'drop', '65-69', '70-74', 
              'drop', '75-79', '80-84', '85 plus', 'drop', 'unknown']

    males.columns = headers
    females.columns = headers
    males['sex_id'] = 1
    females['sex_id'] = 2

    males.drop('drop', axis = 1, inplace = True)
    females.drop('drop', axis = 1, inplace = True)

    #concatenating male and female dfs, stacking cause column on itself to account for stacking of male/female dfs
    both = pd.concat([males, females])
    causes = df[['cause_name', 'value']]
    causes2 = causes.copy()
    stacked_causes = pd.concat([causes, causes2])
    df = pd.concat([stacked_causes, both], axis = 1)
    
    return df, initial_total

def get_age_ids(df):
    #renaming age names to match with cod_ages name
    df['age'] = df['age'].str.replace('-', ' to ')
    df.rename(columns={'age':'age_group_name'}, inplace = True)

    #using get_ages() to merge on age_group_ids
    ages = get_ages()
    df = df.merge(ages[['age_group_id', 'age_group_name']], on = 'age_group_name', how = 'left')

    #manually assigning age_group_id for 'unknown'
    df.loc[df.age_group_name == 'unknown', 'age_group_id'] = 283
    assert df.age_group_id.notnull().all()

    return df

def format_rdp_frac(rdp):
    #cleaning rdp_frac dataframe
    rdp.drop(['percent1', 'percent2', 'percent4', 'percent5', 'percent6',
              'percent26', 'percent92', 'num'], axis = 1, inplace = True)

    #keeping only frmat 0 and im_frmat 2
    rdp = rdp.loc[(rdp.frmat == 0) & (rdp.im_frmat == 2)]

    #setting super region to 4 (the one that corresponds to Sri Lanka)
    rdp = rdp.loc[rdp.super_region == 4]

    #reshaping, renaming, and standardizing column types
    rdp.drop(['super_region'], axis = 1, inplace = True)
    rdp = pd.melt(rdp, id_vars = ['cause', 'target', 'sex', 'frmat', 'im_frmat'], var_name = 'cod_age', value_name = 'pct')
    rdp['cod_age'] = rdp['cod_age'].str.replace('percent', "")

    rdp['frmat'] = rdp['frmat'].astype(int)
    rdp['im_frmat'] = rdp['im_frmat'].astype(int)
    rdp['cod_age'] = rdp['cod_age'].astype(int)

    #getting codebooks to map rdp age_group_ids to cod age_group_ids
    adult_cb = get_adult_age_codebook()
    adult_cb['frmat'] = adult_cb['frmat'].astype(int)
    adult_cb['cod_age'] = adult_cb['cod_age'].astype(int)

    infant_cb = get_infant_age_codebook()
    infant_cb['im_frmat'] = infant_cb['im_frmat'].astype(int)
    infant_cb['cod_age'] = infant_cb['cod_age'].astype(int)

    #merging on cod age_group_ids so we can merge rdp onto our vr dataframe later
    rdp = rdp.merge(adult_cb[['cod_age', 'age_group_id', 'frmat']], on = ['frmat', 'cod_age'], how = 'left')
    rdp = rdp.merge(infant_cb[['cod_age', 'age_group_id', 'im_frmat']], on = ['im_frmat', 'cod_age'], how = 'left')

    rdp.loc[rdp.age_group_id_x.isnull(), 'age_group_id_x'] = rdp['age_group_id_y']

    rdp.rename(columns={'age_group_id_x':'age_group_id', 
                        'cause':'code', 'sex':'sex_id'}, inplace = True)
    rdp.drop(['age_group_id_y', 'frmat', 'im_frmat', 'cod_age'], axis = 1, inplace = True)
    assert rdp.age_group_id.notnull().all()

    #standardizing rdp column types to ensure successful merge
    rdp['code'] = rdp['code'].astype(int)
    rdp['age_group_id'] = rdp['age_group_id'].astype(int)
    rdp['sex_id'] = rdp['sex_id'].astype(int)
    
    return rdp

def disaggregate(df, rdp):
    pre_merge_deaths = df.deaths.sum()
    #column renaming and type standardizing to merge with rdp
    df['value'] = df['value'].str.replace('-', "")
    df.rename(columns = {'value':'code'}, inplace = True)
    df['code'] = df['code'].astype(int)
    df['age_group_id'] = df['age_group_id'].astype(int)
    df['sex_id'] = df['sex_id'].astype(int)

    #merging targets/percents from rdp onto dataframe
    df = df.merge(rdp, on = ['code', 'sex_id', 'age_group_id'], how = 'left')

    #fixing codes/death totals where merge successful
    df.loc[df.target.notnull(), 'target'] = 'acause_' + df['target']
    df.loc[df.pct.notnull(), 'deaths'] = df['deaths'] * df['pct']
    df.loc[df.target.notnull(), 'code'] = df['target']
    df.drop(['target', 'pct',], axis = 1, inplace = True)

    #asserting no null values and death totals stayed consistent
    assert df.notnull().values.all()
    assert np.allclose(df.deaths.sum(), pre_merge_deaths)

    return df

def map_code_id(df, cause_map):
    #merging code ids on using cause map
    df.rename(columns={'code':'value'}, inplace = True)
    df['value'] = df['value'].astype(str)
    df = df.merge(cause_map[['value', 'code_id']], on = 'value', how = 'left')

    #some code_ids can't merge due to slight differences in engine room codes, fixing those manually
    df.loc[(df.code_id.isnull()) & (df.value.str.contains('_cancer')), 'value'] = df['value'].str.replace('_cancer', "")
    df.loc[(df.code_id.isnull()) & (df.value.str.contains('_benign')), 'value'] = df['value'].str.replace('_benign', "")
    df.loc[df.value == 'acause_digest_gastrititis', 'value'] = 'acause_digest_gastritis'
    df.loc[df.value == 'acause_neo_brain', 'value'] = 'acause_neo_ben_brain'
    df.loc[df.value == 'acause_inj_mech_suffocate', 'value'] = 'acause_inj_foreign_aspiration'
    df.loc[df.value == 'acause_strep', 'value'] = 'acause_infectious'
    df.loc[df.value == 'acause_mono', 'value'] = 'acause_infectious'

    #dropping code_id, then re merging with fixes in place
    df.drop('code_id', axis = 1, inplace = True)
    df = df.merge(cause_map[['value', 'code_id']], on = 'value', how = 'left')
    assert df.code_id.notnull().all()

    return df

def format_sri_lanka():
    #importing raw data from excel file
    df = pd.read_excel(path)

    #initial cleaning to fix rows/columns imported from excel
    df = clean_df(df)

    #incoming data has sex data in wide format, following function splits df by sex and manually sets age groups
    #function returns initial_total (a float to compare against deaths later to ensure no deaths were lost in process)
    df, initial_total = split_sexes(df)

    #reshaping df age groups wide to long and assuring no deaths were lost
    df = pd.melt(df, id_vars = ['cause_name', 'sex_id', 'value'], var_name = 'age', value_name = 'deaths')
    assert np.allclose(initial_total, df.deaths.sum())

    df = get_age_ids(df)

    #importing and formatting rdp_frac dataframe to disaggregate tabulated icd10
    rdp = pd.read_stata(rdp_path)
    rdp = format_rdp_frac(rdp)

    #disaggregating tabulated icd10 codes
    df = disaggregate(df, rdp)

    #mapping code_ids using cause map from engine room
    cause_map = get_cause_map(code_system_id = 9)
    df = map_code_id(df, cause_map)

    #addition of manually added columns
    #Sri Lanka location id 17
    df['location_id'] = 17
    #nid 327524
    df['nid'] = 327524
    #data_type_id 9 (VR)
    df['data_type_id'] = 9
    #code_system_id 9 (ICD10_tabulated)
    df['code_system_id'] = 9
    #year: 2013, site: blank, representative_id: 1
    df['year_id'] = 2013
    df['site'] = ""
    df['representative_id'] = 1

    #grouping by ID_COLS and assigning system source
    df = df[FINAL_FORMATTED_COLS]
    df = df.groupby(ID_COLS, as_index = False)[VALUE_COL].sum()

    system_source = "ICD10_tabulated"

    #df.to_csv('/home/j/temp/mwcunnin/test_lka_out.csv', index = False, encoding = 'utf8')

    #run finalize formatting
    finalize_formatting(df, system_source, write = True)

if __name__ == "__main__":
	format_sri_lanka()