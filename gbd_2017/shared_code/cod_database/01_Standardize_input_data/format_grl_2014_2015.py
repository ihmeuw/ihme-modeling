import pandas as pd
import numpy as np

import os
import sys
import re

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../../..'))
sys.path.append(repo_dir)

from cod_prep.downloaders.ages import get_cod_ages
from cod_prep.downloaders import get_cause_map
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.utils import report_if_merge_fail
from cod_prep.utils import print_log_message

GRL_PATH_2014 = "FILEPATH"
GRL_PATH_2015 = "FILEPATH"

ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id', 'site'
]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL

def clean_df(df):
    df.rename(columns={'Year':'year_id', 'Deaths':'deaths', 'ICD-10':'value'}, inplace = True)
    df['location_id'] = 349
    df['representative_id'] = 1
    df['data_type_id'] = 9
    df['site'] = ""
    df['code_system_id'] = 1
    return df

def get_sex_id(df):
    df['sex_id'] = 1
    df['sex_id'] = df.apply(
        lambda x: x['sex_id'] if x['Sex'] == 'Male'
        else 2, axis = 1
        )
    df.drop(['Sex'], axis = 1, inplace = True)
    return df

def get_nid(df):
    df['nid'] = 0
    df.loc[df.year_id == 2014, 'nid'] = 336992
    df.loc[df.year_id == 2015, 'nid'] = 336994
    assert (df.nid.values != 0).all()
    return df

def get_age_group_id(df):
    all_ages = df.Age.tolist()

    age_df = ages.get_ages()
    relevant_ids = list(range(5, 21)) + list(range(30, 34))
    age_df = age_df.loc[age_df.age_group_id.isin(relevant_ids)].reset_index()
    age_df = age_df[['age_group_id', 'age_group_name']]

    start_age = []
    end_age = []

    for row in range(0, len(age_df)):
        start_age.append(int(age_df.loc[:, 'age_group_name'][row].split(' ')[0]))
        end_age.append(int(age_df.loc[:, 'age_group_name'][row].split(' ')[2]))

    groups = []

    for ele in all_ages:
        lower_list = [i for i in start_age if i <= ele]
        upper_list = [i for i in end_age if i >= ele]
        if not lower_list:
            low = min(all_ages)
        else:
            low = max(lower_list)
        if not upper_list:
            high = max(all_ages)
        else:
            high = min(upper_list)
        groups.append(str(low) + ' to ' + str(high))

    df['age_group_name'] = groups

    df = df.merge(age_df, on = 'age_group_name', how = 'left')

    df.loc[df.Age == 0, 'age_group_id'] = 28
    df.loc[df.Age >= 95, 'age_group_id'] = 235

    assert df.age_group_id.notnull().all()

    df.drop(['Age', 'age_group_name'], axis = 1, inplace = True)
    
    return df

def fix_codes(df):
    df.loc[df.value == 'NDC', 'value'] = 'R99'
    df.loc[df.value == 'c34', 'value'] = 'C34'
    return df

def map_code_id(df, code_map, remove_decimal=True, value_col='value'):

    if remove_decimal:
        df[value_col] = df[value_col].str.replace(".", "")
        code_map[value_col] = code_map[value_col].str.replace(".", "")
        df[value_col] = df[value_col].str.strip()
        code_map[value_col] = code_map[value_col].str.strip()
    code_ids = code_map[
        ['code_system_id', value_col, "code_id"]].drop_duplicates()
    df.loc[df[value_col] == 'acause_digest_gastrititis',
           value_col] = 'acause_digest_gastritis'
    assert not code_ids[
        ['code_system_id', value_col, "code_id"]
    ].duplicated().values.any()
    df = pd.merge(df, code_ids, on=['code_system_id', value_col], how='left')

    for num_dig_retry in [4, 3]:
        if df.code_id.isnull().any():
            print("Trying mapping again at {} digits...".format(
                num_dig_retry))
            # try again at 4 digits
            filled_mappings = remap_code_id(df, code_ids, num_dig_retry)
            df = df.loc[df['code_id'].notnull()]
            df = df.append(filled_mappings, ignore_index=True)

    report_if_merge_fail(df, 'code_id', ['code_system_id', value_col])

    return df


def remap_code_id(df, code_ids, num_digits, value_col='value'):
    """Retry code id mapping at num_digits length."""
    missing_mappings = df[df['code_id'].isnull()].copy()
    missing_mappings = missing_mappings.drop('code_id', axis=1)
    is_icd9_ecode = (missing_mappings['code_system_id'] == 6) & \
        (missing_mappings[value_col].str.startswith("E"))
    missing_mappings.loc[
        is_icd9_ecode, value_col
    ] = missing_mappings[value_col].apply(lambda x: x[:(num_digits + 1)])
    missing_mappings.loc[
        ~is_icd9_ecode, value_col
    ] = missing_mappings[value_col].apply(lambda x: x[:num_digits])
    filled_mappings = missing_mappings.merge(
        code_ids, on=['code_system_id', value_col], how='left')
    return filled_mappings

def format_greenland():
	grl_14 = pd.read_excel(GRL_PATH_2014)
	grl_15 = pd.read_excel(GRL_PATH_2015)
	grl_14 = grl_14[['Year', 'Sex', 'ICD-10', 'Age', 'Deaths']]
	assert (grl_14.columns.values == grl_15.columns.values).all()
	df = pd.concat([grl_14, grl_15])

	df = clean_df(df)

	df = get_sex_id(df)

	df = get_nid(df)

	df = get_age_group_id(df)


	df = fix_codes(df)

	cause_map = get_cause_map(code_system_id = 1)
	df = map_code_id(df, cause_map)


	df = df[FINAL_FORMATTED_COLS]
	df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

	system_source = 'Greenland_BoH_ICD10'


	finalize_formatting(df, system_source, write = True)


if __name__ == "__main__":
	format_greenland()