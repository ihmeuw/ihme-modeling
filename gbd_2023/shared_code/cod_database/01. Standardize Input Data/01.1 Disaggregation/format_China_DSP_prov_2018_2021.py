import pandas as pd
import numpy as np

import os
import sys
import re

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../../..'))
sys.path.append(repo_dir)
from cod_prep.utils.formatting import ages
from cod_prep.downloaders.ages import get_ages
from cod_prep.downloaders import get_cause_map, add_code_metadata, add_nid_metadata
from cod_prep.downloaders.locations import get_current_location_hierarchy, add_location_metadata
from cod_prep.claude.formatting import finalize_formatting, update_nid_metadata_status
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import report_if_merge_fail, report_duplicates
from cod_prep.utils import print_log_message
from cod_prep.claude.claude_io import get_claude_data
CONF = Configurator('standard')
pd.options.mode.chained_assignment = None

ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
    'data_type_id', 'representative_id', 'code_system_id', 'code_id', 'site'
]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL
INT_COLS = ['nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
            'data_type_id', 'representative_id', 'code_system_id', 'code_id']

SOURCE = 'China_DSP_prov_ICD10'
DATA_DIR = 'FILEPATH'
WRITE = False
IS_ACTIVE = True
IS_MORT_ACTIVE = False
PROJECT_ID = CONF.get_id('project')


def read_clean_data():
    icd_map = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='ICD codes')
    m18 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2018 -1-2 (male)')
    f18 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2018 -1-3 (female)')
    m18['sex_id'] = 1
    f18['sex_id'] = 2
    df18 = pd.concat([m18, f18])
    df18['year_id'] = 2018
    df18['nid'] = 469163
    m19 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2019 -1-2 (male)')
    f19 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2019 -1-3 (female)')
    m19['sex_id'] = 1
    f19['sex_id'] = 2
    df19 = pd.concat([m19, f19])
    df19['year_id'] = 2019
    df19['nid'] = 469165
    m20 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2020 -1-2 (male)')
    f20 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2020 -1-3 (female)')
    m20['sex_id'] = 1
    f20['sex_id'] = 2
    df20 = pd.concat([m20, f20])
    df20['year_id'] = 2020
    df20['nid'] = 528380
    m21 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2021 -1-2 (male)')
    f21 = pd.read_excel(DATA_DIR + 'FILENAME', sheet_name='2021 -1-3 (female)')
    m21['sex_id'] = 1
    f21['sex_id'] = 2
    df21 = pd.concat([m21, f21])
    df21['year_id'] = 2021
    df21['nid'] = 528381

    df = pd.concat([df18, df19, df20, df21])
    df = pd.melt(
        df, id_vars=['nid', 'DSP code', 'region', 'sex_id', 'year_id'], value_vars=[col for col in df.columns if col is not 'DSP code'],
        var_name='ages', value_name='deaths'
    )

    df = df.merge(icd_map, on=['DSP code'], how='left')
    df = df.loc[df['is_aggregate'] != 1, ]

    df = df.loc[df['region'] != 'national', ]
    df['region'] = df['region'].str.lower()
    df.loc[df['region'] == 'central', 'region'] = 'middle'

    df['ages'] = df['ages'].str.strip()

    return df


def clean_region_map():
    mp = pd.read_excel(DATA_DIR + 'FILENAME')
    mp['location_name'] = mp['location_name'].str.lower()
    lh = get_current_location_hierarchy()
    chn = lh.loc[lh['parent_id'] == 6, ]
    chn['location_name'] = chn['location_name'].str.lower()

    mp = mp.merge(chn[['location_id', 'location_name']], on='location_name', how='left')
    assert mp['location_id'].notnull().values.all()

    return mp


def get_existing_data():

    df = get_claude_data(
        'formatted', is_active=True, project_id=PROJECT_ID, source='China_DSP_prov_ICD10',
        data_type_id=[9, 10], iso3='CHN', year_id=list(range(2010, 2018))
    )
    df = df.loc[df['location_id'] != 354, ]
    df = add_code_metadata(df, ['value'], 1)
    df = df.loc[~df['age_group_id'].isin([281, -1]), ]
    df.loc[df['age_group_id'].isin([2, 3, 388, 389, 4]), 'age_group_id'] = 28
    df.loc[df['age_group_id'].isin([238, 34]), 'age_group_id'] = 5
    df.loc[df['age_group_id'].isin([31, 32, 235]), 'age_group_id'] = 160
    df = df.loc[df['age_group_id'] != 283, ]
    df = df.groupby(['location_id', 'age_group_id', 'sex_id', 'code_id', 'value'], as_index=False)['deaths'].sum()

    return df


def split_ICD(df, existing_df, region_map):
    print_log_message('Splitting ICD codes by age, sex, region')
    start_deaths = df['deaths'].sum()

    df = df.loc[df['deaths'] != 0, ]
    build_df = []
    existing_df = existing_df.groupby(['age_group_id', 'sex_id', 'value', 'location_id'], as_index=False)['deaths'].sum()
    for region in ['east', 'middle', 'west']:
        region_df = df.loc[df['region'] == region, ]
        prov_ids = region_map.loc[region_map['region'] == region, 'location_id'].unique().tolist()
        existing_region = existing_df.loc[existing_df['location_id'].isin(prov_ids), ]
        existing_region = existing_region.groupby(['age_group_id', 'sex_id', 'value'], as_index=False)['deaths'].sum()
        for code in region_df['DSP code'].unique().tolist():
            row = region_df.loc[region_df['DSP code'] == code, ]
            if '-' in row['ICD code'].unique().item():
                expand_codes = row['ICD code'].str.split(', ', expand=True)
                exclude_codes = []
                if (row['excludes'].notnull().values.all()):
                    exclude_codes = row['excludes'].unique().item()
                    if (',' in exclude_codes):
                        exclude_codes = exclude_codes.split(', ')
                    else:
                        exclude_codes = [exclude_codes]
                keep_codes = []
                for code_col in expand_codes.columns:
                    codes = expand_codes[code_col].unique().item()
                    if '-' in codes:
                        codes = codes.split('-')
                        if (codes[0][0] == codes[1][0]):
                            chapter = codes[0][0]
                            start_num = float(codes[0][1:])
                            if not (len(codes[1]) == 3):
                                end_num =  float(codes[1][1:])
                            else:
                                end_num =  float(codes[1][1:]) + .1
                            def get_full_code(i, chapter=chapter):
                                return chapter +  str(round(i, 1))
                            code_list = [get_full_code(i) for i in np.arange(start_num, end_num, .1)] + [codes[0], codes[1]]
                            keep_codes += code_list
                        else:
                            icd_index = existing_df[['value']].drop_duplicates().sort_values(by=['value'])
                            icd_index['index'] = icd_index.reset_index().index
                            index1 = icd_index.loc[icd_index['value'] == codes[0], 'index'].item()
                            index2 = icd_index.loc[icd_index['value'] == codes[1], 'index'].item()
                            code_list = icd_index.loc[icd_index['index'].between(index1, index2), 'value'].unique().tolist()
                            keep_codes += code_list
                    else:
                        keep_codes += [codes]
                existing_subset = existing_region.loc[
                        (existing_region['value'].isin(keep_codes)) &
                        ~(existing_region['value'].isin(exclude_codes)), ]
                if len(existing_subset) == 0:
                    row['value'] = codes[0]
                    build_df.append(row)
                else:    
                    existing_subset['total'] = existing_subset \
                        .groupby(['age_group_id', 'sex_id'], as_index=False)['deaths'].transform(sum)
                    existing_subset['prop'] = existing_subset['deaths'] / existing_subset['total']
                    existing_subset = existing_subset.loc[~existing_subset['prop'].isna(), ]
                    row = row.merge(existing_subset, on=['age_group_id', 'sex_id'], how='left', suffixes=('_raw', '_prop'))
                    if ~row['prop'].notnull().values.all():
                        nullrow = row.loc[row['prop'].isna(), ]
                        row = row.loc[~row['prop'].isna(), ]
                        existing_subset = existing_subset.groupby(['value'], as_index=False)['deaths'].sum()
                        existing_subset['total'] = existing_subset['deaths'].sum()
                        existing_subset['prop'] = existing_subset['deaths'] / existing_subset['total']
                        existing_subset['index'] = 1
                        nullrow['index'] = 1
                        nullrow.drop(columns=['value', 'deaths_prop', 'total', 'prop'], inplace=True)
                        nullrow.rename(columns={'deaths_raw': 'deaths'}, inplace=True)
                        nullrow = nullrow.merge(existing_subset, on=['index'], how='left', suffixes=('_raw', '_prop'))
                        nullrow.drop(columns=['index'], inplace=True)
                        row = pd.concat([row, nullrow])
                    row['deaths'] = row['deaths_raw'] * row['prop']
                    build_df.append(row)
            else:
                row['value'] = row['ICD code'].unique().item()
                build_df.append(row)
    df = pd.concat(build_df)

    assert (np.isclose(start_deaths, df['deaths'].sum()))

    drop_cols = ['ICD code', 'deaths_raw', 'deaths_prop', 'total', 'prop', 'excludes', 'is_aggregate']
    df = df.drop(columns=drop_cols)

    return df


def map_code_id(df, code_map, remove_decimal=True, value_col='value'):
    """Add code id to the df using the given code map."""
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


def get_age_ids(df):
    df[['age_start', 'age_end']] = df['ages'].str.split('- ', expand=True)
    df['age_start'] = df['age_start'].astype(int)
    df['age_end'] = df['age_end'].astype(int)
    df['age_unit'] = 'year'
    age_formatter = ages.AgeRangeFormatter()

    df = age_formatter.run(df)
    df = df.drop(columns=['age_start', 'age_end', 'age_unit'])

    return df


def split_provinces(df, existing_df, region_map):
    print_log_message('Splitting each region into provinces by age, sex, gbd cause')
    build_df = []
    existing_df = add_code_metadata(existing_df, 'cause_id', 1)

    for region in ['east', 'middle', 'west']:
        region_df = df.loc[df['region'] == region, ]
        prov_ids = region_map.loc[region_map['region'] == region, 'location_id'].unique().tolist()
        existing_region = existing_df.loc[existing_df['location_id'].isin(prov_ids), ]
        existing_region = existing_region.groupby(['age_group_id', 'sex_id', 'location_id', 'cause_id'], 
            as_index=False)['deaths'].sum()
        existing_region['total'] = existing_region.groupby(
            ['age_group_id', 'sex_id', 'cause_id'], 
            as_index=False
        )['deaths'].transform(sum)
        existing_region['prop'] = existing_region['deaths'] / existing_region['total']

        region_df = add_code_metadata(region_df, 'cause_id', 1)
        region_df = region_df.loc[region_df['deaths'] != 0, ]
        start_deaths = region_df['deaths'].sum()

        region_df = region_df.merge(existing_region, on=['age_group_id', 'sex_id', 'cause_id'], how='left', suffixes=('_raw', '_prop'))

        unmerge = region_df.loc[region_df['prop'].isna(), ]
        unmerge.drop(columns=['location_id', 'deaths_prop', 'total', 'prop'], inplace=True)
        unmerge.rename(columns={'deaths_raw': 'deaths'}, inplace=True)
        region_df = region_df.loc[~region_df['prop'].isna(), ]
        existing_region = existing_region.groupby(['location_id', 'cause_id'], as_index=False)['deaths'].sum()
        existing_region['total'] = existing_region.groupby(['cause_id'], as_index=False)['deaths'].transform(sum)
        existing_region['prop'] = existing_region['deaths'] / existing_region['total']
        unmerge = unmerge.merge(existing_region, on=['cause_id'], how='left', suffixes=('_raw', '_prop'))
        if unmerge['location_id'].isna().any():
            age_sex_only = existing_df.loc[existing_df['location_id'].isin(prov_ids), ] \
                .groupby(['age_group_id', 'sex_id', 'location_id'], as_index=False)['deaths'].sum()
            age_sex_only['total'] = age_sex_only.groupby(['age_group_id', 'sex_id'], as_index=False)['deaths'].transform(sum)
            age_sex_only['prop'] = age_sex_only['deaths'] / age_sex_only['total']
            unmerge_success1 = unmerge.loc[unmerge['location_id'].notnull(), ]
            unmerge = unmerge.loc[unmerge['location_id'].isna(), ]
            unmerge.drop(columns=['location_id', 'deaths_prop', 'total', 'prop'], inplace=True)
            unmerge.rename(columns={'deaths_raw': 'deaths'}, inplace=True)
            unmerge = unmerge.merge(age_sex_only, on=['age_group_id', 'sex_id'], how='left', suffixes=('_raw', '_prop'))
            unmerge = unmerge.append(unmerge_success1)

        region_df = pd.concat([region_df, unmerge])
        region_df['deaths'] = region_df['deaths_raw'] * region_df['prop']
        assert np.isclose(start_deaths, region_df['deaths'].sum())
        assert region_df['location_id'].notnull().values.all()
        
        region_df.drop(columns=['deaths_raw', 'deaths_prop', 'total', 'prop'], inplace=True)
        build_df.append(region_df)

    df = pd.concat(build_df)

    return df


def split_covid_from_lri(df):

    start_deaths = df['deaths'].sum()
    props = pd.read_csv(DATA_DIR + 'FILENAME')
    df = df.merge(props, on=['location_id', 'year_id', 'age_group_id', 'sex_id'], how='left')

    covid = df.loc[(df['DSP code'] == 'U039') & (df['year_id'].isin([2020, 2021])), ]
    covid['deaths'] = covid['deaths'] * covid['prop_covid_in_lri']
    covid['code_id'] = 160788

    df.loc[(df['DSP code'] == 'U039') & (df['year_id'].isin([2020, 2021])), 'deaths'] = df['deaths'] * (1-df['prop_covid_in_lri'])

    df = pd.concat([df, covid])
    assert np.isclose(start_deaths, df['deaths'].sum())

    return df


def format_china_2018_2021():
    df = read_clean_data()
    df = get_age_ids(df)

    existing_df = get_existing_data()
    region_map = clean_region_map()

    df = split_ICD(df, existing_df, region_map)
    
    df['code_system_id'] = 1
    cause_map = get_cause_map(code_system_id = 1)
    df = map_code_id(df, cause_map)

    df = split_provinces(df, existing_df, region_map)

    df = split_covid_from_lri(df)

    df['data_type_id'] = 10
    df['representative_id'] = 1
    df['site'] = ''

    df = df[FINAL_FORMATTED_COLS]
    for col in INT_COLS:
        df[col] = df[col].astype(int)
    assert df.notnull().values.all()
    df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

    locals_present = finalize_formatting(df, SOURCE, PROJECT_ID, write=WRITE)
    nid_meta_df = locals_present['nid_meta_df']

    if WRITE:
        nid_extracts = nid_meta_df[
            ['nid', 'extract_type_id']
        ].drop_duplicates().to_records(index=False)
        for nid, extract_type_id in nid_extracts:
            nid = int(nid)
            extract_type_id = int(extract_type_id)
            update_nid_metadata_status(PROJECT_ID, nid, extract_type_id, is_active=IS_ACTIVE,
                                       is_mort_active=IS_MORT_ACTIVE)
    return df


if __name__ == "__main__":
    df = format_china_2018_2021()
