"""

Inputs:
    Data should come with sex, year, location, and cause of death
    (or just deaths if prepping for mortality)
Outputs:
    A processed data frame with the standard columns

Notes:  
    Due to recollection bias, recommend for VA studies
    that are not done on a yearly bases, drop non-injury deaths
    that happened more than a year of the study and drop injury
    deaths that happen more than 5 years of a study
"""

import sys
import os
import pandas as pd

this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../../../'))
sys.path.append(repo_dir)
from cod_prep.utils import print_log_message
from cod_prep.claude.formatting import finalize_formatting, update_nid_metadata_status
from cod_prep.downloaders.ages import get_cod_ages
from cod_prep.downloaders.engine_room import get_cause_map
from cod_prep.utils import report_if_merge_fail
from cod_prep.utils.formatting import ages
from cod_prep.claude.configurator import Configurator

CONF = Configurator('standard')
IN_DIR_DHS = "FILEPATH"
IN_DIR_DHS_SP = "FILEPATH"

ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id', 'data_type_id',
    'representative_id', 'code_system_id', 'code_id', 'site'
]
VALUE_COL = ['deaths']
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL

INT_COLS = ['nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
            'code_system_id', 'code_id', 'data_type_id', 'representative_id']

SOURCE = "Bangladesh_DHS_VA"

WRITE = False
IS_ACTIVE = True
IS_MORT_ACTIVE = False
PROJ_ID = CONF.get_id('project')


def read_clean():
    df2011 = pd.read_stata('{}FILEPATH'
                           .format(IN_DIR_DHS))
    df2011 = df2011[['qn302', 'qn304', 'qc304n', 'vaweight',
                     'qc304u', 'qfinicd', 'qn305y', 'qc305y', 'qndistrict']]
    df2011.rename(columns={'qn302': 'sex', 'vaweight': 'weight',
                           'qn304': 'age1', 'qc304n': 'age2',
                           'qc304u': 'age_unit', 'qfinicd': 'value',
                           'qn305y': 'year_death', 'qc305y': 'year_death2', 'qndistrict':'admin2'},
                  inplace=True)
    df2011['year_id'] = 2011
    df2011['nid'] = 55956
    past_1year_2011 = (df2011['year_death'] < 2010) | (df2011['year_death2'] < 2010)
    df2011 = df2011.loc[~past_1year_2011, ]

    df2001 = pd.read_stata('{}FILEPATH'
                           .format(IN_DIR_DHS_SP))
    df2001 = df2001[['qh25_01', 'qh26n_01', 'qweight',
                     'qh26u_01', 'qicd10', 'qicddes', 'v301y', 'qhdistri']]
    df2001.rename(columns={'qh25_01': 'sex', 'qweight': 'weight',
                           'qh26n_01': 'age', 'qh26u_01': 'age_unit',
                           'qicd10': 'value', 'qicddes': 'description',
                           'v301y': 'year_death', 'qhdistri':'admin2'},
                  inplace=True)
    df2001['year_id'] = 2001
    df2001['nid'] = 18920
    past_1year_2001 = (df2001['year_death'] < 2000)
    df2001 = df2001.loc[~past_1year_2001, ]

    df = pd.concat([df2011, df2001])
    df.reset_index(inplace=True)

    df['age'] = df['age'].astype(float)

    return df


def format_ages(df):
    """Format age groups."""
    df.loc[df['year_id'] == 2011, 'age'] = df['age1']
    df.loc[(df['year_id'] == 2011) & (df['age'].isna()),
           'age'] = df['age2']
    df.drop(columns=['age1', 'age2'], inplace=True)
    df['age_unit'] = df.age_unit.astype(str)

    df.loc[df['age_unit'].str.contains('months'), 'age_unit'] = 'month'
    df.loc[df['age_unit'].str.contains('days'), 'age_unit'] = 'day'
    df.loc[df['age_unit'].str.contains('years'), 'age_unit'] = 'year'
    df.loc[(df['year_id'] == 2011) & (df['age_unit'] == 'nan'),
           'age_unit'] = 'day'

    age_formatter = ages.PointAgeFormatter()
    df = age_formatter.run(df)

    year_2001 = df['year_id'] == 2001
    year_2011 = df['year_id'] == 2011
    female = df['sex_id'] == 2
    maternal_age = df['age_group_id'].between(7, 14)
    df = df.loc[(year_2001 & female & maternal_age) | year_2011, ]

    assert df['age_group_id'].notnull().values.all()

    return df


def map_causes(df, CS_IDs):
    BGD_map = get_cause_map(CS_IDs[0])
    icd10_map = get_cause_map(CS_IDs[1])

    df2011 = df.loc[df['year_id'] == 2011, ]
    df2011 = df2011.merge(BGD_map, how='left', on='value')
    assert df2011['code_id'].notnull().values.all()

    df2001 = df.loc[df['year_id'] == 2001, ]
    df2001 = df2001.merge(icd10_map, how='left', on='value')
    assert df2001['code_id'].notnull().values.all()

    df = pd.concat([df2011, df2001])
    df.reset_index(inplace=True)

    return df


def multiply_weights(df):
    df['weight'] = df['weight'] / 1000000
    df['deaths'] = df['deaths'] * df['weight']

    return df


def format_source():
    """Format source."""

    df = read_clean()

    df.loc[df['sex'] == 'male', 'sex_id'] = 1
    df.loc[df['sex'] == 'female', 'sex_id'] = 2
    df.loc[df['sex'].isna(), 'sex_id'] = 9
    assert df['sex_id'].isin([1, 2, 9]).values.all()

    df = format_ages(df)

    CS_IDs = [713, 2]
    df = map_causes(df, CS_IDs)

    df['data_type_id'] = 8
    df['representative_id'] = 1
    df['location_id'] = 161
    df['site'] = ''

    df['deaths'] = 1
    df = multiply_weights(df)

    for col in INT_COLS:
        df[col] = df[col].astype(int)

    df = df[FINAL_FORMATTED_COLS]

    assert df.notnull().values.all()
    df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

    locals_present = finalize_formatting(df, SOURCE, PROJ_ID, write=WRITE)
    nid_meta_df = locals_present['nid_meta_df']

    if WRITE:
        nid_extracts = nid_meta_df[
            ['nid', 'extract_type_id']
        ].drop_duplicates().to_records(index=False)
        for nid, extract_type_id in nid_extracts:
            nid = int(nid)
            extract_type_id = int(extract_type_id)
            update_nid_metadata_status(PROJ_ID, nid, extract_type_id, is_active=IS_ACTIVE,
                                       is_mort_active=IS_MORT_ACTIVE)


if __name__ == '__main__':
    format_source()