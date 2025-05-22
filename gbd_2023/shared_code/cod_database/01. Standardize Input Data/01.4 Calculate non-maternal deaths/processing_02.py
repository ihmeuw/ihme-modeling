from pathlib import Path

import pandas as pd
import numpy as np
from functools import partial, reduce

from cod_prep.downloaders import get_cause_map, get_cod_ages, add_location_metadata, get_current_location_hierarchy, add_location_metadata
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.utils.formatting import ages
from cod_prep.utils import print_log_message, report_if_merge_fail
from cod_prep.claude.configurator import Configurator

CONF = Configurator()


def calculate_gk_sample_weights(df):
    eligible_respondent = df.loc[(df['age_group_id'].between(8, 14)) & (df['if_alive'] == 1), ]
    eligible_respondent['si'] = 1
    eligible_respondent = eligible_respondent.groupby('caseid', as_index=False)['si'].sum()
    df = df.merge(eligible_respondent, on='caseid', how='left')
    df.loc[df['si'].isna(), 'si' ] = 1
    df['gkwt'] = 1 / df['si']
    df['cmb_wt'] = df['gkwt'] * df['v005'] / 1000000
    df['normalized_weights'] = df['cmb_wt']

    return df


def determine_maternal_deaths(df):
    df = df.loc[df['deaths'] == 1, ]
    df.loc[df['sib_death_pregnancy'].isin([2, 3, 4, 5, 6]), 'maternal_deaths'] = df['deaths']
    df.loc[df['maternal_deaths'].isna(), 'maternal_deaths'] = 0

    df['deaths'] = df['deaths'] * df['normalized_weights']
    df['maternal_deaths'] = df['maternal_deaths'] * df['normalized_weights']

    return df


def year_age_drops(df):
    df = df.loc[df['yob'] < df['surveyyear'], ]
    df = df.loc[df['yod'] < df['surveyyear'], ]

    df = df.loc[(df['yod'] >= (df['surveyyear'] - 15)), ]

    df = df.loc[df['age_group_id'].between(8, 14), ]

    df.rename(columns={'yod': 'year_id'}, inplace=True)

    return df


def create_location_ids(df):

    iso3 = df['iso3'].unique().item()
    lh = get_current_location_hierarchy()
    loc_type = lh.loc[(lh['most_detailed'] == 1) & (lh['iso3'] == iso3), 'location_type'].unique().item()

    if loc_type == 'admin0':
        df['location_id'] = lh.loc[(lh['most_detailed'] == 1) & (lh['iso3'] == iso3), 'location_id'].unique().item()
    elif iso3 in ['BRA', 'IDN']:
        dfs = []
        for surveyyear in df['surveyyear'].unique():
            ydf = df.loc[df['surveyyear'] == surveyyear, ]
            ydf = merge_location_ids_data(ydf, iso3, surveyyear)
            dfs.append(ydf)
        df = pd.concat(dfs)
    else:
        df = merge_location_ids_gps(df, iso3)

    return df


def merge_location_ids_gps(df, iso3):

    loc_map = pd.read_csv(
        'FILEPATH'
    )
    loc_map.rename(columns={'loc_id': 'location_id', 'DHSCLUST': 'v001'}, inplace=True)
    loc_map = add_location_metadata(loc_map, 'iso3', release_id=9, location_set_id=137, location_set_version_id=1143)
    df = df.merge(
        loc_map[['location_id', 'iso3', 'v001']].drop_duplicates(),
        on=['v001', 'iso3'],
        how='left'
    )
    df = add_location_metadata(
        df, ['parent_id', 'location_name'], release_id=9, location_set_id=137, location_set_version_id=1143
    )

    if iso3 == 'ETH':
        df = ethiopia_remaps(df)

    unknowns = df.loc[df['location_id'].isna(), ]
    df = df.loc[~df['location_id'].isna(), ]
    if len(unknowns) != 0:
        print_log_message(
            'Redistributing ' + str(len(unknowns) / (len(df) + len(unknowns))) +
            ' percent of unknown location rows across all known locations of the country' 
        )
        all_locs = df[['location_id', 'parent_id']].drop_duplicates()
        all_locs['index'] = 1
        all_locs['total'] = len(all_locs)
        unknowns['index'] = 1
        unknowns.drop(columns=['location_id', 'parent_id'], inplace=True)
        all_locs = all_locs.merge(unknowns, how='outer', on='index')
        all_locs['deaths'] = all_locs['deaths'] / all_locs['total']
        all_locs['maternal_deaths'] = all_locs['maternal_deaths'] / all_locs['total']
        all_locs.drop(columns=['index', 'total'], inplace=True)
        df = pd.concat([df, all_locs])

    df.drop(columns=['location_id', 'location_name'], inplace=True)
    df.rename(columns={'parent_id': 'location_id'}, inplace=True)
    df = add_location_metadata(df, ['location_name', 'most_detailed'])
    assert df['location_name'].notnull().values.all()
    assert (df['most_detailed'] == 1).values.all()
    assert df['location_id'].notnull().values.all()

    return df


def merge_on_regency_prov_codes(df, iso3, surveyyear):
    idn_kab_map = pd.read_csv(
        'FILEPATH'
    )
    idn_kab_map = idn_kab_map.loc[(idn_kab_map['source'] == 'DHS'), ]
    idn_kab_map.rename(columns={'kab_code': 'v024', 'prov_mapped': 'prov_num'}, inplace=True)
    deaths_start = df['maternal_deaths'].sum()

    dfs = []
    for idn_surveyyear in [2012, 2007, 2002, 1997, 1994]:
        idn_kab_map_yr = idn_kab_map.loc[(idn_kab_map['source_year'] == idn_surveyyear), ]
        df = df.merge(idn_kab_map_yr[['prov_num', 'v024']], on='v024', how='left')
        success = df.loc[~df['prov_num'].isna(), ]
        dfs.append(success)
        df = df.loc[df['prov_num'].isna(), ]
        if len(df) == 0:
            break
        df.drop(columns=['prov_num'], inplace=True)
    df = pd.concat(dfs)
    assert np.isclose(deaths_start, df['maternal_deaths'].sum())

    df = df.loc[df['prov_num'] != 54, ]

    idn_prov_map = pd.read_excel(
        'FILEPATH'
    )
    df = df.merge(idn_prov_map[['location_name', 'prov_num']], how='left', on='prov_num')

    return df


def ethiopia_remaps(df):

    sw_admin2 = [
        'Bench Maji',
        'Dawro',
        'Keffa',
        'Sheka',
        'Debub Omo',
        'Konta'
    ]
    df.loc[df['location_name'].isin(sw_admin2), 'parent_id'] = 94364
    df.loc[df['parent_id'] == 60916, 'parent_id'] = 95069

    return df


def merge_location_ids_data(df, iso3, surveyyear):
    if iso3 == 'IDN':
        df = merge_on_regency_prov_codes(df, iso3, surveyyear)
    else:
        cluster_prov_name_map = pd.read_csv(
            'FILEPATH'
            .format(iso3, surveyyear)
        )
        cluster_prov_name_map.columns = ['v001', 'location_name']
        df = df.merge(cluster_prov_name_map, on='v001', how='left')
    assert df['location_name'].notnull().values.all(), 'Some clusters could not merge location names'
    lh = get_current_location_hierarchy()
    if iso3 in ['BRA', 'IDN']:
        level = 4
    lh = lh.loc[(lh['iso3'] == iso3) & (lh['level'] == level), ]
    lh['location_name'] = lh['location_name'].str.lower()
    lh = lh[['location_id', 'location_name']]
    df['location_name'] = df['location_name'].str.lower()
    if iso3 == 'PNG':
        df['location_name'] = df['location_name'].str.replace(' province', '')
    df = df.merge(lh, on='location_name', how='left')
    nl_df = df.loc[df['location_id'].isna(), ]
    df = df.loc[~df['location_id'].isna(), ]
    names = df['location_name'].unique().tolist()
    nl_df = dhs_prov_renames(nl_df, iso3, names, lh)
    nl_df = nl_df.merge(lh, on='location_name', how='left')
    df = pd.concat([df, nl_df])
    if not df['location_id'].notnull().values.any():
        print('These location_names have no IDs, please fix and merge or else they all get redistributed')
        print(df.loc[df['location_id'].isna(), ['location_name']].drop_duplicates())

    return df


def dhs_prov_renames(nl_df, iso3, names, lh):
    iso3_renames_dict = {
        'AFG': {
            'sari pul': 'sar-e-pul',
            'urozgan': 'uruzgan',
            'helmand': 'hilmand'
        },
        'BGD': {
            'gazirpur': 'gazipur',
            'naratanganj': 'narayanganj',
            'brahmanbaria': 'brahamanbaria',
            'chanpur': 'chandpur',
            'netrokona': 'netrakona',
            'jhenaidaha': 'jhenaidah',
            'siraganj': 'sirajganj',
            'maulvi bazar': 'maulvibazar'
        },
        'BRA': {
            'para': 'pará',
            'maranhao': 'maranhão',
            'ceara': 'ceará',
            'paraiba': 'paraíba',
            'espirito santo': 'espírito santo',
            'sao paulo': 'são paulo',
            'parana': 'paraná',
            'goias': 'goiás',
            'rondonia': 'rondônia',
            'amapa': 'amapá'
        },
        'COG': {
            'cuvette ouest': 'cuvette-ouest',
            'cuvette - ouest': 'cuvette-ouest',
            'pointe-noire': 'point-noire',
        },
        'ERI': {
            'central': 'maekel',
            'southern': 'debub',
            'gash - barka': 'gash barka',
            'southern red sea': 'debubawi keih bahri',
            'northern red sea': 'semenawi Keih bahri'
        },
        'GMB': {
            'janjanbureh': 'central river',
            'mansakonko': 'lower river',
            'kerewan': 'north bank',
            'basse': 'upper river',
            'brikama': 'western',
        },
        'IDN': {
            'baru': 'kota baru',
            'labuhan batu': 'labuhanbatu',
            'binjai': 'kota binjai',
            'banyuasin': 'banyu asin',
            'yogyakarta': 'kota yogyakarta',
            'pangkajene kepulauan': 'pangkajene dan kepulauan',
            'selayar': 'kepulauan selayar',
            'polewali mamasa': 'polewali mandar',
            'medan': 'kota medan',           
        },
        'JOR': {
            'tafielah': 'tafiela'
        },
        'SDN': {
            's. kordofan': 'south kurdufan',
            'gazeira': 'al jazirah',
            'n. darfur': 'north darfur',
            's. darfur': 'south darfur',
            'n. kordofan': 'north kurdufan'
        },
        'STP': {
            'príncipe': 'pagué'
        },
        'MRT': {
            'hogh charghi': 'hodh ech chargui',
            'hodh gharbi': 'hodh el gharbi',
            'guidimagha': 'guidimaka',
            'tiris zemmou': 'tiris zemmour',
            'taganti': 'tagant',
            'nouadhibou': 'dakhlet nouadhibou',
        },
        'PNG': {
            'autonomous region of bougainville': 'bougainville',
            'northern (oro)': 'oro',
            'chimbu (simbu)': 'chimbu',
            'west sepik (sandaun)': 'sandaun',
        }
    }
    if iso3 not in iso3_renames_dict.keys():
        print(nl_df['location_name'].sort_values().unique())
        raise Exception(
            "There are location strings in the raw data that do not match GBD hierarchy \
            Please update or fix dhs_location_renames and/or iso3_rename_dict for " + iso3
        )
    dictionary = iso3_renames_dict[iso3]
    nl_df.drop(columns=['location_id'], inplace=True)
    nl_df['location_name'] = nl_df['location_name'].map(dictionary)

    return nl_df


def process_dhs_02(df):
    df = calculate_gk_sample_weights(df)
    df = determine_maternal_deaths(df)
    df = year_age_drops(df)
    df = create_location_ids(df)

    return df


