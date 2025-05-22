
import pandas as pd
import numpy as np
from pathlib import Path

j_input_dir = 'FILEPATH'


def check_and_obtain_if_needed(dictionary, iso3, surveyyear):
    '''
    Takes an existing dictionary of iso3 and survey years and checks if the passed
    iso3/surveyyear exists
    '''
    keys = [x for x in dictionary.keys()]
    iso3_surveyyear = iso3 + '_' + str(surveyyear)
    if iso3_surveyyear in keys:
        return dictionary[iso3_surveyyear]
    else:
        return None


def needs_province_name_map(iso3, surveyyear, filepath):
    dictionary = {
        'AFG_2010': ['qhclust', 'qprov'],
        'BGD_2001': ['qcluster', 'qdistri'],
        'BRA_1996': ['v001', 'sstate'],
        'COG_2005': ['V001', 'sdepar'],
        'COG_2011': ['v001', 'v024'],
        'ERI_1995': ['v001', 'v024'],
        'GMB_2013': ['v001', 'v024'],
        'GMB_2019': ['v001', 'v024'],
        'JOR_1997': ['v001', 'sgovern'],
        'SDN_1989': ['v001', 'sprov'],
        'STP_2008': ['V001', 'sdistr'],
        'MRT_2000': ['v001', 'swila'],
        'MRT_2019': ['v001', 'v024'],
        'PNG_2016': ['v001', 'sprovince'],
    }
    loc_cluster_columns = check_and_obtain_if_needed(dictionary, iso3, surveyyear)
    if loc_cluster_columns is not None:
        if Path(j_input_dir + 'FILEPATH'.format(iso3, surveyyear)).is_file():
            return None
        else:
            prov_clstr_map = pd.read_stata(filepath, columns=loc_cluster_columns)
            prov_clstr_map = prov_clstr_map.drop_duplicates()
            prov_clstr_map.columns = prov_clstr_map.columns.str.lower()
            prov_clstr_map.to_csv(
                j_input_dir + 'FILEPATH'.format(iso3, surveyyear),
                index=False
            )
    else:
        return None
    

def needs_index_col_renames(df, iso3, surveyyear):
    dictionary = {
        'AFG_2010': {
            'qhclust': 'v001',
            'qweight': 'v005',
            'qintmg': 'v006',
            'qintyg': 'v007',
            'q102m': 'v009',
            'q102yg': 'v010',
            'q102cg': 'v011',
            'q103': 'v012',
            'qprov': 'v024',
        },
        'BGD_2001': {
            'qcluster': 'v001',
            'qweight': 'v005',
            'qintm': 'v006',
            'qinty': 'v007',
            'q105m': 'v009',
            'q105y': 'v010',
            'q105c': 'v011',
            'q106': 'v012',
            'qdistri': 'v024',
            'q201a': 'mmc1',
        },
        'BRA_1996': {
            'v024': 'loc_placeholder',
            'sstate': 'v024'
        },
        'COG_2005': {
            'v024': 'region',
            'sdepar': 'v024'
        },
        'GHA_2007': {
            'qcluster': 'v001',
            'qweight': 'v005',
            'qintm': 'v006',
            'qinty': 'v007',
            'q103m': 'v009',
            'q103y': 'v010',
            'q103c': 'v011',
            'q104': 'v012',
            'q801': 'mmc1',
        },
        'GHA_2017': {
            'qhclust': 'v001',
            'qweight': 'v005',
            'qintm': 'v006',
            'qinty': 'v007',
            'q105m': 'v009',
            'q105y': 'v010',
            'q105c': 'v011',
            'q106': 'v012',
        },
        'IDN_1997': {
            'v024': 'loc_placeholder',
            'sprov': 'v024',
        },
        'JOR_1997': {
            'v024': 'loc_placeholder',
            'sgovern': 'v024',
        },
        'SDN_1989': {
            'v024': 'loc_placeholder',
            'sprov': 'v024',
        },
        'STP_2008': {
            'v024': 'loc_placeholder',
            'sdistr': 'v024',
        },
        'MRT_2000': {
            'v024': 'loc_placeholder',
            'swila': 'v024',
        },
        'PNG_2016': {
            'v024': 'loc_placeholder',
            'sprovince': 'v024'
        }
    }
    index_remap_dict = check_and_obtain_if_needed(dictionary, iso3, surveyyear)
    if index_remap_dict is not None:
        df.rename(columns=index_remap_dict, inplace=True)
    
    if 'caseid' not in df.columns:
        df['caseid'] = df.index.astype(str)

    if iso3 == 'IDN':
        if surveyyear == 1994:
            df['v024'] = df['v001'].astype(str).str[0:2]
        df['v024'] = df['v024'].astype('int64')
        df['sregmun'] = df['sregmun'].astype('int64')
        df['v024'] = (df['v024'] * 100) + df['sregmun']

    return df


def needs_maternal_column_renames(df, iso3, surveyyear):
    dictionary = {
        'AFG_2010': {
            '^q605_': 'mm1_',
            '^q606_': 'mm2_',
            '^q607_': 'mm3_',  
            '^q607c_': 'mm4_',
            '^q610_': 'mm6_',
            '^q611_': 'mm7_' ,
            '^q610c_': 'mm8_',
            '^q614_': 'mm9_',
        },
        'BGD_2001': {
            '^q205_': 'mm1_',
            '^q206_': 'mm2_',
            '^q207_': 'mm3_',
            '^q207c_': 'mm4_',
            '^q208_': 'mm6_',
            '^q209_': 'mm7_',
            '^q208c_': 'mm8_',
            '^q210_': 'mm9_',
        },
        'GHA_2007': {
            'q805_': 'mm1_',
            'q806_': 'mm2_',
            'q807_': 'mm3_',
            'q807c_': 'mm4_',
            'q808_': 'mm6_',
            'q809_': 'mm7_',
            'q808c_': 'mm8_',
            'q810_': 'mm9_',
        },
        'GHA_2017': {
            'q814_': 'mm1_',
            'q815_': 'mm2_',
            'q816c_': 'mm4_',
            'q816_': 'mm3_',
            'q817_': 'mm6_',
            'q818_': 'mm7_',
            'q817c_': 'mm8_',
            'q819_': 'mm9_'
        },
        'SDN_1989': {
            's803_': 'mm1_',
            's804_': 'mm2_',
            's805_': 'mm3_',
            's807_': 'mm6_',
            's808_': 'mm7_',
            's810_': 'mm9_',
        }
    }
    maternal_rename_dict = check_and_obtain_if_needed(dictionary, iso3, surveyyear)
    if maternal_rename_dict is not None:
        for key, value in maternal_rename_dict.items():
            df.columns = df.columns.str.replace(key, value, regex=True)

    return df


def maternal_col_value_remaps(df, iso3, surveyyear, index_end):
    ''''''
    iso3_surveyyear = iso3 + '_' + str(surveyyear)
    adj_type1 = ['AFG_2010']
    adj_type2 = {
        'BGD_2001': {'q211': 3, 'q212': 6},
        'GHA_2007': {'q811': 3, 'q812': 6},
        'GHA_2017': {'q820': 3, 'q821': 6},
        'SDN_1989': {'s811': 6}
    }

    mm9 = df.columns.str.startswith('mm9')
    mm2 = df.columns.str.startswith('mm2')
    mm15_cols = [col for col in df.columns if 'mm15' in col]
    mm6_cols = [col for col in df.columns if 'mm6' in col]
    mm9_cols = [col for col in df.columns if 'mm9' in col]

    if (iso3_surveyyear in adj_type1) or (iso3_surveyyear in adj_type2.keys()):
        mm9_value_dict = {
            2: 1,
            1: 2,
        }
        df.loc[:, mm9] = df.loc[:, mm9].replace(mm9_value_dict)

    if iso3_surveyyear in adj_type2.keys():
        for key, value in adj_type2[iso3_surveyyear].items():
            for index in range(1, 10):
                df.loc[
                    (df['mm9_0' + str(index)].isin([1, np.nan])) &
                    (df[key + '_0' + str(index)] == 1), 
                    'mm9_0' + str(index)
                ] = value
            for index in range(10, index_end):
                df.loc[
                    (df['mm9_' + str(index)].isin([1, np.nan])) &
                    (df[key + '_' + str(index)] == 1), 
                    'mm9_' + str(index)
                ] = value

    if (iso3_surveyyear in adj_type1) or (iso3_surveyyear in adj_type2.keys()):
        df.loc[:, mm2] = df.loc[:, mm2].replace({2: 0})

    if (iso3_surveyyear in adj_type1) or (iso3_surveyyear in adj_type2.keys()):
        df[mm15_cols] = surveyyear - df[mm6_cols]
        df.loc[df[mm6_cols].isin([98, 99]).all(1), mm15_cols] = 99

    return df


def fill_in_missing_cols(df, iso3, surveyyear, index_end):
    adjust_dict = {
        'SDN_1989': ['mm4_', 'mm5_', 'mm8_']
    }
    iso3_surveyyear = iso3 + '_' + str(surveyyear)

    if (iso3_surveyyear in adjust_dict):
        missing_cols = adjust_dict[iso3_surveyyear]
        for col in missing_cols:
            col_index1 = [col + '0' + str(x) for x in range(0, 10)]
            col_index2 = [col + str(x) for x in range(10, index_end)]
            df[col_index1 + col_index2] = np.nan

    return df


def convert_calendar_years(df, iso3, surveyyear):
    '''Calendar adjustments, so far only for AFG and NPL'''

    iso3_surveyyear = iso3 + '_' + str(surveyyear)
    persian_convert_list = ['AFG_2010']
    npl_convert_list = ['NPL_2006', 'NPL_2016']

    mm4_cols = [col for col in df.columns if 'mm4' in col]
    mm8_cols = [col for col in df.columns if 'mm8' in col]
    mm15_cols = [col for col in df.columns if 'mm15' in col]
    index_year_cols = ['v007', 'v008', 'v011']

    if iso3_surveyyear in persian_convert_list:
        df[mm4_cols + mm8_cols] = df[mm4_cols + mm8_cols] + 255

    if iso3_surveyyear in npl_convert_list:
        seconds_change = -(56*365*24*3600 + 8*30*24*3600 + 15*24*3600)
        df[index_year_cols + mm4_cols + mm8_cols] = \
            (df[index_year_cols + mm4_cols + mm8_cols]*30*24*3600 + seconds_change) / (30*24*3600)
        df['v007'] = ((df['v008'] - 1)/12) + 1900

    return df


def drop_mmc1_others(df, iso3, surveyyear):
    if 'mmc1' in df.columns:
        df = df.loc[~df['mmc1'].isna(), ]

    if (iso3 == 'CIV') and (surveyyear == 2005):
        df = df.loc[df['aidsex'] != 1, ]

    return df


def all_corrections_check(df, iso3, surveyyear, index_end):
    df = needs_index_col_renames(df, iso3, surveyyear)
    df = needs_maternal_column_renames(df, iso3, surveyyear)
    df = fill_in_missing_cols(df, iso3, surveyyear, index_end)
    df = maternal_col_value_remaps(df, iso3, surveyyear, index_end)
    df = convert_calendar_years(df, iso3, surveyyear)
    df = drop_mmc1_others(df, iso3, surveyyear)

    return df

