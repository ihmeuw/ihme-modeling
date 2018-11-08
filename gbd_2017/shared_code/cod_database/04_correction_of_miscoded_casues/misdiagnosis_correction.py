import os

import pandas as pd
import numpy as np
from scipy.special import logit

from configurator import Configurator
from cod_prep.downloaders import (add_code_metadata,
                                  get_clean_package_map_for_misdc,
                                  get_cause_map)
from cod_prep.utils import clean_icd_codes, print_log_message


def store_intermediate_data(df, move_df, mc_process_dir,
                            adjust_id, nid, extract_type_id):

    id_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']

    df = df.groupby(['nid', 'extract_type_id', 'site_id', 'cause_id'] + id_cols,
                    as_index=False).deaths.sum()
    ss_df = get_ss(df)


    draws_df = load_dismod(df.location_id.unique().tolist(),
                           df.year_id.unique().tolist(),
                           adjust_id, 'best_draws')
    draw_cols = [d for d in list(draws_df) if 'draw_' in d]


    draws_df = draws_df[
        id_cols + draw_cols + ['est_frac', 'population', 'est_mx']
    ].merge(move_df.loc[move_df.map_id == str(adjust_id)], how='right')
    draws_df = draws_df.merge(ss_df)
    draws_df['added_scalar'] = draws_df['misdiagnosed_scaled'] / \
        (draws_df['est_frac'] * draws_df['sample_size'])
    draws_df['completeness_scalar'] = draws_df['sample_size'] / \
        (draws_df['est_mx'] * draws_df['population'])
    draws_df = pd.concat(
        [
            draws_df[id_cols + ['site_id']],
            draws_df[draw_cols].apply(
                lambda x:
                    x * draws_df['population'] *
                    draws_df['completeness_scalar'] * draws_df['added_scalar']
            ),
            draws_df[['misdiagnosed_scaled']]
        ],
        axis=1
    )

    draws_df = draws_df.merge(df.loc[df.cause_id == adjust_id], how='right')
    draws_df['deaths'] = draws_df['deaths'] - draws_df['misdiagnosed_scaled']


    draws_df['deaths_variance'] = draws_df[draw_cols].apply(
        lambda x: x + draws_df['deaths']
    ).var(axis=1)
    draws_df['deaths_mean'] = draws_df[draw_cols].apply(
        lambda x: x + draws_df['deaths']
    ).mean(axis=1)

    draws_df[draw_cols] = draws_df[draw_cols].apply(
        lambda x: logit((x + 1e-5) / (x + 1e-5 + draws_df['deaths']))
    )
    draws_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    draws_df[draw_cols] = draws_df[draw_cols].fillna(logit(1 - 1e-5))
    draws_df['logit_frac_variance'] = draws_df[draw_cols].var(axis=1)
    draws_df['logit_frac_mean'] = draws_df[draw_cols].mean(axis=1)

    # store
    draws_df = draws_df[
        id_cols + ['nid', 'extract_type_id', 'site_id',
                   'deaths_mean', 'deaths_variance',
                   'logit_frac_mean', 'logit_frac_variance']
    ]
    draws_df.to_csv("FILEPATH",
        index=False
    )

    # store deaths moved
    move_df = move_df.loc[move_df.map_id != str(adjust_id)]
    move_df['pct_moved'] = move_df['misdiagnosed_scaled'] / \
        move_df['orig_deaths']
    move_df.to_csv("FILEPATH",
        index=False
    )


def assign_packages(df, code_system_id, remove_decimal, package_dir):
    '''
    Load packages from redistribution location
    '''
    package_df = get_clean_package_map_for_misdc(code_system_id,
                                                 remove_decimal=remove_decimal)
    df = df.merge(package_df, how='left')
    df.loc[
        df.map_id.astype(str) == 'nan', 'map_id'
    ] = df.loc[df.map_id.astype(str) == 'nan', 'cause_id'].astype(str)

    return df


def add_packages(df, code_system_id, remove_decimal, package_dir):
    '''
    Assing map value to garbage based on package
    '''
    df = add_code_metadata(df, ['value'], code_system_id=code_system_id,
                           force_rerun=False, block_rerun=True)
    df['value'] = clean_icd_codes(df['value'], remove_decimal)
    df = assign_packages(df, code_system_id, remove_decimal, package_dir)
    df.drop('value', axis=1, inplace=True)
    assert len(df.loc[(df.cause_id != 743) &
                      (df.map_id.str.contains('_p_'))]) == 0, \
        'Code(s) mapped to both a cause and a package'
    bad_garbage = df.loc[(df.cause_id == 743) &
                         ~(df.map_id.str.contains('_p_'))]
    assert len(bad_garbage) == 0, \
        'Code(s) mapped to garbage but not a package: {}'.format(bad_garbage)

    return df


def get_ss(df):
    '''
    Get total deaths in dataset (by demographic group)
    '''
    df = df.groupby(['location_id', 'year_id', 'site_id',
                     'age_group_id', 'sex_id'],
                    as_index=False).deaths.sum()
    df.rename(index=str, inplace=True, columns={'deaths': 'sample_size'})

    return df


def load_dismod(location_id, year_id, adjust_id, file_name='best'):

    dm_dir = 'FILEPATH'.format(adjust_id)
    dm_files = os.listdir(dm_dir)
    dm_files = sorted(dm_files)
    assert file_name + '.h5' in dm_files, \
        'Referenced file_name not found. Files found in ' + \
        dm_dir + ': ' + ', '.join(dm_files)
    if len(location_id) < 30 and len(year_id) < 30:
        location_id = [str(int(l)) for l in location_id]
        location_id = ','.join(location_id)
        year_id = [str(int(y)) for y in year_id]
        year_id = ','.join(year_id)
        df = pd.read_hdf("FILEPATH",
            where='location_id=[{}] and year_id=[{}]'.format(location_id,
                                                             year_id)
        )
    else:
        df = pd.read_hdf("FILEPATH")
        df = df.loc[(df.location_id.isin(location_id)) &
                    (df.year_id.isin(year_id))]
    return df


def get_deficit(df, adjust_id):

    df = df.copy()
    est_df = load_dismod(df.location_id.unique().tolist(),
                         df.year_id.unique().tolist(),
                         adjust_id)

    # calculate deficit
    ss_df = get_ss(df)
    est_df = est_df.merge(ss_df)
    df = df.loc[df.cause_id == adjust_id]
    df = df.groupby(['location_id', 'year_id', 'site_id',
                     'age_group_id', 'sex_id'],
                    as_index=False).deaths.sum()
    df = df.merge(
        est_df[
            ['location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id',
             'est_frac', 'sample_size']
        ],
        how='right'
    )
    df['deaths'].fillna(0, inplace=True)

    df['deficit'] = (df['est_frac'] * df['sample_size']) - df['deaths']
    df.loc[df.deficit < 0, 'deficit'] = 0

    return df[['location_id', 'year_id', 'site_id',
               'age_group_id', 'sex_id', 'deficit']]


def add_props(df, misdiagnosis_path):
    '''
    Add misdiagnosis probabilies calculated from multiple cause data
    '''
    prop_df = pd.read_hdf(misdiagnosis_path)
    prop_df = prop_df.loc[prop_df.recode_deaths > 0]
    df = df.merge(prop_df[['cause_mapped', 'age_group_id', 'sex_id', 'ratio']],
                  left_on=['map_id', 'age_group_id', 'sex_id'],
                  right_on=['cause_mapped', 'age_group_id', 'sex_id'])
    df.drop('cause_mapped', axis=1, inplace=True)

    return df


def add_adjust_vals(df, adjust_id):
    '''
    Add row of deaths to be added to adjust cause
    '''
    adjust_df = df.groupby(
        ['location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id'],
        as_index=False
    )['misdiagnosed_scaled', 'deaths'].sum()
    adjust_df['map_id'] = str(adjust_id)
    df = df.append(adjust_df)

    return df


def apply_empirical_caps(df, adjust_id, code_system_id):
    '''
    Based on 5-star VR countries, enforce caps.
    '''
    lim_df = pd.read_csv(
        'FILEPATH'.format(
            adjust_id, code_system_id
        )
    )
    df = df.merge(lim_df, how='left')
    df.loc[df.pct_limit.isnull(), 'pct_limit'] = 0.95
    df['deaths_limit'] = df['deaths'] * df['pct_limit']
    df.loc[df.misdiagnosed_scaled > df.deaths_limit,
           'misdiagnosed_scaled'
           ] = df.loc[df.misdiagnosed_scaled > df.deaths_limit, 'deaths_limit']
    df.drop(['pct_limit', 'deaths_limit'], axis=1, inplace=True)

    return df


def get_deaths_to_move(df, adjust_id, misdiagnosis_path, mc_process_dir,
                       nid, extract_type_id, code_system_id):

    df = df.groupby(['location_id', 'year_id', 'site_id',
                     'age_group_id', 'sex_id', 'cause_id', 'map_id'],
                    as_index=False).deaths.sum()
    def_df = get_deficit(df, adjust_id)
    df = add_props(df, misdiagnosis_path)
    df['misdiagnosed'] = df['deaths'] * df['ratio']
    df['potential_misdiagnosed'] = df.groupby(
        ['location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id'],
        as_index=True
    ).misdiagnosed.transform('sum')
    df = df.merge(def_df)
    df['misdiagnosed_scaled'] = df['misdiagnosed'] * \
        (df['deficit'] / df['potential_misdiagnosed'])
    df = apply_empirical_caps(df, adjust_id, code_system_id)
    df = df[['location_id', 'year_id', 'site_id', 'age_group_id', 'sex_id',
             'map_id', 'misdiagnosed_scaled', 'deaths']]

    df = add_adjust_vals(df, adjust_id)

    df.rename(index=str, inplace=True, columns={'deaths': 'orig_deaths'})

    return df


def merge_on_scaled(df, move_df, adjust_id, code_system_id):

    df = df.merge(
        move_df[
            ['location_id', 'year_id', 'site_id', 'age_group_id',
             'sex_id', 'map_id', 'misdiagnosed_scaled']
        ],
        how='outer'
    )
    if len(df.loc[df.cause_id.isnull()]) > 0:
        assert all(df.loc[df.cause_id.isnull(),
                          'map_id'].values == str(adjust_id)), 'Other missing map_ids'
        cs_map = get_cause_map(code_system_id=code_system_id,
                               force_rerun=False, block_rerun=True)
        possible_codes = cs_map.loc[cs_map.cause_id == adjust_id,
                                    'code_id'].values
        use_target = True
        if len(possible_codes) == 0:
            possible_codes = cs_map.loc[
                cs_map.cause_id == 919, 'code_id'
            ].values
            use_target = False
        target_code = possible_codes[0]
        df.loc[df.code_id.isnull(), 'code_id'] = target_code
        if use_target:
            df.loc[df.cause_id.isnull(), 'cause_id'] = adjust_id
        else:
            df.loc[df.cause_id.isnull(), 'cause_id'] = 919
        df['deaths'].fillna(0, inplace=True)
        for exravar in ['nid', 'extract_type_id']:
            df[exravar].fillna(method='pad', inplace=True)
        for idvar in [i for i in list(df) if i.endswith('_id')] + ['nid']:
            if df[idvar].dtype == 'float64':
                df[idvar] = df[idvar].astype(int)

    return df


def death_jumble(df, move_df, adjust_id, code_system_id):

    df = merge_on_scaled(df, move_df, adjust_id, code_system_id)
    df['cause_total'] = df.groupby(
        ['location_id', 'year_id', 'site_id',
         'age_group_id', 'sex_id', 'map_id'],
        as_index=False
    ).deaths.transform('sum')
    df['misdiagnosed_scalar'] = df.apply(
        lambda x:
        (x['cause_total'] + x['misdiagnosed_scaled']) / x['cause_total']
        if x['map_id'] == str(adjust_id) and x['cause_total'] > 0
        else
        (
            (x['cause_total'] - x['misdiagnosed_scaled']) / x['cause_total']
            if x['map_id'] != str(adjust_id)
            else
            0
        ),
        axis=1
    )
    df['misdiagnosed_scalar'].fillna(1, inplace=True)
    df['deaths'] = df['deaths'] * df['misdiagnosed_scalar']
    df.loc[
        (df.map_id == str(adjust_id)) & (df.cause_total == 0), 'deaths'
    ] = df.loc[
        (df.map_id == str(adjust_id)) & (df.cause_total == 0),
        'misdiagnosed_scaled'
    ]
    df = df.groupby(['nid', 'extract_type_id', 'site_id',
                     'location_id', 'year_id', 'age_group_id', 'sex_id',
                     'code_id', 'cause_id'],
                    as_index=False).deaths.sum()

    return df


def correct_misdiagnosis(df, nid, extract_type_id, code_system_id,
                         adjust_id, remove_decimal):

    conf = Configurator('standard')
    mc_process_dir = conf.get_directory('mc_process_data')
    package_dir = conf.get_directory('rd_process_inputs') + "FILEPATH"
    misdiagnosis_path = conf.get_resource('misdiagnosis_prob_path')
    if adjust_id == 543:
        misdiagnosis_version_id = 4
    elif adjust_id == 544:
        misdiagnosis_version_id = 3
    elif adjust_id == 500:
        misdiagnosis_version_id = 3

    misdiagnosis_path = misdiagnosis_path.format(
        adjust_id=adjust_id,
        version_id=misdiagnosis_version_id,
        code_system_id=code_system_id
    )


    start_deaths = df['deaths'].sum()
    start_deaths_target = df.loc[df.cause_id == adjust_id, 'deaths'].sum()
    start_deaths_cc = df.loc[df.cause_id == 919, 'deaths'].sum()


    df = df.loc[df.deaths > 0]


    print_log_message("Adding packages")
    df = add_packages(df, code_system_id, remove_decimal, package_dir)
    print_log_message("Getting deaths to move")
    move_df = get_deaths_to_move(df, adjust_id, misdiagnosis_path,
                                 mc_process_dir, nid, extract_type_id,
                                 code_system_id)
    print_log_message("Jumbling up deaths")
    df = death_jumble(df, move_df, adjust_id, code_system_id)


    print_log_message("Checking deaths jumbled well")
    end_deaths = df['deaths'].sum()
    end_deaths_target = df.loc[df.cause_id == adjust_id, 'deaths'].sum()
    end_deaths_cc = df.loc[df.cause_id == 919, 'deaths'].sum()

    assert abs(int(start_deaths) - int(end_deaths)) <= 5, \
        'Bad jumble - added/lost deaths ' \
        '(started: {}, ended: {})'.format(str(int(start_deaths)),
                                          str(int(end_deaths)))


    print_log_message("Storing intermediate data")
    store_intermediate_data(df, move_df, mc_process_dir, adjust_id,
                            nid, extract_type_id)

    print_log_message(
        'Deaths moved: ' + str(int((end_deaths_target + end_deaths_cc) -
                                   (start_deaths_target + start_deaths_cc)))
    )

    return df
