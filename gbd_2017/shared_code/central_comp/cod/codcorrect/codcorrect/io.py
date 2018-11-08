import os
import sys

import pandas as pd
import logging
import subprocess
import gc

from transmogrifier.cod import draws
from transmogrifier.directories import DirectoryNotFoundException
from codcorrect.error_check import check_data_format


def read_hdf_draws(draws_filepath, location_id, key="draws", filter_sexes=None,
                   filter_ages=None, filter_years=None):
    """Read in model draws: read in CODEm/custom model draws from a given
    filepath and filter by location_id."""
    # Get data
    where_clause, filter_location = handle_bad_index(draws_filepath, key,
                                                     location_id)
    dfs = []
    for chunk in pd.read_hdf(draws_filepath, key=key, where=where_clause,
                             chunksize=20000):
        # Filter if necessary
        if filter_location and 'location_id' in chunk.columns:
            chunk = chunk.loc[chunk['location_id'].isin(filter_location)]
        if filter_sexes and 'sex_id' in chunk.columns:
            chunk = chunk.loc[chunk['sex_id'].isin(filter_sexes)]
        if filter_ages and 'age_group_id' in chunk.columns:
            chunk = chunk.loc[chunk['age_group_id'].isin(filter_ages)]
        if filter_years and 'year_id' in chunk.columns:
            chunk = chunk.loc[chunk['year_id'].isin(filter_years)]
        dfs.append(chunk)
        del chunk
        gc.collect()
    data = pd.concat(dfs)
    # Return data
    return data


def handle_bad_index(draws_filepath, key, location_id):
    '''for some reason the summary dataset in envelope.h5 doesnt have location
    id in its index, despite being saved in same way as draws dataset'''
    where_clause = ["location_id=={location_id}"
                    .format(location_id=location_id)]
    try:
        pd.read_hdf(draws_filepath, key=key, where=where_clause, stop=1000)
    except ValueError:
        # we need to filter locations in memory
        return (None, set([location_id]))
    # we can filter location in where_clause
    return (where_clause, None)


def import_cod_model_draws(model_version_id, location_id, cause_id, sex_id,
                           required_columns, filter_years=None, db_env=None,
                           envelope=None):
    """Import model draws from CODEm/custom models.

    Read in CODEm/custom model draws from a given filepath (filtered by a
    specific location_id) and then check to make sure that the imported draws
    are not missing any columns and do not have null values.
    """
    logger = logging.getLogger('io.import_cod_model_draws')
    try:
        data = draws(cause_id, lids=[int(location_id)], sids=[int(sex_id)],
                     yids=filter_years, status=model_version_id, n_draws='max',
                     db_env=db_env)
    except DirectoryNotFoundException:
        data = draws(cause_id, lids=[int(location_id)], sids=[int(sex_id)],
                     yids=filter_years, status=model_version_id, n_draws='max',
                     db_env='prod')
    except Exception:
        logger.exception("Failed to read" + '/n' +
                         'Problem demographics were mvid {} cause {}, '
                         'location {}, sex {}, and years {}'
                         .format(model_version_id, cause_id, location_id,
                                 sex_id, ','.join(str(y) for y in filter_years)
                                 ))
        sys.exit(1)
    data = data[data.age_group_id.isin(list(range(2, 22)) + [30, 31, 32, 235])]
    data = add_envelope(data, envelope)
    r = check_data_format(data, required_columns)
    if not r:
        print(model_version_id, r)
        return None
    data = data.loc[:, required_columns]
    return data


def add_envelope(df, env):
    if 'envelope' in df or env is None:
        return df
    merged = df.merge(env, how='left')
    assert merged['envelope'].notnull().all(), 'missing envelope'
    return merged


def read_scalars(df, locations, years):
    """Read in regional scalars.

    Returns:
        df with scalar as an added column
    """
    root_dir = 'FILEPATH'
    folders = os.listdir(root_dir)
    folders = filter(lambda a: 'archive' not in a, folders)
    folders = [int(f) for f in folders]
    folders.sort()
    inner_folder = int(folders[-1])
    all_files = ['{rd}/{inf}/{loc}_{yr}_scaling_pop.dta'.format(
        rd=root_dir, inf=inner_folder, loc=location, yr=year)
        for location in locations for year in years]
    from multiprocessing import Pool
    pool = Pool(20)
    scalar = pool.map(pd.read_stata, all_files)
    scalars = pd.concat(scalar).reset_index(drop=True)
    df = df.merge(scalars,
                  on=['location_id', 'year_id', 'age_group_id', 'sex_id'],
                  how='inner')
    return df


def read_envelope_draws(draws_filepath, location_id, filter_sexes=None,
                        filter_ages=None, filter_years=None, key='draws'):
    """Read in envelope file draws.

    Read in envelope draws from a given filepath and filter by location_id.
    """
    data = read_hdf_draws(draws_filepath, location_id, key=key,
                          filter_sexes=filter_sexes,
                          filter_ages=filter_ages,
                          filter_years=filter_years)
    return data


def save_hdf(data, filepath, key='draws', mode='w', format='table',
             data_columns=None):
    if data_columns:
        data = data.sort_values(data_columns).reset_index(drop=True)
    data.to_hdf(filepath, key, mode=mode, format=format,
                data_columns=data_columns)


def change_permission(folder_path, recursively=False):
    if recursively:
        change_permission_cmd = ['chmod',
                                 '-R', '775',
                                 folder_path]
    else:
        change_permission_cmd = ['chmod',
                                 '775',
                                 folder_path]
    print(' '.join(change_permission_cmd))
    subprocess.check_output(change_permission_cmd)
