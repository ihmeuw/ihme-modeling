import argparse
import os
import time
import numpy as np
import pandas as pd
from db_queries import get_demographics
from transmogrifier.gopher import draws
from cause_mvid_info import pull_mvid, get_cause_ids
from test_assertions import compare_dfs
from draw_io import read_hdf_draws
from sorter import set_sort_index

def get_models(cause_set, ages, years, locations):
    #############################################
    #Pull in active oldCorrect models given a
    #cause set, a set of ages, and a set of years
    #############################################
    sources, targets = get_cause_ids(cause_set)

    source_model_codem_dfs = []
    source_model_dismod_dfs = []
    hybrid_scale_input_mvt = 8
    custom_mvt = 4
    hybrid_mvt = 3
    for cause_id in sources:
        source_models_codem = pull_mvid(cause_id, hybrid_scale_input_mvt)
        source_models_dismod = pull_mvid(cause_id, custom_mvt)
        source_ids = {'cause_ids':[cause_id]}
        #############################################
        #Pull in codem models
        #############################################
        for source_model_codem in source_models_codem:
            source_codem = draws(source_ids, source='codem',
                                  measure_ids=[1], year_ids=years,
                                  age_group_ids=ages, location_ids=locations,
                                  status=source_model_codem)
            source_codem.drop('model_version_id', axis=1, inplace=True)
            try:
                source_codem.drop('measure_id', axis=1, inplace=True)
            except:
                pass
            try:
                source_codem.drop(['envelope', 'pop'], axis=1, inplace=True)
            except:
                pass

            source_model_codem_dfs.append(source_codem)
        #############################################
        #Pull in custom/dismod models
        #############################################
        for source_model_dismod in source_models_dismod:
            source_dismod = draws(source_ids, source='codem',
                                   measure_ids=[1], year_ids=years,
                                   age_group_ids=ages, location_ids=locations,
                                   status=source_model_dismod)
            source_dismod.drop('model_version_id', axis=1, inplace=True)
            try:
                source_dismod.drop('measure_id', axis=1, inplace=True)
            except:
                pass
            try:
                source_dismod.drop(['envelope', 'pop'], axis=1, inplace=True)
            except:
                pass

            source_model_dismod_dfs.append(source_dismod)

    source_codem = pd.concat(source_model_codem_dfs)
    source_dismod = pd.concat(source_model_dismod_dfs)

    target_dfs = []
    target_nulls = []
    for cause_id in targets:
        target_models = pull_mvid(cause_id, hybrid_mvt)
        target_ids = {'cause_ids':[cause_id]}
        #############################################
        #Pull in target models
        #############################################
        for target_model in target_models:
            target = draws(target_ids, source='codem', measure_ids=[1],
                           year_ids=years, age_group_ids=ages,
                           location_ids=locations, status=target_model)
            target.drop('model_version_id', axis=1, inplace=True)
            try:
                target.drop('measure_id', axis=1, inplace=True)
            except:
                pass
            try:
                target.drop(['envelope', 'pop'], axis=1, inplace=True)
            except:
                pass
            if len(target[target.isnull().any(axis=1)]) != 0:
                target_nulls.append(cause_id)
            target_dfs.append(target)

    assert len(target_nulls) == 0, "Nulls target: %s" % target_nulls
    target_df = pd.concat(target_dfs)

    assert len(source_dismod[source_dismod.isnull().any(axis=1)]) ==\
        0, "Nulls dismod"
    assert len(source_codem[source_codem.isnull().any(axis=1)]) ==\
        0, "Nulls codem"

    return source_codem, source_dismod, target_df

def prep_envelope(temp_dir, year_id, index_cols, draw_cols, ages):
    env_path = '%s/envelope.hdf' % temp_dir
    envelope = read_hdf_draws(env_path, year_id, filter_ages=ages)

    rename_columns = {}
    for x in range(1000):
        rename_columns['env_{}'.format(x)] = 'draw_{}'.format(x)
    envelope = envelope.rename(columns=rename_columns)

    envelope.reset_index(inplace=True)
    envelope = envelope.loc[envelope['age_group_id'].isin(ages)]
    env_locs = envelope['location_id'].unique().tolist()
    envelope = envelope[index_cols + draw_cols]
    envelope = set_sort_index(envelope, index_cols)

    return envelope, env_locs

def prep_pop(temp_dir, year_id, index_cols, ages):
    pop_path = '%s/population.csv' % temp_dir
    population = pd.read_csv(pop_path)

    population = population.loc[population['age_group_id'].isin(ages)]
    population = population[index_cols + ['population']]

    return population

def create_env(df, index_cols, draw_cols):
    #############################################
    #Sum a given df by index columns
    #############################################
    if isinstance(df, list):
        df = pd.concat(df)
    df = df.reset_index()
    env = df.groupby(index_cols).sum().reset_index()
    for col in ['cause_id', 'index']:
        try:
            env.drop(col, axis=1, inplace=True)
        except:
            pass
    env = env[index_cols + draw_cols]
    env = set_sort_index(env, index_cols)
    return env

def run_scale(cause_set, upload_dir, vers, year_id):
    ###################################
    # Set constants
    ###################################
    draw_cols = ['draw_%s' % i for i in range(0,1000)]
    index_cols = ['year_id', 'sex_id', 'age_group_id', 'location_id']
    all_age = get_demographics('epi')['age_group_ids']
    ages = [15, 16, 17, 18, 19, 20, 30, 31, 32, 235]

    ###################################
    # Pull envelope and envelope
    # locations
    ###################################
    temp_dir = '%s/v%s/temps' % (upload_dir, vers)
    envelope, env_locs = prep_envelope(temp_dir, year_id, index_cols,
                                       draw_cols, ages)

    ###################################
    # Pull models, subset on age,
    # format
    ###################################
    all_codem, all_dismod, all_target = get_models(cause_set, all_age, year_id,
                                                   env_locs)
    all_codem = all_codem[index_cols + draw_cols + ['cause_id']]
    all_dismod = all_dismod[index_cols + draw_cols + ['cause_id']]
    all_target = all_target[index_cols + draw_cols + ['cause_id']]

    ###################################
    # Subset as necessary
    ###################################
    source_codem = all_codem[all_codem['age_group_id'].isin(ages)]
    source_dismod = all_dismod[all_dismod['age_group_id'].isin(ages)]
    target_df = all_target[all_target['age_group_id'].isin(ages)]

    all_target = all_target[~all_target['age_group_id'].isin(ages)]
    all_codem = all_codem[~all_codem['age_group_id'].isin(ages)]
    all_dismod = all_dismod[~all_dismod['age_group_id'].isin(ages)]

    ###################################
    # Convert everything to CF space
    ###################################
    source_codem = set_sort_index(source_codem, index_cols)
    source_dismod = set_sort_index(source_dismod, index_cols)
    target_df = set_sort_index(target_df, index_cols)

    source_codem[draw_cols] = source_codem[draw_cols].divide(
            envelope[draw_cols], axis='index')
    source_dismod[draw_cols] = source_dismod[draw_cols].divide(
            envelope[draw_cols], axis='index')
    target_df[draw_cols] = target_df[draw_cols].divide(
            envelope[draw_cols], axis='index')

    source_codem.reset_index(inplace=True)
    source_dismod.reset_index(inplace=True)
    target_df.reset_index(inplace=True)

    ###################################
    # Scale models
    ###################################
    assert len(source_dismod) > 0, "Zero length dismod"
    assert len(source_codem) > 0, "Zero length codem"
    assert len(target_df) > 0, "Zero length target"
    env1 = create_env([source_codem, target_df], index_cols, draw_cols)
    assert len(env1.loc[env1.isin([0]).any(axis=1)]) == 0, "Zero in env1"
    assert len(env1.loc[env1.isnull().any(axis=1)]) == 0, "Nulls in env1"
    dis_models = pd.concat([source_dismod, target_df])
    dis_models = set_sort_index(dis_models, index_cols)
    assert len(dis_models.loc[dis_models.isin([0]).any(axis=1)]) == 0, ("Zero "
                                                            "in dismod models")
    assert len(dis_models.loc[dis_models.isnull().any(axis=1)]) == 0, ("Nulls "
                                                            "in dismod models")
    temp_dis = create_env(dis_models, index_cols, draw_cols)
    dis_models[draw_cols] = dis_models[draw_cols].divide(temp_dis[draw_cols],
              axis='index')
    dis_models[draw_cols] = dis_models[draw_cols].multiply(env1[draw_cols],
              axis='index')
    env2 = create_env(dis_models, index_cols, draw_cols)

    ###################################
    # Check envs match post scale
    ###################################
    assert compare_dfs(env1, env2, strict=False), "Envelopes do not match"

    ###################################
    # Convert back to death space
    ###################################
    dis_models[draw_cols] = dis_models[draw_cols].multiply(
            envelope[draw_cols], axis='index')

    ###################################
    # Split models back out
    ###################################
    source_dismod_ids = source_dismod['cause_id'].unique().tolist()
    target_ids = target_df['cause_id'].unique().tolist()
    scaled_dismod = dis_models.loc[dis_models['cause_id'].isin(
                                                            source_dismod_ids)]
    scaled_dismod.reset_index(inplace=True)
    scaled_target = dis_models.loc[dis_models['cause_id'].isin(target_ids)]
    scaled_target.reset_index(inplace=True)

    ###################################
    # Put subset scaled models back
    # into complete models
    ###################################
    all_dismod.reset_index(inplace=True)
    all_dismod = pd.concat([all_dismod, scaled_dismod])
    all_target.reset_index(inplace=True)
    all_target = pd.concat([all_target, scaled_target])

    ###################################
    # Temporariliy drop duplicates.
    # Need to figure out why these
    # exist
    ###################################
    all_dismod = all_dismod.drop_duplicates(keep='first')
    all_target = all_target.drop_duplicates(keep='first')

    ###################################
    # Save in directories for
    # save_results
    ###################################
    for cause in all_target['cause_id'].unique():
        to_save = all_target.loc[all_target['cause_id'] == cause]
        save_dir = '%s/v%s/c%s' % (upload_dir, vers, int(cause))
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            time.sleep(1)
        i = 0
        for df in np.array_split(to_save, 3):
            file_name = '%s/%s_%s.csv' % (save_dir, int(year_id), i)
            df.to_csv(file_name)
            assert os.path.isfile(file_name), "Save failed"
            i += 1

    for cause in all_dismod['cause_id'].unique():
        to_save = all_dismod.loc[all_dismod['cause_id'] == cause]
        save_dir = '%s/v%s/c%s' % (upload_dir, vers, int(cause))
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
            time.sleep(1)
        i = 0
        for df in np.array_split(to_save, 3):
            file_name = '%s/%s_%s.csv' % (save_dir, int(year_id), i)
            df.to_csv(file_name)
            assert os.path.isfile(file_name), "Save failed"
            i += 1

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--cause_set",
            help="cause_set",
            dUSERt=10,
            type=int)
    parser.add_argument(
            "--upload_dir",
            help="save directory",
            dUSERt="/FILEPATH",
            type=str)
    parser.add_argument(
            "--vers",
            help="version",
            dUSERt=0,
            type=int)
    parser.add_argument(
            "--year_id",
            help="year",
            dUSERt=2010,
            type=int)
    args = parser.parse_args()
    cause_set = args.cause_set
    upload_dir = args.upload_dir
    vers = args.vers
    year_id = args.year_id

    run_scale(cause_set, upload_dir, vers, year_id)
