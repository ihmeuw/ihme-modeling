import os

import pandas as pd
import time
import argparse
import logging
import sys

from db_queries import get_population
import gbd.constants as GBD
from gbd_outputs_versions import GBDProcessVersion
from gbd_outputs_versions.compare_version import CompareVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv

from codcorrect.core import write_json, read_json
from codcorrect.database import (get_best_model_version,
                                 get_best_envelope_version,
                                 GET_BEST_PANIC_SHOCK_MODELS)
from codcorrect.database import get_best_shock_models, new_CoDCorrect_version
from codcorrect.database import (get_cause_hierarchy_version,
                                 get_cause_hierarchy, get_cause_metadata)
from codcorrect.database import get_location_hierarchy
from codcorrect.database import get_age_weights, get_spacetime_restrictions
from codcorrect.database import (create_new_process_version,
                                 create_new_output_version_row)
from codcorrect.database import get_pred_ex, get_regional_scalars
from codcorrect.database import (add_metadata_to_process_version,
                                 activate_new_process_version)
from codcorrect.io import change_permission
from codcorrect.restrictions import expand_id_set
from codcorrect.restrictions import (get_eligible_age_group_ids,
                                     get_eligible_sex_ids)
from codcorrect.error_check import check_envelope, validate_model_types
from codcorrect.job_swarm import CoDCorrectJobSwarm
import codcorrect.log_utilities as cc_log


"""
Example usage:

python launch.py [output_version_id], where output_version_id is the number you
want to create. If output_version_id == new, the current max version + 1 will
be used.

This is the version I'm using to test:
python launch.py _test
"""


_COD_CONN_DEF_MAP = {'prod': 'cod', 'dev': 'cod-test'}
_GBD_CONN_DEF_MAP = {'prod': 'gbd', 'dev': 'gbd-test'}


def parse_args():
    """Parse command line arguments.

    Arguments are output_version_id

    Returns:
        string
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("output_version_id", type=str, default='new')
    parser.add_argument("-y", "--years", default="est",
                        help="years to correct: est(imation), full", type=str,
                        choices=["est", "full"])
    parser.add_argument("-l", "--locations", default="est",
                        help="""locations to run on: est(imation),
                              full (including SDI)""", type=str,
                        choices=["est", "full"])
    parser.add_argument("-r", "--resume", help="Resume run of CoDCorrect",
                        action="store_true")
    parser.add_argument("-upcodsum", "--upload_cod_summaries",
                        help="Upload cod summaries", action="store_true")
    parser.add_argument("-upgbdsum", "--upload_gbd_summaries",
                        help="Upload gbd summaries", action="store_true")
    parser.add_argument("-updiag", "--upload_diagnostics",
                        help="Upload diagnostics", action="store_true")
    parser.add_argument('-upgbdcollab', '--upload_gbd_collab',
                        help="Upload gbd to the collaborator db",
                        action='store_true')
    parser.add_argument("-test", "--test_upload",
                        help="Upload to test instead of prod",
                        action="store_true")
    parser.add_argument("-b", "--best",
                        help="Mark best", action="store_true")
    # Parse arguments
    args = parser.parse_args()
    output_version_id = args.output_version_id
    resume = args.resume
    upload_to_cod = args.upload_cod_summaries
    upload_to_gbd = args.upload_gbd_summaries
    upload_to_diagnostics = args.upload_diagnostics
    upload_to_concurrent = args.upload_gbd_collab
    test_upload = args.test_upload
    best = args.best
    # Determine the current CoDCorrect version
    if output_version_id == 'new':
        output_version_id = str(new_CoDCorrect_version())
    # Determine years to correct
    if args.years == "est": 
        codcorrect_years = [1990, 2007, 2017]
    elif args.years == "full":
        codcorrect_years = list(range(1980, GBD.GBD_ROUND + 1))
    # Determine location hierarchies to use
    if args.locations == "est": 
        location_set_id = [35, 40]
    elif args.locations == "full":
        SPECIAL_LOCATIONS = [3, 5, 11, 20, 24, 26, 28, 31, 32, 46]
        location_set_id = [35, 40] + SPECIAL_LOCATIONS
    if test_upload:
        db_env = DBEnv.DEV
    else:
        db_env = DBEnv.PROD
    # Print arguments
    print("output_version_id: {}".format(output_version_id))
    print("codcorrect_years: {}".format(
        ','.join([str(x) for x in codcorrect_years])))
    print("location_set_ids: {}".format(
        ','.join([str(x) for x in location_set_id])))
    print("resume: {}".format(resume))
    print("upload cod summaries: {}".format(upload_to_cod))
    print("upload gbd summaries: {}".format(upload_to_gbd))
    print("upload diagnostics: {}".format(upload_to_diagnostics))
    print("upload to collaborator db (gbd): {}".format(
        upload_to_concurrent))
    print("mark best: {}".format(best))
    print("test run: {}".format(test_upload))
    print("database environment: {}".format(db_env))
    # Return outputs
    return (output_version_id, codcorrect_years, location_set_id, resume,
            upload_to_cod, upload_to_gbd, upload_to_diagnostics, db_env,
            best, upload_to_concurrent)


def set_up_folders(output_directory, output_version_id):
    """Create a new CoDCorrect id, and then set up _temp, scalars, draws
    returns parent directory string.
    """

    new_folders = ['_temp', 'input_data', 'draws', 'shocks', 'models',
                   'unaggregated', 'unaggregated/unscaled', 'logs',
                   'unaggregated/rescaled', 'unaggregated/shocks',
                   'aggregated', 'aggregated/unscaled', 'aggregated/rescaled',
                   'aggregated/shocks', 'debug', 'diagnostics', 'summaries',
                   'summaries/gbd', 'summaries/gbd/single',
                   'summaries/gbd/multi', 'summaries/gbd/single/1',
                   'summaries/gbd/single/4', 'summaries/gbd/multi/1',
                   'summaries/gbd/multi/4', 'summaries/cod']

    for folder in new_folders:
        directory = '{d}/{v}/{f}'.format(d=output_directory,
                                         v=output_version_id, f=folder)
        if not os.path.exists(directory):
            os.makedirs(directory)
            change_permission(directory)
    change_permission('{d}/{v}'.format(d=output_directory,
                                       v=output_version_id))
    return '{d}/{v}'.format(d=output_directory, v=output_version_id)


def set_up_summary_folders(parent_dir, years, change_years):
    new_folders = []
    change_start_years = change_years[:2]
    for year in years:
        new_folders.extend(['summaries/gbd/single/1/{}'.format(year),
                            'summaries/gbd/single/4/{}'.format(year)])
    new_folders.extend(['summaries/gbd/multi/1/{}'.format(year)
                       for year in change_start_years] +
                       ['summaries/gbd/multi/4/{}'.format(year)
                       for year in change_start_years])
    for folder in new_folders:
        if not os.path.exists(os.path.join(parent_dir, folder)):
            os.makedirs(os.path.join(parent_dir, folder))
            change_permission(os.path.join(parent_dir, folder))

    if not os.path.exists(parent_dir):
        change_permission(parent_dir)
    if not os.path.exists(os.path.join(parent_dir, '_temp')):
        change_permission(os.path.join(parent_dir, '_temp'))
    if not os.path.exists(os.path.join(parent_dir, 'summaries')):
        change_permission(os.path.join(parent_dir, 'summaries'),
                          recursively=True)


def prepare_envelope(gbd_round=2017):
    # Get best envelope version
    envelope_version_id = get_best_envelope_version(gbd_round)
    print("Best envelope version: {}".format(envelope_version_id))
    # Read in file
    file_path = 'FILEPATH'
    all_files = ['{fp}/combined_env_aggregated_{yr}.h5'.format(
        fp=file_path, yr=year) for year in eligible_year_ids]
    envelope_list = []
    for f in all_files:
        envelope_list.append(pd.read_hdf(f))
    envelope = pd.concat(envelope_list)
    # Keep only what we need
    envelope = envelope[['location_id', 'year_id', 'sex_id',
                         'age_group_id'] + ['env_{}'.format(x)
                                            for x in range(1000)]]
    # Filter to just the most-detailed
    envelope = envelope.loc[
        (envelope['location_id'].isin(eligible_location_ids)) &
        (envelope['year_id'].isin(eligible_year_ids)) &
        (envelope['sex_id'].isin(eligible_sex_ids)) &
        (envelope['age_group_id'].isin(eligible_age_group_ids))
                           ].reset_index(drop=True)
    # Check envelope
    check_envelope(envelope, eligible_location_ids, eligible_year_ids,
                   eligible_sex_ids, eligible_age_group_ids)
    # Save
    print("Saving envelope draws")
    envelope = envelope.sort_values(['location_id', 'year_id', 'sex_id',
                                     'age_group_id']).reset_index(drop=True)
    envelope.to_hdf(os.path.join(parent_dir, '_temp/envelope.h5'), 'draws',
                    mode='w', format='table',
                    data_columns=['location_id', 'year_id', 'sex_id',
                                  'age_group_id'])
    # Make means and save
    print("Saving envelope and pop summaries")
    envelope['envelope'] = envelope[
        ['env_{}'.format(x) for x in range(1000)]].mean(axis=1)
    envelope = envelope[['location_id', 'year_id',
                         'sex_id', 'age_group_id', 'envelope']]
    envelope = envelope.sort_values(['location_id', 'year_id', 'sex_id',
                                     'age_group_id']).reset_index(drop=True)
    envelope.to_hdf(os.path.join(parent_dir, '_temp/envelope.h5'), 'summary',
                    mode='a', format='table',
                    data_columns=['location_id' 'year_id', 'sex_id',
                                  'age_group_id'])
    pops = []
    for lsid in location_set_ids:
        pop = get_population(age_group_id=eligible_age_group_ids,
                             location_id=-1,
                             location_set_id=lsid,
                             year_id=eligible_year_ids,
                             sex_id=eligible_sex_ids)
        pops.append(pop)
    pop = pd.concat(pops)
    pop.drop_duplicates(subset=['location_id', 'year_id',
                                'age_group_id', 'sex_id'], inplace=True)
    pop.rename(columns={'population': 'pop'}, inplace=True)
    pop = pop.sort_values(['location_id', 'year_id', 'sex_id', 'age_group_id'])
    pop_version_id = int(pop.loc[0, 'run_id'])
    pop = pop[['location_id', 'year_id', 'sex_id', 'age_group_id', 'pop']]
    pop.to_hdf(os.path.join(parent_dir + '_temp/pop.h5'), 'summary',
               mode='w', format='table',
               data_columns=['location_id', 'year_id', 'sex_id',
                             'age_group_id'])
    return envelope_version_id, pop_version_id


if __name__ == '__main__':
    # Set some core variables
    code_directory = os.path.dirname(os.path.abspath(__file__))
    output_directory = 'FILEPATH'

    # set up folders
    (output_version_id, codcorrect_years, location_set_ids, resume,
     upload_to_cod, upload_to_gbd, upload_to_diagnostics, db_env, best,
     upload_to_concurrent) = parse_args()

    parent_dir = set_up_folders(output_directory, output_version_id)
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log.setup_logging(log_dir, 'launch', time.strftime("%m_%d_%Y_%H"))

    change_years = [1990, 2007, 2017]

    if not resume:
        # Retrieve cause resources from database.
        # Uses the codcorrect cause set (1) to create the cause data and
        # metadata for the current round. Used in the correct step to rescale
        # the cause fractions down the hierarchy.
        (cause_set_version_id,
         cause_metadata_version_id) = get_cause_hierarchy_version(
            1, GBD.GBD_ROUND)
        cause_data = get_cause_hierarchy(cause_set_version_id)
        cause_metadata = get_cause_metadata(cause_metadata_version_id)
        #
        (cause_agg_set_version_id,
         cause_agg_metadata_version_id) = get_cause_hierarchy_version(
            3, GBD.GBD_ROUND)
        cause_aggregation_hierarchy = get_cause_hierarchy(
            cause_agg_set_version_id)

        # Retrieve location resources from database
        location_data = []
        for lsid in location_set_ids:
            location_data.append(get_location_hierarchy(lsid))
        location_data = pd.concat(location_data).drop_duplicates(
            subset=['location_id'])

        # Set the eligible locations, years, sexes, and ages that will appear
        # in the input data
        eligible_age_group_ids = list(range(2, 21)) + [30, 31, 32, 235]
        eligible_sex_ids = [1, 2]
        eligible_cause_ids = cause_data.loc[
            cause_data['level'] > 0, 'cause_id'].tolist()
        eligible_cause_ids = [x for x in eligible_cause_ids
                              if x not in [388, 390]]
        eligible_year_ids = list(range(1980, GBD.GBD_ROUND + 1))
        eligible_location_ids = location_data.loc[
            location_data['is_estimate'] == 1, 'location_id'].tolist()

        # Pull Space-Time (Geographic) restrictions
        spacetime_restrictions = get_spacetime_restrictions(GBD.GBD_ROUND)

        # Create a DataFrame of all eligible cause, age, sex combinations
        eligible_data = pd.DataFrame(eligible_cause_ids, columns=['cause_id'])
        eligible_data = expand_id_set(
            eligible_data, eligible_age_group_ids, 'age_group_id')
        eligible_data = expand_id_set(
            eligible_data, eligible_sex_ids, 'sex_id')

        # Add a restriction variable to the eligible DataFrame to factor in
        # age-sex restrictions of causes
        eligible_data['restricted'] = True
        for cause_id in eligible_cause_ids:
            non_restricted_age_group_ids = get_eligible_age_group_ids(
                cause_metadata[cause_id]['yll_age_start'],
                cause_metadata[cause_id]['yll_age_end'])
            non_restricted_sex_ids = get_eligible_sex_ids(
                cause_metadata[cause_id]['male'],
                cause_metadata[cause_id]['female'])
            eligible_data.loc[(eligible_data['cause_id'] == cause_id) & (
                (eligible_data['age_group_id'].isin(
                    non_restricted_age_group_ids)) &
                (eligible_data['sex_id'].isin(
                    non_restricted_sex_ids))
            ), 'restricted'] = False

        # Get a list of best models currently marked and those used in the
        # shock aggregator
        all_best_models = get_best_model_version(
            GBD.GBD_ROUND_ID)[['cause_id', 'sex_id', 'model_version_id',
                               'model_version_type_id']]
        shock_aggregator_best_models = GET_BEST_PANIC_SHOCK_MODELS(
            GBD.GBD_ROUND)[['cause_id', 'sex_id', 'model_version_id',
                            'model_version_type_id']]
        all_best_models = pd.concat(
            [all_best_models, shock_aggregator_best_models])
        codcorrect_models = all_best_models.loc[
            all_best_models['model_version_type_id'].isin(
                [0, 1, 2, 3, 4])].copy(deep=True)
        shock_models = all_best_models.loc[~all_best_models[
            'model_version_type_id'].isin([0, 1, 2, 3, 4])
            ].copy(deep=True)

        # Check against a list of causes for which we should have models
        eligible_models = eligible_data.loc[
            eligible_data['restricted'] == False,
            ['cause_id', 'sex_id']].drop_duplicates().copy(deep=True)
        codcorrect_models = pd.merge(eligible_models, codcorrect_models, on=[
                                     'cause_id', 'sex_id'], how='left')

        # Add on some metadata for use as inputs to correct.py
        codcorrect_models = pd.merge(
            cause_data[['cause_id', 'acause', 'level', 'parent_id']],
            codcorrect_models,
            on=['cause_id'])
        shock_models = pd.merge(cause_aggregation_hierarchy[
                                ['cause_id', 'acause', 'level', 'parent_id']],
                                shock_models,
                                on=['cause_id'])
        eligible_data = pd.merge(eligible_data,
                                 cause_data[
                                     ['cause_id', 'level', 'parent_id']],
                                 on=['cause_id'], how='left')

        # Make single list of best models
        best_models = pd.concat(
            [codcorrect_models, shock_models]).reset_index(drop=True)

        # Get a list of models that are missing
        for i in best_models.loc[best_models['model_version_id'].isnull()
                                 ].index:
            logging.info("Missing cause {} and sex {}"
                         .format(best_models.loc[i, 'cause_id'],
                                 best_models.loc[i, 'sex_id']))
        best_models = best_models.loc[best_models['model_version_id']
                                      .notnull()]
        # Validate model type ids
        validate_model_types(best_models, gbd_round_id=GBD.GBD_ROUND_ID)

        # Read in pred_ex from mortality database
        logging.info("Reading pred_ex from mortality database")
        pred_ex_data = get_pred_ex(gbd_round_id=GBD.GBD_ROUND_ID)

        # Read in regional scalars and cache to an .hdf
        logging.info("Reading in regional scalars for cache")
        region_scalars = get_regional_scalars(GBD.GBD_ROUND_ID)

        # Save helper files
        best_models.to_csv(
            os.path.join(parent_dir, '_temp/best_models.csv'),
            index=False)
        eligible_data.to_csv(
            os.path.join(parent_dir, '_temp/eligible_data.csv'),
            index=False)
        spacetime_restrictions.to_csv(
            os.path.join(parent_dir, '_temp/spacetime_restrictions.csv'),
            index=False)
        cause_aggregation_hierarchy.to_csv(
            os.path.join(parent_dir, '_temp/cause_aggregation_hierarchy.csv'),
            index=False)
        location_data.to_csv(
            os.path.join(parent_dir, '_temp/location_hierarchy.csv'),
            index=False)
        get_age_weights().to_csv(
            os.path.join(parent_dir, '_temp/age_weights.csv'),
            index=False)
        pred_ex_data.to_csv(
            os.path.join(parent_dir, '_temp/pred_ex.csv'),
            index=False)
        region_scalars.to_hdf(
            os.path.join(parent_dir, '_temp/region_scalars.h5'),
            mode='w', format='table', key='draws',
            data_columns=['location_id', 'year_id']
        )

        # Save envelope
        envelope_version_id, pop_version_id = prepare_envelope()

        # Create database assets before job swarm.
        logging.info("Create output_version in CoD database")
        cod_description = "New Version of CoDCorrect"
        create_new_output_version_row(output_version_id,
                                      cod_description,
                                      envelope_version_id,
                                      _COD_CONN_DEF_MAP[db_env.value])
        # Begin by creating the necessary data that must be passed to
        # gbd_outputs_versions.GBDProcessVersion.add_new_version() method.
        # This is dependent on an output version id for the current run already
        # existing in the cod database.
        logging.info("Create process_version in GBD database")
        version_note = ('CoDCorrect v{ov}, Envelope v{env}, Pop v{pop}'
                        .format(ov=output_version_id,
                                env=envelope_version_id,
                                pop=pop_version_id))

        process_version_id = create_new_process_version(
            gbd_round_id=5, gbd_process_version_note=version_note,
            conn_def=_GBD_CONN_DEF_MAP[db_env.value])

        add_metadata_to_process_version(
            process_version_id, output_version_id, envelope_version_id,
            pop_version_id, conn_def=_GBD_CONN_DEF_MAP[db_env.value])

        # Save config file
        config = {}
        config['envelope_version_id'] = int(envelope_version_id)
        config['pop_version_id'] = pop_version_id
        config['envelope_column'] = 'envelope'
        config['envelope_index_columns'] = [
            'location_id', 'year_id', 'sex_id', 'age_group_id']
        config['index_columns'] = ['location_id', 'year_id', 'sex_id',
                                   'age_group_id', 'cause_id', 'measure_id']
        config['data_columns'] = ['draw_{}'.format(x) for x in range(1000)]
        config['eligible_age_group_ids'] = eligible_age_group_ids
        config['eligible_sex_ids'] = eligible_sex_ids
        config['eligible_cause_ids'] = eligible_cause_ids
        config['eligible_year_ids'] = codcorrect_years
        config['eligible_location_ids'] = eligible_location_ids
        config['diagnostic_year_ids'] = [1990, 2005, 2017]
        config['change_years'] = change_years
        config['process_version_id'] = process_version_id

        write_json(config, parent_dir + r'/_temp/config.json')
    else:
        # Read in location data
        location_data = pd.read_csv(os.path.join(
            parent_dir, '_temp/location_hierarchy.csv'))

        # Read in config file
        config = read_json(os.path.join(parent_dir, '_temp/config.json'))

        # Read in variables
        eligible_location_ids = config['eligible_location_ids']
        envelope_version_id = config['envelope_version_id']
        pop_version_id = config['pop_version_id']
        process_version_id = config['process_version_id']
        change_years = config['change_years']

        # if eligible_year_ids do not match, then do not resume jobs
        if config['eligible_year_ids'] != codcorrect_years:
            logging.info("CoDCorrect years do not match!")
            logging.info("Can't just resume jobs")
            config['eligible_year_ids'] != codcorrect_years
            write_json(config, os.path.join(parent_dir, '_temp/config.json'))
            resume = False

    database_list = []
    if upload_to_gbd:
        database_list.append('gbd')
    if upload_to_cod:
        database_list.append('cod')
    if upload_to_diagnostics:
        database_list.append('codcorrect')

    # Set up summary files
    set_up_summary_folders(parent_dir, codcorrect_years, change_years)

    # Generate CoDCorrect jobs
    logging.info("Create CoDCorrect DAG")
    job_swarm = CoDCorrectJobSwarm(
        code_dir=code_directory,
        version_id=output_version_id,
        year_ids=codcorrect_years,
        start_years=change_years[:2],
        location_set_ids=location_set_ids,
        databases=database_list,
        db_env=db_env.value)

    job_swarm.create_shock_and_correct_jobs()
    job_swarm.create_agg_cause_jobs()
    job_swarm.create_yll_jobs()
    job_swarm.create_agg_location_jobs()
    job_swarm.create_append_shock_jobs()
    job_swarm.create_summary_jobs()
    job_swarm.create_append_diagnostic_jobs()
    job_swarm.create_upload_jobs()
    job_swarm.create_post_scriptum_upload()

    logging.info("Run CoDCorrect DAG")
    successful_run = job_swarm.run()

    if successful_run:
        logging.info("Done!")
    else:
        logging.info("There was an error running the dag, did not complete "
                     "successfully.")
        sys.exit(1)
