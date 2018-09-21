import os

import pandas as pd
import time
import argparse
import logging

from adding_machine.agg_locations import grouper
from db_tools.ezfuncs import query
from db_queries import get_population
from codcorrect.core import write_json, read_json
from codcorrect.database import (get_best_model_version,
                                 get_best_envelope_version)
from codcorrect.database import get_best_shock_models, new_CoDCorrect_version
from codcorrect.database import (get_cause_hierarchy_version,
                                 get_cause_hierarchy, get_cause_metadata)
from codcorrect.database import get_location_hierarchy
from codcorrect.database import get_age_weights, get_spacetime_restrictions
from codcorrect.database import (create_new_compare_version,
                                 create_new_process_version,
                                 add_metadata_to_process_version,
                                 add_process_version_to_compare_version_output,
                                 unmark_gbd_best, mark_gbd_best,
                                 unmark_cod_best, mark_cod_best)
from codcorrect.io import change_permission
from codcorrect.restrictions import expand_id_set
from codcorrect.restrictions import (get_eligible_age_group_ids,
                                     get_eligible_sex_ids)
from codcorrect.error_check import check_envelope, check_pred_ex
from codcorrect.submit_jobs import Task, TaskList, check_upload
import codcorrect.log_utilities as l


"""
Example usage:

python launch.py [output_version_id], where output_version_id is the number you
want to create. If output_version_id == new, the current max version + 1 will
be used.

This is the version I'm using to test:
python launch.py _test
"""


def parse_args():
    """
        Parse command line arguments

        Arguments are output_version_id

        Returns:
        string
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("output_version_id", type=str, dUSERt='new')
    parser.add_argument("-y", "--years", dUSERt="est",
                        help="years to correct: est(imation), full", type=str,
                        choices=["est", "full"])
    parser.add_argument("-l", "--locations", dUSERt="est",
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
    parser.add_argument("-test", "--test_upload",
                        help="Upload to test instead of prod",
                        action="store_true")
    parser.add_argument("-b", "--best",
                        help="Mark best", action="store_true")
    # Parse arguments
    args = parser.parse_args()
    output_version_id = args.output_version_id
    resume = args.resume
    upload_cod_summary_status = args.upload_cod_summaries
    upload_gbd_summary_status = args.upload_gbd_summaries
    upload_diagnostics_status = args.upload_diagnostics
    test_upload = args.test_upload
    best = args.best
    # Determine the current CoDCorrect version
    if output_version_id == 'new':
        output_version_id = str(new_CoDCorrect_version())
    # Determine years to correct
    if args.years == "est":
        codcorrect_years = [1990, 1995, 2000, 2005, 2006, 2010, 2016]
    elif args.years == "full":
        codcorrect_years = range(1980, 2017)
    # Determine location hierarchies to use
    if args.locations == "est":
        location_set_id = [35]
    elif args.locations == "full":
        location_set_id = [35, 40]
    # Print arguments
    print "output_version_id: {}".format(output_version_id)
    print "codcorrect_years: {}".format(','.join([str(x)
                                                  for x in codcorrect_years]))
    print "location_set_ids: {}".format(','.join([str(x)
                                                  for x in location_set_id]))
    print "resume: {}".format(resume)
    print "upload cod summaries: {}".format(upload_cod_summary_status)
    print "upload gbd summaries: {}".format(upload_gbd_summary_status)
    print "upload diagnostics: {}".format(upload_diagnostics_status)
    print "mark best: {}".format(best)
    # Return outputs
    return (output_version_id, codcorrect_years, location_set_id, resume,
            upload_cod_summary_status, upload_gbd_summary_status,
            upload_diagnostics_status, test_upload, best)


def set_up_folders(output_directory, output_directory_j, output_version_id):
    """
       Create a new CoDCorrect id, and then set up _temp, scalars, draws
       returns parent directory string
    """

    new_folders = ['LIST', 'OF', 'FILEPATHS']

    for folder in new_folders:
        directory = 'FILEPATH'
        if not os.path.exists(directory):
            os.makedirs(directory)
            change_permission(directory)
    change_permission('{d}/{v}'.format(d=output_directory,
                                       v=output_version_id))
    return '{d}/{v}'.format(d=output_directory, v=output_version_id)


def set_up_summary_folders(parent_dir, years):
    new_folders = []
    change_start_years = [1990, 2006]
    for year in years:
        new_folders.extend(['FILEPATH/{}'.format(year)])
    new_folders.extend(['FILEPATH/{}'.format(year)
                       for year in change_start_years])
    for folder in new_folders:
        if not os.path.exists(os.path.join(parent_dir, folder)):
            os.makedirs(os.path.join(parent_dir, folder))
            change_permission(os.path.join(parent_dir, folder))

    if not os.path.exists(parent_dir):
        change_permission(parent_dir)


def save_temp_data(best_models, eligible_data, parent_dir):
    """
       Save csv of best models and eligible data for use as
       inputs by other processes
    """
    best_models.to_csv(parent_dir + r'FILEPATH.csv', index=False)
    eligible_data.to_csv(parent_dir + r'FILEPATH.csv', index=False)
    return None


def prepare_envelope(locations, gbd_round=2016):
    # Get best envelope version
    envelope_version_id = get_best_envelope_version(gbd_round)
    print "Best envelope version: {}".format(envelope_version_id)
    # Read in file
    file_path = 'FILEPATH'
    all_files = ['FILEPATH_{}.h5'.format(yr=year)
                 for year in eligible_year_ids]
    envelope_list = []
    for f in all_files:
        envelope_list.append(pd.read_hdf(f, 'draws'))
    envelope = pd.concat(envelope_list)
    # Keep only what we need
    envelope = envelope[['location_id', 'year_id', 'sex_id',
                         'age_group_id'] + ['env_{}'.format(x)
                                            for x in xrange(1000)]]
    # Filter to just the most-detailed
    envelope = envelope.ix[(envelope['location_id'].isin(eligible_location_ids)
                            ) & (envelope['year_id'].isin(eligible_year_ids)
                                 ) & (envelope['sex_id'].isin(eligible_sex_ids)
                                      ) & (envelope['age_group_id']
                                           .isin(eligible_age_group_ids))
                           ].reset_index(drop=True)
    # Check envelope
    check_envelope(envelope, eligible_location_ids, eligible_year_ids,
                   eligible_sex_ids, eligible_age_group_ids)
    # Save
    print "Saving envelope draws"
    envelope = envelope.sort(['location_id', 'year_id', 'sex_id',
                              'age_group_id']).reset_index(drop=True)
    envelope.to_hdf(parent_dir + r'FILEPATH.h5', 'draws', mode='w',
                    format='table',
                    data_columns=['location_id', 'year_id', 'sex_id',
                                  'age_group_id'])
    # Make means and save
    print "Saving envelope and pop summaries"
    envelope['envelope'] = envelope[
        ['env_{}'.format(x) for x in xrange(1000)]].mean(axis=1)
    envelope = envelope[['location_id', 'year_id',
                         'sex_id', 'age_group_id', 'envelope']]
    envelope = envelope.sort(['location_id', 'year_id', 'sex_id',
                              'age_group_id']).reset_index(drop=True)
    envelope.to_hdf(parent_dir + r'FILEPATH.h5', 'summary', mode='a',
                    format='table',
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
    pop = pop.sort(['location_id', 'year_id', 'sex_id', 'age_group_id'])
    pop_version_id = int(pop.ix[0, 'process_version_map_id'])
    pop = pop[['location_id', 'year_id', 'sex_id', 'age_group_id', 'pop']]
    pop.to_hdf(parent_dir + r'FILEPATH.h5', 'summary', mode='w',
               format='table', data_columns=['location_id', 'year_id',
                                             'sex_id', 'age_group_id'])
    return envelope_version_id, pop_version_id


def calculate_slots(years, base_slots):
    """Determine number of slots to request, depending on if we're running
       estimation years or all years"""
    if len(years) > 8:
        return base_slots * 6
    else:
        return base_slots


def generate_rescale_jobs(task_list, code_directory, log_directory, locations,
                          years, resume=False):
    """ Generate one job for each location-sex for most-detailed locations """
    for location_id in locations:
        for sex_id in ["1", "2"]:
            job_name = "correct-{}-{}-{}".format(
                output_version_id, location_id, sex_id)
            job_command = ["{c}/python_shell.sh".format(c=code_directory),
                           "{c}/correct.py".format(c=code_directory),
                           "--output_version_id", str(output_version_id),
                           "--location_id", str(location_id),
                           "--sex_id", sex_id]
            job_log = ("{ld}/FILEPATH.txt"
                       .format(ld=log_directory))
            job_project = "proj_codcorrect"
            job_slots = calculate_slots(years, 3)
            job_dependencies = []
            task_list.add_task(Task(job_name, job_command, job_log,
                                    job_project,
                                    slots=job_slots, resume=resume),
                               job_dependencies)
    return task_list


def generate_shock_jobs(task_list, code_directory, log_directory, locations,
                        years, resume=False):
    """ Generate one job for each location-sex for most-detailed locations """
    for location_id in locations:
        for sex_id in ["1", "2"]:
            job_name = "shocks-{}-{}-{}".format(output_version_id,
                                                location_id, sex_id)
            job_command = ["{c}/python_shell.sh".format(c=code_directory),
                           "{c}/shocks.py".format(c=code_directory),
                           "--output_version_id", str(output_version_id),
                           "--location_id", str(location_id),
                           "--sex_id", sex_id]
            job_log = ("{ld}/FILEPATH.txt"
                       .format(ld=log_directory))
            job_project = "proj_codcorrect"
            job_slots = calculate_slots(years, 3)
            job_dependencies = []
            task_list.add_task(Task(job_name, job_command, job_log,
                                    job_project,
                                    slots=job_slots, resume=resume),
                               job_dependencies)
    return task_list


def generate_cause_aggregation_jobs(task_list, code_directory, log_directory,
                                    locations, years, resume=False):
    """ Generate one job for each location for most-detailed locations """
    for location_id in locations:
        job_name = "agg-cause-{}-{}".format(output_version_id, location_id)
        job_command = ["{c}/python_shell.sh".format(c=code_directory),
                       "{c}/aggregate_causes.py".format(c=code_directory),
                       "--output_version_id", str(output_version_id),
                       "--location_id", str(location_id)]
        job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
        job_project = "proj_codcorrect"
        job_slots = calculate_slots(years, 7)
        job_dependencies = ["{}-{}-{}-{}".format(process, output_version_id,
                                                 location_id, sex_id)
                            for sex_id in ["1", "2"]
                            for process in ["correct", "shocks"]]
        task_list.add_task(Task(job_name, job_command, job_log, job_project,
                                slots=job_slots, resume=resume),
                           job_dependencies)
    return task_list


def generate_yll_jobs(task_list, code_directory, log_directory, locations,
                      years, resume=False):
    """Generate one job for each location for most-detailed locations """
    for location_id in locations:
        job_name = "ylls-{}-{}".format(output_version_id, location_id)
        job_command = ["{c}/python_shell.sh".format(c=code_directory),
                       "{c}/ylls.py".format(c=code_directory),
                       "--output_version_id", str(output_version_id),
                       "--location_id", str(location_id)]
        job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
        job_project = "proj_codcorrect"
        job_slots = calculate_slots(years, 3)
        job_dependencies = ["agg-cause-{}-{}".format(output_version_id,
                                                     location_id)]
        task_list.add_task(Task(job_name, job_command, job_log, job_project,
                                slots=job_slots, resume=resume),
                           job_dependencies)
    return task_list


def generate_location_aggregation_jobs(task_list, code_directory,
                                       log_directory, location_set_ids,
                                       locations, years, resume=False):
    """ Generate three job for each measure: unscaled, rescaled, shocks """
    for lsid in location_set_ids:
        for measure in ["1", "4"]:
            for df_type in ['shocks', 'unscaled', 'rescaled']:
                if measure == "4" and df_type == 'unscaled':
                    continue  # don't need unscaled ylls
                for yrs in grouper(years, 2, None):
                    job_name = "agg-location-{}-{}-{}-{}-{}".format(
                        output_version_id, df_type, measure, lsid,
                        "".join(str(yr) for yr in yrs if yr is not None))
                    job_command = ["{c}/python_shell.sh".format(
                                   c=code_directory),
                                   "{c}/aggregate_locations.py".format(
                                   c=code_directory),
                                   "--output_version_id",
                                   str(output_version_id),
                                   "--df_type", df_type,
                                   "--measure_id", measure,
                                   "--location_set_id", str(lsid),
                                   "--year_ids",
                                   "{}".format(" ".join([str(yr)
                                                        for yr in yrs
                                                        if yr is not None]))]
                    job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
                    job_project = "proj_codcorrect"
                    job_slots = 20
                    if measure == "4":
                        job_dependencies = ["ylls-{}-{}".format(
                            output_version_id, location_id)
                            for location_id in locations]
                    else:
                        job_dependencies = ["agg-cause-{}-{}".format(
                            output_version_id, location_id)
                            for location_id in locations]
                    task_list.add_task(Task(job_name, job_command, job_log,
                                            job_project, slots=job_slots,
                                            resume=resume), job_dependencies)
    return task_list


def generate_append_shock_jobs(task_list, code_directory, log_directory,
                               all_locations, location_set_ids, years,
                               resume=False):
    """Generate one job for each location for all locations """
    for location_id in all_locations:
        job_name = "append-shocks-{}-{}".format(output_version_id, location_id)
        job_command = ["{c}/python_shell.sh".format(c=code_directory),
                       "{c}/append_shocks.py".format(c=code_directory),
                       "--output_version_id", str(output_version_id),
                       "--location_id", str(location_id)]
        job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
        job_project = "proj_codcorrect"
        job_slots = calculate_slots(years, 7)
        year_list = []
        for yrs in grouper(years, 2, None):
            year_list.append(''.join(str(yr) for yr in yrs if yr is not None))
        job_dependencies = ["agg-location-{}-{}-{}-{}-{}".format(
            output_version_id, job, measure, str(lsid), yrs)
            for job in ["rescaled", "shocks"]
            for measure in ["1", "4"]
            for lsid in location_set_ids
            for yrs in year_list]
        job_dependencies.extend(['agg-location-{}-unscaled-1-{}-{}'
                                 .format(output_version_id, str(lsid), yrs)
                                 for lsid in location_set_ids
                                 for yrs in year_list])
        task_list.add_task(Task(job_name, job_command, job_log, job_project,
                                slots=job_slots, resume=resume),
                           job_dependencies)
    return task_list


def generate_summary_jobs(task_list, code_directory, log_directory,
                          all_locations, resume=False):
    for location_id in all_locations:
        for db in ['cod', 'gbd']:
            job_name = "summary-{}-{}-{}".format(output_version_id,
                                                 location_id, db)
            job_command = ["{c}/python_shell.sh".format(c=code_directory),
                           "{c}/summary.py".format(c=code_directory),
                           "--output_version_id", str(output_version_id),
                           "--location_id", str(location_id),
                           "--db", db]
            job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
            job_project = "proj_codcorrect"
            if db == 'cod':
                job_slots = 15
            else:
                job_slots = 26
            job_dependencies = ["append-shocks-{}-{}".format(output_version_id,
                                                             location_id)]
            task_list.add_task(Task(job_name, job_command, job_log,
                                    job_project, slots=job_slots,
                                    resume=resume), job_dependencies)
    return task_list


def generate_append_diagnostic_jobs(task_list, code_directory, log_directory,
                                    locations, resume=False):
    job_name = "append_diagnostics-{}".format(output_version_id)
    job_command = ["{c}/python_shell.sh".format(c=code_directory),
                   "{c}/append_diagnostics.py".format(c=code_directory),
                   "--output_version_id", str(output_version_id)]
    job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
    job_project = "proj_codcorrect"
    job_slots = 18
    job_dependencies = ["append-shocks-{}-{}".format(output_version_id,
                                                     location)
                        for location in locations]
    task_list.add_task(Task(job_name, job_command, job_log, job_project,
                            slots=job_slots, resume=resume), job_dependencies)
    return task_list


def generate_upload_jobs(code_directory, log_directory, db, measure_id,
                         conn_def, change=False):
    job_name = "upload-{}-{}-{}-{}".format(output_version_id, db, measure_id,
                                           change)
    job_command = ["{c}/python_shell.sh".format(c=code_directory),
                   "{c}/upload.py".format(c=code_directory),
                   "--output_version_id", str(output_version_id),
                   "--db", str(db),
                   "--measure_id", str(measure_id),
                   "--conn_def", str(conn_def)]
    if change:
        job_command.append("--change")
    job_log = ("{ld}/FILEPATH.txt".format(ld=log_directory))
    job_project = "proj_codcorrect"
    job_slots = 10
    job = Task(job_name, job_command, job_log, job_project, slots=job_slots)
    job.submit_job()


if __name__ == '__main__':

    # Set some core variables
    code_directory = 'FILEPATH'
    output_directory = 'FILEPATH'
    output_directory_j = 'FILEPATH'

    # set up folders
    (output_version_id, codcorrect_years, location_set_ids, resume,
     upload_cod_summary_status, upload_gbd_summary_status,
     upload_diagnostics_status, test_upload, best) = parse_args()
    parent_dir = set_up_folders(output_directory, output_directory_j,
                                output_version_id)
    log_dir = parent_dir + r'/logs'

    # Start logging
    l.setup_logging(log_dir, 'launch', time.strftime("%m_%d_%Y_%H"))

    if not resume:
        # Retrieve cause resources from database
        (cause_set_version_id,
         cause_metadata_version_id) = get_cause_hierarchy_version(
            1, 2016)
        cause_data = get_cause_hierarchy(cause_set_version_id)
        cause_metadata = get_cause_metadata(cause_metadata_version_id)
        (cause_agg_set_version_id,
         cause_agg_metadata_version_id) = get_cause_hierarchy_version(3, 2016)
        cause_aggregation_hierarchy = get_cause_hierarchy(
            cause_agg_set_version_id)

        # Retrieve location resources from database
        location_data = []
        for lsid in location_set_ids:
            location_data.append(get_location_hierarchy(lsid))
        location_data = pd.concat(location_data).drop_duplicates(
            subset=['location_id'])

        # Get location & cause names
        location_name_data = query("SELECT * FROM shared.location",
                                   conn_def='cod')
        cause_name_data = query("SELECT * FROM shared.cause", conn_def='cod')
        age_name_data = query("SELECT * FROM shared.age_group", conn_def='cod')

        # Set the eligible locations, years, sexes, and ages that will appear
        # in the input data
        eligible_age_group_ids = range(2, 21) + [30, 31, 32, 235]
        eligible_sex_ids = [1, 2]
        eligible_cause_ids = cause_data.ix[
            cause_data['level'] > 0, 'cause_id'].tolist()
        eligible_year_ids = range(1980, 2017)
        eligible_location_ids = location_data.ix[
            location_data['is_estimate'] == 1, 'location_id'].tolist()

        # Pull Space-Time (Geographic) restrictions
        spacetime_restrictions = get_spacetime_restrictions(2016)

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
            eligible_data.ix[(eligible_data['cause_id'] == cause_id) & (
                (eligible_data['age_group_id'].isin(
                    non_restricted_age_group_ids)) &
                (eligible_data['sex_id'].isin(
                    non_restricted_sex_ids))
            ), 'restricted'] = False

        # Get a list of best models currently marked and those used in the
        # shock aggregator
        all_best_models = get_best_model_version(
            2016)[['cause_id', 'sex_id', 'model_version_id',
                   'model_version_type_id']]
        shock_aggregator_best_models = get_best_shock_models(
            2016)[['cause_id', 'sex_id', 'model_version_id',
                   'model_version_type_id']]
        all_best_models = pd.concat(
            [all_best_models, shock_aggregator_best_models])
        codcorrect_models = all_best_models.ix[
            all_best_models['model_version_type_id'].isin(
                [0, 1, 2, 3, 4, 10])].copy(deep=True)
        shock_models = all_best_models.ix[~all_best_models[
            'model_version_type_id'].isin([0, 1, 2, 3, 4, 10])
            ].copy(deep=True)

        # Check against a list of causes for which we should have models)
        eligible_models = eligible_data.ix[
            eligible_data['restricted'] == False,
            ['cause_id', 'sex_id']].drop_duplicates().copy(deep=True)
        codcorrect_models = pd.merge(eligible_models, codcorrect_models, on=[
                                     'cause_id', 'sex_id'], how='left')

        # Add on some metadata for use as inputs to correct.py
        codcorrect_models = pd.merge(
            cause_data[['cause_id', 'acause', 'level',
                        'parent_id']], codcorrect_models,
            on=['cause_id'])
        shock_models = pd.merge(cause_aggregation_hierarchy[
                                ['cause_id', 'acause', 'level', 'parent_id']],
                                shock_models,
                                on=['cause_id'])

        # Make single list of best models
        best_models = pd.concat(
            [codcorrect_models, shock_models]).reset_index(drop=True)

        # Get a list of models that are missing
        for i in best_models.ix[best_models['model_version_id'].isnull()
                                ].index:
            logging.info("Missing cause {} and sex {}"
                         .format(best_models.ix[i, 'cause_id'],
                                 best_models.ix[i, 'sex_id']))
        best_models = best_models.ix[best_models['model_version_id'].notnull()]

        # Save helper files
        best_models.to_csv(parent_dir + '/_temp/best_models.csv', index=False)
        eligible_data = pd.merge(eligible_data,
                                 cause_data[
                                     ['cause_id', 'level', 'parent_id']],
                                 on=['cause_id'], how='left')
        eligible_data.to_csv(
            parent_dir + 'FILEPATH.csv', index=False)
        spacetime_restrictions.to_csv(
            parent_dir + 'FILEPATH.csv', index=False)
        cause_aggregation_hierarchy.to_csv(
            parent_dir + 'FILEPATH.csv', index=False)
        location_data.to_csv(
            parent_dir + 'FILEPATH.csv', index=False)
        get_age_weights().to_csv(parent_dir + 'FILEPATH.csv',
                                 index=False)

        # Save envelope
        envelope_version_id, pop_version_id = prepare_envelope(
            eligible_location_ids)

        # Call get_life_table on arbitrary demographics just to get version
        lifetable_version_id = check_pred_ex(eligible_location_ids,
                                             eligible_year_ids,
                                             eligible_sex_ids,
                                             eligible_age_group_ids,
                                             fail=True)

        # Save config file
        config = {}
        config['envelope_version_id'] = envelope_version_id
        config['lifetable_version_id'] = lifetable_version_id
        config['pop_version_id'] = pop_version_id
        config['envelope_column'] = 'envelope'
        config['envelope_index_columns'] = [
            'location_id', 'year_id', 'sex_id', 'age_group_id']
        config['index_columns'] = ['location_id', 'year_id', 'sex_id',
                                   'age_group_id', 'cause_id']
        config['data_columns'] = ['draw_{}'.format(x) for x in xrange(1000)]
        config['eligible_age_group_ids'] = eligible_age_group_ids
        config['eligible_sex_ids'] = eligible_sex_ids
        config['eligible_cause_ids'] = eligible_cause_ids
        config['eligible_year_ids'] = codcorrect_years
        config['eligible_location_ids'] = eligible_location_ids
        config['diagnostic_year_ids'] = [
            1990, 1995, 2000, 2005, 2006, 2010, 2016]

        write_json(config, parent_dir + r'FILEPATH.json')
    else:
        # Read in location data
        location_data = pd.read_csv(
            parent_dir + 'FILEPATH.csv')

        # Read in config file
        config = read_json(parent_dir + r'FILEPATH.json')

        # Read in variables
        eligible_location_ids = config['eligible_location_ids']
        envelope_version_id = config['envelope_version_id']
        lifetable_version_id = config['lifetable_version_id']
        pop_version_id = config['pop_version_id']

        # if eligible_year_ids do not match, then do not resume jobs
        if config['eligible_year_ids'] != codcorrect_years:
            logging.info("CoDCorrect years do not match!")
            logging.info("Can't just resume jobs")
            config['eligible_year_ids'] != codcorrect_years
            write_json(config, parent_dir + r'/FILEPATH.json')
            resume = False

    # Set up summary files
    set_up_summary_folders(parent_dir, codcorrect_years)

    # Generate CoDCorrect jobs
    codcorrect_job_list = TaskList()
    codcorrect_job_list = generate_shock_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        eligible_location_ids, codcorrect_years, resume=resume)
    codcorrect_job_list = generate_rescale_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        eligible_location_ids, codcorrect_years, resume=resume)
    codcorrect_job_list = generate_cause_aggregation_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        eligible_location_ids, codcorrect_years, resume=resume)
    codcorrect_job_list = generate_yll_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        eligible_location_ids, codcorrect_years, resume=resume)
    codcorrect_job_list = generate_location_aggregation_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        location_set_ids, eligible_location_ids, codcorrect_years,
        resume=resume)
    codcorrect_job_list = generate_append_shock_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        location_data['location_id'].drop_duplicates(), location_set_ids,
        codcorrect_years, resume=resume)
    codcorrect_job_list = generate_summary_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        location_data['location_id'].drop_duplicates(), resume=resume)
    codcorrect_job_list = generate_append_diagnostic_jobs(
        codcorrect_job_list, code_directory, parent_dir + '/logs',
        location_data['location_id'].drop_duplicates(), resume=resume)

    # Run jobs
    logging.info("Checking status")
    codcorrect_job_list.update_status(resume=resume)
    while ((codcorrect_job_list.completed < codcorrect_job_list.all_jobs) and
            (codcorrect_job_list.retry_exceeded == 0)):
        if codcorrect_job_list.submitted > 0 or codcorrect_job_list.running > 0:
            time.sleep(60)
        codcorrect_job_list.update_status()
        codcorrect_job_list.run_jobs()
        logging.info("There are:")
        logging.info("    {} all jobs".format(codcorrect_job_list.all_jobs))
        logging.info("    {} submitted jobs"
                     .format(codcorrect_job_list.submitted))
        logging.info("    {} running jobs"
                     .format(codcorrect_job_list.running))
        logging.info("    {} not started jobs"
                     .format(codcorrect_job_list.not_started))
        logging.info("    {} completed jobs"
                     .format(codcorrect_job_list.completed))
        logging.info("    {} failed jobs".format(codcorrect_job_list.failed))
        logging.info("    {} jobs whose retry attempts are exceeded"
                     .format(codcorrect_job_list.retry_exceeded))
    if codcorrect_job_list.retry_exceeded > 0:
        codcorrect_job_list.display_jobs(status="Retry exceeded")
    else:
        logging.info("Preparing to upload")
        if test_upload:
            cod_conn_def = 'cod-test'
            gbd_conn_def = 'gbd-test'
        else:
            cod_conn_def = 'cod'
            gbd_conn_def = 'gbd'
        if upload_diagnostics_status:
            generate_upload_jobs(code_directory, parent_dir + '/logs',
                                 db='codcorrect', measure_id=1,
                                 conn_def='codcorrect')
        if upload_cod_summary_status:
            generate_upload_jobs(code_directory, parent_dir + '/logs',
                                 db='cod', measure_id=1, conn_def=cod_conn_def)
        if upload_gbd_summary_status:
            compare_version = create_new_compare_version(
                gbd_round_id=4,
                description=('CoDCorrect v{ccv}, LifeTable v{ltv}, '
                             'Envelope v{ev}, Pop v{pv}')
                .format(ccv=output_version_id, ltv=lifetable_version_id,
                        ev=envelope_version_id, pv=pop_version_id),
                conn_def=gbd_conn_def, status=2)
            process_version = create_new_process_version(
                gbd_round_id=4,
                gbd_process_version_note=('CoDCorrect v{ccv}, '
                                          'LifeTable v{ltv}, '
                                          'Envelope v{ev}, Pop v{pv}')
                .format(ccv=output_version_id, ltv=lifetable_version_id,
                        ev=envelope_version_id, pv=pop_version_id),
                compare_version_id=compare_version, conn_def=gbd_conn_def)
            add_metadata_to_process_version(process_version,
                                            output_version_id,
                                            lifetable_version_id,
                                            envelope_version_id,
                                            pop_version_id,
                                            gbd_conn_def)
            config['process_version_id'] = process_version
            write_json(config, parent_dir + r'FILEPATH.json')
            for measure_id in [1, 4]:
                for change in [True, False]:
                    generate_upload_jobs(code_directory, parent_dir + '/logs',
                                         db='gbd', measure_id=measure_id,
                                         conn_def=gbd_conn_def, change=change)
        tracker = TaskList()
        while True:
            qstat = tracker.qstat()
            if any('upload-{ov}'.format(ov=output_version_id)
                   in name for name in list(qstat.name.unique())):
                time.sleep(60)
            else:
                break
        if upload_gbd_summary_status:
            add_process_version_to_compare_version_output(process_version,
                                                          compare_version,
                                                          gbd_conn_def)
            if best:
                check_upload(output_version_id, upload_cod_summary_status,
                             upload_gbd_summary_status)
                logging.info("Marking gbd upload as best")
                unmark_gbd_best(output_version_id, process_version,
                                gbd_conn_def)
                mark_gbd_best(output_version_id, process_version, gbd_conn_def)
        if upload_cod_summary_status and best:
            check_upload(output_version_id, upload_cod_summary_status,
                         upload_gbd_summary_status)
            logging.info("Marking cod upload as best")
            unmark_cod_best(output_version_id, cod_conn_def)
            mark_cod_best(output_version_id, cod_conn_def)

    logging.info("Done!")
