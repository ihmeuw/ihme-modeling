'''
    Description: Launch the non-fatal COVID pipeline using Jobmon to manage script dependencies
    Contributors: 

    example call:

    python run_nf_covid_prep.py \
        --output-version 2022-01-12.01 \
        --input-data-version 2022_01_05.06 \
        --definition who \
        --location-set-id 35 \
        --release-id 7  \
        --estimation-years 2022 2021 2022
        
'''
import getpass
import argparse
import sys
import os
import timeit
from db_queries import get_location_metadata, get_age_metadata
from datetime import datetime
import logging

from utils.utils import (
     finalize_metadata,
     get_core_ref,
     init_metadata,
     roots
)
from run.run_nf_covid import RunNfCovid

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(asctime)s: %(message)s', 
    datefmt='%d-%b-%y %H:%M:%S'
)

def send_email(job_name, wf_id, exit_code, logs_loc):
    ''' Convenience function to send workflow completion email

    Arguments:
        job_name : str
            Name of the output version, assigned to Jobmon workflow args
        wf_id : int
            The workflow ID of the Jobmon run
        exit_code : int
            Code that relays the success or failure of the pipeline run
        logs_loc : str
            Filepath to the jobmon error and output logs
    '''
    # Put together message and command
    message = ('echo "Workflow {} (wf_id: {}) has completed '.format(job_name, wf_id),
               'with exit code {}.\n\n'.format(exit_code),
               'Check out logs for more runtime info: {}\n\n'.format(logs_loc),
               '\n\nThis is an automated message from an unmonitored server. ',
               'Do not reply to this email."')
    command = 'mail -s "Workflow Finished" {}@uw.edu'.format(getpass.getuser())

    # Send command to CLI (and capture output to mute command)
    mute = os.system('{} | {}'.format(''.join(message), command))
    del mute


def main(output_version, input_data_version, definition, location_set_id,
        release_id, estimation_years, save_to_db, db_description, mark_as_best,
        save_incidence):
    ''' Generate, compile, and launch NF COVID pipeline
    Arguments:
        output_version : str
            Name of the output version the data is being written to
        input_data_version : str
            The hospitalizations and infections input data version
        definition : str
            How the acute phase is defined
        location_set_id : int
            The location set to run the pipeline on
        release_id : int
            The release/gbd round id to use for shared functions
        estimation_years : list[int]
            List of estimation years (starts after 2019)
        db_description : str
             Description to add to the database if saving run to Epi db
        save_to_db : bool
            Whether to save to Epi db
        mark_as_best : bool
            Whether to mark run as best if saving to Epi db
        save_incidence : bool
            Whether to save measure 6 for all MEs
    '''

    logging.info(('\n\n  ###########################\n'
                '  #### RUN NF COVID PREP ####\n'
                '  ###########################\n\n'))

    logging.info(
        f"Using {get_core_ref('hsp_icu_input_date')} as hospitalizations and "
        "infections input date."
    )
    logging.info(
        f"Using {input_data_version} as infections input version.")

    start = timeit.default_timer()

    # Set job name
    job_name = f"nf_covid_{output_version}"

    # Create the log output file directory
    date = output_version.split('.')[0]
    logs_loc = f"{roots['jobmon_logs_base']}{date}/{job_name}/"

    os.makedirs(logs_loc)

    hsp_icu_input_path =  (
        f"{roots['ROOT']}FILEPATH"
        f"{input_data_version}/FILEPATH"
    )
    infect_death_input_path = (
        f"{roots['ROOT']}FILEPATH"
        f"{input_data_version}/FILEPATH"
    )

    # locations
    location_df = get_location_metadata(
        location_set_id=location_set_id, release_id=release_id
        )[['location_id', 'location_name', 'level',  'most_detailed',
        'is_estimate']]
    location_df = location_df[location_df.most_detailed == 1]
    locs = location_df.location_id.values

    # ages
    age_df = get_age_metadata(release_id=release_id)
    age_groups = age_df.age_group_id.values
    age_groups = ','.join(map(str, age_groups))

    # years for pipeline
    estimation_years = estimation_years.replace(' ', '')

    gbd_estimation_years = get_core_ref('gbd_estimation_years')
    gbd_estimation_years = ','.join(map(str, gbd_estimation_years)) 
    gbd_estimation_years = gbd_estimation_years.replace(' ', '')

    all_gbd_estimation_years = gbd_estimation_years + ',' + estimation_years

    # testing single location
    # locs = [99]

    # Generate metadata file
    init_metadata(job_name, logs_loc, output_version)


    # Initialize nonfatal covid jobmon pipeline
    pipeline = RunNfCovid(
        output_version,
        job_name,
        locs,
        logs_loc,
        age_groups,
        location_set_id,
        input_data_version,
        hsp_icu_input_path,
        infect_death_input_path,
        definition,
        release_id,
        estimation_years,
        all_gbd_estimation_years,
        db_description,
        mark_as_best,
        save_incidence,
    )

    logging.info(f"Creating jobmon workflow for job {job_name}")
    pipeline.create_workflow()

    logging.info(f"Creating task templates for all parts of pipeline")
    pipeline.create_task_template()

    logging.info(f"Creating the short and long covid tasks")
    pipeline.create_short_long_tasks()

    if save_to_db:
        logging.info(f"Creating the tasks for short covid save results")
        pipeline.create_short_save_results_tasks()

        logging.info(f"Creating the tasks for long covid save results")
        pipeline.create_long_save_results_tasks()

    # logging.info(f"Creating the tasks for diagnostics")
    # pipeline.create_diagnostics_tasks()

    # Run the workflow
    exit_code = pipeline.run()


    # Stop the timer and wrap up the metadata file
    stop = timeit.default_timer()

    finalize_metadata(start, stop, exit_code, logs_loc, pipeline.wf.workflow_id)

    # Send email to launcher
    send_email(job_name, pipeline.wf.workflow_id, exit_code, logs_loc)


    # Print success or failure to command prompt
    if exit_code == 0:
        logging.info((
            f"\nWorkflow for workflow_id {pipeline.wf.workflow_id} completed"
            " successfully! You're awesome! Have a great day!"))
    else:
        logging.info((
            f"Workflow for workflow_id {pipeline.wf.workflow_id} failed midway."
            f" Check Jobmon DB, QPID, or error logs in {logs_loc}"))



if __name__ == "__main__":
 
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--output-version",
        type=str,
        required=True,
        help=("What name to label the data output version. "
            "Formatted by: yyyy-mm-dd.<version run> e.g 2022-03-29.05")
    )
    parser.add_argument(
        "--input-data-version",
        type=str,
        required=True,
        help=("The hospitalizations and infections input data version. "
            "Formatted by: yyyy_mm_dd.<version run> e.g 2022_03_25.06")
    )
    parser.add_argument(
        "--definition",
        type=str,
        required=True,
        choices=['who', 'gbd', '12mo'],
        help="How the acute phase is defined. Options are 'who' or 'gbd'."
    )
    parser.add_argument(
        "--location-set-id",
        type=int,
        required=True,
        help="The location set to run the pipeline on e.g 35 for modeling locs"
    )
    parser.add_argument(
        "--release-id",
        type=int,
        required=True,
        help="The release/gbd round id to use for shared functions e.g 6"
    )
    parser.add_argument(
        "--estimation-years", type=int, required=True, nargs="+",
        help="List of estimation years (starts after 2019)"
    )
    parser.add_argument(
        "--save-to-db", action="store_true",
        help="Whether to save to Epi db."
    )
    parser.add_argument(
        "--db-description",
        type=str,
        required=False,
        help="Description to add to the database if saving run to Epi db."
    )
    parser.add_argument(
        "--mark-as-best", action="store_true",
        help="Whether to mark run as best if saving to Epi db."
    )
    parser.add_argument(
        "--save-incidence", action="store_true",
        help=("Whether to save measure 6 for all MEs. Defaults to only saving "
            "measure 6 for the asymp ME and not for any other.")
    )

    args = parser.parse_args()

    estimation_years = str(args.estimation_years)[1:-1]

    # if db description is passed in on the command line, use that as
    # description, else use output version as description
    if args.db_description:
        db_description = "Run " + args.output_version + " " + args.db_description
    else:
        db_description = "Run " + args.output_version

    main(args.output_version, args.input_data_version, args.definition, 
            args.location_set_id, args.release_id, estimation_years,
            args.save_to_db, db_description, args.mark_as_best, 
            args.save_incidence)
