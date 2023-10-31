"""
Purpose: Launch the air_pmhap PAF calculation workflow using Jobmon (guppy version)
"""

from jobmon.client.api import Tool, ExecutorParameters # import Jobmon
from db_queries import get_location_metadata # import shared functions
import numpy as np
import pandas as pd
import getpass
import os
import sys

def tool_air_pmhap_paf():
    """
    This workflow consists of 3 phases: calc_cataract, calc_bwga, and calc_rr_paf.
    To run these phases we will:
        1. create tool and workfow
        2. create task template
        3. define executor parameters for task
        4. create task by specifying task template (which is created at step 2)
        5. add tasks to workflow
        6. run workflow

    To Run:
        with jobmon installed in your conda environment from the root of the repo, run:
           $ python FILEPATH.py
    """

    # versioning
    output_version = VERSION

    # define variables
    years = [1990,1995,2000,2005,2010,2015,2019,2020] # estimation years
    hap_map_date = "DATE"
    hap_stgpr_runid = ID
    draws_required = 1000
    exposure_grid_version = "ID"
    rr_version = "ID"
    squeeze_version = ID

    rshell = "FILEPATH/execRscript.sh"

    locs = get_location_metadata(location_set_id=35, gbd_round_id=7)
    locations = pd.DataFrame(data=locs)
    locations = locations[locations["most_detailed"]==1]
    locations = locations["location_id"].unique().tolist()

    # define filepaths
    script_path = "FILEPATH"
    home_dir = "FILEPATH"



    # create a tool, workflow and set executor
    air_pmhap_tool = Tool.create_tool(name="air_pmhap_paf_tool")
    """
    Only call this when you explicitly want to create a new version of your tool 
    (when you have done an overhaul of your workflow or want to indicate widespread change for the tool). 
    We do not recommend creating a new version for every run because that will make it hard to relate similar runs.

    air_pmhap_tool = Tool(name="air_pmhap_paf_tool")
    air_pmhap_tool.create_new_tool_version()
    """
    workflow = air_pmhap_tool.create_workflow(name="air_pmhap_paf_workflow")
    workflow.set_executor(
        executor_class="SGEExecutor",
        project="PROJECT",  # specify your team's project
        stderr="FILEPATH",
        stdout="FILEPATH"
    )

    # create templates

    template_cataract = air_pmhap_tool.get_task_template(
        template_name = "calc_cataract",
        command_template = "{rshell} "
                           "{script} "
                           "{output_version} "
                           "{draws_required} "
                           "{year_id} "
                           "{hap_stgpr_runid} ",
        node_args = ["year_id"],
        task_args = ["output_version","draws_required","hap_stgpr_runid"],
        op_args = ["rshell", "script"]
    )
    template_bwga= air_pmhap_tool.get_task_template(
        template_name = "calc_bwga",
        command_template = "{rshell} "
                           "{script} "
                           "{location_id} "
                           "{year_id} "
                           "{exposure_grid_version} "
                           "{rr_version} "
                           "{output_version} "
                           "{draws_required} "
                           "{hap_stgpr_runid} "
                           "{hap_map_date} "
                           "{squeeze_version} ",
        node_args = ["year_id","location_id"],
        task_args = ["exposure_grid_version","rr_version","output_version","draws_required","hap_stgpr_runid","hap_map_date","squeeze_version"],
        op_args = ["rshell", "script"]
    )
    template_rr_paf = air_pmhap_tool.get_task_template(
        template_name = "calc_rr_paf",
        command_template = "{rshell} "
                           "{script} "
                           "{location_id} "
                           "{year_id} "
                           "{exposure_grid_version} "
                           "{rr_version} "
                           "{output_version} "
                           "{draws_required} "
                           "{hap_stgpr_runid} "
                           "{threads} "
                           "{hap_map_date} "
                           "{outcome_row} "
                           "{squeeze_version} ",
        node_args = ["location_id","year_id","outcome_row"],
        task_args = ["exposure_grid_version", "rr_version","output_version","draws_required","hap_stgpr_runid","threads","hap_map_date","squeeze_version"],
        op_args = ["rshell", "script"]
    )



    # create task
    task_all_list = []

    # tasks for calculate cataract phase
    task_cataract_list = []

    executor_parameters_cataract = ExecutorParameters(
        m_mem_free="20G",
        num_cores=5,
        queue="long.q",
        max_runtime_seconds=8 * 60 * 60
    )

    for year_id in years:
        task = template_cataract.create_task(
            executor_parameters = executor_parameters_cataract,
            name = f"air_paf_cataract_{year_id}",
            upstream_tasks = [],
            max_attempts = 3,
            rshell = rshell,
            script = f"-s {script_path}01_calc_cataract.R",
            output_version = output_version,
            draws_required = draws_required,
            year_id = year_id,
            hap_stgpr_runid = hap_stgpr_runid
        )
        # append task to workflow and the list
        task_all_list.append(task)
        task_cataract_list.append(task)


    # tasks for calculate bwga shifts phase
    task_bwga_list = []

    out_bwga_dir = f"FILEPATH/{output_version}/"
    os.makedirs(out_bwga_dir, exist_ok=True)
    bwga_files = os.listdir(out_bwga_dir)

    for year_id in years:
        for location_id in locations:

            # check to see if we need to run the job or if it has finished
            if f"{location_id}_{year_id}.csv" not in bwga_files:

                # profile the job's resources based on file size
                if year_id in [YEARS]:
                    size = os.path.getsize(f"FILEPATH/{exposure_grid_version}/FILEPATH/{location_id}_YEAR.fst")
                else:
                    size = os.path.getsize(f"FILEPATH/{exposure_grid_version}/FILEPATH/{location_id}_{year_id}.fst")

                mem = .22*size/1e6 + 4
                time = 5*60*60

                executor_parameters_bwga = ExecutorParameters(
                    m_mem_free=f"{mem}G",
                    num_cores=2,
                    queue="long.q",
                    max_runtime_seconds=time
                )

                task = template_bwga.create_task(
                    executor_parameters = executor_parameters_bwga,
                    name = f"air_bwga_{location_id}_{year_id}",
                    upstream_tasks = [],
                    max_attempts = 3,
                    rshell = rshell,
                    script = f"-s {script_path}02_bw_ga_shifts.R",
                    location_id = location_id,
                    year_id = year_id,
                    exposure_grid_version = exposure_grid_version,
                    rr_version = rr_version,
                    output_version = output_version,
                    draws_required = draws_required,
                    hap_stgpr_runid = hap_stgpr_runid,
                    hap_map_date = hap_map_date,
                    squeeze_version = squeeze_version
                )
                task_all_list.append(task)
                task_bwga_list.append(task)


    # tasks for calc_rr_paf phase
    task_rr_paf_list = []

    outcomes = pd.read_csv(f"FILEPATH.csv")
    n_outcomes = outcomes.shape[0]

    status_dir = f"{home_dir}/FILEPATH/{output_version}/"
    os.makedirs(status_dir, exist_ok=True)
    finished_files = os.listdir(status_dir)

    summary_dir = f"FILEPATH/{output_version}/"
    os.makedirs(summary_dir, exist_ok=True)

    for year_id in years:
        for outcome_row in range(1,(n_outcomes+1)):
            print(f"Working on year {year_id}, row {outcome_row} of {n_outcomes}...")

            for location_id in locations:

                # check to see if we need to run the job or if it has finished
                if f"{location_id}_{year_id}_{outcome_row}.csv" not in finished_files:

                    # profile the job's resources based on file size
                    if year_id in [YEARS]:
                        size = os.path.getsize(
                            f"FILEPATH/{exposure_grid_version}/FILEPATH/{location_id}_YEAR.fst")
                    else:
                        size = os.path.getsize(
                            f"FILEPATH/{exposure_grid_version}/FILEPATH/{location_id}_{year_id}.fst")

                    mem = .7 * size/1e6 + 4
                    threads = 2

                    executor_parameters_rr_paf = ExecutorParameters(
                        m_mem_free=f"{mem}G",
                        num_cores=2,
                        queue="long.q",
                        max_runtime_seconds=1400 * 60
                    )

                    task = template_rr_paf.create_task(
                        executor_parameters = executor_parameters_rr_paf,
                        name = f"air_paf_{location_id}_{year_id}",
                        upstream_tasks = [],
                        max_attempts = 3,
                        rshell = rshell,
                        script = f"-s {script_path}03_calc_rr_paf.R",
                        location_id = location_id,
                        year_id = year_id,
                        exposure_grid_version = exposure_grid_version,
                        rr_version = rr_version,
                        output_version = output_version,
                        draws_required = draws_required,
                        hap_stgpr_runid = hap_stgpr_runid,
                        threads = threads,
                        hap_map_date = hap_map_date,
                        outcome_row = outcome_row,
                        squeeze_version = squeeze_version
                    )
                    task_all_list.append(task)
                    task_rr_paf_list.append(task)


    # add tasks to workflow
    workflow.add_tasks(task_all_list)

    print("Launching workflow...")
    print(f"There are {len(task_all_list)} tasks...")

    # run workflow
    wfr = workflow.run()

    """
    Use this if you want to resume a workflow that already started:
    wfr = workflow.run(resume=True)
    """

    if wfr.status == "D":
        print("The workflow succeeded:)")
    else:
        raise RuntimeError("The workflow failed...")

if __name__ == '__main__':
    tool_air_pmhap_paf()