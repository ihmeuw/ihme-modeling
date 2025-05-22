"""
Author: 
Date: 
Purpose: Launch the air_no2 PAF calculation workflow using Jobmon (guppy version)
"""

from jobmon.client.api import Tool # import Jobmon
from jobmon.client.swarm.workflow_run import WorkflowRun
from jobmon.client.tool import Tool
from db_queries import get_location_metadata # import shared functions
import numpy as np
import pandas as pd
import getpass
import os
import sys
import uuid
import pyreadr
import math

def tool_air_no2_paf():
    """
    This workflow consists of 2 phases: calc_rr_paf and resave_large_locs.
    To run these phases we wilL:
        1. create tool and workfow
        2. create task template
        3. define executor parameters for task
        4. create task by specifying task template (which is created at step 2)
        5. add tasks to workflow
        6. run workflow

    To Run:
        with jobmon installed in your conda environment from the root of the repo, run:
           $ python FILEPATH/paf/air_no2_workflow.py
    """

    # versioning

    output_version = 23 # GBD 2023 with updated inputs (rescaled GBD 2021 inputs for most years;)

    years = [1990,1995,2000] + list(range(2005, 2024))
    grid_version = "12" # 8 is the final version for GBD 2020
    rr_version = 22
    draws_required = 250 # 1000

    rshell = "FILEPATH/execRscript.sh"

    """
    We can't use the normal GBD location set anymore because we made custom split files.
        locs = get_location_metadata(location_set_id=35, gbd_round_id=7)
        locations = pd.DataFrame(data=locs)
        locations = locations[locations["most_detailed"]==1]
        locations = locations["location_id"].unique().tolist()
    """

    # define locations to run

    locations = os.listdir(f"FILEPATH/draws/")
    locations = [x[:-9] for x in locations] 
    # locations = [x[:-13] for x in locations] # for COVID counterfactual ("_covid_cf.csv")
    locations = np.unique(locations)
    locations = ["349"] # rerunning Greenland with Iceland data

    
    # define filepaths
    script_path = "FILEPATH"
    home_dir = "FILEPATH"
    scratch_dir = "FILEPATH"


    # create a tool, workflow, and set executor
    air_no2_tool = Tool(name="air_no2_paf_tool")
    """
    Only call this when you explicitly want to create a new version of your tool 
    (when you have done an overhaul of your workflow or want to indicate widespread change for the tool). 
    We do not recommend creating a new version for every run because that will make it hard to relate similar runs.

    air_no2_tool = Tool(name="air_no2_paf_tool")
    air_no2_tool.create_new_tool_version()
    """
    
    workflow = air_no2_tool.create_workflow(
        name="air_no2_paf_workflow",
        default_cluster_name = "slurm",
        default_compute_resources_set = {
                "slurm": {
                "queue": "all.q",
                "cores": 5,
                "memory": "80G",
                "runtime": "6:00:00",
                "stdout": scratch_dir,
                "stderr": scratch_dir,
                "project": "proj_erf"}
        })

    # create templates

    template_rr_paf = air_no2_tool.get_task_template(
        template_name = "calc_rr_paf",
        default_compute_resources={
            "queue": "all.q",
            "cores": 5,
            "memory": "20G",
            "runtime": "8h",
            "stdout": scratch_dir,
            "stderr": scratch_dir,
            "project": "proj_erf"
        },
        default_cluster_name="slurm",
        command_template = "PYTHONPATH= OMP_NUM_THREADS=10 {rshell} "
                           "{script} "
                           "{location_id} "
                           "{year_id} "
                           "{grid_version} "
                           "{rr_version} "
                           "{output_version} "
                           "{draws_required} ",
        node_args = ["location_id","year_id"],
        task_args = ["grid_version","rr_version","output_version","draws_required"],
        op_args = ["rshell", "script"]
    )

    # template_resave = air_no2_tool.get_task_template(
    #     template_name = "resave_large_locs",
    #     default_compute_resources={
    #         "queue": "all.q",
    #         "cores": 5,
    #         "memory": "20G",
    #         "runtime": "8h",
    #         "stdout": scratch_dir,
    #         "stderr": scratch_dir,
    #         "project": "proj_erf"
    #     },
    #     default_cluster_name="slurm",
    #     command_template = "PYTHONPATH= OMP_NUM_THREADS=10 {rshell} "
    #                        "{script} "
    #                        "{location_id} "
    #                        "{year_id} "
    #                        "{grid_version} "
    #                        "{output_version} "
    #                        "{draws_required} ",
    #     node_args = ["location_id","year_id"],
    #     task_args = ["grid_version","output_version","draws_required"],
    #     op_args = ["rshell", "script"]
    # )



    # create tasks
    task_all_list = []

    status_dir = f"FILEPATH"
    os.makedirs(status_dir, exist_ok=True)
    finished_files = os.listdir(status_dir)

    # tasks for calc_rr_paf phase
    task_rr_paf_list = []

    for year_id in years:
        for location_id in locations:

            # check to see if we need to run the job or if it has finished
            if (f"{location_id}_{year_id}.csv" not in finished_files) and (f"{location_id.split('_')[0]}_{year_id}.csv" not in finished_files) :

           
                
                size = os.path.getsize(
                    f"FILEPATH/{grid_version}/draws/{location_id}_{year_id}.csv")

                mem = math.ceil(2*size/1e6 + 4)
                if mem > 730:
                    mem = 730 # cap at 730 G so jobs can get scheduled

                time = math.ceil(3*size/1e6 + 40) # time in minutes
                time = math.ceil(time/60) # time in hours

                task = template_rr_paf.create_task(
                    compute_resources={
                        "queue": "all.q",
                        "cores": 20,
                        "memory": f"{mem}G",
                        "runtime": f"{time}h",
                        "stdout": scratch_dir,
                        "stderr": scratch_dir,
                        "project": "proj_erf"
                    },
                    name = f"no2_paf_{location_id}_{year_id}",
                    upstream_tasks = [],
                    max_attempts = 3,
                    rshell = rshell,
                    script = f"-s {script_path}01_calc_rr_paf.R",
                    location_id = location_id,
                    year_id = year_id,
                    grid_version = grid_version,
                    rr_version = rr_version,
                    output_version = output_version,
                    draws_required = draws_required
                )
                task_all_list.append(task)
                task_rr_paf_list.append(task)

  


    # add tasks to workflow
    print("Adding tasks to workflow...")
    workflow.add_tasks(task_all_list)

    # run workflow
    print("Launching workflow...")
    wfr = workflow.run()

    """
    Use this if you want to resume a workflow that already started:
    wfr = workflow.run(resume=True)
    """

    if wfr == "D":
        print("The workflow succeeded:)")
    else:
        raise RuntimeError("The workflow failed...")

if __name__ == '__main__':
    tool_air_no2_paf()
