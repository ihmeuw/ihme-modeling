"""
Author: 
Date: 12/09/20
Purpose: Launch the air_pmhap PAF calculation workflow using Jobmon (guppy version)
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

user = getpass.getuser()
wf_uuid = uuid.uuid4()

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
           $ python3 air_pollution/air/paf/air_pmhap_workflow.py
    """

    # versioning
    output_version = 95 # same as above but with updated RR curves for IHD, stroke, GA, and LRI.

    # define variables

    years = [1990, 1995] + list(range(2000, 2023)) # all years that we have v5 data for (1998-2022 for GBD 2023)
    hap_map_date = "082124" # current version for GBD 2023
    hap_stgpr_runid = 212054 # 164939
    draws_required = 250 # 500 # 100
    exposure_grid_version = 'v5_4' # "48"
    rr_version = "98" #TODO don't forget to change this manually in outcomes_no_ages.csv
    
    rshell = "FILEPATH/execRscript.sh"

    locs = get_location_metadata(location_set_id=35, gbd_round_id=9, release_id = 16)
    locations = pd.DataFrame(data=locs)
    locations = locations[locations["most_detailed"]==1]
    locations = locations["location_id"].unique().tolist()

    # define filepaths
    script_path = "FILEPATH" # 
    home_dir = "FILEPATH" # 
    scratch_dir = "FILEPATH"
    output_path = "FILEPATH"
    error_path = "FILEPATH"

    # create a tool, workflow and set executor
    air_pmhap_paf_tool_testing = Tool(name="air_pmhap_paf_tool_testing")
    """
    Only call this when you explicitly want to create a new version of your tool 
    (when you have done an overhaul of your workflow or want to indicate widespread change for the tool). 
    We do not recommend creating a new version for every run because that will make it hard to relate similar runs.

    air_pmhap_tool = Tool(name="air_pmhap_paf_tool")
    air_pmhap_tool.create_new_tool_version()
    """
    workflow = air_pmhap_paf_tool_testing.create_workflow(
        workflow_args="airpmhap_nh_v5_4_updatedRR_02", ############# DON'T FORGET TO UPDATE ARGS ############# 
        name="air_pmhap_paf_workflow",
        default_cluster_name = "slurm",
        default_compute_resources_set = {
                "slurm": {
                "queue": "all.q",
                "cores": 1, # originally 5
                "memory": "1G",
                "runtime": "00:10:00",
                "stdout": output_path,
                "stderr": error_path,
                "project": "proj_erf"}
        })
    
    # template_bwga= air_pmhap_tool.get_task_template(
    template_bwga= air_pmhap_paf_tool_testing.get_task_template(
        template_name = "calc_bwga",
        default_compute_resources={
            "queue": "long.q",
            "cores": 2,
            "memory": "80G",
            "runtime": "5h",
            "stdout": output_path,
            "stderr": error_path,
            "project": "proj_erf"
        },
        default_cluster_name="slurm",
        command_template = "PYTHONPATH= OMP_NUM_THREADS=10 {rshell} "
                           "{script} "
                           "{location_id} "
                           "{year_id} "
                           "{exposure_grid_version} "
                           "{rr_version} "
                           "{output_version} "
                           "{draws_required} "
                           "{hap_stgpr_runid} "
                           "{hap_map_date} ",
                           # "{squeeze_version} ",
        node_args = ["year_id","location_id"],
        task_args = ["exposure_grid_version","rr_version","output_version","draws_required","hap_stgpr_runid","hap_map_date"], #,"squeeze_version"],
        op_args = ["rshell", "script"]
    )


    # create task
    task_all_list = []

    # tasks for calculate bwga shifts phase
    task_bwga_list = []

    out_bwga_dir = f"FILEPATH/air_pmhap/rr/bwga/draws/{output_version}/"
    os.makedirs(out_bwga_dir, exist_ok=True)
    bwga_files = os.listdir(out_bwga_dir)

    for year_id in years:
        for location_id in locations:

            # check to see if we need to run the job or if it has finished
            if f"{location_id}_{year_id}.csv" not in bwga_files:

                size = os.path.getsize(f"FILEPATH/gridded/{exposure_grid_version}/draws_final/{location_id}_{year_id}.fst")

                mem = math.ceil(.22*size/1e6 + 4)

                task = template_bwga.create_task(
                    compute_resources={
                        "queue": "long.q",
                        "cores": 2,
                        "memory": f"{mem}G",
                        "runtime": "5h",
                        "stdout": output_path,
                        "stderr": error_path,
                        "project": "proj_erf"
                    },
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
                    hap_map_date = hap_map_date#,
                    # squeeze_version = squeeze_version
                )
                task_all_list.append(task)
                task_bwga_list.append(task)


    # add tasks to workflow
    workflow.add_tasks(task_all_list)

    print("Launching workflow...")
    print(f"There are {len(task_all_list)} tasks...")

    # run workflow
    #wfr = workflow.run()

    """
    Use this if you want to resume a workflow that already started:
    wfr = workflow.run(resume=True)
    """

    wfr = workflow.run(resume=True)
    if wfr == "D":
        print("The workflow succeeded:)")
    else:
        raise RuntimeError("The workflow failed...")

if __name__ == '__main__':
    tool_air_pmhap_paf()
