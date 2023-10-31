from jobmon.client.api import Tool, ExecutorParameters # import Jobmon
from db_queries import get_location_metadata # import shared functions
import numpy as np
import pandas as pd
import getpass
import os
import sys

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
           $ python FILEPATH/air_no2_workflow.py
    """

    # versioning
    output_version = VERSION

    # define variables
    years = [1990,1995,2000,2005,2010,2015,2019,2020] # estimation years
    grid_version = "7"
    rr_version = 20
    draws_required = 1000

    rshell = "FILEPATH.sh"

    # define locations to run

    locations = os.listdir(f"FILEPATH/{grid_version}/FILEPATH/")
    locations = [x[:-9] for x in locations] # year will always be 4 digits, so I am removing "_YEAR.csv" from each name
    locations = np.unique(locations)

    # location_ids <- 197 # I use eswatini for testing because it is pretty small

    # define filepaths
    script_path = "FILEPATH"
    home_dir = "FILEPATH"



    # create a tool, workflow, and set executor
    air_no2_tool = Tool.create_tool(name="air_no2_paf_tool")
    """
    Only call this when you explicitly want to create a new version of your tool 
    (when you have done an overhaul of your workflow or want to indicate widespread change for the tool). 
    We do not recommend creating a new version for every run because that will make it hard to relate similar runs.

    air_no2_tool = Tool(name="air_no2_paf_tool")
    air_no2_tool.create_new_tool_version()
    """
    workflow = air_no2_tool.create_workflow(name="air_no2_paf_workflow")
    workflow.set_executor(
        executor_class="SGEExecutor",
        project="PROJECT"  # specify your team's project
    )


    # create templates

    template_rr_paf = air_no2_tool.get_task_template(
        template_name = "calc_rr_paf",
        command_template = "{rshell} "
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


    template_resave = air_no2_tool.get_task_template(
        template_name = "resave_large_locs",
        command_template = "{rshell} "
                           "{script} "
                           "{location_id} "
                           "{year_id} "
                           "{grid_version} "
                           "{output_version} "
                           "{draws_required} ",
        node_args = ["location_id","year_id"],
        task_args = ["grid_version","output_version","draws_required"],
        op_args = ["rshell", "script"]
    )



    # create tasks
    task_all_list = []

    status_dir = f"{home_dir}/FILEPATH/{output_version}/"
    os.makedirs(status_dir, exist_ok=True)
    finished_files = os.listdir(status_dir)

    # tasks for calc_rr_paf phase
    task_rr_paf_list = []

    for year_id in years:
        for location_id in locations:

            # check to see if we need to run the job or if it has finished
            if (f"{location_id}_{year_id}.csv" not in finished_files) and (f"{location_id.split('_')[0]}_{year_id}.csv" not in finished_files) :

                # profile the job's resources based on file size
                if year_id in [2021, 2022]:
                    size = os.path.getsize(
                        f"/FILEPATH/{grid_version}/FILEPATH/{location_id}_2019.csv")
                else:
                    size = os.path.getsize(
                        f"/FILEPATH/{grid_version}/FILEPATH/{location_id}_{year_id}.csv")

                mem = 2*size/1e6 + 4
                if mem > 730:
                    mem = 730 # cap at 730 G so jobs can get scheduled

                time = 3*size/1e6 + 40 # time in minutes


                executor_parameters_rr_paf = ExecutorParameters(
                    m_mem_free = f"{mem}G",
                    num_cores = 20,
                    queue = "all.q",
                    max_runtime_seconds = time*60
                )

                task = template_rr_paf.create_task(
                    executor_parameters = executor_parameters_rr_paf,
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

    # tasks for resave_large_locs phase
    task_resave_list = []

    tracker_dir = f"{home_dir}FILEPATH/{grid_version}/split_locs.csv"
    tracker = pd.read_csv(tracker_dir) # first column is loc, second column is year

    for tracker_row in range(0,tracker.shape[0]):
        loc = int(tracker["location_id"].iloc[tracker_row])
        year = int(tracker["year_id"].iloc[tracker_row])

        if f"{loc}_{year}.csv" not in finished_files:

            executor_parameters_resave = ExecutorParameters(
                m_mem_free=f"100G",
                num_cores=20,
                queue="all.q",
                max_runtime_seconds=8 * 60 * 60  # 8 hours
            )

            task = template_resave.create_task(
                executor_parameters=executor_parameters_resave,
                name=f"no2_resave_{loc}_{year}",
                upstream_tasks=task_rr_paf_list,
                max_attempts=3,
                rshell=rshell,
                script=f"-s {script_path}03_resave_large_locs.R",
                location_id=loc,
                year_id=year,
                grid_version=grid_version,
                output_version=output_version,
                draws_required=draws_required
            )
            task_all_list.append(task)
            task_resave_list.append(task)


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

    if wfr.status == "D":
        print("The workflow succeeded:)")
    else:
        raise RuntimeError("The workflow failed...")

if __name__ == '__main__':
    tool_air_no2_paf()