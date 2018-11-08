import os
import logging
import datetime
import glob

import pandas as pd

from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow
from jobmon_operator.python import PythonOperator
from jobmon_operator.singularity import RSingularityOperator
from jobmon_operator.stata import StataOperator

from db_queries import get_envelope, get_location_metadata


def make_ihme_loc_id_dict(data):
    loc_dict = {}
    for i in data.index:
        location_id = data.loc[i, 'location_id']
        ihme_loc_id = data.loc[i, 'ihme_loc_id']
        loc_dict[location_id] = ihme_loc_id
    return loc_dict

def generate_u5_task(location_id, ihme_loc_id, version_id, version_age_sex_id,
                     upstream_tasks):
    job_hash_name = "u5_{}_{}".format(version_id, location_id)
    slots = 12
    mem_free = 24
    project = "CLUSTER_PROJECT"
    runfile = code_dir + "01_u5.py"
    args = ["--location_id", str(int(location_id)),
            "--ihme_loc_id", ihme_loc_id,
            "--version_id", str(int(version_id)),
            "--version_age_sex_id", str(int(version_age_sex_id))]
    process_timeout = 10000

    return PythonOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_python_binary=path_to_python_binary,
        upstream_tasks=upstream_tasks)



# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Start up DAG
d = datetime.datetime.now()
dag_name = "mort_u5_{}_{}".format(version_id, d.strftime("%Y%m%d%H%M"))
dag = TaskDag(name=dag_name)


# Get locations
location_hierarchy = get_location_metadata(location_set_id = 21, gbd_round_id = 5)
location_hierarchy = location_hierarchy.loc[(location_hierarchy['level'] >= 3) & (location_hierarchy['location_id'] != 6)]
ihme_loc_dict = make_ihme_loc_id_dict(location_hierarchy)

all_files = glob.glob((draws_dir + "*").format(version_id))

# Create tasks
u5_tasks = {}
for location_id in location_hierarchy['location_id'].tolist():
    output_file = (draws_dir + "{}.csv").format(version_id, location_id)
    if output_file not in all_files:
        print(output_file)
        ihme_loc_id = ihme_loc_dict[location_id]
        u5_tasks[location_id] = generate_u5_task(
            location_id, ihme_loc_id, version_id, version_age_sex_id, [])
        dag.add_task(u5_tasks[location_id])

logger.debug("DAG: {}".format(dag))
wf = Workflow(dag, "u5", project='CLUSTER_PROJECT')
success = wf.run()
if success:
    print("Completed")
else:
    print("FAILED!!!")
