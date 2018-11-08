import os
import logging
import datetime
import getpass
import shutil

import pandas as pd

from db_queries import get_covariate_estimates

from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow
from jobmon_operator.python import PythonOperator
from jobmon_operator.singularity import RSingularityOperator
from jobmon_operator.stata import StataOperator

from gbd5q0.config import Config5q0

start_year = 1950
end_year = 2017

version_id = AGE_SEX_VERSION_ID
version_data_id = AGE_SEX_DATA_ID
version_5q0_id = MORTALITY_5Q0_VERSION_ID
version_ddm_id = DDM_VERSION_ID


def get_live_births():
    live_birth_data = get_covariate_estimates(covariate_id=1106)

    # Filter down to Telangana and Andhra Pradesh
    temp_ap = live_birth_data.loc[live_birth_data['location_id'].isin([4841, 4871])].copy(deep=True)

    # Sum Telangana and Andhra Pradesh covariate numbers and population
    index_cols = ['model_version_id', 'covariate_id', 'covariate_name_short', 'location_id', 'location_name',
                  'year_id', 'age_group_id', 'age_group_name', 'sex_id']
    data_cols = ['mean_value', 'lower_value', 'upper_value']
    temp_ap['location_id'] = 44849
    temp_ap['location_name'] = "Old Andhra Pradesh"
    temp_ap = temp_ap.groupby(index_cols)[data_cols].sum().reset_index()

    live_birth_data = pd.concat([live_birth_data, temp_ap]).reset_index(drop=True)

    live_birth_data = live_birth_data.sort_values(['location_id', 'year_id', 'sex_id'])
    live_birth_data.loc[live_birth_data['sex_id'] == 1, 'sex'] = "male"
    live_birth_data.loc[live_birth_data['sex_id'] == 2, 'sex'] = "female"
    live_birth_data.loc[live_birth_data['sex_id'] == 3, 'sex'] = "both"
    live_birth_data['year'] = live_birth_data['year_id']
    live_birth_data['births'] = live_birth_data['mean_value']

    return live_birth_data


def make_ihme_loc_id_dict(data):
    loc_dict = {}
    for i in data.index:
        location_id = data.loc[i, 'location_id']
        ihme_loc_id = data.loc[i, 'ihme_loc_id']
        loc_dict[location_id] = ihme_loc_id
    return loc_dict

def make_region_dict(data):
    loc_dict = {}
    for i in data.index:
        location_id = data.loc[i, 'location_id']
        region_name = data.loc[i, 'region_name']
        loc_dict[location_id] = region_name
    return loc_dict


def generate_fit_models_task(version_id, version_5q0_id, upstream_tasks):
    job_hash_name = "agesex_{}_fit_models".format(version_id)
    slots = 15
    mem_free = 30
    project = "CLUSTER_PROJECT"
    path_to_stata_binary = "FILEPATH"
    username = getpass.getuser()
    runfile = ("FILEPATH"
               "02_fit_models.do")
    args = [username, str(version_id), str(version_5q0_id)]
    process_timeout = 10000

    return StataOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_stata_binary=path_to_stata_binary,
        upstream_tasks=upstream_tasks)


def generate_sex_stage_1_task(version_id, version_5q0_id, location_id,
                              ihme_loc_id, start_year, end_year,
                              upstream_tasks):
    job_hash_name = "agesex_{}_sex_stage_1_{}".format(
        version_id, location_id)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    path_to_stata_binary = "FILEPATH"
    runfile = ("FILEPATH"
               "03_predict_sex_model_stage1.do")
    args = [str(version_id), str(version_5q0_id), str(location_id),
            ihme_loc_id, str(start_year), str(end_year)]
    process_timeout = 10000

    return StataOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_stata_binary=path_to_stata_binary,
        upstream_tasks=upstream_tasks)

def generate_sex_stage_2_task(version_id, version_5q0_id, version_ddm_id,
                              upstream_tasks):
    job_hash_name = "agesex_{}_sex_stage_2".format(version_id)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    runfile = ("FILEPATH"
               "04_predict_sex_model_stage2.R")
    path_to_singularity_image = "FILEPATH"
    args = ["--version_id", str(version_id),
            "--version_5q0_id", str(version_5q0_id),
            "--version_ddm_id", str(version_ddm_id)]
    process_timeout = 10000

    return RSingularityOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_singularity_image=path_to_singularity_image,
        upstream_tasks=upstream_tasks)

def generate_sex_gpr_task(version_id, ihme_loc_id, region_name, upstream_tasks):
    job_hash_name = "agesex_{}_sex_gpr_{}".format(
        version_id, ihme_loc_id)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    path_to_python_binary="FILEPATH"
    runfile = ("FILEPATH"
               "05_predict_sex_model_gpr.py")
    args = ["--version_id", str(version_id),
            "--region_name", region_name,
            "--ihme_loc_id", ihme_loc_id]
    process_timeout = 10000

    return PythonOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_python_binary=path_to_python_binary,
        upstream_tasks=upstream_tasks)

def generate_age_model_task(version_id, sex, version_5q0_id, version_ddm_id,
                            upstream_tasks):
    job_hash_name = "agesex_{}_age_model_{}".format(version_id, sex)
    slots = 10
    mem_free = 20
    project = "CLUSTER_PROJECT"
    runfile = ("FILEPATH"
               "06_predict_age_model_stages1_2.R")
    path_to_singularity_image = "FILEPATH"
    args = ["--version_id", str(version_id),
            "--sex", sex,
            "--version_5q0_id", str(version_5q0_id),
            "--version_ddm_id", str(version_ddm_id)]
    process_timeout = 10000

    return RSingularityOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_singularity_image=path_to_singularity_image,
        upstream_tasks=upstream_tasks)

def generate_age_gpr_task(version_id, ihme_loc_id, region_name, sex, age,
                          upstream_tasks):
    job_hash_name = "agesex_{}_age_gpr_{}_{}_{}".format(
        version_id, ihme_loc_id, sex, age)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    path_to_python_binary="FILEPATH"
    runfile = ("FILEPATH"
               "07_predict_age_model_gpr.py")
    args = ["--version_id", str(version_id),
            "--region_name", region_name,
            "--ihme_loc_id", ihme_loc_id,
            "--sex", sex,
            "--age", age]
    process_timeout = 10000

    return PythonOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_python_binary=path_to_python_binary,
        upstream_tasks=upstream_tasks)

def generate_scale_prep_task(version_id, version_5q0_id, location_id,
                             ihme_loc_id, upstream_tasks):
    job_hash_name = "agesex_{}_prep_scale_{}".format(
        version_id, location_id)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    runfile = ("FILEPATH"
               "08_prep_age_model_for_scaling.R")
    path_to_singularity_image = ("FILEPATH"
    args = ["--version_id", str(version_id),
            "--version_5q0_id", str(version_5q0_id),
            "--location_id", str(location_id),
            "--ihme_loc_id", ihme_loc_id]
    process_timeout = 10000

    return RSingularityOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_singularity_image=path_to_singularity_image,
        upstream_tasks=upstream_tasks)

def generate_scale_agesex_task(version_id, version_5q0_id, location_id,
                               ihme_loc_id, upstream_tasks):
    job_hash_name = "agesex_{}_scale_{}".format(
        version_id, location_id)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    path_to_stata_binary = ("FILEPATH"
    runfile = ("FILEPATH"
               "09_scale_age_sex.do")
    args = [str(version_id), str(version_5q0_id), str(location_id),
            ihme_loc_id]
    process_timeout = 10000

    return StataOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_stata_binary=path_to_stata_binary,
        upstream_tasks=upstream_tasks)

def generate_age_sex_graph_task(version_id, upstream_tasks):
    job_hash_name = "agesex_{}_graph".format(version_id)
    slots = 5
    mem_free = 10
    project = "CLUSTER_PROJECT"
    runfile = ("FILEPATH"
               "graph_age_sex.R")
    path_to_singularity_image = ("FILEPATH"
    args = []
    process_timeout = 10000

    return RSingularityOperator(
        job_hash_name, slots=slots, mem_free=mem_free, project=project,
        runfile=runfile, args=args, process_timeout=process_timeout,
        path_to_singularity_image=path_to_singularity_image,
        upstream_tasks=upstream_tasks)


# Set directories
input_dir = "FILEPATH"
output_dir = "FILEPATH"
output_5q0_dir = "FILEPATH"

# Read in config file
c = Config5q0.from_json("FILEPATH")

# Get location data
location_data = pd.read_csv("FILEPATH")
ihme_loc_dict = make_ihme_loc_id_dict(location_data)
region_dict = make_region_dict(location_data)

model_locations = [l for s in c.submodels for l in s.output_location_ids]
model_locations = list(set(model_locations) - set([44849]))

# Save live births data
live_births = get_live_births()
live_births.to_csv("FILEPATH", index=False)

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Start up DAG
d = datetime.datetime.now()
dag_name = "mortagesex_{}_{}".format(version_id, d.strftime("%Y%m%d%H%M"))
dag = TaskDag(name=dag_name)


# Submit fit model task
task_fit_models = generate_fit_models_task(version_id, version_5q0_id, [])
dag.add_task(task_fit_models)

# Submit stage 1 sex prediction tasks
task_sex_stage_1 = {}
for location_id in model_locations:
    ihme_loc_id = ihme_loc_dict[location_id]
    task_sex_stage_1[location_id] = generate_sex_stage_1_task(
        version_id, version_5q0_id, location_id, ihme_loc_id,
        start_year, end_year, [task_fit_models])
    dag.add_task(task_sex_stage_1[location_id])

# Submit stage 2 sex prediction tasks
task_sex_stage_2 = generate_sex_stage_2_task(
    version_id, version_5q0_id, version_ddm_id,
    [v for k, v in task_sex_stage_1.items()])
dag.add_task(task_sex_stage_2)

# Submit gpr sex prediction tasks
task_sex_gpr = {}
for location_id in model_locations:
    ihme_loc_id = ihme_loc_dict[location_id]
    region_name = region_dict[location_id].replace(" ", "_")
    task_sex_gpr[location_id] = generate_sex_gpr_task(
        version_id, ihme_loc_id, region_name, [task_sex_stage_2])
    dag.add_task(task_sex_gpr[location_id])

# Submit stages 1 & 2 age prediction tasks
task_age_model = {}
for sex in ['male', 'female']:
    task_age_model[sex] = generate_age_model_task(
        version_id, sex, version_5q0_id, version_ddm_id,
        [v for k, v in task_sex_gpr.items()])
    dag.add_task(task_age_model[sex])

# Submit gpr age prediction tasks
task_age_gpr = {}
for location_id in model_locations:
    ihme_loc_id = ihme_loc_dict[location_id]
    region_name = region_dict[location_id].replace(" ", "_")
    for sex in ['male', 'female']:
        for age in ["enn", "lnn", "pnn", "inf", "ch"]:
            output_file = "FILEPATH"
            if not os.path.exists(output_file):
                print(output_file)
                key = "{}_{}_{}".format(location_id, sex, age)
                task_age_gpr[key] = generate_age_gpr_task(
                    version_id, ihme_loc_id, region_name, sex, age,
                    [v for k, v in task_age_model.items()])
                dag.add_task(task_age_gpr[key])

# Submit age-sex scaling prep jobs
task_scale_prep = {}
for location_id in model_locations:
    ihme_loc_id = ihme_loc_dict[location_id]
    task_age_gpr_keys = []
    for sex in ['male', 'female']:
        for age in ["enn", "lnn", "pnn", "inf", "ch"]:
            key = "{}_{}_{}".format(location_id, sex, age)
            if key in task_age_gpr:
                task_age_gpr_keys.append(key)
    task_scale_prep[location_id] = generate_scale_prep_task(
        version_id, version_5q0_id, location_id, ihme_loc_id,
        [task_age_gpr[k] for k in task_age_gpr_keys])
    dag.add_task(task_scale_prep[location_id])

# Submit age-sex scaling jobs
task_scale_agesex = {}
for location_id in model_locations:
    ihme_loc_id = ihme_loc_dict[location_id]
    task_scale_agesex[location_id] = generate_scale_agesex_task(
        version_id, version_5q0_id, location_id, ihme_loc_id,
        [task_scale_prep[location_id]])
    dag.add_task(task_scale_agesex[location_id])


logger.debug("DAG: {}".format(dag))
wf = Workflow(dag, dag_name, project="CLUSTER_PROJECT")
success = wf.run()
if success:
    print("Completed")
else:
    print("FAILED!!!")
