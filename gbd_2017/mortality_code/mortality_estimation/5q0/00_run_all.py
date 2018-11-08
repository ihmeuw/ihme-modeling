import os
import logging
import time
import datetime
import argparse
import getpass

import pandas as pd

from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow
from jobmon_operator.python import PythonOperator
from jobmon_operator.singularity import RSingularityOperator
from jobmon_operator.stata import StataOperator

from db_queries import get_location_metadata

from gbd5q0py.config import Config5q0


def make_ihme_loc_id_dict(data):
    loc_dict = {}
    for i in data.index:
        location_id = data.loc[i, 'location_id']
        ihme_loc_id = data.loc[i, 'ihme_loc_id']
        loc_dict[location_id] = ihme_loc_id
    return loc_dict

class Swarm5q0:
    def __init__(self, code_dir, config, cluster_project='CLUSTER_PROJECT'):
        self.code_dir = code_dir
        self.config = config
        self.cluster_project = cluster_project

        self.version_id = self.config.version_id
        self.gbd_round_id = self.config.gbd_round_id
        self.start_year = self.config.start_year
        self.end_year = self.config.end_year

        self.python_binary_path = "FILEPATH"
        self.python_gpr_binary_path = "FILEPATH"
        self.stata_binary_path = "FILEPATH"
        self.r_singularity_image = "FILEPATH"

    # Create folder structure
    def make_version_folders(self):
        # Define the top-level directories
        output_dirs = ["data", "submodel", "raking", "draws", "summaries",
                       "graphs"]

        # Add submodel directories to the output directories
        output_dirs += ["submodel/{}".format(s.submodel_id) for s in self.config.submodels]

        # Add raking directories to the output directories
        output_dirs += ["raking/{}".format(r.raking_id) for r in self.config.rakings]

        # Make folders
        for d in output_dirs:
            os.makedirs(os.path.join(self.config.output_dir, d), exist_ok=True)


    # Generate jobmon tasks
    def generate_get_input_task(self):
        job_hash_name = "m5q0_{}_get_data".format(self.version_id)
        slots = 2
        mem_free = 4
        process_timeout = 10000

        runfile = "{}/01_input.R".format(self.code_dir)
        args = ["--version_id", str(self.version_id),
                "--gbd_round_id", str(self.gbd_round_id),
                "--start_year", str(self.start_year),
                "--end_year", str(self.end_year)]

        upstream_tasks = []

        return RSingularityOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_singularity_image=self.r_singularity_image,
            upstream_tasks=upstream_tasks)

    def generate_assess_bias_task(self, upstream_tasks):
        job_hash_name = "m5q0_{}_assess_bias".format(self.version_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/02_assess_vr_bias.py".format(self.code_dir)
        args = ["--version_id", str(self.version_id)]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_format_data_task(self, upstream_tasks):
        job_hash_name = "m5q0_{}_format_data".format(self.version_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/03_format_covariates_for_prediction_models.R".format(self.code_dir)
        args = ["--version_id", str(self.version_id),
                "--gbd_round_id", str(self.gbd_round_id),
                "--start_year", str(self.start_year),
                "--end_year", str(self.end_year)]

        return RSingularityOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_singularity_image=self.r_singularity_image,
            upstream_tasks=upstream_tasks)

    def generate_generate_hyperparameters_task(self, upstream_tasks):
        job_hash_name = "m5q0_{}_generate_hyperparameters".format(self.version_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/03_generate_hyperparameters.py".format(self.code_dir)
        args = ["--version_id", str(self.version_id)]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_submodel_a_task(self, submodel_id, input_location_ids,
                                 output_location_ids, upstream_tasks):
        job_hash_name = "m5q0_{}_submodel_a_{}".format(
            self.version_id, submodel_id)
        slots = 20
        mem_free = 40
        process_timeout = 10000

        runfile = "{}/04a_fit_submodel.R".format(self.code_dir)

        args = ["--version_id", str(self.version_id),
                "--submodel_id", str(submodel_id),
                "--st_loess", str(1),
                "--input_location_ids", ' '.join(
                    [str(x) for x in input_location_ids]),
                "--output_location_ids", ' '.join(
                    [str(x) for x in output_location_ids]),
                "--gbd_round_id", str(self.gbd_round_id),
                "--start_year", str(self.start_year),
                "--end_year", str(self.end_year)]

        return RSingularityOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_singularity_image=self.r_singularity_image,
            upstream_tasks=upstream_tasks)

    def generate_submodel_b_task(self, submodel_id, input_location_ids,
                                 output_location_ids, upstream_tasks):
        job_hash_name = "m5q0_{}_submodel_b_{}".format(
            self.version_id, submodel_id)
        slots = 20
        mem_free = 40
        process_timeout = 10000

        runfile = "{}/04b_fit_submodel.R".format(self.code_dir)

        args = ["--version_id", str(self.version_id),
                "--submodel_id", str(submodel_id),
                "--st_loess", str(1),
                "--input_location_ids", ' '.join(
                    [str(x) for x in input_location_ids]),
                "--output_location_ids", ' '.join(
                    [str(x) for x in output_location_ids]),
                "--gbd_round_id", str(self.gbd_round_id),
                "--start_year", str(self.start_year),
                "--end_year", str(self.end_year)]

        return RSingularityOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_singularity_image=self.r_singularity_image,
            upstream_tasks=upstream_tasks)

    def generate_submodel_c_task(self, submodel_id, input_location_ids,
                                 output_location_ids, upstream_tasks):
        job_hash_name = "m5q0_{}_submodel_c_{}".format(
            self.version_id, submodel_id)
        slots = 20
        mem_free = 40
        process_timeout = 10000

        runfile = "{}/04c_fit_submodel.R".format(self.code_dir)

        args = ["--version_id", str(self.version_id),
                "--submodel_id", str(submodel_id),
                "--st_loess", str(1),
                "--input_location_ids", ' '.join(
                    [str(x) for x in input_location_ids]),
                "--output_location_ids", ' '.join(
                    [str(x) for x in output_location_ids]),
                "--gbd_round_id", str(self.gbd_round_id),
                "--start_year", str(self.start_year),
                "--end_year", str(self.end_year)]

        return RSingularityOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_singularity_image=self.r_singularity_image,
            upstream_tasks=upstream_tasks)

    def generate_submodel_variance_task(self, submodel_id, upstream_tasks):
        job_hash_name = "m5q0_{}_submodel_{}_variance".format(
            self.version_id, submodel_id)
        slots = 15
        mem_free = 30
        process_timeout = 10000

        runfile = "{}/05_calculate_data_variance.do".format(self.code_dir)
        args = [str(self.version_id), str(submodel_id)]

        return StataOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_stata_binary=self.stata_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_submodel_gpr_task(self, submodel_id, location_id, ihme_loc_id,
                                   upstream_tasks):
        job_hash_name = "m5q0_{}_submodel_{}_gpr_{}".format(
            self.version_id, submodel_id, location_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/06_fit_gpr.py".format(self.code_dir)

        args = ["--version_id", str(self.version_id),
                "--submodel_id", str(submodel_id),
                "--location_id", str(location_id),
                "--ihme_loc_id", ihme_loc_id]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_gpr_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_submodel_gpr_compile_task(self, submodel_id, location_ids,
                                           upstream_tasks):
        job_hash_name = "m5q0_{}_submodel_{}_gpr_compile".format(
            self.version_id, submodel_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/07_append_gpr.py".format(self.code_dir)

        args = ["--version_id", str(self.version_id),
                "--submodel_id", str(submodel_id),
                "--location_ids", ' '.join([str(l) for l in location_ids])]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_raking_task(self, raking_id, submodel_ids, parent_id,
                             child_ids, direction, upstream_tasks):
        job_hash_name = "m5q0_{}_raking_{}".format(self.version_id, raking_id)
        slots = 15
        mem_free = 30
        process_timeout = 10000

        runfile = "{}/08_rake.R".format(self.code_dir)

        args = ["--version_id", str(self.version_id),
                "--raking_id", str(raking_id),
                "--submodel_ids", ' '.join(str(s) for s in submodel_ids),
                "--parent_id", str(parent_id),
                "--child_ids", ' '.join([str(x) for x in child_ids]),
                "--gbd_round_id", str(self.gbd_round_id),
                "--start_year", str(self.start_year),
                "--end_year", str(self.end_year)]

        return RSingularityOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_singularity_image=self.r_singularity_image,
            upstream_tasks=upstream_tasks)

    def generate_save_draws_task(self, location_id, upstream_tasks):
        job_hash_name = "m5q0_{}_save_draws_{}".format(
            self.version_id, location_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/09_save_draws.py".format(self.code_dir)

        args = ["--version_id", self.version_id,
                "--location_id", location_id]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_upload_prep_task(self, upstream_tasks):
        job_hash_name = "m5q0_{}_upload_prep".format(self.version_id)
        slots = 15
        mem_free = 30
        process_timeout = 10000

        runfile = "{}/10_prep_upload_file.py".format(self.code_dir)

        args = ["--version_id", str(self.version_id)]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_compile_submodels_task(self, upstream_tasks):
        job_hash_name = "m5q0_{}_compile_submodels".format(self.version_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/compile_submodel_outputs.py".format(self.code_dir)

        args = ["--version_id", str(self.version_id)]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    def generate_comparison_task(self, upstream_tasks):
        job_hash_name = "m5q0_{}_comparison".format(self.version_id)
        slots = 5
        mem_free = 10
        process_timeout = 10000

        runfile = "{}/mortality_5q0_data_comparison.py".format(self.code_dir)

        args = ["--version_id", str(self.version_id)]

        return PythonOperator(
            job_hash_name, slots=slots, mem_free=mem_free,
            project=self.cluster_project, runfile=runfile, args=args,
            process_timeout=process_timeout,
            path_to_python_binary=self.python_binary_path,
            upstream_tasks=upstream_tasks)

    # Generate DAG
    def generate_dag(self, dag_name):
        # Start up DAG
        dag = TaskDag(name=dag_name)

        # Get input
        task_get_input = self.generate_get_input_task()
        dag.add_task(task_get_input)

        # Assess VR bias
        task_assess_vr_bias = self.generate_assess_bias_task([task_get_input])
        dag.add_task(task_assess_vr_bias)

        # Format data for prediction
        task_format_data = self.generate_format_data_task([task_assess_vr_bias])
        dag.add_task(task_format_data)

        # Generate hyperparameters
        task_generate_hyperparameters = self.generate_generate_hyperparameters_task(
            [task_format_data])
        dag.add_task(task_generate_hyperparameters)

        # Get location data for next step
        location_data = get_location_metadata(
            location_set_id = 82, gbd_round_id = self.gbd_round_id)
        ihme_loc_dict = make_ihme_loc_id_dict(location_data)

        # Run submodels
        task_submodel_a = {}
        task_submodel_b = {}
        task_submodel_c = {}
        task_variance = {}
        task_gpr = {}
        task_compile_gpr = {}
        output_files = []
        for s in self.config.submodels:
            # Submodel A: First stage regression
            task_submodel_a[s.submodel_id] = self.generate_submodel_a_task(
                s.submodel_id, s.input_location_ids, s.output_location_ids,
                [task_generate_hyperparameters])
            dag.add_task(task_submodel_a[s.submodel_id])

            # Submodel B: Bias correction
            task_submodel_b[s.submodel_id] = self.generate_submodel_b_task(
                s.submodel_id, s.input_location_ids, s.output_location_ids,
                [task_submodel_a[s.submodel_id]])
            dag.add_task(task_submodel_b[s.submodel_id])

            # Submodel C: Space-time smoothing
            task_submodel_c[s.submodel_id] = self.generate_submodel_c_task(
                s.submodel_id, s.input_location_ids, s.output_location_ids,
                [task_submodel_b[s.submodel_id]])
            dag.add_task(task_submodel_c[s.submodel_id])

            # Calculate variance
            task_variance[s.submodel_id] = self.generate_submodel_variance_task(
                s.submodel_id, [task_submodel_c[s.submodel_id]])
            dag.add_task(task_variance[s.submodel_id])

            # GPR
            task_gpr[s.submodel_id] = {}
            for location_id in s.output_location_ids:
                ihme_loc_id = ihme_loc_dict[location_id]
                task_gpr[s.submodel_id][location_id] = self.generate_submodel_gpr_task(
                    s.submodel_id, location_id, ihme_loc_id,
                    [task_variance[s.submodel_id]])
                dag.add_task(task_gpr[s.submodel_id][location_id])

            # Submodel compile
            task_compile_gpr[s.submodel_id] = self.generate_submodel_gpr_compile_task(
                s.submodel_id, s.output_location_ids,
                [v for k, v in task_gpr[s.submodel_id].items()])
            dag.add_task(task_compile_gpr[s.submodel_id])

        # Rake data
        task_rake = {}
        for r in self.config.rakings:
            # Submit raking jobs
            task_rake[r.raking_id] = self.generate_raking_task(
                r.raking_id, [s.submodel_id for s in self.config.submodels],
                r.parent_id, r.child_ids, r.direction,
                [v for k, v in task_compile_gpr.items()])
            dag.add_task(task_rake[r.raking_id])

        # Save draws and summary files in one spot
        task_save_draws = {}
        for s in self.config.submodels:
            for location_id in s.output_location_ids:
                task_save_draws[location_id] = self.generate_save_draws_task(
                    location_id, [v for k, v in task_rake.items()])
                dag.add_task(task_save_draws[location_id])

        # Upload
        task_upload = self.generate_upload_prep_task(
            [v for k, v in task_save_draws.items()])
        dag.add_task(task_upload)

        # Compile submodels together to be able to generate graphs
        task_compile_submodels = self.generate_compile_submodels_task(
            [v for k, v in task_save_draws.items()])
        dag.add_task(task_compile_submodels)

        # Generate comparisons of current location inputs to previous GBD
        task_generate_comparison = self.generate_comparison_task(
            [v for k, v in task_save_draws.items()])
        dag.add_task(task_generate_comparison)

        return dag

    def run(self):
        # Create folders
        self.make_version_folders()

        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        # Save out the config file
        current_config_file = "{}/5q0_{}_config.json".format(
            self.config.output_dir, self.version_id)
        config.to_json(current_config_file)

        # Start up DAG
        d = datetime.datetime.now()
        dag_name = "mort5q0_{}_{}".format(
            self.version_id, d.strftime("%Y%m%d%H%M"))
        dag = self.generate_dag(dag_name)

        # Run workflow
        logger.debug("DAG: {}".format(dag))

        wf = Workflow(dag, dag_name, project='CLUSTER_PROJECT')
        success = wf.run()
        if success:
            print("Completed")
        else:
            raise("FAILED!!!")


# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--version_id', type=int, required=True,
                    action='store', help='The version_id to run')
args = parser.parse_args()

# Define code directory
code_dir = "FILEPATH"

# Import config file
config_file = "FILEPATH"
config = Config5q0.from_json(config_file)

# Set the config version equal to the version we are running
config.version_id = args.version_id

# Check config file
config.is_submodel_outputs_unique()

# Run 5q0
swarm = Swarm5q0(code_dir, config)
swarm.run()
