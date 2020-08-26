import os
import sys
import argparse
import pandas as pd
import datetime
import random
import subprocess
import numpy as np
import rpy2.robjects as robjects

from subprocess import call
from multiprocessing import Process
from shutil import move
from sqlalchemy import create_engine

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask
from mort_wrappers.call_mort_function import call_mort_function

class full_life_table:
    def __init__(self, username, process_run_table, run_finalizer, aggregate_full_lts, gbd_year, upload, enable_assertions_flag, mark_best, aggregate_locations):
        self.no_shock_death_number_estimate_version = process_run_table['no shock death number estimate']
        self.no_shock_life_table_estimate_version = process_run_table['no shock life table estimate']
        self.shock_aggregator_version = process_run_table['shock aggregator']
        self.age_sex_estimate_version = process_run_table['age sex estimate']
        self.population_estimate_version = process_run_table['population estimate']
        self.population_single_year_estimate_version = process_run_table['population single year estimate']
        self.shock_death_number_estimate = process_run_table['shock death number estimate']
        self.shock_life_table_estimate = process_run_table['shock life table estimate']
        self.full_life_table_estimate_version = process_run_table['full life table estimate']
        self.mlt_life_table_estimate_version = process_run_table['mlt life table estimate']
        self.hiv_run_name = process_run_table['hiv']

        self.user = username
        self.gbd_year = gbd_year
        self.mark_best = mark_best
        self.upload_all_lt_params_flag = "True"
        self.enable_assertions_flag = enable_assertions_flag
        self.run_finalizer = run_finalizer
        self.aggregate_full_lts = aggregate_full_lts
        self.upload = upload
        self.aggregate_locations = aggregate_locations

        self.code_dir = "FILEPATH"
        self.finalizer_code_dir = "FILEPATH"
        self.r_singularity_shell = "FILEPATH"
        self.r_singularity_shell_3501 = "FILEPATH"
        self.stdout = "FILEPATH"
        self.stderr = "FILEPATH"

        self.master_dir = "FILEPATH"
        self.input_dir = "{}/inputs".format(self.master_dir)
        self.reckoning_output_dir = "FILEPATH + self.no_shock_death_number_estimate_version"
        self.full_lt_dir = "{}/full_lt".format(self.master_dir)
        self.abridged_lt_dir = "{}/abridged_lt".format(self.master_dir)
        self.upload_dir = "{}/upload".format(self.master_dir)
        self.log_dir = "{}/logs".format(self.master_dir)

    def create_directories(self, master_dir, subdirs=[]):
        # Create initial set of directories
        if not os.path.exists(master_dir):
            os.makedirs(master_dir)

        for dir in subdirs:
            new_dir = master_dir + "/" + dir
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

    def generate_save_inputs_task(self, upstream_tasks):
        job_hash_name = "agg_save_inputs_{}".format(self.no_shock_death_number_estimate_version)
        num_cores = 4
        m_mem_free = "36G"

        runfile = "{}/01_save_inputs.R".format(self.code_dir)
        args = ["--no_shock_death_number_estimate_version", str(self.no_shock_death_number_estimate_version),
                "--population_estimate_version", str(self.population_estimate_version),
                "--population_single_year_estimate_version", str(self.population_single_year_estimate_version),
                "--gbd_year", str(self.gbd_year)]
        argsstr = " ".join(args)

        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 9000,
                        j_resource = True,
                        queue = "all.q")

    def generate_full_lt_task(self, upstream_tasks, loc):
        job_hash_name = "full_lt_{loc}_{version}".format(loc=loc, version=self.no_shock_death_number_estimate_version)
        num_cores = 3
        m_mem_free = "30G"

        runfile = "{}/02_full_lt.R".format(self.code_dir)
        args = ["--no_shock_death_number_estimate_version", str(self.no_shock_death_number_estimate_version),
                "--mlt_life_table_estimate_version", str(self.mlt_life_table_estimate_version),
                "--hiv_run", self.hiv_run_name,
                "--loc", str(loc),
                "--shock_aggregator_version", str(self.shock_aggregator_version),
                "--gbd_year", str(self.gbd_year),
                "--enable_assertions_flag", str(self.enable_assertions_flag)]
        argsstr = " ".join(args)

        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 90000,
                        j_resource = True,
                        queue = "all.q")

    def generate_aggregate_lt_task(self, upstream_tasks, loc, lt_type, num_children):
        job_hash_name = "agg_full_lts_{loc}_{lt_type}_{version}".format(loc=loc, lt_type=lt_type, version = self.no_shock_death_number_estimate_version)
        if num_children < 10:
            num_cores = 10
            m_mem_free = "100G"
        elif num_children >= 10 and num_children < 50:
            num_cores = 20
            m_mem_free = "300G"
        else:
            num_cores = 30
            m_mem_free = "500G"
        

        runfile = "{}/03_aggregate_lts.R".format(self.code_dir)
        args = ["--no_shock_death_number_estimate_version", str(self.no_shock_death_number_estimate_version),
                "--loc", str(loc),
                "--lt_type", lt_type,
                "--gbd_year", str(self.gbd_year),
                "--enable_assertions_flag", str(self.enable_assertions_flag)]
        argsstr = " ".join(args)

        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell_3501, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 90000,
                        j_resource = True,
                        queue = "all.q")

    def generate_full_upload_task(self, upstream_tasks):
        job_hash_name = "full_life_table_upload_{}".format(self.no_shock_death_number_estimate_version)
        num_cores = 10
        m_mem_free = "50G"

        runfile = "{}/04_compile_upload_results.R".format(self.code_dir)
        args = ["--no_shock_death_number_estimate_version", str(self.no_shock_death_number_estimate_version),
                "--gbd_year", str(self.gbd_year),
                "--full_life_table_estimate_version", str(self.full_life_table_estimate_version),
                "--upload_all_lt_params_flag", str(self.upload_all_lt_params_flag)]
        argsstr = " ".join(args)

        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell_3501, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 90000,
                        queue = "all.q")

    def generate_finalizer_task(self, upstream_tasks):
        job_hash_name = "finalizer_run_{}".format(self.no_shock_death_number_estimate_version)
        num_cores = 1
        m_mem_free = "2G"
        runfile = "{}/finalizer_run_all.py".format(self.finalizer_code_dir)

        # How do we pass args to the finalizer run_all?
        args = ["--shock_death_number_estimate_version", self.shock_death_number_estimate,
                "--shock_life_table_estimate_version", self.shock_life_table_estimate,
                "--no_shock_death_number_estimate_version", self.no_shock_death_number_estimate_version,
                "--no_shock_life_table_estimate_version", self.no_shock_life_table_estimate_version,
                "--username", self.user,
                "--gbd_year", self.gbd_year,
                "--mark_best", self.mark_best,
                "--shock_version_id", self.shock_aggregator_version,
                "--aggregate_locations", self.aggregate_locations,
                "--workflow_args", self.shock_death_number_estimate]

        return PythonTask(name = job_hash_name,
                        num_cores = num_cores,
                        m_mem_free = m_mem_free,
                        script = runfile,
                        args = args,
                        upstream_tasks = upstream_tasks,
                        queue = "long.q", 
						max_runtime_seconds = 180000,
                        j_resource = True,
                        max_attempts = 1)


    def generate_workflow(self, wf_name):
        wf = Workflow(workflow_args=wf_name,
                      project="proj_mortenvelope",
                      stdout=self.stdout,
                      stderr=self.stderr, seconds_until_timeout = 777600, resume = True)


        self.create_directories(master_dir=self.master_dir, subdirs=['inputs', 'shock_numbers', 'hiv_adjust', 'logs', 'upload', 'abridged_lt', 'full_lt'])
        self.create_directories(master_dir=self.reckoning_output_dir, subdirs=['lt_whiv', 'lt_hivdel', 'envelope_whiv', 'envelope_hivdel'])
        self.create_directories(master_dir=self.full_lt_dir, subdirs=['no_hiv', 'with_hiv', 'with_shock'])
        self.create_directories(master_dir=self.abridged_lt_dir, subdirs=['no_hiv', 'with_hiv', 'with_shock'])
        self.create_directories(master_dir=self.abridged_lt_dir, subdirs=['no_hiv', 'with_hiv', 'with_shock'])
        self.create_directories(master_dir=self.upload_dir)
        self.create_directories(master_dir=self.log_dir, subdirs=['full_with_hiv_mx_vs_no_hiv', 'full_shock_mx_vs_with_hiv', 'abridged_with_hiv_mx_vs_no_hiv', 
                                                                'abridged_shock_mx_vs_with_hiv', 'abridged_no_hiv_qx_1_5', 'abridged_with_hiv_qx_1_5', 
                                                                'abridged_shock_qx_1_5', 'shock_rate_compare', 'ax_compare'])

        # Get locations
        most_detail_locations = call_mort_function("get_locations", {"level" : "lowest", "gbd_year" : self.gbd_year})
        most_detail_loc_ids = most_detail_locations.location_id.tolist()
        
        # Generate save inputs task
        # job name: agg_save_inputs_{}
        # script being ran: save_inputs.R
        save_inputs_task = self.generate_save_inputs_task(upstream_tasks=[])
        wf.add_task(save_inputs_task)

        # Generate full lt tasks
        # job name: gen_full_{loc}_{version}
        # script being ran: full_lt.R

        full_lt_tasks = {}
        for loc in most_detail_loc_ids:
            full_lt_tasks[loc] = self.generate_full_lt_task(upstream_tasks=[save_inputs_task], loc=loc)
            wf.add_task(full_lt_tasks[loc])

        # Run finalizer
        if self.run_finalizer:
            finalizer_run_task = self.generate_finalizer_task(upstream_tasks=full_lt_tasks.values())
            wf.add_task(finalizer_run_task)

        # Generate rest of full_lt tasks and add to the workflow
        # job names: "agg_full_{loc}_{lt_type}_{version}"
        # script being ran: aggregate_lts.R
        if self.aggregate_full_lts:
            # Get aggregate locations
            locations = call_mort_function("get_locations", {"level" : "all", "gbd_year" : self.gbd_year})
            agg_locations = locations[(locations.level == 3) & (~locations.location_id.isin(most_detail_loc_ids))]
            agg_loc_ids = agg_locations.location_id.tolist()

            # Generate agg tasks
            agg_tasks = {}
            for loc in agg_loc_ids:
                num_children = len(most_detail_locations.loc[most_detail_locations.path_to_top_parent.str.contains("," + str(loc) + ",")])
                for lt_type in ['with_shock', 'with_hiv', 'no_hiv']:
                    agg_task_key = str(loc) + "_" + lt_type
                    agg_tasks[agg_task_key] = self.generate_aggregate_lt_task(upstream_tasks=full_lt_tasks.values(), loc=loc, lt_type=lt_type, num_children=num_children)
                    wf.add_task(agg_tasks[agg_task_key])

            # Generate upload task
            # job name: full_life_table_upload_{}
            # script name: compile_upload_results.R
            if self.upload:
                upload_task = self.generate_full_upload_task(upstream_tasks=agg_tasks.values())
                wf.add_task(upload_task)

        return wf

    def run(self):
        d = datetime.datetime.now()
        wf_name = "full_lt_{}".format(self.shock_death_number_estimate)
        wf = self.generate_workflow(wf_name)
        success = wf.run()
        if success == 0:
            print("Completed!")
            if(self.mark_best == "True"):
                command = """
                library(mortdb, lib = "FILEPATH/r-pkg")
                update_status(model_name = "full life table", model_type = "estimate", run_id = {}, new_status = "best", send_slack = TRUE)
                """.format(self.full_life_table_estimate_version)
            else:
                command = """
                library(mortdb, lib = "FILEPATH/r-pkg")
                update_status(model_name = "full life table", model_type = "estimate", run_id = {}, new_status = "completed", send_slack = TRUE)
                """.format(self.full_life_table_estimate_version)
            robjects.r(command)
        else:
            print("FAILED!!!")
            command = """
            library(mortdb, lib = "FILEPATH/r-pkg")
            send_slack_message()	
            """.format(version_id)
            robjects.r(command) 
            raise Exception()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version_id', type=int, required=True,
                        action='store', help="Version id of full life table")
    parser.add_argument('--shock_death_number_estimate_version', type=int, required=False,
                        action='store', help="Version id of with shock death number estimate")
    parser.add_argument('--shock_life_table_estimate_version', type=int, required=False,
                        action='store', help="Version id of with shock life table estimate")
    parser.add_argument('--mark_best', type=str, required=True,
                        action='store', help="True/False mark run as best")      
    parser.add_argument('--gbd_year', type=int, required=True,
                        action='store', help="GBD Year")
    parser.add_argument('--username', type=str, required=True,
                        action='store', help="User conducting the run")
    parser.add_argument('--run_finalizer', type=int, required=True,
                        action='store', help="True/False run finalizer")
    parser.add_argument('--aggregate_full_lts', type=int, required=True,
                        action='store', help="True/False aggregate full lt's")
    parser.add_argument('--upload', type=int, required=True,
                        action='store', help="True/False upload full life table estimate")
    parser.add_argument('--enable_assertions_flag', type=str, required=True,
                        action='store', help="True/False whether or not to assert qx")
    parser.add_argument('--aggregate_locations', type=str, required=True,
                        action='store', help="True/False whether or not to aggregate locations")

    args = parser.parse_args()
    full_lt_version_id = args.version_id
    shock_death_number_estimate = args.shock_death_number_estimate_version
    shock_life_table_estimate = args.shock_life_table_estimate_version
    mark_best = args.mark_best
    gbd_year = args.gbd_year
    username = args.username
    run_finalizer = args.run_finalizer
    aggregate_full_lts = args.aggregate_full_lts
    upload = args.upload
    enable_assertions_flag = args.enable_assertions_flag
    aggregate_locations = args.aggregate_locations

    # Get gbd round id based on gbd_year
    gbd_round_id = int(call_mort_function("get_gbd_round", {"gbd_year" : gbd_year}))

    # Pull shock aggregator version
    external_inputs = call_mort_function("get_external_input_map", {"process_name" : "full life table estimate", "run_id" : full_lt_version_id})
    external_inputs = dict(zip(external_inputs.external_input_name, external_inputs.external_input_version))
    shock_version_id = external_inputs['shock_aggregator']
    hiv_run_name = external_inputs['hiv']

    run_parents = call_mort_function("get_proc_lineage", {"model_name" : "full life table", "model_type" : "estimate", "run_id" : full_lt_version_id})
    run_parents = dict(zip(run_parents.parent_process_name, run_parents.parent_run_id))
    
    run_parents['shock aggregator'] = shock_version_id
    run_parents['hiv'] = hiv_run_name
    run_parents['shock death number estimate'] = shock_death_number_estimate
    run_parents['shock life table estimate'] = shock_life_table_estimate
    run_parents['full life table estimate'] = full_lt_version_id

    full_lt = full_life_table(username, run_parents, run_finalizer, aggregate_full_lts, gbd_year, upload, enable_assertions_flag, mark_best, aggregate_locations)

    full_lt.run()

if __name__ == "__main__":
    main()

# DONE
