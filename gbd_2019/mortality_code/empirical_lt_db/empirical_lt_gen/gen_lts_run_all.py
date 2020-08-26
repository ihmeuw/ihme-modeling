from datetime import datetime
import numpy as np
import os
import pandas as pd
from shutil import move
import sys
import getpass
import argparse

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask
from jobmon.client.swarm.workflow.python_task import PythonTask

sys.path.insert(0, 'FILEPATH/python/wrappers')
from call_mort_function import call_mort_function

class empirical_lts:
    def __init__(self, username, version_id, gbd_year, mark_best, apply_outliers):
        self.version_id = version_id
        self.user = username
        self.gbd_year = gbd_year
        self.mark_best = mark_best
        self.apply_outliers = apply_outliers
        self.moving_average_weights = [1, 1.5, 2, 6, 2, 1.5, 1]

        self.code_dir = "FILEPATH/{}/empirical_lt".format(self.user)
        self.r_singularity_shell = "FILEPATH/r-shell"  #3403
        self.stdout = "FILEPATH/logs/{}/output".format(self.user)
        self.stderr = "FILEPATH/logs/{}/errors".format(self.user)


    def get_datetime_arg(self):
        return str(datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_")

    def generate_empirical_lt_prep_task(self, upstream_tasks):
        job_hash_name = "gen_empirical_lts_{}".format(self.version_id)
        num_cores = 5
        m_mem_free = "12G"

        runfile = "{}/gen_empir_lts.R".format(self.code_dir)
        args  = ["--version_id", self.version_id, 
                "--apply_outliers", self.apply_outliers,
                "--mark_best", self.mark_best, 
                "--moving_average_weights", ",".join([str(w) for w in self.moving_average_weights])]
        argsstr = " ".join(['''"{}"'''.format(str(arg)) for arg in args])

        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        m_mem_free=m_mem_free,
                        max_runtime_seconds = 60000,
                        j_resource = True,
                        queue = "all.q")

    def generate_mv_plots_task(self, upstream_tasks, loc):
        job_hash_name = "save_mv_plots_{}_{}".format(self.version_id, loc)
        num_cores = 2
        m_mem_free = "10G"
        runfile = "{}/mv_input_plots_child.R".format(self.code_dir)
        args = ["--version_id", self.version_id,
                "--loc", loc]
        argsstr = " ".join(['''"{}"'''.format(str(arg)) for arg in args])
        command = "{r_shell} {codefile} {passargs}".format(
            r_shell = self.r_singularity_shell,
            codefile = runfile,
            passargs = argsstr)
        return BashTask(command = command,
                        upstream_tasks = upstream_tasks,
                        name = job_hash_name,
                        num_cores = num_cores,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 60000,
                        j_resource = True,
                        queue = "all.q")
    
    def generate_run_mv_task(self, upstream_tasks, loc):
        job_hash_name = "run_mv_{}_{}".format(self.version_id, loc)
        num_cores = 5
        m_mem_free = "80G"
        runfile = "{}/predict_machine_vision.py".format(self.code_dir)
        args = ["--version_id", self.version_id,
                "--ihme_loc_id", loc]
        return PythonTask(name = job_hash_name,
            num_cores = num_cores,
            m_mem_free = m_mem_free,
            max_runtime_seconds = 90000,
            j_resource = False,
            script = runfile,
            args = args,
            upstream_tasks = upstream_tasks,
            queue = "all.q")

    def generate_select_lts_task(self, upstream_tasks, run_mv):
        job_hash_name = "select_lts_{}".format(self.version_id)
        num_cores = 2
        m_mem_free = "50G"
        runfile = "{}/select_lts.R".format(self.code_dir)
        args = ["--version_id", self.version_id,
                "--mark_best", self.mark_best,
                "--apply_outliers", self.apply_outliers,
                "--run_mv", run_mv]
        argsstr = " ".join(['''"{}"'''.format(str(arg)) for arg in args])
        command = "{r_shell} {codefile} {passargs}".format(
            r_shell = self.r_singularity_shell,
            codefile = runfile,
            passargs = argsstr)
        return BashTask(command = command,
                        upstream_tasks = upstream_tasks,
                        name = job_hash_name,
                        num_cores = num_cores,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 60000,
                        j_resource = False,
                        queue = "all.q")

    def generate_workflow(self, wf_name, run_mv):
        wf = Workflow(workflow_args=wf_name,
                      project="proj_mortenvelope",
                      stdout=self.stdout,
                      stderr=self.stderr,
                      resume = True,
                      seconds_until_timeout = 174000)

        model_locations = call_mort_function("get_locations", {"gbd_type" : "ap_old", "level" : "estimate", "gbd_year" : self.gbd_year})
        model_locations = model_locations["ihme_loc_id"].tolist()

        lt_task = self.generate_empirical_lt_prep_task(upstream_tasks=[])
        wf.add_task(lt_task)

        # optional machine vision prediction tasks
        if run_mv == True:

            mv_plot_task = {}
            for ihme_loc_id in model_locations:
                mv_plot_task[ihme_loc_id] = self.generate_mv_plots_task([lt_task], ihme_loc_id)
                wf.add_task(mv_plot_task[ihme_loc_id])
        
            mv_run_task = {}
            for ihme_loc_id in model_locations:
                mv_run_task[ihme_loc_id] = self.generate_run_mv_task(mv_plot_task.values(), ihme_loc_id)
                wf.add_task(mv_run_task[ihme_loc_id])
        
            select_lts_task = self.generate_select_lts_task(mv_run_task.values(), run_mv)
            wf.add_task(select_lts_task)

        else:

            select_lts_task = self.generate_select_lts_task([lt_task], run_mv)
            wf.add_task(select_lts_task)
              
        return wf
    
    def run(self):
        run_mv = False # toggle for machine vision prediction
        wf_name = "empirical_life_tables_{}".format(self.version_id) 
        wf = self.generate_workflow(wf_name=wf_name, run_mv = run_mv)
        print("Starting workflow: {}".format(wf_name))
        
        lt_wf_succeeded = wf.run() == 0
        if lt_wf_succeeded:
            print("Empirical life table generation succeeded")
        else:
            raise Exception("Empirical life table generation failed")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version_id', type=int, required=True,
                        action='store', help="Version id of empirical lt run")
    parser.add_argument('--username', type=str, required="True",
                        action='store', help="User conducting the run")
    parser.add_argument('--gbd_year', type=int, required=True,
                        action='store', help="GBD Year")
    parser.add_argument('--mark_best', type=str, required=True,
                        action='store', help="True/False mark run as best")
    parser.add_argument('--apply_outliers', type=str, required=True,
                        action='store', help="True/False apply outliers")

    args = parser.parse_args()
    version_id = args.version_id
    gbd_year = args.gbd_year
    username = args.username
    mark_best = args.mark_best
    apply_outliers = args.apply_outliers

    empirical_life_tables = empirical_lts(username, version_id, gbd_year, mark_best, apply_outliers)
    empirical_life_tables.run()
    
if __name__ == "__main__":
    main()
