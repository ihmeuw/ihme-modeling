import os
import sys
import argparse
import pandas as pd
import datetime
from multiprocessing import Process
from shutil import move
from subprocess import call
import rpy2.robjects as robjects

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.bash_task import BashTask

sys.path.insert(0, 'FILEPATH/python/wrappers')
from call_mort_function import call_mort_function

class model_life_table:
    def __init__(self, username, version_id, spectrum_name, gbd_year, mark_best, input_process_versions, 
                mlt_envelope_version, map_estimate_version, file_del = "True", send_slack = True, slack_username = None, start=1, end=3):   
        self.version_id = version_id
        self.user = username
        self.spectrum_name = spectrum_name
        self.gbd_year = gbd_year
        self.slack_username = slack_username
        self.mark_best = mark_best
        self.start = start
        self.end = end
        self.file_del = file_del
        self.run_years = list(range(1950, self.gbd_year + 1))
        self.send_slack = send_slack

        self.age_sex_estimate_version = input_process_versions['age sex estimate']
        self.estimate_45q15_version = input_process_versions['45q15 estimate']
        self.estimate_5q0_version = input_process_versions['5q0 estimate']
        self.lt_empirical_data_version = input_process_versions['life table empirical data']
        self.population_estimate_version = input_process_versions['population estimate']
        self.u5_envelope_estimate_version = input_process_versions['u5 envelope estimate']
        self.mlt_envelope_version = mlt_envelope_version
        self.map_estimate_version = map_estimate_version

        self.code_dir = "FILEPATH"
        self.r_singularity_shell = "FILEPATH"
        self.stdout = "FILEPATH"
        self.stderr = "FILEPATH"
        self.master_dir = "FILEPATH"
        self.input_dir = self.master_dir + "/inputs"

    def create_directories(self):
        # Create initial set of directories
        if not os.path.exists(self.master_dir):
            os.makedirs(self.master_dir)

        subdirs = ['diagnostics', 'inputs', 'lt_hiv_free', 'lt_with_hiv', 'env_with_hiv', 'summary', 'standard_lts']
        for dir in subdirs:
            new_dir = self.master_dir + "/" + dir
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

        # Create subdirs in lt_hiv_free, lt_with_hiv, env_with_hiv
        for dir in ['lt_hiv_free', 'lt_with_hiv', 'env_with_hiv']:
            for sub_dir in ['/pre_scaled', '/scaled']:
                new_dir = self.master_dir + "/" + dir + sub_dir
                if not os.path.exists(new_dir):
                    os.makedirs(new_dir)

        # Create subdirs in summary directory
        for dir in ['/upload', '/intermediary']:
            new_dir = self.master_dir + "/summary" + dir
            if not os.path.exists(new_dir):
                os.makedirs(new_dir)

    def save_location_metadata(self):
        locations = call_mort_function("get_locations", {"level" : "estimate", "gbd_year" : self.gbd_year, "hiv_metadata" : "True"})
        locations = locations[['ihme_loc_id', 'location_id', 'region_id', 'super_region_id', 'parent_id', 'local_id', 'group']]

        parent_locs = locations.parent_id.unique()
        parents = call_mort_function("get_locations", {"level" : "all", "gbd_year" : self.gbd_year})
        parents = parents.loc[parents.location_id.isin(parent_locs), ['location_id', 'ihme_loc_id']]
        parents.rename(columns = {'location_id' : 'parent_id', 'ihme_loc_id' : 'parent_ihme'}, inplace = True)
        locations = locations.merge(parents, on = 'parent_id', how = 'left')

        lt_match_map = locations[['ihme_loc_id']].copy()
        lt_match_map["match"] = 100
        assert(set(lt_match_map.ihme_loc_id.unique()) == set(locations.ihme_loc_id.unique())), "Some locations are different"
        assert set(lt_match_map.columns) == {"match", "ihme_loc_id"}, "DataFrame lt_match_map has different columns than it should."

        lt_match_map.to_csv(self.input_dir + "/lt_match_map.csv", index = False)
        locations.to_csv(self.input_dir + "/lt_env_locations.csv", index = False)

    # Only generate upon new run
    def generate_prep_task(self, upstream_tasks):
        job_hash_name = "mlt_prep"
        num_cores = 2
        m_mem_free = "10G"
        runfile = "{}/00_prep_mlt.R".format(self.code_dir)
        args = ["--spectrum_name", self.spectrum_name,
                "--start", str(self.start),
                "--end", str(self.end),
                "--file_del", self.file_del,
                "--gbd_year", str(self.gbd_year),
                "--version_id", str(self.version_id),
                "--lt_empirical_data_version", str(self.lt_empirical_data_version),
                "--mlt_envelope_version", str(self.mlt_envelope_version),
                "--map_estimate_version", str(self.map_estimate_version)]
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
                        max_runtime_seconds = 3600,
                        j_resource = True,
                        queue = "all.q")

    def generate_gen_lt_task(self, upstream_tasks, loc):
        job_hash_name = "mlt_lt_generation_{}".format(loc)
        num_cores = 5
        m_mem_free = "30G"
        runfile = "{}/01_gen_lts.R".format(self.code_dir)
        args = ["--version_id", str(self.version_id), 
                "--country", loc,
                "--spectrum_name", self.spectrum_name, 
                "--estimate_45q15_version", str(self.estimate_45q15_version), 
                "--estimate_5q0_version", str(self.estimate_5q0_version), 
                "--age_sex_estimate_version", str(self.age_sex_estimate_version), 
                "--u5_envelope_version", str(self.u5_envelope_estimate_version), 
                "--lt_empirical_data_version", str(self.lt_empirical_data_version), 
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
                        max_runtime_seconds = 10800,
                        queue = "all.q")

    def generate_scaling_task(self, upstream_tasks, year):
        job_hash_name = "mlt_scale_agg_{}".format(year)
        num_cores = 10
        m_mem_free = "90G"
        runfile = "{}/02_scale_results.R".format(self.code_dir)
        args = ["--version_id", str(self.version_id), 
                "--year", str(year),
                "--age_sex_estimate_version", str(self.age_sex_estimate_version),
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
                        max_runtime_seconds = 30000,
                        queue = "all.q")

    def generate_compile_upload_task(self, upstream_tasks):
        job_hash_name = "mlt_compile_upload"
        num_cores = 15
        m_mem_free = "40G"
        runfile = "{}/03_compile_upload.R".format(self.code_dir)
        args = ["--version_id", str(self.version_id), 
                "--mlt_envelope_version", str(self.mlt_envelope_version),
                "--map_estimate_version", str(self.map_estimate_version),
                "--gbd_year", str(self.gbd_year),
                "--mark_best", str(self.mark_best)]
        argsstr = " ".join(args)

        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        max_attempts = 2,
                        j_resource = True,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 30800,
                        queue = "all.q")

    def generate_notify_task(self, upstream_tasks):
        job_hash_name = "mlt_notify"
        num_cores = 1
        m_mem_free = "2G"
        runfile = "{}/mlt_send_notification.R".format(self.code_dir)
        args = ["--version_id", str(self.version_id), 
                "--slack_username", self.slack_username if self.slack_username else self.user]
        argsstr = " ".join(args)
        
        command = "{r_shell} {codefile} {passargs}".format(
            r_shell=self.r_singularity_shell, 
            codefile=runfile, 
            passargs=argsstr)

        return BashTask(command=command, 
                        upstream_tasks=upstream_tasks, 
                        name=job_hash_name, 
                        num_cores=num_cores,
                        max_attempts = 1,
                        m_mem_free = m_mem_free,
                        max_runtime_seconds = 1200,
                        queue = "all.q")

    def generate_workflow(self, wf_name):
        wf = Workflow(workflow_args=wf_name,
                      project="proj_mortenvelope",
                      stdout=self.stdout,
                      stderr=self.stderr,
                      resume = True,
                      seconds_until_timeout = 864000)

        # Create folder structure
        self.create_directories()

        # Save location metadata if they haven't been saved yet
        if os.path.isfile(self.input_dir + "/lt_match_map.csv") == False | os.path.isfile(self.input_dir + "lt_env_locations.csv") == False:
            self.save_location_metadata()

        # Step 1: prep input data
        prep_task = self.generate_prep_task([])
        wf.add_task(prep_task)

        run_countries = pd.read_csv(self.input_dir + "/lt_match_map.csv")["ihme_loc_id"].tolist()
        
        # Step 2: generate life tables
        gen_lt_tasks = []
        for country in run_countries:
            country_task = self.generate_gen_lt_task([prep_task], country)
            wf.add_task(country_task)
            gen_lt_tasks.append(country_task)

        # Step 3: scale results
        scaling_tasks = []
        for year in self.run_years:
            year_task = self.generate_scaling_task(gen_lt_tasks, year)
            wf.add_task(year_task)
            scaling_tasks.append(year_task)

        # Step 4: Compile upload
        compile_task = self.generate_compile_upload_task(scaling_tasks)
        wf.add_task(compile_task)

        if self.send_slack:
            notify_task = self.generate_notify_task([compile_task])
            wf.add_task(notify_task)

        return wf

    def run(self):
        d = datetime.datetime.now()
        wf_name = "model_life_table_{}".format(self.version_id)
        wf = self.generate_workflow(wf_name)
        success = wf.run()
        if success == 0:
            print("Completed!")
        else:
            print("FAILED!")
            command = """
            library(mortdb, lib = "FILEPATH/r-pkg")
            send_slack_message()
            """.format(version_id)
            robjects.r(command)
            raise Exception()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--version_id', type=int, required=True,
                        action='store', help="Version id of run")
    parser.add_argument('--mlt_death_number_estimate', type=int, required=True,
                        action='store', help="Version id of mlt death number estimate")
    parser.add_argument('--gbd_year', type=int, required=True,
                        action='store', help="GBD Year")
    parser.add_argument('--mark_best', type=str, required=True,
                        action='store', help="True/False mark best")
    parser.add_argument('--username', type=str, required="True",
                        action='store', help="User conducting the run")
    parser.add_argument('--start', type=str, required=False,
                       action='store', help="Starting stage of attempted model run")
    parser.add_argument('--end', type=str, required=False,
                        action='store', help="Ending stage of attempted model run")

    args = parser.parse_args()
    version_id = args.version_id
    mlt_death_number_estimate = args.mlt_death_number_estimate
    mark_best = args.mark_best
    gbd_year = args.gbd_year
    username = args.username
    start = args.start
    end = args.end

    proc_lineage = call_mort_function("get_proc_lineage", {"model_name" : "mlt life table", "model_type" : "estimate", "run_id" : version_id})
    input_process_versions = dict(zip(proc_lineage.parent_process_name, proc_lineage.parent_run_id))

    external_inputs = call_mort_function("get_external_input_map", {"process_name" : "mlt life table estimate", "run_id" : version_id})
    external_inputs = dict(zip(external_inputs.external_input_name, external_inputs.external_input_version))
    spectrum_name = external_inputs['hiv']

    mlt_envelope_version = mlt_death_number_estimate
    map_estimate_version = input_process_versions['life table map estimate']

    mlt = model_life_table(username, version_id, spectrum_name, gbd_year, mark_best, input_process_versions, mlt_envelope_version, map_estimate_version)

    mlt.run()

if __name__ == "__main__":
    main()

