import getpass
import json
import logging
import os
import pickle
import re
import subprocess
import sys

from glob import glob

from adding_machine import summarizers as sm

from hierarchies import dbtrees

from jobmon.requester import Requester

from jobmon.sge import true_path
from jobmon.qmaster import JobQueue
from jobmon.connection_config import ConnectionConfig
from jobmon.executors.sge_exec import SGEExecutor
from jobmon.schedulers import RetryScheduler

from hierarchies.dbtrees import loctree
import dalynator.get_input_args as get_input_args
import gbd.constants as gbd
from dalynator.compute_dalys import ComputeDalys
from dalynator.compute_summaries import MetricConverter
from dalynator.data_source import GetPopulationDataSource
from dalynator.meta_info import load_meta
from dalynator.get_yld_data import get_folder_structure
from dalynator.makedirs_safely import makedirs_safely

from gbd_outputs_versions.gbd_process import GBDProcessVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv
import dalynator.check_input_files_exist as check_input

logger = logging.getLogger(__name__)


class DalynatorJobSwarm(object):
    """
    Run all leaf location jobs in parallel, by location-year.
    Output directory structure:
        dalynator/draws/location-year/{location_id}/{year_id}/daly_{location}_{year}.h5

    The run() method composes the _loop_submit_* functions appropriately given
    the __init__ args.

    The internal methods _loop_submit_* and _submit_* are responsible for
    actually qsubbing the variety of jobs: dalynator, burdenator, percentage
    change, aggregation over locations, and file tidying. _loop_ methods call
    their name-mirrored _submit_ methods with the proper scoping (e.g.
    location-year for the dalynator, location for percent change).
    """

    MONITOR_POLLING_INTERVAL = 30  # in seconds
    SUCCESS_LOG_MESSAGE = "DONE pipeline complete"
    END_1 = ".*" + ComputeDalys.END_MESSAGE + ".*"
    END_2 = ".*" + SUCCESS_LOG_MESSAGE + ".*"
    DALYNATOR_TABLE_TYPES = ["single_year", "multi_year"]
    BURDENATOR_TABLE_TYPES = ["single_year", "multi_year"]

    STDERR_PHASE_DIR_TEMPLATE = "{}/stderr/{}/{}"
    STDERR_FILE_TEMPLATE = "{}/stderr/{}/{}/stderr-$JOB_ID-$JOB_NAME.txt"

    ERROR_STOPPED_ON_FAILURE = 1

    def __init__(self, args):
        self.args = args
        permanent_monitor = ConnectionConfig(
            monitor_host=args.monitor_host,
            monitor_port=args.monitor_port,
            request_retries=args.request_retries,
            request_timeout=args.request_timeout)

        permanent_publisher = ConnectionConfig(
            monitor_host=args.publisher_host,
            monitor_port=args.publisher_port,
            request_retries=args.request_retries,
            request_timeout=args.request_timeout)
        self.job_queue = JobQueue(scheduler=RetryScheduler,
                                  executor=SGEExecutor,
                                  monitor_connection=permanent_monitor,
                                  publisher_connection=permanent_publisher)

        # Used to register the new batch
        self.requester = Requester(monitor_connection=permanent_monitor)

        self.conda_info = self._read_conda_info()
        self.root = os.path.dirname(os.path.realpath(__file__))
        self.batch_id = self._register_with_monitor()

        # Tracks the location & year of each jid, makes debugging and resuming easier
        # self.job_location_year_by_jid = {}
        self.args.all_year_ids = self.args.year_ids_1 + self.args.year_ids_2
        # It does not matter that all_year_ids is not in sorted order

    def job_done(self, log_file):
        logger.debug("  resume mode, checking log file {}".format(log_file))
        match1 = DalynatorJobSwarm.grep(log_file, DalynatorJobSwarm.END_1)
        match2 = DalynatorJobSwarm.grep(log_file, DalynatorJobSwarm.END_2)
        is_done = (match1 is not None) and (match2 is not None)
        return is_done

    @staticmethod
    def grep(file_to_search, pattern):
        compiled = re.compile(pattern)
        with open(file_to_search, "r") as f:
            for line in f:
                match = compiled.match(line)
                if match:
                    return match
        return None

    def run(self, remote_program):
        # Get location id lists
        core_location_ids, aggregate_location_ids = (
            self._get_location_lists(remote_program))

        self._check_input_files(remote_program, core_location_ids)

        # Cache population
        self._load_caches()

        # Run the core ___nator program
        self._loop_submit_xnator(remote_program, core_location_ids)

        # Run percent change for the dalynator
        if "dalynator" in remote_program:
            if self.args.start_years and self.args.end_years:
                self._loop_submit_pct_change("dalynator", core_location_ids, [gbd.measures.DALY])
            if self.args.upload:
                gbd_process_version_id = self.get_process_version(
                    gbd.gbd_process.SUMMARY)
                self._loop_submit_dalynator_upload(gbd_process_version_id,
                                                   core_location_ids,
                                                   [gbd.measures.DALY])

        # Run location-aggregation, PAF back-calculation, and summarization
        # jobs if all burdenator jobs have finished successfully
        if "burdenator" in remote_program:
            # Requires a location_set_id, so make sure that's either in or
            # derivable from the input args
            if not hasattr(self.args, 'location_set_id'):
                raise ValueError("To aggregate risk-attributable burden "
                                 "up the location hierarchy, either "
                                 "--location_set_id or "
                                 "--location_set_version_id must be "
                                 "provided")

            qsub_measure_ids = []
            if self.args.write_out_ylds_paf:
                qsub_measure_ids.append(gbd.measures.YLD)
            if self.args.write_out_ylls_paf:
                qsub_measure_ids.append(gbd.measures.YLL)
            if self.args.write_out_deaths_paf:
                qsub_measure_ids.append(gbd.measures.DEATH)
            if self.args.write_out_dalys_paf:
                qsub_measure_ids.append(gbd.measures.DALY)

            # Only aggregate if we have some aggregation to do.
            if aggregate_location_ids:
                self._loop_submit_loc_agg(qsub_measure_ids)
                self._loop_submit_burdenator_cleanup(qsub_measure_ids, aggregate_location_ids)

            if self.args.start_years and self.args.end_years:
                full_location_ids = core_location_ids + aggregate_location_ids
                self._loop_submit_pct_change("burdenator", full_location_ids, qsub_measure_ids)

            if self.args.upload:
                rf_process_version_id = self.get_process_version(
                    gbd.gbd_process.RISK)
                eti_process_version_id = self.get_process_version(
                    gbd.gbd_process.ETIOLOGY)
                self._loop_submit_burdenator_upload(
                    rf_process_version_id, eti_process_version_id,
                    qsub_measure_ids,
                    core_location_ids + aggregate_location_ids)

        msg = "Run complete."
        self.job_queue.stop_scheduler()
        self.requester.disconnect()
        logger.info(msg)
        print(msg)
        return

    def _check_input_files(self, remote_program, core_location_ids):

        if "burdenator" in remote_program:
            paf_dir = "{}/pafs/{}".format(self.args.input_data_root, self.args.paf_version)
            check_input.check_pafs(paf_dir, core_location_ids, self.args.all_year_ids)

            if self.args.write_out_ylls_paf:
                cod_dir = "{}/codcorrect/{}/draws/".format(self.args.input_data_root, self.args.cod_version)
                check_input.check_cod(cod_dir, core_location_ids, gbd.measures.YLL)

            if self.args.write_out_deaths_paf:
                cod_dir = "{}/codcorrect/{}/draws/".format(self.args.input_data_root, self.args.cod_version)
                check_input.check_cod(cod_dir, core_location_ids, gbd.measures.DEATH)

            if self.args.write_out_ylds_paf:
                epi_dir = get_folder_structure(
                    os.path.join(self.args.input_data_root, 'como',
                                 str(self.args.epi_version)))
                check_input.check_epi(epi_dir, core_location_ids, self.args.all_year_ids, gbd.measures.YLD)

            if self.args.write_out_dalys_paf:
                epi_dir = get_folder_structure(
                    os.path.join(self.args.input_data_root, 'como',
                                 str(self.args.epi_version)))
                check_input.check_epi(epi_dir, core_location_ids, self.args.all_year_ids, gbd.measures.YLD)

                cod_dir = "{}/codcorrect/{}/draws/".format(self.args.input_data_root, self.args.cod_version)
                check_input.check_cod(cod_dir, core_location_ids, gbd.measures.YLL)
        elif "dalynator" in remote_program:
            epi_dir = get_folder_structure(
                os.path.join(self.args.input_data_root, 'como',
                             str(self.args.epi_version)))
            check_input.check_epi(epi_dir, core_location_ids, self.args.all_year_ids, gbd.measures.YLD)

            cod_dir = "{}/codcorrect/{}/draws/".format(self.args.input_data_root, self.args.cod_version)
            check_input.check_cod(cod_dir, core_location_ids, gbd.measures.YLL)
        else:
            raise ValueError("remote_program has a wrong name")

    def query(self, query):
        """execute query on sqlite database through server
        Args:
            query (string): raw sql query string to execute on sqlite monitor
                database
        """
        msg = {'action': 'query', 'args': [query]}
        response = self.requester.send_request(msg)
        return response

    def get_job_information(self, sge_id):
        """Returns all information on the given job
        Args:
            sge_id: job ID
        """
        msg = {'action': 'get_job_instance_information', 'args': [sge_id]}
        response = self.requester.send_request(msg)
        return response

    @property
    def _conda_env(self):
        return str(self.conda_info['dUSERt_prefix'].split("/")[-1])

    @property
    def _path_to_conda_bin(self):
        return '{}/bin'.format(self.conda_info['root_prefix'])

    def _cache_pop(self):
        logger.debug("Starting to load population cache")
        core_index = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        if 'location_set_id' in vars(self.args):
            pop_ds = GetPopulationDataSource(
                "population", year_id=self.args.all_year_ids,
                location_set_id=self.args.location_set_id,
                gbd_round_id=self.args.gbd_round_id)
        elif 'location_set_version_id' in vars(self.args):
            pop_ds = GetPopulationDataSource(
                "population", year_id=self.args.all_year_ids,
                location_set_version_id=self.args.location_set_version_id,
                gbd_round_id=self.args.gbd_round_id)
        else:
            pop_ds = GetPopulationDataSource(
                "population", year_id=self.args.all_year_ids,
                location_id=self.args.location_id_list,
                gbd_round_id=self.args.gbd_round_id)

        pop_df = pop_ds.get_data_frame(core_index)
        pop_df = MetricConverter.aggregate_population(pop_df)

        for loc_set_id in self.args.add_agg_loc_set_ids:
            pop_ds = GetPopulationDataSource(
                "population", year_id=self.args.all_year_ids,
                location_set_id=loc_set_id,
                gbd_round_id=self.args.gbd_round_id)

            this_pop_df = pop_ds.get_data_frame(core_index)
            this_pop_df = MetricConverter.aggregate_population(this_pop_df)
            this_pop_df = this_pop_df[
                ~this_pop_df.location_id.isin(pop_df.location_id.unique())]
            pop_df = pop_df.append(this_pop_df)

        if len(pop_df[pop_df.location_id == 44620]) == 0:
            pop_df.append(pop_df[pop_df.location_id == 1].replace({'location_id': {1: 44620}}))
        cache_file = "{}/pop.h5".format(self.args.cache_dir)
        pop_df.to_hdf(cache_file, "pop", data_columns=core_index,
                      format="table")
        logger.debug("Cached population in {}".format(cache_file))

    def _cache_age_weights(self):
        logger.debug("Starting to load age_weights cache")
        age_weights_df = sm.get_age_weights()
        # returns age_group_id, age_group_weight_value as a pandas df
        cache_file = "{}/age_weights.h5".format(self.args.cache_dir)
        age_weights_df.to_hdf(cache_file, "age_weights", data_columns=['age_group_id'],
                              format="table")
        logger.debug("Cached age_weights in {}".format(cache_file))

    def _cache_age_spans(self):
        logger.debug("Starting to load age_spans cache")
        age_spans_df = sm.get_age_spans(12)  # 12 is the GBD 2016 age set...
        # returns age_group_id, age_group_years_start, age_group_years_end as a pandas df
        cache_file = "{}/age_spans.h5".format(self.args.cache_dir)
        age_spans_df.to_hdf(cache_file, "age_spans",
                            data_columns=['age_group_id'], format="table")
        logger.debug("Cached age_spans in {}".format(cache_file))

    def _cache_cause_hierarchy(self):
        logger.debug("Starting to load age_spans cache")
        cause_hierarchy = dbtrees.causetree(None, self.args.cause_set_id)
        cache_file = "{}/cause_hierarchy.pickle".format(self.args.cache_dir)
        pickle.dump(cause_hierarchy, open(cache_file, "wb"))
        logger.debug("Cached age_spans in {}".format(cache_file))

    def _existing_aggregation_files(self):
        return glob("{o}/loc_agg_draws/burden/"
                    "*/*/*.h5".format(o=self.args.out_dir))

    def _expected_aggregation_files(self, qsub_measure_ids):
        _, agg_loc_ids = self._get_location_lists("burdenator")
        meta_info = self._load_rei_metadata(qsub_measure_ids)
        filelist = []
        for meas_id in qsub_measure_ids:
            for loc_id in agg_loc_ids:
                d = "{o}/loc_agg_draws/burden/{l}/{m}".format(
                    o=self.args.out_dir, l=loc_id, m=meas_id)
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for rei_id in meta_info[sex_id][meas_id]['rei_ids']:
                        for year_id in self.args.all_year_ids:
                            f = "{d}/{m}_{y}_{l}_{r}_{s}.h5".format(
                                d=d, m=meas_id, y=year_id, l=loc_id, r=rei_id,
                                s=sex_id)
                            filelist.append(f)
        return filelist

    def _missing_aggregation_files(self, qsub_measure_ids):
        existing = self._existing_aggregation_files()
        expected = self._expected_aggregation_files(qsub_measure_ids)
        missing = list(set(expected)-set(existing))
        logger.info("Missing {} of {} expected files".format(len(missing),
                                                             len(expected)))
        return missing

    def _load_caches(self):
        logger.debug("Caching to dir: {}".format(self.args.cache_dir))
        self._cache_pop()
        self._cache_age_weights()
        self._cache_age_spans()
        self._cache_cause_hierarchy()

    def _get_location_lists(self, remote_program):
        """Returns a list of a "core_location_ids" (i.e. leaves in the location
        tree) and a list of "aggregate_location_ids" (i.e. the rest of the
        tree)"""
        num_locs = len(self.args.location_id_list)
        logger.info("Entering the job submission loop, resume mode = {}, "
                    "number of locations = {}".format(self.args.resume,
                                                      num_locs))
        logger.info("Conda bin dir: {}; Conda env: {}".format(
            self._path_to_conda_bin, self._conda_env))
        logger.info(" cod: {}; epi: {};".format(
            self.args.cod_version, self.args.epi_version))
        logger.info(
            "Full set of arguments passed to DalynatorJobSwarm {}".format(self.args))

        if "burdenator" in remote_program:
            if (hasattr(self.args, 'location_set_id') and self.args.location_set_id) or \
                    (hasattr(self.args, 'location_set_version_id') and self.args.location_set_version_id):
                if hasattr(self.args, 'location_set_id'):
                    lt = loctree(None, location_set_id=self.args.location_set_id)
                else:
                    lt = loctree(location_set_version_id=self.args.location_set_version_id)
                all_loc_ids = [l.id for l in lt.nodes]
                core_location_ids = [l.id for l in lt.leaves()]
                aggregate_location_ids = list(
                    set(all_loc_ids) - set(core_location_ids))

                # Include aggregate locations from supplemental sets
                for supp_set in self.args.add_agg_loc_set_ids:
                    supp_lt = loctree(None, location_set_id=supp_set)
                    supp_leaves = set([l.id for l in supp_lt.leaves()])
                    supp_nonleaves = (set([l.id for l in supp_lt.nodes]) -
                                      set(supp_leaves))
                    aggregate_location_ids = list(
                        set(aggregate_location_ids) | supp_nonleaves)

                aggregate_location_ids = list(
                    set(aggregate_location_ids) - {44620})
            else:
                if hasattr(self.args, 'location_id'):
                    # Passed a single location only.
                    core_location_ids = self.args.location_id_list
                    aggregate_location_ids = []
        else:
            core_location_ids = self.args.location_id_list
            aggregate_location_ids = []
        return core_location_ids, aggregate_location_ids

    def _loc_sets_to_aggregate(self):
        """Returns the set of location sets to be aggregated over, logging
        a warning if the 'supplemental' location sets have any leaves that
        aren't in the original set or if the sets have overlapping non-leaf
        locations"""
        main_set = self.args.location_set_id
        supp_sets = self.args.add_agg_loc_set_ids
        loc_sets = [main_set] + supp_sets

        # These warnings are future-defensive and non-essential, so
        # if there are any failures, we want to log a warning but not
        # stop execution
        try:
            self._loc_sets_to_agg_warnings(main_set, supp_sets)
        except Exception as e:
            logger.warn(
                "Location aggregation warnings are failing {}".format(e))

        return loc_sets

    def _loc_sets_to_agg_warnings(self, main_set, supp_sets):
        main_lt = loctree(None, location_set_id=main_set)
        main_leaves = set([l.id for l in main_lt.leaves()])
        main_nonleaves = set([l.id for l in main_lt.nodes]) - set(main_leaves)

        nl_sets = [main_nonleaves]
        for supp_set in supp_sets:
            supp_lt = loctree(None, location_set_id=supp_set)
            supp_leaves = set([l.id for l in supp_lt.leaves()])
            supp_nonleaves = (set([l.id for l in supp_lt.nodes]) -
                              set(supp_leaves))
            nl_sets.append(supp_nonleaves)
            if not supp_leaves.issubset(main_leaves):
                logger.warn("Some leaf locations from location set {} are not "
                            "included in the main location set "
                            "({})".format(supp_set, main_set))

        nonleaves = reduce(lambda x, y: x | y, nl_sets)
        if not len(nonleaves) == sum(map(len, nl_sets)):
            logger.warn("There are overlapping non-leaf locations across the "
                        "requested location aggregation sets. Watch out for "
                        "race conditions...")

    def _loop_submit_xnator(self, remote_program, core_location_ids):

        if "burdenator" in remote_program:
            tool_name = "Burdenator"
        elif "dalynator" in remote_program:
            tool_name = "DALYnator"
        else:
            tool_name = "No Tool Name"
        logger.info("============")
        logger.info("Begin Most detailed {} loop".format(tool_name))
        for location_id in core_location_ids:
            for year_id in self.args.all_year_ids:

                if self.args.no_details:
                    continue


                if "burdenator" in remote_program:
                    self._submit_burdenator(location_id, year_id)
                elif "dalynator" in remote_program:
                    self._submit_dalynator(location_id, year_id)
                else:
                    raise ValueError("Tool was neither dalynator nor burdenator: {}".format(remote_program))

                logger.info("  Created job ({}, {})".format(location_id, year_id))

        self._block_and_fail(tool_name, "most-detailed")

    def _block_and_fail(self, tool_name, loop_name):
        logger.info("  Created all {} {} jobs, waiting".format(tool_name, loop_name))
        self.job_queue.block_till_done(stop_scheduler_when_done=False)
        logger.info("  DONE, {} {} jobs".format(tool_name, loop_name))
        logger.info("============")

        failed_jobs = self.job_queue.executor.failed_jobs
        if len(failed_jobs):
            logger.error("TERMINATING the entire job swarm because {} jobs failed for {} {}"
                         .format(len(failed_jobs), tool_name, loop_name))
            for j in failed_jobs:
                logger.info("   {}".format(str(j)))

            sys.exit(DalynatorJobSwarm.ERROR_STOPPED_ON_FAILURE)

    def _loop_submit_pct_change(self, tool_name, location_ids, measure_ids):
        """For each measure id, compare start and end year pairs. Submit ALL the jobs and then wait for them"""
        logger.info('Submitting pct change jobs for measures {}'.format(measure_ids))

        for measure_id in measure_ids:
            logger.debug('Pct change, specific measure {}'.format(measure_id))
            for start_year, end_year in zip(self.args.start_years, self.args.end_years):
                for location_id in location_ids:
                    if self.args.no_pct_change:
                        continue

                    self._submit_pct_change(tool_name, location_id, start_year, end_year, measure_id)
        self._block_and_fail(tool_name, "percentage-change")

    def _load_rei_metadata(self, qsub_measure_ids):
        meta_files = glob("{}/draws/*/meta.json".format(self.args.out_dir))
        meta_info = {}
        for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE, gbd.sex.BOTH]:
            meta_info[sex_id] = {}
            for measure_id in qsub_measure_ids:
                meta_info[sex_id][measure_id] = {'rei_ids': []}
        for mf in meta_files:
            m = load_meta(os.path.dirname(mf))
            for sex_id in m.keys():
                for measure_id in m[sex_id].keys():
                    meta_info[int(sex_id)][int(measure_id)]['rei_ids'] = (
                        list(set(
                            meta_info[int(sex_id)][int(measure_id)]['rei_ids'] +
                            m[sex_id][measure_id]['rei_ids'])))
        return meta_info

    def _loop_submit_loc_agg(self, qsub_measure_ids):
        logger.info("Start Burdenator location aggregation")
        submitted_pairs = []

        meta_info = self._load_rei_metadata(qsub_measure_ids)

        for loc_set in self._loc_sets_to_aggregate():
            for year_id in self.args.all_year_ids:
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for measure_id in qsub_measure_ids:
                        if self.args.no_loc_agg:
                            continue
                        qsub_rei_ids = (
                            meta_info[sex_id][measure_id]['rei_ids'])
                        for rei_id in qsub_rei_ids:
                            self._submit_loc_agg(loc_set, year_id,
                                                 sex_id, measure_id,
                                                 rei_id)
                            submitted_pairs.append((measure_id, rei_id,
                                                    year_id, sex_id))

                self._block_and_fail("burdenator", "location-aggregation")

    def _loop_submit_burdenator_cleanup(self, qsub_measure_ids,
                                        aggregate_location_ids):
        # Submit cleanup jobs for aggregate locations

        logger.info("Start Burdenator 'cleanup'")
        logger.info("Looking for missing location_aggregation files...")
        missing = self._missing_aggregation_files(qsub_measure_ids)
        if len(missing) > 0:
            logger.error("Missing files {}".format(missing))
            logger.error("Missing location aggregation files. Cannot proceed "
                         "to Cleanup phase")
            sys.exit(DalynatorJobSwarm.ERROR_STOPPED_ON_FAILURE)

        submitted_pairs = []
        for measure_id in qsub_measure_ids:
            for location_id in aggregate_location_ids:
                for year_id in self.args.all_year_ids:
                    if self.args.no_cleanup:
                        continue

                    self._submit_burdenator_cleanup(location_id,
                                                          year_id,
                                                          measure_id)
                    submitted_pairs.append((measure_id, location_id,
                                           year_id))
                self._block_and_fail("burdenator", "cleanup for location {} and measure {}".format(location_id, measure_id))

    def _create_gbd_process_version(self, gbd_process_id, version_note,
                                    code_version, process_metadata):
        if self.args.upload_to_test:
            upload_env = DBEnv.DEV
        else:
            upload_env = DBEnv.PROD

        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_process_id,
            gbd_process_version_note=version_note,
            code_version=code_version,
            gbd_round_id=self.args.gbd_round_id,
            metadata=process_metadata,
            env=upload_env)
        return pv.gbd_process_version_id

    def get_process_version(self, gbd_process_id):
        """
        Retrieves a GBD process version ID for the given GBD process ID

        Code creates a file called gbd_processes.json which is saved after
        creating a process version. This file is a dictionary where the key is
        the GBD process ID and the value is the GBD process version ID. This
        should help to prevent multiple process versions from being created
        during resumes.

        Workflow:
          -Checks to see if there is a file called gbd_processes.json within the
           output folder
          -If there is a file, check to see if there is a GBD process version ID
           for the GBD process specified
          -If no file or data is found, it creates a new GBD process version
          -After the process version has been created, the gbd_processes.json is
           saved to disk
        """
        # Check if existing GBD processes file exists
        gbd_processes_file = "{}/gbd_processes.json".format(self.args.out_dir)
        if os.path.exists(gbd_processes_file):
            gbd_processes = json.load(file(gbd_processes_file, 'r'))
        else:
            gbd_processes = {}
        # Check if existing GBD process version ID exists
        if gbd_processes:
            if str(gbd_process_id) in gbd_processes.keys():
                existing_process_version_id = gbd_processes[str(gbd_process_id)]
            else:
                existing_process_version_id = None
        else:
            existing_process_version_id = None
        # Create GBD process version or return existing one
        if not existing_process_version_id:
            # Create the GBD process version
            if gbd_process_id == gbd.gbd_process.RISK:
                gbd_process_version_id = self.create_rf_process_version()
                gbd_processes[gbd.gbd_process.RISK] = gbd_process_version_id
            elif gbd_process_id == gbd.gbd_process.ETIOLOGY:
                gbd_process_version_id = self.create_eti_process_version()
                gbd_processes[gbd.gbd_process.ETIOLOGY] = gbd_process_version_id
            elif gbd_process_id == gbd.gbd_process.SUMMARY:
                gbd_process_version_id = self.create_daly_process_version()
                gbd_processes[gbd.gbd_process.SUMMARY] = gbd_process_version_id
            else:
                raise ValueError(
                    "GBD process {} is not a valid ".format(gbd_process_id) +
                    "process for either the DALYnator or Burdenator")
            # Write out metadata file
            json.dump(gbd_processes, file(gbd_processes_file, 'w'))
            # Return ID
            return gbd_process_version_id
        else:
            return existing_process_version_id

    def create_rf_process_version(self):
        # Build process version metadata dictionary
        versions = []
        process_metadata = {gbd.gbd_metadata_type.BURDENATOR: self.args.version}
        process_metadata[gbd.gbd_metadata_type.RISK] = self.args.paf_version
        versions.append("RF: {}".format(self.args.paf_version))
        if self.args.write_out_ylds_paf:
            process_metadata[gbd.gbd_metadata_type.COMO] = self.args.epi_version
            versions.append("YLDs: {}".format(self.args.epi_version))
        if self.args.write_out_ylls_paf:
            process_metadata[gbd.gbd_metadata_type.CODCORRECT] = self.args.cod_version
            versions.append("YLLs: {}".format(self.args.cod_version))
        if self.args.write_out_deaths_paf:
            process_metadata[gbd.gbd_metadata_type.CODCORRECT] = self.args.cod_version
            versions.append("Deaths: {}".format(self.args.cod_version))
        if self.args.write_out_dalys_paf and self.args.daly_version:
            process_metadata[gbd.gbd_metadata_type.DALYNATOR] = self.args.daly_version
            versions.append("DALYs: {}".format(self.args.daly_version))
        version_note = "Burdenator {} ({})".format(
            self.args.version, ', '.join(versions))

        # Create new process version
        gbd_process_id = gbd.gbd_process.RISK
        code_version = "GBD2016"
        return self._create_gbd_process_version(gbd_process_id, version_note,
                                                code_version,
                                                process_metadata)

    def create_eti_process_version(self):
        # Build process version metadata dictionary
        versions = []
        process_metadata = {gbd.gbd_metadata_type.BURDENATOR: self.args.version}
        process_metadata[gbd.gbd_metadata_type.RISK] = self.args.paf_version
        versions.append("RF: {}".format(self.args.paf_version))
        if self.args.write_out_ylds_paf:
            process_metadata[gbd.gbd_metadata_type.COMO] = self.args.epi_version
            versions.append("YLDs: {}".format(self.args.epi_version))
        if self.args.write_out_ylls_paf:
            process_metadata[gbd.gbd_metadata_type.CODCORRECT] = self.args.cod_version
            versions.append("YLLs: {}".format(self.args.cod_version))
        if self.args.write_out_deaths_paf:
            process_metadata[gbd.gbd_metadata_type.CODCORRECT] = self.args.cod_version
            versions.append("Deaths: {}".format(self.args.cod_version))
        if self.args.write_out_dalys_paf and self.args.daly_version:
            process_metadata[gbd.gbd_metadata_type.DALYNATOR] = self.args.daly_version
            versions.append("DALYs: {}".format(self.args.daly_version))
        version_note = "Burdenator {} ({})".format(
            self.args.version, ', '.join(versions))

        # Create new process version
        gbd_process_id = gbd.gbd_process.ETIOLOGY
        code_version = "GBD2016"
        return self._create_gbd_process_version(gbd_process_id, version_note,
                                                code_version,
                                                process_metadata)

    def create_daly_process_version(self):
        # Build process version metadata dictionary
        versions = []
        process_metadata = {}
        process_metadata[gbd.gbd_metadata_type.COMO] = self.args.epi_version
        versions.append("YLDs: {}".format(self.args.epi_version))
        process_metadata[gbd.gbd_metadata_type.CODCORRECT] = self.args.cod_version
        versions.append("CoD: {}".format(self.args.cod_version))
        process_metadata[gbd.gbd_metadata_type.DALYNATOR] = self.args.version
        version_note = "DALYnator {} ({})".format(
            self.args.version, ', '.join(versions))

        # Create new process version
        gbd_process_id = gbd.gbd_process.SUMMARY
        code_version = "GBD2016"
        return self._create_gbd_process_version(gbd_process_id, version_note,
                                                code_version,
                                                process_metadata)

    def _loop_submit_burdenator_upload(self, rf_process_version_id,
                                       eti_process_version_id,
                                       qsub_measure_ids, location_ids):
        # Submit cleanup jobs for aggregate locations
        submitted_pairs = []
        gbd_process_versions = [rf_process_version_id, eti_process_version_id]
        for gbd_process_version_id in gbd_process_versions:
            for measure_id in qsub_measure_ids:
                for table_type in DalynatorJobSwarm.BURDENATOR_TABLE_TYPES:
                    self._submit_burdenator_upload(gbd_process_version_id,
                                                   location_ids,
                                                   measure_id, table_type)
                    submitted_pairs.append((measure_id, table_type))
        self._block_and_fail("burdenator", "upload")

    def _loop_submit_dalynator_upload(self, gbd_process_version_id,
                                      core_location_ids, qsub_measure_ids):
        # Submit cleanup jobs for aggregate locations
        submitted_pairs = []
        for measure_id in qsub_measure_ids:
            for table_type in DalynatorJobSwarm.DALYNATOR_TABLE_TYPES:
                self._submit_dalynator_upload(gbd_process_version_id,
                                                    core_location_ids,
                                                    measure_id, table_type)
                submitted_pairs.append((measure_id, table_type))
        self._block_and_fail("dalynator", "upload")

    def _prepare_xnator_job_params(self, location_id, year_id, tool_name):
        """Prepares the job parameters for qsub'ing either DALYnator or
        Burdenator jobs"""
        job_params = [
            "--cod", self.args.cod_version,
            "--epi", self.args.epi_version,
            "--n_draws", self.args.year_n_draws_map[year_id],
            "--location_id", location_id,
            "--year_id", year_id,
            "--out_dir", self.args.out_dir,
            "--version", self.args.version,
            "--verbose",
            "--tool_name", tool_name
        ]
        job_params = self._add_jobmon_parameters(self.args, job_params)

        if self.args.turn_off_null_and_nan_check:
            job_params.append("--turn_off_null_and_nan_check")
        if self.args.no_sex_aggr:
            job_params.append("--no_sex")
        if self.args.no_age_aggr:
            job_params.append("--no_age")
        return job_params

    def _add_jobmon_parameters(self, args, job_params):
        v = vars(args)
        for a in ["monitor_host", "monitor_port", "request_timeout", "request_retries", "publisher_host", "publisher_port"]:
            job_params += ["--"+a] + [str(v[a])]
        return job_params

    def _read_conda_info(self):
        conda_info = json.loads(
            subprocess.check_output(['conda', 'info', '--json']).decode())
        return conda_info

    def _register_with_monitor(self):
        batch_info_file = os.path.join(self.args.out_dir, "batch_info.json")
        has_batch = os.path.isfile(batch_info_file)
        if has_batch:
            with open(batch_info_file, "r") as f:
                batch_info = json.load(f)
            batch_id = int(batch_info['batch_id'])
        else:
            batch_name = 'burdenator_v{}'.format(self.args.version)
            user_name = getpass.getuser()
            batch_resp = self.requester.send_request(
                {'action': 'register_batch',
                 'kwargs': {'name': batch_name, 'user': user_name}})
            batch_id = batch_resp[1]
            with open(batch_info_file, "w") as f:
                json.dump({'batch_id': batch_id,
                           'batch_name': batch_name,
                           'user_name': user_name}, f)
        return batch_id

    def _remote_job_was_completed(self, location_id, year_id):
        """Checks the DALYnator's log files for the given location_id and
        year_id to confirm whether it DALYnated successfully, returning a
        boolean"""
        exact_output_dir = os.path.join(self.args.out_dir, 'draws',
                                        str(location_id))
        exact_log_dir = os.path.join(self.args.out_dir, 'log')

        output_file, single_log = get_input_args.calculate_filenames(
            exact_output_dir, exact_log_dir, gbd.measures.DALY,
            location_id, year_id)
        logger.debug("Resume mode, checking job h5 {} and log {}".format(
            output_file, single_log))
        if os.path.isfile(output_file) and os.path.isfile(single_log):
            logger.debug("  files present, checking times")
            if self.job_done(single_log):
                h5_t = os.path.getmtime(output_file)
                log_t = os.path.getmtime(single_log)
                logger.debug("    file times h5 {} vs log {}".format(h5_t,
                                                                     log_t))
                if abs(h5_t - log_t) < 60:
                    logger.info('    Resume mode, skipping existing '
                                '{}-{}'.format(location_id, year_id))
                    return True
        return False

    def _submit_burdenator(self, location_id, year_id):
        runfile = "{root}/remote_run_pipeline_burdenator.py".format(
            root=self.root)

        # If this is running in "resume" mode, then do not kick off the job if
        # the output file exists, and the log file has DONE, and the dates are
        # within 60 seconds
        if self.args.resume:
            if self._remote_job_was_completed(location_id, year_id):
                return None

        job_params = self._prepare_xnator_job_params(location_id, year_id, "burdenator")
        job_params.append("--paf_version")
        job_params.append(self.args.paf_version)
        qsub_measure_ids = []
        if self.args.write_out_ylds_paf:
            job_params.append("--ylds_paf")
            qsub_measure_ids.append(gbd.measures.YLD)
        if self.args.write_out_ylls_paf:
            job_params.append("--ylls_paf")
            qsub_measure_ids.append(gbd.measures.YLL)
        if self.args.write_out_deaths_paf:
            job_params.append("--deaths_paf")
            qsub_measure_ids.append(gbd.measures.DEATH)
        if self.args.write_out_dalys_paf:
            job_params.append("--dalys_paf")
            qsub_measure_ids.append(gbd.measures.DALY)
        if self.args.daly_version:
            job_params.append("--daly")
            job_params.append(self.args.daly_version)

        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, "burdenator", "detailed"))

        jobname = "burden_{location_id}_{year_id}".format(
            location_id=location_id, year_id=year_id)
        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
            )
        self.job_queue.queue_job(
            job,
            slots=28,
            memory=56,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, "burdenator", "detailed"),
            project=self.args.sge_project,
            process_timeout=(60 * 90))

    def _submit_burdenator_cleanup(self, location_id, year_id, measure_id):
        runfile = "{}/remote_run_pipeline_burdenator_cleanup.py".format(
            self.root)
        job_params = [
            "--out_dir", self.args.out_dir,
            "--measure_id", measure_id,
            "--location_id", location_id,
            "--year_id", year_id,
            "--n_draws", self.args.year_n_draws_map[year_id],
            "--gbd_round_id", self.args.gbd_round_id,
            "--cod", self.args.cod_version,
            "--epi", self.args.epi_version,
            "--version", self.args.version,
            "--verbose",
            "--tool_name", "burdenator"
        ]
        job_params = self._add_jobmon_parameters(self.args, job_params)
        if self.args.turn_off_null_and_nan_check:
            job_params.append("--turn_off_null_and_nan_check")
        jobname = ("burden_clean_"
                   "{mid}_{lid}_{yid}".format(
                       mid=measure_id,
                       lid=location_id,
                       yid=year_id))

        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, "burdenator", "cleanup"))

        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
        )
        self.job_queue.queue_job(
            job,
            slots=25,
            memory=50,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, "burdenator", "cleanup"),
            project=self.args.sge_project,
            process_timeout=(60 * 60))

        logger.debug("  Created burdenator cleanup-job ({}),".format(jobname))

    def _submit_burdenator_upload(self, gbd_process_version_id, location_ids,
                                  measure_id, table_type):
        runfile = "{}/remote_run_pipeline_burdenator_upload.py".format(
            self.root)
        job_params = [
            "--out_dir", self.args.out_dir,
            "--gbd_process_version_id", gbd_process_version_id,
            "--location_ids", " ".join([str(x) for x in location_ids]),
            "--measure_id", measure_id,
            "--table_type", table_type,
            "--verbose",
            "--tool_name", "burdenator"
        ]
        job_params = self._add_jobmon_parameters(self.args, job_params)
        if self.args.upload_to_test:
            job_params.append("--upload_to_test")

        jobname = ("burden_upload_"
                   "{mid}_{pv}_{tt}".format(
                       mid=measure_id,
                       pv=gbd_process_version_id,
                       tt=table_type))

        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, "burdenator", "upload"))

        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
        )
        self.job_queue.queue_job(
            job,
            slots=20,
            memory=40,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, "burdenator", "upload"),
            project=self.args.sge_project,
            process_timeout=(60 * 60 * 30))

        logger.debug("  created burdenator upload-job ({})".format(jobname))

    def _submit_dalynator(self, location_id, year_id):
        runfile = "{root}/remote_run_pipeline_dalynator.py".format(
            root=self.root)

        # If this is running in "resume" mode, then do not kick off the job if
        # the output file exists, and the log file has DONE, and the dates are
        # within 60 seconds
        if self.args.resume:
            if self._remote_job_was_completed(location_id, year_id):
                return None

        job_params = self._prepare_xnator_job_params(location_id, year_id, "dalynator")

        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, "dalynator", "detailed"))

        jobname = "daly_{location_id}_{year_id}".format(
                location_id=location_id, year_id=year_id)

        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
        )
        self.job_queue.queue_job(
            job,
            slots=20,
            memory=40,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, "dalynator", "detailed"),
            project=self.args.sge_project,
            process_timeout=(60 * 60))
        # Timeout in seconds, i.e. 45 minutes

    def _submit_loc_agg(self, location_set_id, year_id, sex_id, measure_id,
                        rei_id):
        runfile = "{}/remote_run_pipeline_burdenator_loc_agg.py".format(
            self.root)
        job_params = [
            "--out_dir", self.args.out_dir,
            "--year_id", year_id,
            "--sex_id", sex_id,
            "--rei_id", rei_id,
            "--measure_id", measure_id,
            "--location_set_id", location_set_id,
            "--version", self.args.version,
            "--verbose",
            "--tool_name", "burdenator",
            "--gbd_round_id", self.args.gbd_round_id
        ]
        job_params = self._add_jobmon_parameters(self.args, job_params)
        jobname = ("burden_lagg_"
                   "{lsid}_{yid}_{rid}_{mid}_{sid}".format(
                       lsid=location_set_id,
                       yid=year_id,
                       rid=rei_id,
                       mid=measure_id,
                       sid=sex_id))

        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, "burdenator", "loc-agg"))

        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
        )
        self.job_queue.queue_job(
            job,
            slots=20,
            memory=40,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, "burdenator", "loc-agg"),
            project=self.args.sge_project,
            process_timeout=(60 * 90))

        logger.info("  Created burdenator agg-job ({})".format(jobname))

    def _submit_pct_change(self, tool_name, location_id, start_year, end_year, measure_id):
        runfile = "{}/remote_run_pipeline_percentage_change.py".format(
            self.root)
        job_params = [
            "--out_dir", self.args.out_dir,
            "--location_id", location_id,
            "--start_year", start_year,
            "--end_year", end_year,
            "--n_draws", self.args.year_n_draws_map[start_year],
            "--measure_id", str(measure_id),  # otherwise parse_args can have problems
            "--version", self.args.version,
            "--tool_name", tool_name
        ]
        job_params = self._add_jobmon_parameters(self.args, job_params)
        jobname = "dn_pct_change_{lid}_{s}_{e}".format(lid=location_id, s=start_year, e=end_year)
        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, tool_name, "pct-change"))

        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
        )
        self.job_queue.queue_job(
            job,
            slots=30,
            memory=60,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, tool_name, "pct-change"),
            project=self.args.sge_project,
            process_timeout=(60 * 90))

        logger.info("  Created dalynator pct-change job ({}: loc {} {}-{})".format(
            jobname, location_id, start_year, end_year))

    def _submit_dalynator_upload(self, gbd_process_version_id,
                                 core_location_ids, measure_id, table_type):
        runfile = "{}/remote_run_pipeline_dalynator_upload.py".format(
            self.root)
        job_params = [
            "--out_dir", self.args.out_dir,
            "--gbd_process_version_id", gbd_process_version_id,
            "--location_ids", ' '.join([str(x) for x in core_location_ids]),
            "--measure_id", measure_id,
            "--table_type", table_type,
            "--verbose",
            "--tool_name", "dalynator"
        ]
        job_params = self._add_jobmon_parameters(self.args, job_params)
        if self.args.upload_to_test:
            job_params.append("--upload_to_test")

        jobname = ("dalynator_upload_"
                   "{mid}_{pv}_{tt}".format(
                       mid=measure_id,
                       pv=gbd_process_version_id,
                       tt=table_type))

        logger.debug("job_params = {}".format(job_params))
        makedirs_safely(DalynatorJobSwarm.STDERR_PHASE_DIR_TEMPLATE.format(self.args.out_dir, "dalynator", "upload"))

        job = self.job_queue.create_job(
            jobname=jobname,
            runfile=true_path(runfile),
            parameters=job_params
        )
        self.job_queue.queue_job(
            job,
            slots=20,
            memory=40,
            stderr=DalynatorJobSwarm.STDERR_FILE_TEMPLATE.format(self.args.out_dir, "dalynator", "upload"),
            project=self.args.sge_project,
            process_timeout=(60 * 60 * 30))

        logger.debug("  created dalynator upload-job ({})".format(jobname))
