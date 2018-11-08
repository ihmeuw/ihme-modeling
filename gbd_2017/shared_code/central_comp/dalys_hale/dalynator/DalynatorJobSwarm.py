import json
import logging
import os
import pickle
import subprocess
import sys
from functools import reduce
from glob import glob
import socket
from contextlib import closing
import pandas as pd
import requests

from db_tools.ezfuncs import query
from db_queries import get_location_metadata
import gbd.constants as gbd
import gbd.gbd_round as gbr
from db_queries.get_age_metadata import get_age_weights, get_age_spans
from hierarchies import dbtrees
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow

import dalynator.app_common as ac
import dalynator.check_input_files_exist as check_input
import dalynator.type_checking as tp
from dalynator.compute_dalys import ComputeDalys
from dalynator.compute_summaries import MetricConverter
from dalynator.constants import STDERR_PHASE_DIR_TEMPLATE
from dalynator.data_source import GetPopulationDataSource
from dalynator.get_yld_data import get_como_folder_structure
from dalynator.makedirs_safely import makedirs_safely

from dalynator.tasks.burdenator_most_detailed_task import \
    BurdenatorMostDetailedTask
from dalynator.tasks.dalynator_most_detailed_task import \
    DalynatorMostDetailedTask
from dalynator.tasks.pct_change_task import PercentageChangeTask
from dalynator.tasks.dbsync_task import DBSyncTask
from dalynator.tasks.upload_task import UploadTask
from dalynator.tasks.location_aggregation_task import LocationAggregationTask
from dalynator.tasks.burdenator_cleanup_task import BurdenatorCleanupTask
from dalynator.version_manager import VersionManager

logger = logging.getLogger(__name__)


class DalynatorJobSwarm(object):
    """
    Run all leaf location jobs in parallel, by location-year.
    Output directory structure:
        DIRECTORY/{location_id}/
        {year_id}/daly_{location}_{year}.h5

    The run() method composes the _loop_submit_* functions appropriately given
    the __init__ args.

    The internal methods _loop_submit_* and _submit_* are responsible for
    actually qsubbing the variety of jobs: dalynator, burdenator, percentage
    change, aggregation over locations, and file tidying. _loop_ methods call
    their name-mirrored _submit_ methods with the proper scoping (e.g.
    location-year for the dalynator, location for percent change).
    """

    SUCCESS_LOG_MESSAGE = "DONE pipeline complete"
    END_1 = ".*" + ComputeDalys.END_MESSAGE + ".*"
    END_2 = ".*" + SUCCESS_LOG_MESSAGE + ".*"
    NATOR_TABLE_TYPES = ["single_year", "multi_year"]
    SINGLE_YEAR_TABLE_TYPE = ["single_year"]
    STORAGE_ENGINES = ["INNODB", "COLUMNSTORE"]

    ERROR_STOPPED_ON_FAILURE = 1

    def __init__(
            self,

            tool_name=None,

            input_data_root=None,
            out_dir=None,

            cod_version=None,
            epi_version=None,
            paf_version=None,
            daly_version=None,
            version=None,

            cause_set_id=None,
            gbd_round_id=None,

            year_ids_1=None,
            n_draws_1=None,
            year_ids_2=None,
            n_draws_2=None,

            start_year_ids=None,
            end_year_ids=None,

            location_set_version_id=None,

            add_agg_loc_set_ids=None,

            no_sex_aggr=None,
            no_age_aggr=None,
            write_out_ylds_paf=None,
            write_out_ylls_paf=None,
            write_out_deaths_paf=None,
            write_out_dalys_paf=None,
            write_out_star_ids=None,

            # The following arguments control whether certain phases execute
            # (or not)
            start_at=None,
            end_at=None,
            upload_to_test=None,

            turn_off_null_and_nan_check=None,

            cache_dir=None,

            sge_project=None,
            verbose=None,
            raise_on_paf_error=None,
            do_not_execute=None,
    ):

        # Input validation:
        # No nulls, sensible values, ie type checking!

        self.tool_name = tp.is_string(tool_name, "tool name")

        self.input_data_root = tp.is_string(input_data_root,
                                            "root directory for input data")
        self.out_dir = tp.is_string(out_dir, "root directory for output data")

        self.cod_version = tp.is_positive_int(cod_version, "cod version")
        self.epi_version = tp.is_positive_int(epi_version, "epi version")
        self.daly_version = daly_version

        if self.tool_name == "burdenator":
            self.paf_version = tp.is_positive_int(paf_version, "paf version")
            self.cause_set_id = tp.is_positive_int(cause_set_id,
                                                   "cause set id")
            if write_out_dalys_paf:
                self.daly_version = tp.is_positive_int(daly_version,
                                                       "daly version")
        else:
            self.paf_version = None

        self.version = tp.is_positive_int(version,
                                          "my dalynator/burdenator version")
        self.gbd_round_id = tp.is_positive_int(gbd_round_id, "gbd round id")
        self.gbd_round = gbr.gbd_round_from_gbd_round_id(self.gbd_round_id)

        self.sge_project = tp.is_string(sge_project, "sge project")
        self.verbose = verbose
        self.year_ids_1 = tp.is_list_of_year_ids(year_ids_1,
                                                 "first list of year_ids")
        self.n_draws_1 = tp.is_positive_int(
            n_draws_1, "number of draws in first year set")
        self.year_ids_2 = tp.is_list_of_year_ids(year_ids_2,
                                                 "second list of year_ids")
        self.n_draws_2 = tp.is_positive_int(
            n_draws_2, "number of draws in second year set")

        self.year_n_draws_map = ac.construct_year_n_draws_map(
            self.year_ids_1, self.n_draws_1, self.year_ids_2, self.n_draws_2)

        self.start_year_ids = tp.is_list_of_year_ids(
            start_year_ids, "list of start years for pct change")
        self.end_year_ids = tp.is_list_of_year_ids(
            end_year_ids, "list of end years for pct change")

        self.location_set_version_id = tp.is_None_or_positive_int(
            location_set_version_id, "location set version id")
        if not self.location_set_version_id:
            raise ValueError("location set version id was None")
        self.add_agg_loc_set_ids = tp.is_None_or_list_of_int(
            add_agg_loc_set_ids, "aggregate location set ids")

        self.location_set_id = ac.location_set_version_to_location_set(
            self.location_set_version_id)
        # The following call also validates those lists
        self.most_detailed_location_ids, self.aggregate_location_ids = \
            ac.expand_and_validate_location_lists(self.tool_name,
                                                  self.location_set_version_id,
                                                  self.add_agg_loc_set_ids,
                                                  self.gbd_round_id)
        self.all_location_ids = (self.most_detailed_location_ids +
                                 self.aggregate_location_ids)
        self.all_location_set_ids = [self.location_set_id]
        if self.aggregate_location_ids:
            self.all_location_set_ids, self.lsid_to_loc_map = \
                self._loc_sets_to_aggregate()

        self.no_sex_aggr = tp.is_boolean(no_sex_aggr,
                                         "do not perform sex aggregation")
        self.no_age_aggr = tp.is_boolean(no_age_aggr,
                                         "do not perform age aggregation")
        if self.tool_name == "burdenator":
            self.write_out_ylds_paf = tp.is_boolean(write_out_ylds_paf,
                                                    "write out yld paf draws")
            self.write_out_ylls_paf = tp.is_boolean(write_out_ylls_paf,
                                                    "write out yll paf draws")
            self.write_out_deaths_paf = tp.is_boolean(
                write_out_deaths_paf, "write out death paf draws")
            self.write_out_dalys_paf = tp.is_boolean(
                write_out_dalys_paf, "write out daly paf draws")
            self.write_out_star_ids = tp.is_boolean(
                write_out_star_ids, "write out star_id's")
        else:
            # Dalynator has no star ids
            self.write_out_star_ids = False

        self.measure_ids = self._get_all_measure_ids()

        self.phases_to_run = ac.validate_start_end_flags(start_at, end_at,
                                                         self.tool_name)
        self.upload_to_test = tp.is_boolean(upload_to_test, "upload to test")
        self.most_detailed_jobs_by_command = {}
        self.loc_agg_jobs_by_command = {}
        self.cleanup_jobs_by_command = {}
        self.pct_change_jobs_by_command = {}

        if 'pct_change' in self.phases_to_run:
            self.table_types = DalynatorJobSwarm.NATOR_TABLE_TYPES
        else:
            self.table_types = DalynatorJobSwarm.SINGLE_YEAR_TABLE_TYPE

        self.turn_off_null_and_nan_check = tp.is_boolean(
            turn_off_null_and_nan_check, "null check")
        self.raise_on_paf_error = tp.is_boolean(
            raise_on_paf_error, "paf check")
        self.do_not_execute = tp.is_boolean(
            do_not_execute, "do not execute"
        )

        self.cache_dir = tp.is_string(cache_dir,
                                      "path to internal cache directory")

        self.conda_info = self._read_conda_info()

        # Tracks the location & year of each jid, makes debugging and resuming
        # easier
        # self.job_location_year_by_jid = {}
        self.all_year_ids = self.year_ids_1 + self.year_ids_2
        # It does not matter that all_year_ids is not in sorted order

        # Load/create version info
        self._set_db_version_metadata()

        # Initiliaze a run attribute, to store the success/failure of the
        # run (mostly for testing)
        self.success = None

    def _post_to_slack(self, success, channel, null_inf_message, dag_id):
        headers = {'Content-type': 'application/json'}
        if success:
            data = {"text": "{} run v{} complete with dag_id {}! :tada: {}"
                    .format(self.tool_name, self.version, dag_id,
                            null_inf_message)}
        else:
            data = {"text": "{} run v{} failed with dag_id {}. :sob: {}"
                    .format(self.tool_name, self.version, dag_id,
                            null_inf_message)}

        with open('FILEPATH/private_url.json', "r") as f:
            private_url = json.load(f)[channel]
        response = requests.post(private_url, headers=headers, json=data)

    def _set_db_version_metadata(self):
        """Handles creation of GBD process versions and compare version (or
        reloads them from a file if resuming a previous run)"""
        versions_file = os.path.join(self.out_dir, "gbd_processes.json")
        logger.info("Looking for process version file '{}'"
                    .format(versions_file))
        if os.path.isfile(versions_file):
            # Preferentially load from a file if resuming a
            # previous run
            logger.info("    Reading from process version file")
            self.version_manager = VersionManager.from_file(versions_file)
            self.version_manager.validate_metadata(
                gbd.gbd_metadata_type.CODCORRECT, self.cod_version)
            self.version_manager.validate_metadata(
                gbd.gbd_metadata_type.COMO, self.epi_version)
            self.version_manager.validate_metadata(
                gbd.gbd_metadata_type.RISK, self.paf_version)
            if self.tool_name == "dalynator":
                self.version_manager.validate_metadata(
                    gbd.gbd_metadata_type.DALYNATOR, self.version)
            elif self.tool_name == "burdenator":
                self.version_manager.validate_metadata(
                    gbd.gbd_metadata_type.BURDENATOR, self.version)
        else:
            # If the version file is not present in the output directory,
            # the assumption is this run needs to create brand new PVs and CV
            # in the database
            logger.info("    No process version file")
            self.version_manager = VersionManager(
                self.gbd_round_id, upload_to_test=self.upload_to_test)
            self.version_manager.set_tool_run_version(
                gbd.gbd_metadata_type.CODCORRECT, self.cod_version)
            self.version_manager.set_tool_run_version(
                gbd.gbd_metadata_type.COMO, self.epi_version)
            self.version_manager.set_tool_run_version(
                gbd.gbd_metadata_type.RISK, self.paf_version)
            if self.tool_name == "dalynator":
                self.version_manager.set_tool_run_version(
                    gbd.gbd_metadata_type.DALYNATOR, self.version)
            elif self.tool_name == "burdenator":
                self.version_manager.set_tool_run_version(
                    gbd.gbd_metadata_type.BURDENATOR, self.version)
            self.version_manager.freeze(versions_file)

    def run(self, remote_program):
        try:
            # Get location id lists
            self._write_start_of_run_log_messages()

            self.task_dag = TaskDag(
                name='{t}_v{v}'.format(t=self.tool_name, v=self.version))

            if 'most_detailed' in self.phases_to_run:
                # This looks for inputs for most detailed stage
                self._check_input_files(self.tool_name,
                                        self.most_detailed_location_ids)

            # Cache population
            self._load_caches()

            # Run the core ___nator program
            if self.tool_name == "dalynator":
                self._loop_submit_xnator(remote_program, self.all_location_ids)
            elif self.tool_name == "burdenator":
                self._loop_submit_xnator(remote_program,
                                         self.most_detailed_location_ids)

            # Run percent change for the dalynator
            running_process_versions = []
            if self.tool_name == "dalynator":
                if self.start_year_ids and self.end_year_ids:
                    self._loop_submit_pct_change(
                        "dalynator", self.all_location_ids,
                        [gbd.measures.DALY])
                gbd_process_version_id = (
                    self.version_manager.get_output_process_version_id(
                        gbd.gbd_process.SUMMARY))
                running_process_versions.append(gbd_process_version_id)
                self._loop_submit_dalynator_upload(gbd_process_version_id,
                                                   self.all_location_ids,
                                                   [gbd.measures.DALY])

            # Run location_SET-aggregation, PAF back-calculation, and
            # summarization jobs if all burdenator jobs have finished
            # successfully
            if self.tool_name == "burdenator":
                # Only aggregate if we have some aggregation to do.
                if self.aggregate_location_ids:
                    self._loop_submit_loc_agg(self.measure_ids)
                    self._loop_submit_burdenator_cleanup(
                        self.measure_ids, self.aggregate_location_ids)

                if self.start_year_ids and self.end_year_ids:
                    full_location_ids = (self.most_detailed_location_ids +
                                         self.aggregate_location_ids)
                    self._loop_submit_pct_change("burdenator",
                                                 full_location_ids,
                                                 self.measure_ids)

                rf_process_version_id = (
                    self.version_manager.get_output_process_version_id(
                        gbd.gbd_process.RISK))
                running_process_versions.append(rf_process_version_id)
                eti_process_version_id = (
                    self.version_manager.get_output_process_version_id(
                        gbd.gbd_process.ETIOLOGY))
                running_process_versions.append(eti_process_version_id)
                self._loop_submit_burdenator_upload(
                    rf_process_version_id, eti_process_version_id,
                    self.measure_ids, self.all_location_ids)

            makedirs_safely(STDERR_PHASE_DIR_TEMPLATE.format(
                self.out_dir, self.tool_name))
            wf = Workflow(self.task_dag, str(self.version),
                          stderr=STDERR_PHASE_DIR_TEMPLATE.format(
                          self.out_dir, self.tool_name),
                          project=self.sge_project)
            if not self.do_not_execute:
                success = wf.run()
                if success:
                    self.version_manager.activate_compare_version()
                    for pv in running_process_versions:
                        self.version_manager.activate_process_version(pv)
                    msg = ("Run complete, Unicorns have returned home.")
                    logger.info(msg)
                    print(msg)
                else:
                    msg = ("Did not execute.")
                    logger.info(msg)
                    print(msg)
                null_inf_message = self.summarize_null_inf()
                if not self.upload_to_test:
                    self._post_to_slack(self.success, 'test-da-burdenator',
                                        null_inf_message, wf.task_dag.dag_id)
        except:
            raise

    def _get_all_measure_ids(self):
        if self.tool_name == "burdenator":
            # Requires a location_set_id, so make sure that's either in or
            # derivable from the input args
            measure_ids = []
            if self.write_out_ylds_paf:
                measure_ids.append(gbd.measures.YLD)
            if self.write_out_ylls_paf:
                measure_ids.append(gbd.measures.YLL)
            if self.write_out_deaths_paf:
                measure_ids.append(gbd.measures.DEATH)
            if self.write_out_dalys_paf:
                measure_ids.append(gbd.measures.DALY)
        else:
            measure_ids = [gbd.measures.DALY]
        return measure_ids

    def _check_input_files(self, tool_name, core_location_ids):

        if "burdenator" == tool_name:
            paf_dir = "{}/pafs/{}".format(self.input_data_root,
                                          self.paf_version)
            check_input.check_pafs(paf_dir, core_location_ids,
                                   self.all_year_ids)

            if self.write_out_ylls_paf:
                cod_dir = "{}/codcorrect/{}/draws/".format(
                    self.input_data_root, self.cod_version)
                check_input.check_cod(cod_dir, core_location_ids,
                                      gbd.measures.YLL)

            if self.write_out_deaths_paf:
                cod_dir = "{}/codcorrect/{}/draws/".format(
                    self.input_data_root, self.cod_version)
                check_input.check_cod(cod_dir, core_location_ids,
                                      gbd.measures.DEATH)

            if self.write_out_ylds_paf:
                epi_dir = get_como_folder_structure(
                    os.path.join(self.input_data_root, 'como',
                                 str(self.epi_version)))
                check_input.check_epi(epi_dir, core_location_ids,
                                      self.all_year_ids, gbd.measures.YLD)

            if self.write_out_dalys_paf:
                epi_dir = get_como_folder_structure(
                    os.path.join(self.input_data_root, 'como',
                                 str(self.epi_version)))
                check_input.check_epi(epi_dir, core_location_ids,
                                      self.all_year_ids, gbd.measures.YLD)

                cod_dir = "{}/codcorrect/{}/draws/".format(
                    self.input_data_root, self.cod_version)
                check_input.check_cod(cod_dir, core_location_ids,
                                      gbd.measures.YLL)
        elif "dalynator" == tool_name:
            epi_dir = get_como_folder_structure(
                os.path.join(self.input_data_root, 'como',
                             str(self.epi_version)))
            check_input.check_epi(epi_dir, core_location_ids,
                                  self.all_year_ids, gbd.measures.YLD)

            cod_dir = "{}/codcorrect/{}/draws/".format(self.input_data_root,
                                                       self.cod_version)
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

    @property
    def _conda_env(self):
        return str(self.conda_info['default_prefix'].split("/")[-1])

    @property
    def _path_to_conda_bin(self):
        return '{}/bin'.format(self.conda_info['root_prefix'])

    def _read_conda_info(self):
        conda_info = json.loads(
            subprocess.check_output(['conda', 'info', '--json']).decode())
        return conda_info

    def _get_region_locs(self):
        return get_location_metadata(
            location_set_id=self.location_set_id,
            gbd_round_id=self.gbd_round_id).query(
                "location_type_id==6").location_id.unique()

    def _cache_regional_scalars(self):
        if self.gbd_round_id < 5:  # no scalars before round 5 are in the db
            gbd_round_id = 5  # so need to do this for the test suite
        else:
            gbd_round_id = self.gbd_round_id
        sql_q = ("SELECT location_id, year_id, mean as scaling_factor "
                 "FROM mortality.upload_population_scalar_estimate WHERE "
                 "run_id = (SELECT run_id FROM mortality.process_version "
                 "WHERE process_id = 23 AND gbd_round_id = {} AND status_id = "
                 "5)".format(gbd_round_id))
        scalars = query(sql_q, conn_def='mortality')
        cache_file = "{}/scalars.h5".format(self.cache_dir)
        scalars.to_hdf(cache_file, "scalars",
                       data_columns=['location_id', 'year_id'],
                       format="table")
        logger.debug("Cached regional scalars in {}".format(cache_file))

    def _cache_pop(self):
        """Caches the call to the database for population"""
        logger.debug("Starting to load population cache")
        core_index = ['location_id', 'year_id', 'age_group_id', 'sex_id']
        pop_ds = GetPopulationDataSource(
            "population", year_id=self.all_year_ids,
            location_id=self.all_location_ids,
            gbd_round_id=self.gbd_round_id,
            desired_index=core_index)

        pop_df = pop_ds.get_data_frame()
        try:
            pop_df = MetricConverter.aggregate_population(pop_df)
        except ValueError as e:
            if str(e) != "No objects to concatenate":
                raise

        for loc_set_id in self.add_agg_loc_set_ids:
            pop_ds = GetPopulationDataSource(
                "population", year_id=self.all_year_ids,
                location_set_id=loc_set_id,
                gbd_round_id=self.gbd_round_id,
                desired_index=core_index)

            this_pop_df = pop_ds.get_data_frame()
            try:
                this_pop_df = MetricConverter.aggregate_population(this_pop_df)
            except ValueError as e:
                if e.message != "No objects to concatenate":
                    raise
            this_pop_df = this_pop_df[
                ~this_pop_df.location_id.isin(pop_df.location_id.unique())]
            pop_df = pop_df.append(this_pop_df)

        if len(pop_df[pop_df.location_id == 44620]) == 0:
            pop_df.append(pop_df[pop_df.location_id == 1].replace(
                {'location_id': {1: 44620}}))
        cache_file = "{}/pop.h5".format(self.cache_dir)
        pop_df.to_hdf(cache_file, "pop", data_columns=core_index,
                      format="table")
        logger.debug("Cached population in {}".format(cache_file))

    def _cache_age_weights(self):
        logger.debug("Starting to load age_weights cache")
        age_weights_df = get_age_weights(gbd_round_id=int(self.gbd_round_id))
        # returns age_group_id, age_group_weight_value as a pandas df
        cache_file = "{}/age_weights.h5".format(self.cache_dir)
        age_weights_df.to_hdf(cache_file, "age_weights",
                              data_columns=['age_group_id'], format="table")
        logger.debug("Cached age_weights in {}".format(cache_file))

    def _cache_age_spans(self):
        logger.debug("Starting to load age_spans cache")
        age_spans_df = get_age_spans(12)
        # returns age_group_id, age_group_years_start, age_group_years_end as a
        # pandas df
        cache_file = "{}/age_spans.h5".format(self.cache_dir)
        age_spans_df.to_hdf(cache_file, "age_spans",
                            data_columns=['age_group_id'], format="table")
        logger.debug("Cached age_spans in {}".format(cache_file))

    def _cache_cause_hierarchy(self):
        logger.debug("Starting to load cause_hierarchy cache")
        cause_hierarchy = dbtrees.causetree(cause_set_id=self.cause_set_id,
                                            gbd_round_id=self.gbd_round_id)
        cache_file = "{}/cause_hierarchy.pickle".format(self.cache_dir)
        pickle.dump(cause_hierarchy, open(cache_file, "wb"))
        logger.debug("Cached cause_hierarchy in {}".format(cache_file))

    def _cache_location_hierarchy(self, location_set_id):
        logger.debug("Starting to load location_hierarchy cache")
        location_hierarchy = dbtrees.loctree(location_set_id=location_set_id,
                                             gbd_round_id=self.gbd_round_id)
        cache_file = "{}/location_hierarchy_{}.pickle".format(self.cache_dir,
                                                              location_set_id)
        pickle.dump(location_hierarchy, open(cache_file, "wb"))
        logger.debug("Cached location_hierarchy {} in {}".format(
            cache_file, location_set_id))

    def _cache_cause_risk_metadata(self):
        logger.debug("Starting to load cause_risk_metadata cache")
        metadata_df = self._load_cause_risk_metadata()
        cache_file = "{}/cause_risk_metadata.csv".format(self.cache_dir)
        metadata_df.to_csv(cache_file, columns=['cause_id', 'rei_id'],
                           index=False)
        logger.debug("Cached cause_risk_metadata in {}".format(cache_file))

    def _cache_all_reis(self):
        logger.debug("Starting to load all_reis cache")
        meta_info = self._load_rei_restrictions(self.measure_ids)
        all_reis = []
        for sex in [gbd.sex.MALE, gbd.sex.FEMALE]:
            for measure in self.measure_ids:
                all_reis.extend(meta_info[sex][measure]['rei_ids'])
        all_reis = pd.DataFrame(all_reis, columns=['rei_id'])
        cache_file = "{}/all_reis.csv".format(self.cache_dir)
        all_reis.to_csv(cache_file, index=False)
        logger.debug("Cached all_reis in {}".format(cache_file))

    def _existing_aggregation_files(self):
        return glob("{o}/loc_agg_draws/burden/"
                    "*/*/*.h5".format(o=self.out_dir))

    def _expected_aggregation_files(self, measure_ids):
        filelist = []
        for meas_id in measure_ids:
            for loc_id in self.aggregate_location_ids:
                d = "{o}/loc_agg_draws/burden/{loc}/{m}".format(
                    o=self.out_dir, loc=loc_id, m=meas_id)
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for rei_id in self.meta_info[sex_id][meas_id]['rei_ids']:
                        for year_id in self.all_year_ids:
                            f = "{d}/{m}_{y}_{loc}_{r}_{s}.h5".format(
                                d=d, m=meas_id, y=year_id, loc=loc_id,
                                r=rei_id, s=sex_id)
                            filelist.append(f)
        return filelist

    def _missing_aggregation_files(self, measure_ids):
        existing = self._existing_aggregation_files()
        expected = self._expected_aggregation_files(measure_ids)
        missing = list(set(expected) - set(existing))
        logger.info("Missing {} of {} expected files".format(len(missing),
                                                             len(expected)))
        return missing

    def _load_caches(self):
        logger.debug("Caching to dir: {}".format(self.cache_dir))
        self._cache_pop()
        self._cache_regional_scalars()
        self._cache_age_weights()
        self._cache_age_spans()
        if self.tool_name == "burdenator":
            self._cache_cause_hierarchy()
            for loc_set in self.all_location_set_ids:
                self._cache_location_hierarchy(loc_set)
            self._cache_cause_risk_metadata()
            self._cache_all_reis()

    def _write_start_of_run_log_messages(self):
        """
        Write log messages, no side effects.
        """

        logger.info("Full set of arguments: ")
        for k, v in vars(self).items():
            logger.info("{} == {}".format(k, v))
        num_locs = len(self.all_location_ids)
        logger.info("Conda bin dir: {}; Conda env: {}".format(
            self._path_to_conda_bin, self._conda_env))
        if self.tool_name == "burdenator":
            logger.info(" cod: {}; epi: {}; paf {}".format(
                self.cod_version, self.epi_version, self.paf_version))
        else:
            logger.info(" cod: {}; epi: {};".format(self.cod_version,
                                                    self.epi_version))

        logger.info("{} Entering the job submission loop, number of locations "
                    "= {}".format(self.tool_name, num_locs))

    def _loc_sets_to_aggregate(self):
        """
        Returns the set of location sets to be aggregated over, logging
        a warning if the 'supplemental' location sets have any leaves that
        aren't in the original set or if the sets have overlapping non-leaf
        locations.

        Note that these are location_SET ids, not the ids of the locations
        within the sets.
        """
        main_set = self.location_set_id
        supp_sets = self.add_agg_loc_set_ids
        loc_sets = [main_set] + supp_sets

        # Create all loctrees, adding the location_set_id as an attribute
        main_lt = dbtrees.loctree(location_set_id=main_set,
                                  gbd_round_id=self.gbd_round_id)
        main_lt.location_set_id = main_set
        supp_lts = []
        for supp_set in supp_sets:
            supp_lt = dbtrees.loctree(location_set_id=supp_set,
                                      gbd_round_id=self.gbd_round_id)
            supp_lt.location_set_id = supp_set
            supp_lts.append(supp_lt)
        self._loc_sets_to_agg_warnings(main_lt, supp_lts)

        # Use the loctrees created above to map loc_set_ids to location_ids
        lsid_to_loc_ids_map = {}
        for lt in [main_lt] + supp_lts:
            lsid_to_loc_ids_map[lt.location_set_id] = [n.id for n in
                                                       lt.leaves()]

        return loc_sets, lsid_to_loc_ids_map

    def _loc_sets_to_agg_warnings(self, main_lt, supp_lts):
        """
        :param main_set: location_set_id of the main aggregation set
        :param supp_sets: location_set_ids of supplementary sets
        :return:
        """
        main_leaves = set([l.id for l in main_lt.leaves()])
        main_nonleaves = set([l.id for l in main_lt.nodes]) - set(main_leaves)

        nl_sets = [main_nonleaves]
        for supp_lt in supp_lts:
            supp_leaves = set([l.id for l in supp_lt.leaves()])
            supp_nonleaves = (set([l.id for l in supp_lt.nodes]) -
                              set(supp_leaves))
            nl_sets.append(supp_nonleaves)
            if not supp_leaves.issubset(main_leaves):
                logger.warn("Some most-detailed locations from location set "
                            "{} are not included in the main location set ({})"
                            .format(supp_lt.location_set_id,
                                    main_lt.location_set_id))

        nonleaves = reduce(lambda x, y: x | y, nl_sets)
        if not len(nonleaves) == sum(map(len, nl_sets)):
            logger.warning("There are overlapping non-most-detailed locations "
                           "across the requested location aggregation sets. "
                           "Watch out for race conditions...")

    def _get_rei_from_paf_output(self):
        paf_dir = "{}/pafs/{}".format(self.input_data_root, self.paf_version)
        existing_reis = pd.read_csv(os.path.join(paf_dir,
                                                 'existing_reis.csv.gz'))
        return existing_reis

    def _get_causes_from_cc_output(self):
        if gbd.measures.YLL in self.measure_ids or gbd.measures.DEATH in self.measure_ids:
            cod_dir = ("DIRECTORY")
            global_df = pd.read_csv("{}/1.csv".format(cod_dir))
            global_df = global_df.query("metric_id==1 & age_group_id==22"
                                        )[['measure_id', 'sex_id', 'cause_id']]
            return global_df
        return pd.DataFrame()

    def _get_causes_from_como_output(self):
        if gbd.measures.YLD in self.measure_ids:
            epi_dir = ("DIRECTORY")
            global_df = pd.read_csv('{}/{}.csv'.format(epi_dir,
                                                       self.gbd_round))
            global_df = global_df.query("metric_id==1 & age_group_id==22"
                                        )[['measure_id', 'sex_id', 'cause_id']]
            return global_df
        return pd.DataFrame()

    def _log_mismatch_with_pafs(self, rei_df, measure_df):
        ''' do outer join for each measure group (como/codcorrect),
        log mismatch, return only matched results'''
        paf_match = pd.merge(rei_df, measure_df, how='outer', indicator=True,
                             on=['measure_id', 'sex_id', 'cause_id'])
        paf_only = paf_match.query("_merge == 'left_only'")
        for cause in paf_only.cause_id.unique():
            logger.info("Cause {} exists in existing_reis but is missing "
                        "in central machinery output for measures {}"
                        .format(cause,
                                paf_only.query("cause_id=={}".format(cause)
                                               ).measure_id.unique()))
        paf_match = paf_match.query("_merge == 'both'")
        return paf_match[['measure_id', 'sex_id', 'cause_id', 'rei_id']]

    def _load_rei_restrictions(self, measure_ids):
        """ Get measure/sex restrictions for reis """
        # get cause/reis that exist in paf output
        existing_reis = self._get_rei_from_paf_output()

        # get causes that exist in codcorrect output
        cc_output = self._get_causes_from_cc_output()

        # get causes that exist in como output
        como_output = self._get_causes_from_como_output()

        central_machinery_output = pd.concat([cc_output, como_output]
                                             ).reset_index(drop=True)
        restricted_reis = self._log_mismatch_with_pafs(existing_reis,
                                                       central_machinery_output
                                                       )

        measure_dict = {gbd.measures.YLL: [gbd.measures.YLL],
                        gbd.measures.DEATH: [gbd.measures.YLL],
                        gbd.measures.YLD: [gbd.measures.YLD],
                        gbd.measures.DALY: [gbd.measures.YLL, gbd.measures.YLD]
                        }
        sex_dict = {gbd.sex.MALE: [gbd.sex.MALE],
                    gbd.sex.FEMALE: [gbd.sex.FEMALE],
                    gbd.sex.BOTH: [gbd.sex.MALE, gbd.sex.FEMALE]}
        meta_info = {}
        for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE, gbd.sex.BOTH]:
            meta_info[sex_id] = {}
            for measure_id in measure_ids:
                meta_info[sex_id][measure_id] = {
                    'rei_ids': list(restricted_reis.query(
                        "sex_id==[{}] & measure_id==[{}]"
                        .format(','.join(str(s) for s in sex_dict[sex_id]),
                                ','.join(str(m) for m in
                                         measure_dict[measure_id])
                                )).rei_id.unique())}
        return meta_info

    def _get_cause_risk_metadata_from_database(self):
        q = """
            SELECT
                cm.cause_id,
                cm.rei_id,
                cm.cause_risk_metadata_type_id,
                cm.cause_risk_metadata_value as metadata_val
            FROM
                shared.cause_risk_metadata_history cm
            JOIN
                (SELECT
                    max(mh.cause_risk_metadata_version_id)
                    as cause_risk_metadata_version_id
                FROM
                    shared.cause_risk_metadata_version cmv
                JOIN
                    shared.gbd_round gr ON gr.gbd_round = cmv.gbd_round
                JOIN
                    shared.cause_risk_metadata_history mh
                    ON cmv.cause_risk_metadata_version_id =
                    mh.cause_risk_metadata_version_id
                WHERE
                    gbd_round_id = {gbd_round}) cmv ON
                cmv.cause_risk_metadata_version_id=cm.cause_risk_metadata_version_id
            WHERE
                cm.cause_risk_metadata_type_id =1
                and cm.cause_risk_metadata_value = 1
                """.format(gbd_round=self.gbd_round_id)
        metadata = query(q, conn_def='cod')
        return metadata

    def _load_cause_risk_metadata(self):
        """ Get 100 percent pafs metadata for cause-risk pairs """
        # get reis that exist in paf output
        existing_reis = self._get_rei_from_paf_output()
        existing_reis = set(existing_reis.rei_id.unique())

        # get cause-risk metadata from the database
        metadata = self._get_cause_risk_metadata_from_database()
        metadata_reis = set(metadata.rei_id.unique())

        # filter down to the risks that we have paf output for AND metadata for
        usable_reis = list(existing_reis & metadata_reis)
        metadata = metadata.loc[metadata.rei_id.isin(usable_reis)]
        return metadata[['cause_id', 'rei_id']]

    def parse_file_path(self, fp, measures, process_types):
        # remove filepath and file ending to get needed variables
        parsed = fp.split("/")[-1].rstrip(".json").split("_")
        measures.add(parsed[2])
        process_types.add("{}_{}".format(parsed[5], parsed[6]))
        return measures, process_types

    def write_summarized_null_infs(self, counts, new_fn):
        with open(new_fn, 'w') as f:
            for table_type in self.table_types:
                msg = "{} nulls and {} infs were found for {} ".format(
                    counts[table_type]['nulls'], counts[table_type]['infs'],
                    table_type)
                if counts[table_type]['nulls'] or counts[table_type]['infs']:
                    msg += (" for measures {}, process_types {}"
                            .format(counts[table_type]['measures'],
                                    counts[table_type]['process_types']))
                f.write(msg)

    def summarize_null_inf(self):
        counts = {'single_year': {'nulls': 0, 'infs': 0, 'measures': set(),
                                  'process_types': set()},
                  'multi_year': {'nulls': 0, 'infs': 0, 'measures': set(),
                                 'process_types': set()},
                  'num_files': 0}
        for table_type in self.table_types:
            file_pattern = "{}/null_inf_*_{}_*.json".format(self.out_dir,
                                                            table_type)
            count_files = glob(file_pattern)
            counts['num_files'] += len(count_files)
            if not count_files:
                logger.error("no count files found from individual upload "
                             "jobs for {}".format(table_type))
                continue
            for json_file in count_files:
                with open(json_file, "r") as f:
                    nulls_infs = json.load(f)
                    counts[table_type]['nulls'] += nulls_infs['null']
                    counts[table_type]['infs'] += nulls_infs['inf']
                counts[table_type]['measures'], \
                    counts[table_type]['process_types'] = self.parse_file_path(
                        json_file, counts[table_type]['measures'],
                        counts[table_type]['process_types'])
                os.remove(json_file)
        if counts['num_files'] == 0:
            return "No count files were output from individual upload jobs"
        new_fn = os.path.join(self.out_dir, "null_inf_summary.txt")
        self.write_summarized_null_infs(counts, new_fn)
        return ("{} nulls and {} infs for single_year and {} nulls and {} "
                "infs for multi_year were found. See {}".format(
                    "{:,}".format(counts['single_year']['nulls']),
                    "{:,}".format(counts['single_year']['infs']),
                    "{:,}".format(counts['multi_year']['nulls']),
                    "{:,}".format(counts['multi_year']['infs']), new_fn))

    def _loop_submit_xnator(self, remote_program, location_ids):
        if 'most_detailed' not in self.phases_to_run:
            logger.info("Skipping most detailed phase")
            return

        if "burdenator" in remote_program:
            tool_name = "burdenator"
        elif "dalynator" in remote_program:
            tool_name = "dalynator"
        else:
            tool_name = "No Tool Name"
        logger.info("============")
        logger.info("Submitting most_detailed {} jobs".format(tool_name))

        self.most_detailed_jobs_by_command = {}
        for location_id in location_ids:
            for year_id in self.all_year_ids:
                if "burdenator" in remote_program:
                    task = BurdenatorMostDetailedTask(
                        self.input_data_root, self.out_dir,
                        location_id, year_id, self.cod_version,
                        self.epi_version, self.paf_version,
                        self.year_n_draws_map[year_id],
                        self.write_out_ylds_paf, self.write_out_ylls_paf,
                        self.write_out_deaths_paf, self.write_out_dalys_paf,
                        self.write_out_star_ids,
                        self.gbd_round_id, self.version, self.daly_version,
                        self.verbose, self.turn_off_null_and_nan_check,
                        self.no_sex_aggr, self.no_age_aggr,
                        self.raise_on_paf_error)
                elif "dalynator" in remote_program:
                    task = DalynatorMostDetailedTask(
                        self.input_data_root, self.out_dir,
                        location_id, year_id, self.cod_version,
                        self.epi_version, self.year_n_draws_map[year_id],
                        self.gbd_round_id, self.version, self.verbose,
                        self.turn_off_null_and_nan_check, self.no_sex_aggr,
                        self.no_age_aggr)
                else:
                    raise ValueError("Tool was neither dalynator nor "
                                     "burdenator: {}".format(remote_program))

                self.task_dag.add_task(task)
                self.most_detailed_jobs_by_command[task.command] = task

                logger.info("  Created job ({}, {})"
                            .format(location_id, year_id))

    def _loop_submit_loc_agg(self, measure_ids):
        if 'loc_agg' not in self.phases_to_run:
            logger.info("Skipping loc_agg phase")
            return
        logger.info("Submitting Burdenator location aggregation jobs")

        region_locs = self._get_region_locs()
        self.meta_info = self._load_rei_restrictions(self.measure_ids)
        # Load up argument lists to qsub
        self.loc_agg_jobs_by_command = {}
        for loc_set in self.all_location_set_ids:
            for year_id in self.all_year_ids:
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for measure_id in measure_ids:
                        qsub_rei_ids = (self.meta_info[sex_id][measure_id]
                                        ['rei_ids'])
                        for rei_id in qsub_rei_ids:
                            task = LocationAggregationTask(
                                self.out_dir, loc_set,
                                self.lsid_to_loc_map[loc_set], year_id, rei_id,
                                sex_id, measure_id,
                                self.year_n_draws_map[year_id], self.version,
                                self.write_out_star_ids, region_locs,
                                self.verbose, self.gbd_round_id,
                                self.most_detailed_jobs_by_command)
                            self.task_dag.add_task(task)
                            self.loc_agg_jobs_by_command[task.command] = task

                            logger.info("Created job ({}, {}, {}, {}, {})"
                                        .format(loc_set, year_id, measure_id,
                                                sex_id, rei_id))

    def _loop_submit_burdenator_cleanup(self, measure_ids,
                                        aggregate_location_ids):
        # Submit cleanup jobs for aggregate locations
        if 'cleanup' not in self.phases_to_run:
            logger.info("Skipping cleanup phase")
            return
        logger.info("Submitting Burdenator cleanup jobs")

        self.cleanup_jobs_by_command = {}
        for measure_id in measure_ids:
            for location_id in aggregate_location_ids:
                for year_id in self.all_year_ids:
                    male_reis = (self.meta_info[gbd.sex.MALE]
                                 [measure_id]['rei_ids'])
                    female_reis = (self.meta_info[gbd.sex.FEMALE]
                                   [measure_id]['rei_ids'])
                    task = BurdenatorCleanupTask(
                        self.input_data_root, self.out_dir,
                        measure_id, location_id, year_id,
                        self.year_n_draws_map[year_id], self.cod_version,
                        self.epi_version, self.version,
                        self.write_out_star_ids,
                        self.gbd_round_id,
                        self.verbose, self.turn_off_null_and_nan_check,
                        self.all_location_set_ids, male_reis, female_reis,
                        self.loc_agg_jobs_by_command)
                    self.task_dag.add_task(task)
                    self.cleanup_jobs_by_command[task.command] = task

                    logger.info("Created job ({}, {}, {})"
                                .format(measure_id, location_id, year_id))

    def _loop_submit_pct_change(self, tool_name, location_ids, measure_ids):
        """For each measure id, compare start and end year pairs.
        Submit ALL the jobs and then wait for them"""
        if 'pct_change' not in self.phases_to_run:
            logger.info("Skipping pct_change phase")
            return
        logger.info('Submitting pct change jobs for measures {}'
                    .format(measure_ids))

        if tool_name == 'dalynator':
            upstream_tasks = self.most_detailed_jobs_by_command
        else:
            upstream_tasks = {**self.cleanup_jobs_by_command,
                              **self.most_detailed_jobs_by_command}

        self.pct_change_jobs_by_command = {}
        for measure_id in measure_ids:
            logger.debug('Pct change, specific measure {}'.format(measure_id))
            for start_year, end_year in zip(self.start_year_ids,
                                            self.end_year_ids):
                for location_id in location_ids:
                    is_aggregate = (location_id in self.aggregate_location_ids)
                    task = PercentageChangeTask(
                        self.out_dir, tool_name, location_id,
                        is_aggregate, start_year, end_year, measure_id,
                        self.gbd_round_id, self.version,
                        self.write_out_star_ids,
                        self.n_draws_1,
                        self.verbose, self.phases_to_run, upstream_tasks)
                    self.task_dag.add_task(task)
                    self.pct_change_jobs_by_command[task.command] = task

                    logger.info("  Created job ({}, {})"
                                .format(location_id, start_year, end_year))

    def _loop_submit_burdenator_upload(self, rf_process_version_id,
                                       eti_process_version_id,
                                       measure_ids, location_ids):
        # Submit upload jobs for the burdenator
        if 'upload' not in self.phases_to_run:
            logger.info("Skipping upload phase")
            return
        logger.info("Submitting burdenator upload jobs")
        gbd_process_versions = [rf_process_version_id, eti_process_version_id]

        for gbd_process_version_id in gbd_process_versions:
            sync_task = DBSyncTask(gbd_process_version_id)
            self.task_dag.add_task(sync_task)
            for measure_id in measure_ids:
                for table_type in self.table_types:
                    for storage_engine in DalynatorJobSwarm.STORAGE_ENGINES:
                        task = UploadTask(self.out_dir, 'burdenator',
                                          gbd_process_version_id, location_ids,
                                          self.start_year_ids,
                                          self.end_year_ids, measure_id,
                                          table_type, storage_engine,
                                          self.upload_to_test,
                                          self.verbose,
                                          self.write_out_star_ids,
                                          self.pct_change_jobs_by_command)
                        if storage_engine == "COLUMNSTORE":
                            task.add_upstream(sync_task)
                        self.task_dag.add_task(task)
                        logger.info(
                            "Created upload job ({}, {}, {}, {})" .format(
                                gbd_process_version_id, measure_id, table_type,
                                storage_engine))

    def _loop_submit_dalynator_upload(self, gbd_process_version_id,
                                      location_ids, measure_ids):
        """
        Submit upload jobs for the dalynator
        """

        if 'upload' not in self.phases_to_run:
            logger.info("Skipping upload phase")
            return
        logger.info("Submitting dalynator upload jobs")

        sync_task = DBSyncTask(gbd_process_version_id)
        self.task_dag.add_task(sync_task)
        for measure_id in measure_ids:
            for table_type in self.table_types:
                for storage_engine in DalynatorJobSwarm.STORAGE_ENGINES:
                    task = UploadTask(self.out_dir, 'dalynator',
                                      gbd_process_version_id,
                                      location_ids, self.start_year_ids,
                                      self.end_year_ids, measure_id,
                                      table_type,
                                      storage_engine, self.upload_to_test,
                                      self.verbose, self.write_out_star_ids,
                                      self.pct_change_jobs_by_command)
                    if storage_engine == "COLUMNSTORE":
                        task.add_upstream(sync_task)
                    self.task_dag.add_task(task)
                    logger.info("Created upload job ({}, {}, {}, {})" .format(
                        gbd_process_version_id, measure_id, table_type,
                        storage_engine))
