import json
import logging
import os
import subprocess
from glob import glob
import requests
from typing import List, Dict, Set, Tuple, Any

from db_queries import get_location_metadata
import gbd.constants as gbd
from db_queries.get_age_metadata import get_age_weights, get_age_spans
from hierarchies import dbtrees

from jobmon.client.swarm.workflow.workflow import Workflow, ResumeStatus
from jobmon.models.attributes.constants import workflow_attribute, job_attribute
from jobmon.client.swarm.workflow.bash_task import BashTask

import dalynator.app_common as ac
import dalynator.check_input_files_exist as check_input
import dalynator.type_checking as tp
import dalynator.tool_objects as to
from dalynator.compute_dalys import ComputeDalys
from dalynator.constants import STDERR_PHASE_DIR_TEMPLATE
from dalynator.get_yld_data import get_como_folder_structure
from dalynator.makedirs_safely import makedirs_safely
from dalynator.cache import Cache

from dalynator.tasks.cs_sort_task import CSUpstreamFilter

from dalynator.version_manager import VersionManager

logger = logging.getLogger(__name__)


class DagExecutionStatus(object):
    """Enumerate possible exit statuses for TaskDag._execute()"""

    # TODO: This should be imported from jobmon.workflow.task_dag once
    # we make the dalynator compatible with jobmon>0.6.3
    SUCCEEDED = 0
    FAILED = 1
    STOPPED_BY_USER = 2


class DalynatorJobSwarm(object):
    """
    Run all leaf location jobs in parallel, by location-year.
    Output directory structure:
        dalynator/draws/location-year/{location_id}/
        {year_id}/daly_{location}_{year}.h5

    The run() method composes the _loop_submit_* functions appropriately given
    the __init__ args.

    The internal methods _loop_submit_* and _submit_* are responsible for
    actually qsubbing the variety of jobs: dalynator, burdenator, pct
    change, aggregation over locations, and file tidying. _loop_ methods call
    their name-mirrored _submit_ methods with the proper scoping (e.g.
    location-year for the dalynator, location for pct change).
    """

    SUCCESS_LOG_MESSAGE = "DONE pipeline complete"
    END_1 = ".*" + \
            ComputeDalys.END_MESSAGE + ".*"
    END_2 = ".*" + SUCCESS_LOG_MESSAGE + ".*"
    NATOR_TABLE_TYPES = [
        "single_year", "multi_year"]
    SINGLE_YEAR_TABLE_TYPE = [
        "single_year"]
    STORAGE_ENGINES = [
        "INNODB", "COLUMNSTORE"]

    ERROR_STOPPED_ON_FAILURE = 1

    def __init__(
            self,

            tool_name: str="",

            input_data_root: str="",
            out_dir: str="",

            codcorrect_version=None,
            fauxcorrect_version=None,
            epi_version=None,
            paf_version=None,
            output_version=None,

            cause_set_ids: List=[],
            gbd_round_id: int=None,
            decomp_step: str="",

            years: List=[],
            n_draws: int=None,
            mixed_draw_years: Dict={},

            start_year_ids: List=[],
            end_year_ids: List=[],

            location_set_ids: List=[],
            measures: List=[],
            write_out_star_ids=None,

            # The following arguments control if certain phases execute
            start_at: str="",
            end_at: str="",
            upload_to_test=None,
            read_from_prod=None,
            dual_upload: bool=False,

            turn_off_null_and_nan_check: bool=False,

            cache_dir: str="",

            sge_project: str="",
            verbose: bool=False,
            raise_on_paf_error: bool=False,
            do_not_execute: bool=False,
            skip_cause_agg: bool=False
    ) -> None:

        # Input validation: No nulls, sensible values, ie type checking!
        self.tool_name = tp.is_string(tool_name, "tool name")

        self.input_data_root = tp.is_string(input_data_root,
                                            "root directory for input data")
        self.out_dir = tp.is_string(out_dir, "root directory for output data")

        # cod is special because we can have either codcorrect or fauxcorrect
        self.codcorrect_version = codcorrect_version
        self.fauxcorrect_version = fauxcorrect_version
        self.cod_object = to.cod_or_faux_correct(
            self.input_data_root,
            codcorrect_version=self.codcorrect_version,
            fauxcorrect_version=self.fauxcorrect_version)
        if epi_version:
            self.epi_version = tp.is_positive_int(epi_version, "epi version")
        else:
            self.epi_version = None
        self.epi_object = to.ToolObject.tool_object_by_name(
            'como')(self.input_data_root, self.epi_version)

        if self.tool_name == "burdenator":
            self.paf_version = tp.is_positive_int(paf_version, "paf version")
        else:
            self.paf_version = None
        self.paf_object = to.ToolObject.tool_object_by_name(
                'pafs')(self.input_data_root, self.paf_version)
        self.output_version = tp.is_positive_int(
            output_version, "my dalynator/burdenator version")
        self.gbd_round_id = tp.is_positive_int(gbd_round_id, "gbd round id")
        self.decomp_step = tp.is_string(decomp_step, "decomp step")

        self.sge_project = tp.is_string(sge_project, "sge project")
        self.verbose = verbose

        # year_n_draws_map and n_draws must be set differently based on
        # mixed_draw_years
        self.mixed_draw_years = mixed_draw_years

        if self.mixed_draw_years:
            self.n_draws = list(self.mixed_draw_years.keys())[0]
            self.year_n_draws_map = ac.construct_year_n_draws_map(
                self.mixed_draw_years)
        else:
            self.years = tp.is_list_of_year_ids(years, "list of year_ids")
            self.n_draws = tp.is_positive_int(
                n_draws, "number of draws in first year set")
            self.year_n_draws_map = ac.construct_year_n_draws_map(
                {self.n_draws: self.years})

        self.start_year_ids = tp.is_list_of_year_ids(
            start_year_ids, "list of start years for pct change")
        self.end_year_ids = tp.is_list_of_year_ids(
            end_year_ids, "list of end years for pct change")
        self.wf = Workflow(str(self.output_version),
                           stderr=STDERR_PHASE_DIR_TEMPLATE.format(
                               self.out_dir, self.tool_name),
                           project=self.sge_project,
                           resume=ResumeStatus.RESUME,
                           reset_running_jobs=True,
                           seconds_until_timeout=324000)

        self.cs_upstream_filter = CSUpstreamFilter(self.wf.task_dag)

        # Parse location_set_ids
        self.location_set_ids = location_set_ids
        self.cause_set_ids = cause_set_ids
        self.most_detailed_location_ids, self.aggregate_location_ids = \
            ac.expand_and_validate_location_lists(self.tool_name,
                                                  self.location_set_ids,
                                                  self.gbd_round_id)
        self.full_location_ids = list(self.most_detailed_location_ids.union(
                                      self.aggregate_location_ids))

        if self.tool_name == "burdenator":
            self.write_out_star_ids = tp.is_boolean(
                write_out_star_ids, "write out star_id's")
        else:
            # Dalynator has no star ids
            self.write_out_star_ids = False

        self.measure_ids = tp.is_list_of_int(
            [gbd.measures[m.upper()] for m in measures],
            "measures to include")

        self.phases_to_run = ac.validate_start_end_flags(start_at, end_at,
                                                         self.tool_name)
        self.upload_to_test = tp.is_boolean(
            upload_to_test, "upload to test")
        self.read_from_prod = tp.is_boolean(
            read_from_prod, "read from prod")
        self.skip_cause_agg = tp.is_boolean(
            skip_cause_agg, "skip cause agg")
        self.dual_upload = tp.is_boolean(
            dual_upload, "dual upload")
        self.most_detailed_jobs_by_command: Dict = {}
        self.loc_agg_jobs_by_command: Dict = {}
        self.cleanup_jobs_by_command: Dict = {}
        self.pct_change_jobs_by_command: Dict = {}

        if 'pct_change' in self.phases_to_run:
            self.table_types = DalynatorJobSwarm.NATOR_TABLE_TYPES
        else:
            self.table_types = DalynatorJobSwarm.SINGLE_YEAR_TABLE_TYPE
            self.start_year_ids = None
            self.end_year_ids = None

        self.turn_off_null_and_nan_check = tp.is_boolean(
            turn_off_null_and_nan_check, "null check")
        self.raise_on_paf_error = tp.is_boolean(
            raise_on_paf_error, "paf check")
        self.do_not_execute = tp.is_boolean(
            do_not_execute, "do not execute"
        )

        self.cache_dir = tp.is_string(cache_dir,
                                      "path to internal cache directory")

        # self.conda_info = self._read_conda_info()

        # Tracks the location & year of each jid, makes debugging and resuming
        # easier self.job_location_year_by_jid = {}
        self.all_year_ids = list(self.year_n_draws_map.keys())
        # It does not matter that all_year_ids is not in sorted order

        # Load/create version info
        self._set_db_version_metadata()

        self.cache = Cache(self.tool_name, self.input_data_root,
                           self.codcorrect_version, self.fauxcorrect_version,
                           self.epi_version, self.paf_version,
                           self.cause_set_ids, self.gbd_round_id,
                           self.decomp_step, self.cache_dir,
                           self.location_set_ids, self.all_year_ids,
                           self.full_location_ids, self.measure_ids)

    def _post_to_slack(self, success: int, channel: str, null_inf_message: str,
                       dag_id: int) -> None:
        headers = {
            'Content-type': 'application/json'}
        if success:
            data = {"text": "{} run v{} complete with dag_id {}! :tada: {}"
                    .format(self.tool_name, self.output_version, dag_id,
                            null_inf_message)}
        else:
            data = {"text": "{} run v{} failed with dag_id {}. :sob: {}"
                    .format(self.tool_name, self.output_version, dag_id,
                            null_inf_message)}

        with open('FILEPATH', "r") as f:
            private_url = json.load(f)[
                channel]
        response = requests.post(
            private_url, headers=headers, json=data)

    def _set_db_version_metadata(self) -> None:
        """Handles creation of GBD process versions and compare version (or
        reloads them from a file if resuming a previous run)
        """
        versions_file = os.path.join(
            self.out_dir, "FILEPATH")
        logger.info("Looking for process version file '{}'"
                    .format(versions_file))
        if os.path.isfile(versions_file):
            # Preferentially load from a file if resuming a previous run
            logger.info(
                "    Reading from process version file")
            self.version_manager = VersionManager.from_file(
                versions_file)
            self.version_manager.validate_metadata(
                self.cod_object.metadata_type_id, self.cod_object.version_id)
            self.version_manager.validate_metadata(
                self.epi_object.metadata_type_id, self.epi_object.version_id)
            self.version_manager.validate_metadata(
                self.paf_object.metadata_type_id, self.paf_object.version_id)
            # TODO: Validate the population version here too
            if self.tool_name == "dalynator":
                self.version_manager.validate_metadata(
                    gbd.gbd_metadata_type.DALYNATOR, self.output_version)
            elif self.tool_name == "burdenator":
                self.version_manager.validate_metadata(
                    gbd.gbd_metadata_type.BURDENATOR, self.output_version)
        else:
            # If the version file is not present in the output directory,
            # the assumption is this run needs to create brand new PVs and CV
            # in the database
            logger.info(
                "    No process version file")
            self.version_manager = VersionManager(
                self.gbd_round_id, self.decomp_step,
                upload_to_test=self.upload_to_test, read_from_prod=self.read_from_prod)
            self.version_manager.set_tool_run_version(
                self.cod_object.metadata_type_id, self.cod_object.version_id)
            if self.epi_version:
                self.version_manager.set_tool_run_version(
                    self.epi_object.metadata_type_id, self.epi_object.version_id)
            self.version_manager.set_tool_run_version(
                self.paf_object.metadata_type_id, self.paf_object.version_id)
            # TODO: Set the population version here too
            if self.tool_name == "dalynator":
                self.version_manager.set_tool_run_version(
                    gbd.gbd_metadata_type.DALYNATOR, self.output_version)
            elif self.tool_name == "burdenator":
                self.version_manager.set_tool_run_version(
                    gbd.gbd_metadata_type.BURDENATOR, self.output_version)
            self.version_manager.freeze(
                versions_file)

    def run(self, tool_name: str) -> None:
        try:
            # Get location id lists
            self._write_start_of_run_log_messages()

            if 'most_detailed' in self.phases_to_run:
                # This looks for inputs for most detailed stage
                self._check_input_files(self.tool_name,
                                        self.most_detailed_location_ids)

            # Cache population
            self.cache.load_caches()

            # Run the core ___nator program
            if self.tool_name == "dalynator":
                self._loop_submit_xnator(self.tool_name,
                                         self.full_location_ids)
            elif self.tool_name == "burdenator":
                self._loop_submit_xnator(self.tool_name,
                                         self.most_detailed_location_ids)

            # Run pct change for the dalynator
            running_process_versions = []
            if self.tool_name == "dalynator":
                if self.start_year_ids and self.end_year_ids:
                    self._loop_submit_pct_change(
                        "dalynator", self.full_location_ids,
                        [gbd.measures.DALY])
                gbd_process_version_id = (
                    self.version_manager.get_output_process_version_id(
                        gbd.gbd_process.SUMMARY))
                running_process_versions.append(
                    gbd_process_version_id)
                if self.dual_upload:
                    cs_sort_tasks = self._loop_submit_cs_sort()
                else:
                    cs_sort_tasks = []
                self._loop_submit_dalynator_upload(
                    gbd_process_version_id, self.full_location_ids,
                    [gbd.measures.DALY], cs_sort_tasks)

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
                    self._loop_submit_pct_change("burdenator",
                                                 self.full_location_ids,
                                                 self.measure_ids)

                rf_process_version_id = (
                    self.version_manager.get_output_process_version_id(
                        gbd.gbd_process.RISK))
                running_process_versions.append(
                    rf_process_version_id)
                eti_process_version_id = (
                    self.version_manager.get_output_process_version_id(
                        gbd.gbd_process.ETIOLOGY))
                running_process_versions.append(
                    eti_process_version_id)
                if self.dual_upload:
                    cs_sort_tasks = self._loop_submit_cs_sort()
                else:
                    cs_sort_tasks = []
                self._loop_submit_burdenator_single_upload(
                    rf_process_version_id, eti_process_version_id,
                    self.measure_ids, self.full_location_ids, cs_sort_tasks)
                if self.start_year_ids and self.end_year_ids:
                    self._loop_submit_burdenator_multi_upload(
                        rf_process_version_id, eti_process_version_id,
                        self.measure_ids, self.full_location_ids,
                        cs_sort_tasks)

            makedirs_safely(STDERR_PHASE_DIR_TEMPLATE.format(
                self.out_dir, self.tool_name))

            # need to bind wf before attaching attributes
            self.wf._bind()

            # add attributes to workflow
            self.wf.add_workflow_attribute(workflow_attribute.NUM_LOCATIONS,
                                           len(self.full_location_ids))
            self.wf.add_workflow_attribute(workflow_attribute.NUM_DRAWS,
                                           self.get_n_draws())
            self.wf.add_workflow_attribute(workflow_attribute.NUM_YEARS,
                                           len(self.all_year_ids))
            self.wf.add_workflow_attribute(workflow_attribute.NUM_AGE_GROUPS,
                                           self.get_num_age_groups())
            self.wf.add_workflow_attribute(
                workflow_attribute.NUM_MOST_DETAILED_LOCATIONS,
                len(self.most_detailed_location_ids))
            self.wf.add_workflow_attribute(
                workflow_attribute.NUM_AGGREGATE_LOCATIONS,
                len(self.aggregate_location_ids))
            self.wf.add_workflow_attribute(
                workflow_attribute.NUM_MEASURES,
                len(self.measure_ids))

            # number of sexes always 2
            self.wf.add_workflow_attribute(workflow_attribute.NUM_SEXES, 2)

            # num_metrics always 2: yll is an absolute metric, yld is a rate
            self.wf.add_workflow_attribute(workflow_attribute.NUM_METRICS, 2)

            # add tag attribute for each phase to run
            for phase in self.phases_to_run:
                self.wf.add_workflow_attribute(workflow_attribute.TAG, phase)


            if self.tool_name == "burdenator":
                self.wf.add_workflow_attribute(workflow_attribute.NUM_RISKS,
                                               1)
                self.wf.add_workflow_attribute(workflow_attribute.NUM_CAUSES,
                                               1)

            if self.do_not_execute:
                self.success = DagExecutionStatus.STOPPED_BY_USER
                msg = "Did not execute."
                logger.info(msg)
                print(msg)
            else:
                self.success = self.wf.run()
                if self.success == DagExecutionStatus.SUCCEEDED:
                    self.version_manager.activate_compare_version()
                    for pv in running_process_versions:
                        self.version_manager.activate_process_version(
                            pv)
                    msg = "Run complete."
                    logger.info(msg)
                    print(msg)
                else:
                    msg = "Run failed."
                    logger.error(msg)
                    print(msg)

                null_inf_message = self.summarize_null_inf()
                if not self.upload_to_test:
                    self._post_to_slack(self.success, 'test-da-burdenator',
                                        null_inf_message,
                                        self.wf.task_dag.dag_id)
        except:
            raise

    def _check_input_files(self, tool_name: str, core_location_ids: List[int]
                           ) -> None:

        cod_demo_dict = {'location_id': core_location_ids,
                         'year_id': self.all_year_ids,
                         'sex_id': [1, 2],
                         'measure_id': []}
        if "burdenator" == tool_name:
            paf_dir = self.paf_object.abs_path_to_draws
            check_input.check_pafs(paf_dir, core_location_ids,
                                   self.all_year_ids)


            if (gbd.measures.YLL in self.measure_ids or
                gbd.measures.DALY in self.measure_ids):
                    cod_demo_dict['measure_id'].append(gbd.measures.YLL)

            if gbd.measures.DEATH in self.measure_ids:
                cod_demo_dict['measure_id'].append(gbd.measures.DEATH)

            if (gbd.measures.YLD in self.measure_ids or
                gbd.measures.DALY in self.measure_ids):
                    epi_dir = get_como_folder_structure(
                        os.path.join(self.input_data_root, 'como',
                                     str(self.epi_version)))
                    check_input.check_epi(epi_dir, core_location_ids,
                                          self.all_year_ids, gbd.measures.YLD)

        elif "dalynator" == tool_name:
            epi_dir = get_como_folder_structure(
                os.path.join(self.input_data_root, 'como',
                             str(self.epi_version)))
            check_input.check_epi(epi_dir, core_location_ids,
                                  self.all_year_ids, gbd.measures.YLD)

            cod_demo_dict['measure_id'].append(gbd.measures.YLL)

        else:
            raise ValueError(
                "tool_name has a wrong name")

        check_input.check_cod(self.cod_object, cod_demo_dict)

    @property
    def _conda_env(self) -> str:
        #return str(self.conda_info['default_prefix'].split("/")[-1])
        return "this is not a conda"


    @property
    def _path_to_conda_bin(self) -> str:
        # return '{}/bin'.format(self.conda_info['root_prefix'])
        return "nothing"

    def _read_conda_info(self) -> Dict[str, Any]:
        conda_info = json.loads(
            subprocess.check_output(['conda', 'info', '--json']).decode())
        return conda_info

    def _get_region_locs(self) -> List[int]:
        regions = []
        for loc_set_id in self.location_set_ids:
            regions.extend(list(get_location_metadata(
                location_set_id=loc_set_id,
                gbd_round_id=self.gbd_round_id).query(
                "location_type_id==6").location_id.unique()))
        return list(set(regions))

    def _existing_aggregation_files(self) -> List[str]:
        return glob("FILEPATH".format(o=self.out_dir))

    def _expected_aggregation_files(self, measure_ids: List[str]) -> List[str]:
        filelist = []
        for meas_id in measure_ids:
            for loc_id in self.aggregate_location_ids:
                d = "FILEPATH".format(
                    o=self.out_dir, loc=loc_id, m=meas_id)
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for rei_id in self.meta_info[sex_id][meas_id]['rei_ids']:
                        for year_id in self.all_year_ids:
                            f = "FILEPATH".format(
                                d=d, m=meas_id, y=year_id, loc=loc_id,
                                r=rei_id, s=sex_id)
                            filelist.append(
                                f)
        return filelist

    def _missing_aggregation_files(self, measure_ids: List[str]) -> List[str]:
        existing = self._existing_aggregation_files()
        expected = self._expected_aggregation_files(
            measure_ids)
        missing = list(
            set(expected) - set(existing))
        logger.info("Missing {} of {} expected files".format(len(missing),
                                                             len(expected)))
        return missing

    def _write_start_of_run_log_messages(self) -> None:
        """Write log messages, no side effects."""
        logger.info(
            "Full set of arguments: ")
        for k, v in vars(self).items():
            logger.info(
                "{} == {}".format(k, v))
        num_locs = len(self.full_location_ids)
        logger.info("Conda bin dir: {}; Conda env: {}".format(
            self._path_to_conda_bin, self._conda_env))
        if self.tool_name == "burdenator":
            logger.info(" cod: {}; epi: {}; paf {}".format(
                self.cod_object.version_id, self.epi_version,
                self.paf_version))
        else:
            logger.info(" cod: {}; epi: {};".format(
                self.cod_object.version_id,
                self.epi_version))

        logger.info("{} Entering the job submission loop, number of locations "
                    "= {}".format(self.tool_name, num_locs))

    def parse_file_path(self, fp: str, measures: Set[int],
                        process_types: Set[str]
                        ) -> Tuple[Set[int], Set[str]]:
        # remove filepath and file ending to get needed variables
        parsed = fp.split(
            "/")[-1].rstrip(".json").split("_")
        measures.add(int(parsed[2]))
        process_types.add(
            "{}_{}".format(parsed[5], parsed[6]))
        return measures, process_types

    def write_summarized_null_infs(self, counts: Dict[str, Dict[str, int]],
                                   new_fn: str) -> None:
        with open(new_fn, 'w') as f:
            for table_type in self.table_types:
                msg = "{} nulls and {} infs were found for {} ".format(
                    counts[table_type]['nulls'], counts[
                        table_type]['infs'],
                    table_type)
                if counts[table_type]['nulls'] or counts[table_type]['infs']:
                    msg += (" for measures {}, process_types {}"
                            .format(counts[table_type]['measures'],
                                    counts[table_type]['process_types']))
                f.write(msg)

    def summarize_null_inf(self) -> str:
        counts: Dict = {
            'single_year': {'nulls': 0, 'infs': 0, 'measures': set(),
                            'process_types': set()},
            'multi_year': {'nulls': 0, 'infs': 0, 'measures': set(),
                           'process_types': set()},
            'num_files': 0}
        for table_type in self.table_types:
            file_pattern = "FILEPATH".format(self.out_dir,
                                                            table_type)
            count_files = glob(file_pattern)
            counts['num_files'] += len(count_files)
            if not count_files:
                logger.error("no count files found from individual upload "
                             "jobs for {}".format(table_type))
                continue
            for json_file in count_files:
                with open(json_file, "r") as f:
                    nulls_infs = json.load(
                        f)
                    counts[table_type][
                        'nulls'] += nulls_infs['null']
                    counts[table_type][
                        'infs'] += nulls_infs['inf']
                counts[table_type]['measures'], \
                    counts[table_type]['process_types'] = self.parse_file_path(
                    json_file, counts[
                        table_type]['measures'],
                    counts[table_type]['process_types'])
                os.remove(json_file)
        if counts['num_files'] == 0:
            return "No count files were output from individual upload jobs"
        new_fn = os.path.join(
            self.out_dir, "FILEPATH")
        self.write_summarized_null_infs(
            counts, new_fn)
        return ("{} nulls and {} infs for single_year and {} nulls and {} "
                "infs for multi_year were found. See {}"
                .format("{:,}".format(
                            counts['single_year']['nulls']),
                        "{:,}".format(
                            counts['single_year']['infs']),
                        "{:,}".format(
                            counts['multi_year']['nulls']),
                        "{:,}".format(
                            counts['multi_year']['infs']), new_fn))

    def _loop_submit_xnator(self, tool_name: str, location_ids: List[int]
                            ) -> List[str]:

        if 'most_detailed' not in self.phases_to_run:
            logger.info(
                "Skipping most detailed phase")
            return []

        if "burdenator" in tool_name:
            tool_name = "burdenator"
        elif "dalynator" in tool_name:
            tool_name = "dalynator"
        else:
            tool_name = "No Tool Name"
        logger.info("============")
        logger.info(
            "Submitting most_detailed {} jobs".format(tool_name))

        self.most_detailed_jobs_by_command = {}
        for location_id in location_ids:
            for year_id in self.all_year_ids:
                if self.tool_name == "burdenator":
                    params = (' --input_data_root {i} --out_dir {o} '
                              '--location_id {l} --year_id {y} --tool_name '
                              '{tn} --{cod_type} {cod} --epi {epi} --paf_version '
                              '{paf} --n_draws {n} --output_version {v} '
                              '--gbd_round_id {g} --decomp_step {d} --measure_ids {m}'
                              .format(i=self.input_data_root, o=self.out_dir,
                                      l=location_id, y=year_id,
                                      tn='burdenator',
                                      cod_type=self.cod_object._tool_name,
                                      cod=int(self.cod_object.version_id),
                                      epi=int(self.epi_version),
                                      paf=int(self.paf_version),
                                      n=int(self.year_n_draws_map[year_id]),
                                      v=int(self.output_version),
                                      g=int(self.gbd_round_id),
                                      d=self.decomp_step,
                                      m=' '.join(str(m) for m in
                                                 self.measure_ids)))

                    if self.verbose:
                        params += ' "--verbose"'
                    if self.turn_off_null_and_nan_check:
                        params += ' "--turn_off_null_and_nan_check"'
                    if self.raise_on_paf_error:
                        params += ' "--raise_on_paf_error"'
                    if self.skip_cause_agg:
                        params += ' "--skip_cause_agg"'

                    task = BashTask(
                        command=("run_burdenator_most_detailed"
                                 " {params}".format(params=params)),
                        num_cores=6,
                        mem_free='50G',
                        max_attempts=11,
                        max_runtime_seconds=(60 * 60 * 24),
                        queue='all.q')
                    command = ("run_burdenator_most_detailed "
                               "--location_id {location_id} --year_id {year_id}"
                               .format(location_id=int(location_id),
                                       year_id=int(year_id)))
                    if bool(self.write_out_star_ids):
                        command += " --star_ids"
                    self.most_detailed_jobs_by_command[
                        command] = task

                elif self.tool_name == "dalynator":
                    params = ('--input_data_root {i} --out_dir {o} '
                              '--location_id {l} --year_id {y} --tool_name '
                              '{tn} --{cod_type} {cod} --epi {epi} --n_draws {n} '
                              '--output_version {v} --gbd_round_id {g} --decomp_step {d}'
                              .format(i=self.input_data_root, o=self.out_dir,
                                      l=int(location_id), y=int(year_id),
                                      tn="dalynator",
                                      cod_type=self.cod_object._tool_name,
                                      cod=int(self.cod_object.version_id),
                                      epi=int(self.epi_version),
                                      n=int(self.year_n_draws_map[year_id]),
                                      v=int(self.output_version),
                                      g=int(self.gbd_round_id),
                                      d=self.decomp_step))
                    if self.verbose:
                        params += ' "--verbose"'
                    if self.turn_off_null_and_nan_check:
                        params += ' "--turn_off_null_and_nan_check"'
                    if self.dual_upload:
                        params += ' "--dual_upload"'
                    task = BashTask(
                        command=("run_dalynator_most_detailed "
                                 "{params}"
                                 .format(params=params)),
                        num_cores=6,
                        mem_free='40G',
                        max_attempts=11,
                        max_runtime_seconds=(
                                60* 60 * 8 + max(2, .1 * 5400)),
                        queue='all.q'
                    )
                    self.most_detailed_jobs_by_command[
                        "run_dalynator_most_detailed --location_id "
                        "{location_id} --year_id {year_id}"
                        .format(location_id=int(location_id),
                                year_id=int(year_id))] = task

                else:
                    raise ValueError("Tool was neither dalynator nor "
                                     "burdenator: {}".format(self.tool_name))

                self.cs_upstream_filter.task_loc_map[task.hash] = location_id
                self.cs_upstream_filter.task_years_map[task.hash] = str(year_id)
                self.wf.add_task(task)

                logger.info("  Created job ({}, {})"
                            .format(location_id, year_id))

                # add attributes to most_detailed_task
                task_attributes = {
                    job_attribute.TAG: 'most_detailed',
                    job_attribute.NUM_LOCATIONS: 1,
                    job_attribute.NUM_YEARS: 1,
                    job_attribute.NUM_DRAWS: self.get_n_draws(),
                    job_attribute.NUM_AGE_GROUPS: self.get_num_age_groups(),
                    job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                        len(self.most_detailed_location_ids),
                    job_attribute.NUM_AGGREGATE_LOCATIONS:
                        len(self.aggregate_location_ids),
                    job_attribute.NUM_MEASURES: len(self.measure_ids),
                    job_attribute.NUM_SEXES: 2,
                    job_attribute.NUM_METRICS: 2
                }
                if self.tool_name == "burdenator":
                    task_attributes.update({
                        job_attribute.NUM_CAUSES: 1,
                        job_attribute.NUM_RISKS: 1,
                    })
                task.add_job_attributes(task_attributes)

        return list(self.most_detailed_jobs_by_command.values())

    def _get_lsid_to_loc_map(self) -> Dict[int, List[int]]:
        lsid_to_loc_map = {}
        lts: List = []
        for loc_set_id in self.location_set_ids:
            loctree_list = dbtrees.loctree(location_set_id=loc_set_id,
                                           gbd_round_id=self.gbd_round_id,
                                           return_many=True)
            temp_list = []
            for lt in loctree_list:
                temp_list = temp_list + [n.id for n in lt.leaves()]
            lsid_to_loc_map[loc_set_id] = temp_list
        return lsid_to_loc_map

    def _loop_submit_loc_agg(self, measure_ids: List[int]) -> None:
        if 'loc_agg' not in self.phases_to_run:
            logger.info(
                "Skipping loc_agg phase")
            return
        logger.info(
            "Submitting Burdenator location aggregation jobs")

        upstream_tasks = self.most_detailed_jobs_by_command
        region_locs = self._get_region_locs()
        self.meta_info = self.cache.load_rei_restrictions(
            self.measure_ids)
        lsid_to_loc_map = self._get_lsid_to_loc_map()
        # Load up argument lists to qsub
        self.loc_agg_jobs_by_command = {}
        for loc_set in self.location_set_ids:
            for year_id in self.all_year_ids:
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for measure_id in measure_ids:
                        qsub_rei_ids = (self.meta_info[sex_id][measure_id]
                                        ['rei_ids'])
                        for rei_id in qsub_rei_ids:
                            params = (
                                '--data_root {dr} --location_set_id {lsi} '
                                '--year_id {y} --rei_id {r} --sex_id {s} '
                                '--measure_id {m} --gbd_round_id {g} --decomp_step {d} '
                                '--output_version {v} --n_draws {n} '
                                '--region_locs {rl}'
                                .format(dr=self.out_dir, lsi=int(loc_set),
                                        y=int(year_id), r=int(rei_id),
                                        s=int(sex_id), m=int(measure_id),
                                        g=self.gbd_round_id, d=self.decomp_step, v=self.output_version,
                                        n=self.year_n_draws_map[year_id],
                                        rl=' '.join(str(loc) for loc in
                                                    region_locs)))
                            if self.verbose:
                                params += " --verbose"
                            if bool(self.write_out_star_ids):
                                params += " --star_ids"

                            task = BashTask(
                                command=("run_loc_agg" " {params}".format(
                                    params=params)),
                                num_cores=10,
                                mem_free='50G',
                                max_attempts=11,
                                max_runtime_seconds=(60 * 60 * 8),
                                queue='all.q')
                            if upstream_tasks:
                                for loc in lsid_to_loc_map[loc_set]:
                                    command = ("run_burdenator_most_detailed "
                                               "--location_id {location_id} "
                                               "--year_id {year_id}"
                                               .format(location_id=int(loc),
                                                       year_id=int(year_id)))
                                    if bool(self.write_out_star_ids):
                                        command += " --star_ids"
                                    task.add_upstream(upstream_tasks[command])
                            self.wf.add_task(task)
                            self.loc_agg_jobs_by_command[
                                "run_loc_agg --location_set_id {loc} "
                                "--year_id {y} --rei_id {rei} "
                                "--sex_id {s} --measure_id {m}"
                                .format(loc=int(loc_set), y=int(year_id),
                                        rei=int(rei_id), s=int(sex_id),
                                        m=int(measure_id))] = task

                            logger.info("Created LocAgg job ({}, {}, {}, {}, "
                                        "{})".format(loc_set, year_id,
                                                     measure_id, sex_id,
                                                     rei_id))

                            # add attributes to loc_agg task
                            task_attributes = {
                                job_attribute.TAG: 'loc_agg',
                                job_attribute.NUM_LOCATIONS: len(
                                    lsid_to_loc_map[loc_set]),
                                job_attribute.NUM_DRAWS:
                                    self.get_n_draws(),
                                job_attribute.NUM_CAUSES:
                                    1,
                                job_attribute.NUM_AGE_GROUPS:
                                    self.get_num_age_groups(),
                                job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                                    len(self.most_detailed_location_ids),
                                job_attribute.NUM_AGGREGATE_LOCATIONS:
                                    len(self.aggregate_location_ids),
                                job_attribute.NUM_YEARS: 1,
                                job_attribute.NUM_MEASURES: 1,
                                job_attribute.NUM_SEXES: 1,
                                job_attribute.NUM_RISKS: 1,
                                job_attribute.NUM_METRICS: 2
                            }
                            task.add_job_attributes(task_attributes)

    def _loop_submit_burdenator_cleanup(self, measure_ids: List[int],
                                        aggregate_location_ids: List[int]
                                        ) -> List[str]:
        # Submit cleanup jobs for aggregate locations
        if 'cleanup' not in self.phases_to_run:
            logger.info(
                "Skipping cleanup phase")
            return []
        logger.info(
            "Submitting Burdenator cleanup jobs")

        upstream_tasks = self.loc_agg_jobs_by_command

        self.cleanup_jobs_by_command = {}

        for measure_id in measure_ids:
            for location_id in aggregate_location_ids:
                for year_id in self.all_year_ids:
                    male_reis = (
                        self.meta_info[gbd.sex.MALE][measure_id]['rei_ids'])
                    female_reis = (
                        self.meta_info[gbd.sex.FEMALE][measure_id]['rei_ids'])
                    params = (
                        ' --input_data_root {i} --out_dir {o} --measure_id {m}'
                        ' --location_id {l} --year_id {y} --n_draws {n} '
                        '--{cod_type} {cv} --epi {e} --output_version {v} --gbd_round_id {g} '
                        '--decomp_step {d} --tool_name {tn}'.format(
                            i=self.input_data_root, o=self.out_dir,
                            m=measure_id, l=int(location_id), y=int(year_id),
                            n=int(self.year_n_draws_map[year_id]),
                            cod_type=self.cod_object._tool_name,
                            cv=int(self.cod_object.version_id),
                            e=int(self.epi_version), v=int(self.output_version),
                            g=int(self.gbd_round_id), d=self.decomp_step, tn='burdenator'))
                    if self.verbose:
                        params += " --verbose"
                    if self.turn_off_null_and_nan_check:
                        params += " --turn_off_null_and_nan_check"
                    if bool(self.write_out_star_ids):
                        params += " --star_ids"

                    task = BashTask(
                        command=("run_cleanup" " {params}".format(
                            params=params)),
                        num_cores=1,
                        mem_free='90G',
                        max_attempts=11,
                        max_runtime_seconds=(
                                60 * 60 * 8 + max(2, .1 * 3600)),
                        queue='all.q'
                    )
                    sex_to_sex_specific_reis = {gbd.sex.MALE: male_reis,
                                                gbd.sex.FEMALE: female_reis}
                    if upstream_tasks:
                        for loc_set in self.location_set_ids:
                            for sex in [gbd.sex.MALE, gbd.sex.FEMALE]:
                                for rei in sex_to_sex_specific_reis[sex]:
                                    command = ("run_loc_agg --location_set_id "
                                               "{loc} --year_id {y} --rei_id "
                                               "{rei} --sex_id {s} "
                                               "--measure_id {m}"
                                               .format(loc=int(loc_set),
                                                       y=int(year_id),
                                                       rei=int(rei),
                                                       s=int(sex),
                                                       m=int(measure_id)))
                                    if bool(self.write_out_star_ids):
                                        command += " --star_ids"
                                    task.add_upstream(upstream_tasks[
                                        command])
                    self.cs_upstream_filter.task_loc_map[task.hash
                                                         ] = location_id
                    self.cs_upstream_filter.task_years_map[task.hash
                                                           ] = str(year_id)
                    self.wf.add_task(
                        task)
                    command = ("run_cleanup " "--measure_id {mid} "
                               "--location_id {lid} --year_id {yid}"
                               .format(mid=int(measure_id),
                                       lid=int(location_id),
                                       yid=int(year_id)))
                    if bool(self.write_out_star_ids):
                        command += " --star_ids"
                    if self.dual_upload:
                        command += " --dual_upload"
                    self.cleanup_jobs_by_command[
                        command] = task
                    logger.info("Created job ({}, {}, {})"
                                .format(measure_id, location_id, year_id))

                    # add attributes to cleanup task
                    task_attributes = {
                        job_attribute.TAG: 'cleanup',
                        job_attribute.NUM_LOCATIONS: 1,
                        job_attribute.NUM_DRAWS: self.get_n_draws(),
                        job_attribute.NUM_CAUSES: 1,
                        #job_attribute.NUM_RISKS: (len(male_reis) +
                        #                          len(female_reis)),
                        job_attribute.NUM_RISKS: 1,
                        job_attribute.NUM_AGE_GROUPS: self.get_num_age_groups(),
                        job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                            len(self.most_detailed_location_ids),
                        job_attribute.NUM_AGGREGATE_LOCATIONS:
                            len(self.aggregate_location_ids),
                        job_attribute.NUM_YEARS: 1,
                        job_attribute.NUM_MEASURES: 1,
                        job_attribute.NUM_SEXES: 2,
                        job_attribute.NUM_METRICS: 2
                    }
                    task.add_job_attributes(task_attributes)
        return list(self.cleanup_jobs_by_command.values())

    def _loop_submit_cs_sort(self) -> List[BashTask]:
        # Submit sort jobs for Columnstore upload
        cs_sort_tasks = []
        for location_id in self.full_location_ids:
            params = ('--location_id {l} --out_dir {o} --measure_ids {m} '
                      '--year_ids {y} --tool_name {tn}'
                      .format(l=location_id, o=self.out_dir,
                              m=" ".join(map(str, self.measure_ids)),
                              y=" ".join(map(str, self.all_year_ids)),
                              tn=self.tool_name))

            if self.start_year_ids:
                params += " --start_year_ids {sys} ".format(
                    sys=" ".join(map(str, self.start_year_ids)))
            if self.end_year_ids:
                params += " --end_year_ids {eys} ".format(
                    eys=" ".join(map(str, self.end_year_ids)))
            task = BashTask(command=("run_cs_sort {params}"
                                     .format(params=params)),
                            num_cores=4,
                            mem_free='8G',
                            max_runtime_seconds=240*60,
                            max_attempts=3
                            )
            for us_task_list in self.cs_upstream_filter.get_upstreams(
                location_id, self.all_year_ids):
                for us_task in us_task_list:
                    task.add_upstream(us_task)
            self.wf.add_task(task)
            cs_sort_tasks.append(task)
        return cs_sort_tasks

    def _loop_submit_pct_change(self, tool_name: str, location_ids: List[int],
                                measure_ids: List[int]) -> List[str]:
        """For each measure id, compare start and end year pairs.
        Submit ALL the jobs and then wait for them
        """
        if 'pct_change' not in self.phases_to_run:
            logger.info(
                "Skipping pct_change phase")
            return []
        logger.info('Submitting pct change jobs for measures {}'
                    .format(measure_ids))

        if self.tool_name == 'dalynator':
            upstream_tasks = self.most_detailed_jobs_by_command
        else:
            upstream_tasks = {**self.cleanup_jobs_by_command,
                              **self.most_detailed_jobs_by_command}

        self.pct_change_jobs_by_command = {}
        for measure_id in measure_ids:
            logger.debug(
                'Pct change, specific measure {}'.format(measure_id))
            for start_year, end_year in zip(self.start_year_ids,
                                            self.end_year_ids):
                for location_id in location_ids:
                    is_aggregate = (
                            location_id in self.aggregate_location_ids)
                    params = ('--input_data_root {i} --out_dir {o} '
                              '--tool_name {t} --location_id {loc} '
                              '--start_year {s} --end_year {e} --measure_id '
                              '{m} --gbd_round_id {g} --decomp_step {d} '
                              '--output_version {v} --n_draws '
                              '{n} --{cod_type} {cv} --epi {ev} '
                              .format(i=self.input_data_root, o=self.out_dir,
                                      t=self.tool_name, loc=int(location_id),
                                      s=int(start_year), e=int(end_year),
                                      m=int(measure_id),
                                      g=int(self.gbd_round_id),
                                      d=self.decomp_step,
                                      v=int(self.output_version),
                                      n=int(self.n_draws),
                                      cod_type=self.cod_object._tool_name,
                                      cv=int(self.cod_object.version_id),
                                      ev=int(self.epi_version)))
                    if self.verbose:
                        params += " --verbose"
                    if bool(self.write_out_star_ids):
                        params += " --star_ids"
                    if self.dual_upload:
                        params += " --dual_upload"

                    task = BashTask(command=("run_pct_change" " {params}"
                                             .format(params=params)),
                                    num_cores=1,
                                    mem_free='110G',
                                    max_attempts=11,
                                    max_runtime_seconds=(60 * 60 * 8),
                                    queue='all.q')
                    if upstream_tasks:
                        if self.tool_name == "dalynator":
                            for year in [start_year, end_year]:
                                command = ("run_dalynator_most_detailed "
                                           "--location_id {location_id} "
                                           "--year_id {year_id}"
                                           .format(location_id=int(location_id),
                                                   year_id=int(year)))
                                task.add_upstream(
                                    upstream_tasks[command])
                        else:
                            if is_aggregate:
                                for year in [start_year, end_year]:
                                    command = (
                                        "run_cleanup --measure_id {mid} "
                                        "--location_id {lid} --year_id {yid}"
                                        .format(mid=int(measure_id),
                                                lid=int(location_id),
                                                yid=int(year)))
                                    if bool(self.write_out_star_ids):
                                        command += " --star_ids"
                                    task.add_upstream(
                                        upstream_tasks[command])
                            else:  # no cleanup jobs for most-detailed locs
                                if 'most_detailed' in self.phases_to_run:
                                    # if not, no upstreams
                                    for year in [start_year, end_year]:
                                        command = (
                                            "run_burdenator_most_detailed "
                                            "--location_id {location_id} "
                                            "--year_id {year_id}"
                                            .format(
                                                location_id=int(location_id),
                                                year_id=int(year)))
                                        if bool(self.write_out_star_ids):
                                            command += " --star_ids"
                                        task.add_upstream(
                                            upstream_tasks[command])

                    self.cs_upstream_filter.task_loc_map[task.hash] = location_id

                    self.cs_upstream_filter.task_years_map[task.hash] = str(
                        end_year)
                    self.wf.add_task(task)
                    self.pct_change_jobs_by_command["run_pct_change "
                                                    "--location_id {lid} "
                                                    "--measure_id {m} "
                                                    "--start_year {s} "
                                                    "--end_year {e}"
                        .format(lid=int(location_id), m=int(measure_id),
                                s=int(start_year), e=int(end_year))] = task
                    logger.info("  Created job ({}, {})"
                                .format(location_id, start_year, end_year))

                    # add attributes to pct_change task
                    task_attributes = {
                        job_attribute.TAG: 'pct_change',
                        job_attribute.NUM_LOCATIONS: 1,
                        job_attribute.NUM_DRAWS: self.get_n_draws(),
                        job_attribute.NUM_AGE_GROUPS: self.get_num_age_groups(),
                        job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                            len(self.most_detailed_location_ids),
                        job_attribute.NUM_AGGREGATE_LOCATIONS:
                            len(self.aggregate_location_ids),
                        job_attribute.NUM_YEARS: 2,
                        job_attribute.NUM_MEASURES: 1,
                        job_attribute.NUM_SEXES: 2,
                        job_attribute.NUM_METRICS: 2
                    }
                    if self.tool_name == 'burdenator':
                        task_attributes.update({
                            job_attribute.NUM_CAUSES: 1,
                            job_attribute.NUM_RISKS: 1})
                    task.add_job_attributes(task_attributes)
        return list(self.pct_change_jobs_by_command.values())


    def _loop_submit_burdenator_single_upload(self, rf_process_version_id: int,
                                              eti_process_version_id: int,
                                              measure_ids: List[int],
                                              location_ids: List[int],
                                              cs_sort_tasks: List[BashTask]
                                              ) -> None:
        # Submit upload jobs for the burdenator
        if 'upload' not in self.phases_to_run:
            logger.info(
                "Skipping upload phase")
            return
        table_type = 'single_year'
        logger.info(
            "Submitting burdenator upload single year jobs")
        gbd_process_versions = [
            rf_process_version_id, eti_process_version_id]

        if self.dual_upload:
            last_sync_task = None
            for gbd_process_version_id in gbd_process_versions:
                if last_sync_task:
                    sync_task = BashTask(
                        command=("sync_db_metadata -gv {pv} -remote_load"
                                 .format(pv=int(gbd_process_version_id))),
                        num_cores=1,
                        mem_free='2G',
                        max_attempts=3,
                        max_runtime_seconds=240*60
                    )
                    sync_task.add_upstream(sync_task)
                else:
                    sync_task = BashTask(
                        command=("sync_db_metadata -gv {pv} -remote_load"
                                 .format(pv=int(gbd_process_version_id))),
                        num_cores=1,
                        mem_free='2G',
                        max_attempts=3,
                        max_runtime_seconds=60*30
                    )
                self.wf.add_task(sync_task)
                last_sync_task = sync_task

        for gbd_process_version_id in gbd_process_versions:
            for storage_engine in DalynatorJobSwarm.STORAGE_ENGINES:
                if storage_engine == "INNODB" or self.dual_upload:
                    params = (' --out_dir {o} --gbd_process_version_id'
                              ' {gpvi} --location_ids {loc}'
                              ' --tool_name {tn} '
                              '--table_type {tt} --storage_engine {se}'
                              .format(o=self.out_dir,
                                      gpvi=gbd_process_version_id,
                                      tn='burdenator',
                                      tt=table_type,
                                      se=storage_engine,
                                      loc=' '.join(str(loc) for loc in
                                                   location_ids)))
                    if self.verbose:
                        params += " --verbose"
                    if self.upload_to_test:
                        params += " --upload_to_test"
                    task = BashTask(command=("run_upload"" {params}"
                                             .format(params=params)),
                                    num_cores=10,
                                    mem_free='40G',
                                    max_attempts=3,
                                    max_runtime_seconds=(60 * 60 * 24),
                                    queue='all.q')
                    # set most_detailed as upstream or cleanup
                    # if the location is an aggregate loc.
                    if (self.most_detailed_jobs_by_command or
                        self.most_detailed_jobs_by_command):
                        upstream_tasks = {
                            **self.cleanup_jobs_by_command,
                            **self.most_detailed_jobs_by_command}
                        for location_id in location_ids:
                            is_aggregate = (location_id in
                                            self.aggregate_location_ids)
                            if is_aggregate:
                                for year in self.all_year_ids:
                                    for measure_id in measure_ids:
                                        command = ("run_cleanup "
                                                   "--measure_id {mid} "
                                                   "--location_id {lid} "
                                                   "--year_id {yid}"
                                                   .format(
                                                       mid=int(measure_id),
                                                       lid=int(location_id),
                                                       yid=int(year)))
                                        if bool(self.write_out_star_ids):
                                            command += " --star_ids"
                                        task.add_upstream(
                                            upstream_tasks[command])
                            else:
                                if 'most_detailed' in self.phases_to_run:
                                    for year in self.all_year_ids:
                                        command = (
                                            "run_burdenator_most_detailed "
                                            "--location_id {loc} "
                                            "--year_id {yr}"
                                            .format(loc=int(location_id),
                                                    yr=int(year)))
                                        if bool(self.write_out_star_ids):
                                            command += " --star_ids"
                                        task.add_upstream(
                                            upstream_tasks[command])
                    else:
                        logger.warning("No upstream tasks")
                    if storage_engine == "COLUMNSTORE":
                        task.add_upstream(last_sync_task)
                        for ut in cs_sort_tasks:
                            task.add_upstream(ut)
                    self.wf.add_task(
                        task)
                    logger.info(
                        "Created upload job ({}, {}, {})".format(
                            gbd_process_version_id,
                            table_type, storage_engine))

                # add attributes to upload_task
                task_attributes = {
                    job_attribute.TAG: 'upload',
                    job_attribute.NUM_LOCATIONS: len(
                        self.full_location_ids),
                    job_attribute.NUM_DRAWS: self.get_n_draws(),
                    job_attribute.NUM_CAUSES: 1,
                    job_attribute.NUM_RISKS: 1,
                    job_attribute.NUM_AGE_GROUPS: self.get_num_age_groups(),
                    job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                        len(self.most_detailed_location_ids),
                    job_attribute.NUM_AGGREGATE_LOCATIONS:
                        len(self.aggregate_location_ids),
                    job_attribute.NUM_YEARS: len(self.all_year_ids),
                    job_attribute.NUM_MEASURES: 1,
                    job_attribute.NUM_SEXES: 2,
                    job_attribute.NUM_METRICS: 2
                }
                task.add_job_attributes(task_attributes)

    def _loop_submit_burdenator_multi_upload(self, rf_process_version_id: int,
                                             eti_process_version_id: int,
                                             measure_ids: List[int],
                                             location_ids: List[int],
                                             cs_sort_tasks: List[BashTask]
                                             ) -> None:
        # Submit upload jobs for the burdenator
        if 'upload' not in self.phases_to_run:
            logger.info(
                "Skipping upload phase")
            return
        table_type = 'multi_year'
        logger.info(
            "Submitting burdenator upload jobs")
        gbd_process_versions = [
            rf_process_version_id, eti_process_version_id]

        upstream_tasks = self.pct_change_jobs_by_command

        if self.dual_upload:
            last_sync_task = None
            for gbd_process_version_id in gbd_process_versions:
                if last_sync_task:
                    sync_task = BashTask(
                        command=("sync_db_metadata -gv {pv} -remote_load"
                                 .format(pv=int(gbd_process_version_id))),
                        num_cores=1,
                        mem_free='2G',
                        max_attempts=3,
                        max_runtime_seconds=240*60
                    )
                    sync_task.add_upstream(sync_task)
                else:
                    sync_task = BashTask(
                        command=("sync_db_metadata -gv {pv} -remote_load"
                                 .format(pv=int(gbd_process_version_id))),
                        num_cores=1,
                        mem_free='2G',
                        max_attempts=3,
                        max_runtime_seconds=60*30
                    )
                self.wf.add_task(sync_task)
                last_sync_task = sync_task

        for gbd_process_version_id in gbd_process_versions:
            for storage_engine in DalynatorJobSwarm.STORAGE_ENGINES:
                if storage_engine == "INNODB" or self.dual_upload:
                    params = (' --out_dir {o} --gbd_process_version_id'
                              ' {gpvi} --location_ids {loc}'
                              ' --tool_name {tn} '
                              '--table_type {tt} --storage_engine {se}'
                              .format(o=self.out_dir,
                                      gpvi=gbd_process_version_id,
                                      tn='burdenator',
                                      tt=table_type,
                                      se=storage_engine,
                                      loc=' '.join(str(loc) for loc in
                                                   location_ids)))
                    if self.verbose:
                        params += " --verbose"
                    if self.upload_to_test:
                        params += " --upload_to_test"
                    task = BashTask(command=("run_upload"" {params}"
                                             .format(params=params)),
                                    num_cores=5,
                                    mem_free='40G',
                                    max_attempts=3,
                                    max_runtime_seconds=(60 * 60 * 24),
                                    queue='all.q')
                    if self.pct_change_jobs_by_command:
                        upstream_tasks = self.pct_change_jobs_by_command
                        for location_id in location_ids:
                            for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                                for measure_id in measure_ids:
                                    pct_command = ("run_pct_change "
                                                   "--location_id {lid} "
                                                   "--measure_id {m} "
                                                   "--start_year {s} "
                                                   "--end_year {e}"
                                                   .format(
                                                       lid=int(location_id),
                                                       m=int(measure_id),
                                                       s=int(start_year),
                                                       e=int(end_year)))
                                    if bool(self.write_out_star_ids):
                                        pct_command += " --star_ids"
                                    task.add_upstream(
                                        upstream_tasks[pct_command])
                    else:
                        logger.warning("No upstream tasks")
                    if storage_engine == "COLUMNSTORE":
                        task.add_upstream(last_sync_task)
                        for ut in cs_sort_tasks:
                            task.add_upstream(ut)
                    self.wf.add_task(
                        task)
                    logger.info(
                        "Created upload job ({}, {}, {})".format(
                            gbd_process_version_id,
                            table_type, storage_engine))

                # add attributes to upload_task
                task_attributes = {
                    job_attribute.TAG: 'upload',
                    job_attribute.NUM_LOCATIONS: len(
                        self.full_location_ids),
                    job_attribute.NUM_DRAWS: self.get_n_draws(),
                    job_attribute.NUM_CAUSES: 1,
                    job_attribute.NUM_RISKS: 1,
                    job_attribute.NUM_AGE_GROUPS: self.get_num_age_groups(),
                    job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                        len(self.most_detailed_location_ids),
                    job_attribute.NUM_AGGREGATE_LOCATIONS:
                        len(self.aggregate_location_ids),
                    job_attribute.NUM_YEARS: len(self.all_year_ids),
                    job_attribute.NUM_MEASURES: 1,
                    job_attribute.NUM_SEXES: 2,
                    job_attribute.NUM_METRICS: 2
                }
                task.add_job_attributes(task_attributes)

    def _loop_submit_dalynator_upload(self, gbd_process_version_id: int,
                                      location_ids: List[int],
                                      measure_ids: List[int],
                                      cs_sort_tasks: List[BashTask]) -> None:
        """Submit upload jobs for the dalynator"""
        if 'upload' not in self.phases_to_run:
            logger.info(
                "Skipping upload phase")
            return
        logger.info(
            "Submitting dalynator upload jobs")

        if self.dual_upload:
            sync_task = BashTask(
                command=("sync_db_metadata -gv {pv} -remote_load".format(
                    pv=int(gbd_process_version_id))),
                num_cores=1,
                mem_free='2G',
                max_attempts=3,
                max_runtime_seconds=30*60
            )
            self.wf.add_task(sync_task)

        if len(measure_ids) > 1 or measure_ids[0] != 2:
            raise ValueError("Dalynator only takes measure_id 2, DALY")
        measure_id = measure_ids[0]

        for table_type in self.table_types:
            for storage_engine in DalynatorJobSwarm.STORAGE_ENGINES:
                if storage_engine == "INNODB" or self.dual_upload:
                    params = (
                        ' --out_dir {o} --gbd_process_version_id {gpvi} '
                        '--location_ids {loc} --tool_name {tn} '
                        '--table_type {tt} '
                        '--storage_engine {se}'.format(
                            o=self.out_dir, gpvi=gbd_process_version_id,
                            tn='dalynator',
                            tt=table_type, se=storage_engine,
                            loc=' '.join(str(loc) for loc in location_ids))
                        )
                    if self.verbose:
                        params += " --verbose"
                    if self.upload_to_test:
                        params += " --upload_to_test"

                    task = BashTask(command=("run_upload ""{params}"
                                             .format(params=params)),
                                    num_cores=2,
                                    mem_free='40G',
                                    max_attempts=3,
                                    max_runtime_seconds=(60 * 60 * 24))
                    if self.pct_change_jobs_by_command and self.start_year_ids:
                        upstream_tasks = self.pct_change_jobs_by_command
                        for location_id in location_ids:
                            for start_year, end_year in zip(self.start_year_ids, self.end_year_ids):
                                pct_command = ("run_pct_change "
                                               "--location_id {lid} "
                                               "--measure_id {m} "
                                               "--start_year {s} "
                                               "--end_year {e}"
                                               .format(
                                                   lid=int(location_id),
                                                   m=int(measure_id),
                                                   s=int(start_year),
                                                   e=int(end_year)))
                                if bool(self.write_out_star_ids):
                                    pct_command += " --star_ids"
                                task.add_upstream(upstream_tasks[pct_command])
                    elif self.most_detailed_jobs_by_command:
                        upstream_tasks = self.most_detailed_jobs_by_command
                        for location_id in location_ids:
                            for year_id in self.all_year_ids:
                                md_command = ("run_dalynator_most_detailed"
                                              " --location_id {location_id}"
                                              " --year_id {year_id}"
                                              .format(
                                                  location_id=int(location_id),
                                                  year_id=int(year_id)))
                                task.add_upstream(upstream_tasks[md_command])

                    else:
                        logger.warning("No upstream tasks")
                    if storage_engine == "COLUMNSTORE":
                        task.add_upstream(sync_task)
                        task.add_upstream(cs_sort_tasks)
                    self.wf.add_task(task)
                    logger.info("Created upload job ({}, {}, {})".format(
                        gbd_process_version_id, table_type,
                        storage_engine))

                    # add attributes to upload_task
                    task_attributes = {
                        job_attribute.TAG: 'upload',
                        job_attribute.NUM_LOCATIONS: len(
                            self.full_location_ids),
                        job_attribute.NUM_DRAWS: self.get_n_draws(),
                        job_attribute.NUM_AGE_GROUPS: self.get_num_age_groups(),
                        job_attribute.NUM_MOST_DETAILED_LOCATIONS:
                            len(self.most_detailed_location_ids),
                        job_attribute.NUM_AGGREGATE_LOCATIONS:
                            len(self.aggregate_location_ids),
                        job_attribute.NUM_YEARS: len(self.all_year_ids),
                        job_attribute.NUM_MEASURES: 1,
                        job_attribute.NUM_SEXES: 2,
                        job_attribute.NUM_METRICS: 2
                    }
                    task.add_job_attributes(task_attributes)

    def get_num_causes(self) -> int:
        metadata_df = self.cache.load_cause_risk_metadata()
        return len(metadata_df.cause_id.unique())

    def get_num_risks(self) -> int:
        metadata_df = self.cache.load_cause_risk_metadata()
        return len(metadata_df.rei_id.unique())

    def get_num_age_groups(self) -> int:
        age_weights_df = get_age_weights(gbd_round_id=int(self.gbd_round_id))
        return len(age_weights_df.age_group_id.unique())

    def get_n_draws(self) -> int:
        if not self.mixed_draw_years:
            return self.n_draws
        return sum(self.mixed_draw_years.keys())
