import copy
import enum
import json
import logging
import os
import subprocess
from glob import glob
from typing import List, Dict, Set, Tuple, Any

from db_queries import get_age_weights, get_location_metadata
import gbd.constants as gbd
from hierarchies import dbtrees

from jobmon.client.tool import Tool
from jobmon.client.task import Task
from jobmon.core.constants import WorkflowStatus

import dalynator.app_common as ac
import dalynator.check_input_files_exist as check_input
import dalynator.type_checking as tp
import dalynator.tool_objects as to
from dalynator.compute_dalys import ComputeDalys
from dalynator.constants import (
    STDERR_PHASE_DIR_TEMPLATE,
    NATOR_TABLE_TYPES,
    SINGLE_YEAR_TABLE_TYPE,
    MULTI_YEAR_TABLE_TYPE,
)
from dalynator.get_yld_data import get_como_folder_structure
from dalynator.makedirs_safely import makedirs_safely
from dalynator.cache import Cache
from dalynator.template_manager import TemplateManager
from dalynator.version_manager import VersionManager

logger = logging.getLogger(__name__)


class DalynatorJobSwarm(object):
    """
    Run all leaf location jobs in parallel, by location-year.

    The run() method composes the _loop_submit_* functions appropriately given
    the __init__ args.

    The internal methods _loop_submit_* and _submit_* are responsible for
    actually submitting the variety of jobs: dalynator, burdenator, pct
    change, aggregation over locations, and file tidying. _loop_ methods call
    their name-mirrored _submit_ methods with the proper scoping (e.g.
    location-year for the dalynator, location for pct change).
    """

    SUCCESS_LOG_MESSAGE = "DONE pipeline complete"
    END_1 = ".*" + \
            ComputeDalys.END_MESSAGE + ".*"
    END_2 = ".*" + SUCCESS_LOG_MESSAGE + ".*"
    ERROR_STOPPED_ON_FAILURE = 1

    COMPUTATION_ATTEMPTS = 3
    UPLOAD_ATTEMPTS = 3
    FALLBACK_QUEUES = ["long.q"]

    N_PUBLIC_UPLOADS = 4

    class StorageEngines(str, enum.Enum):
        INNODB="INNODB"
        COLUMNSTORE="COLUMNSTORE"


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
            release_id: int=None,

            years: List=[],
            n_draws: int=None,
            mixed_draw_years: Dict={},

            start_year_ids: List=[],
            end_year_ids: List=[],

            location_set_ids: List=[],
            measures: List=[],
            age_group_ids: List=[],
            write_out_star_ids=None,

            # The following arguments control if certain phases execute
            start_at: str="",
            end_at: str="",
            upload_to_test=None,
            read_from_prod=None,
            public_upload: bool=False,

            turn_off_null_and_nan_check: bool=False,

            cache_dir: str="",

            cluster_project: str="",
            internal_upload_concurrency: int=None,
            verbose: bool=False,
            raise_on_paf_error: bool=False,
            do_not_execute: bool=False,
            skip_cause_agg: bool=False,
            resume: bool=False,
            age_group_set_id: int = None,
    ) -> None:

        # Input validation: No nulls, valid values
        self.tool_name = tp.is_string(tool_name, "tool name")

        self.input_data_root = tp.is_string(input_data_root,
                                            "root directory for input data")
        self.out_dir = tp.is_string(out_dir, "root directory for output data")

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
        self.release_id = tp.is_positive_int(release_id, "release_id")
        self.age_group_set_id = tp.is_None_or_positive_int(age_group_set_id, "age_group_set_id")

        self.cluster_project = tp.is_string(cluster_project, "cluster project")
        self.internal_upload_concurrency = internal_upload_concurrency
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

        # Parse location_set_ids
        self.location_set_ids = location_set_ids
        self.cause_set_ids = cause_set_ids
        self.most_detailed_location_ids, self.aggregate_location_ids = \
            ac.expand_and_validate_location_lists(self.tool_name,
                                                  self.location_set_ids,
                                                  self.release_id)
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

        self.age_group_ids = tp.is_list_of_int(
            age_group_ids,
            "age aggregates to include other than all-age and age-standardized"
        )

        self.phases_to_run = ac.validate_start_end_flags(start_at, end_at,
                                                         self.tool_name)
        self.upload_to_test = tp.is_boolean(
            upload_to_test, "upload to test")
        self.read_from_prod = tp.is_boolean(
            read_from_prod, "read from prod")
        self.skip_cause_agg = tp.is_boolean(
            skip_cause_agg, "skip cause agg")
        self.public_upload = tp.is_boolean(
            public_upload, "public upload")
        self.most_detailed_jobs_by_command: Dict = {}
        self.loc_agg_jobs_by_command: Dict = {}
        self.cleanup_jobs_by_command: Dict = {}
        self.pct_change_jobs_by_command: Dict = {}
        self.public_sort_jobs_by_command: Dict = {}
        self.public_upload_jobs_by_command: Dict = {}
        self.public_sync_job: Task = None

        if start_year_ids:
            self.table_types = NATOR_TABLE_TYPES
        else:
            self.table_types = [SINGLE_YEAR_TABLE_TYPE]
            self.start_year_ids = None
            self.end_year_ids = None

        self.turn_off_null_and_nan_check = tp.is_boolean(
            turn_off_null_and_nan_check, "null check")
        self.raise_on_paf_error = tp.is_boolean(
            raise_on_paf_error, "paf check")
        self.do_not_execute = tp.is_boolean(
            do_not_execute, "do not execute"
        )
        self.resume=tp.is_boolean(resume, "resume")

        self.cache_dir = tp.is_string(cache_dir,
                                      "path to internal cache directory")

        self.all_year_ids = list(self.year_n_draws_map.keys())

        population_run_id = ac.get_population_run_id(release_id)

        self.cache = Cache(
                tool_name=self.tool_name,
                input_data_root=self.input_data_root,
                codcorrect_version=self.codcorrect_version,
                fauxcorrect_version=self.fauxcorrect_version,
                epi_version=self.epi_version,
                paf_version=self.paf_version,
                cause_set_ids=self.cause_set_ids,
                release_id=self.release_id,
                cache_dir=self.cache_dir,
                location_set_ids=self.location_set_ids,
                all_year_ids=self.all_year_ids,
                full_location_ids=self.full_location_ids,
                measure_ids=self.measure_ids,
                age_group_ids=self.age_group_ids,
                population_run_id=population_run_id,
                age_group_set_id=age_group_set_id,
                )

        # Load/create version info
        self._set_db_version_metadata()


    def create_workflow(self):
        # set up some workflow attributes for resource tracking
        if self.resume:
            attributes_dict = {}
        else:
            attributes_dict = {
                "n_locations": len(self.full_location_ids),
                "n_draws": self.get_n_draws(),
                "n_years": len(self.all_year_ids),
                "n_age_groups": self.get_num_age_groups(),
                "n_most_detailed_locations": len(self.most_detailed_location_ids),
                "n_aggregate_locations": len(self.aggregate_location_ids),
                "n_measures": len(self.measure_ids),
            }
            for phase in self.phases_to_run:
                attributes_dict[f"phase_{phase}"] = 1
            if self.tool_name == "burdenator":
                attributes_dict["n_risks"] = self.get_num_risks()
                attributes_dict["n_causes"] = self.get_num_causes()

        # create the workflow
        self.jobmon_tool = Tool(self.tool_name)
        self.wf = self.jobmon_tool.create_workflow(
            workflow_args=f"{self.tool_name}_v{self.output_version}",
            name=f"{self.tool_name} v{self.output_version}",
            workflow_attributes=attributes_dict,
            default_cluster_name=TemplateManager.CLUSTER,
            default_compute_resources_set={
                TemplateManager.CLUSTER: {
                    "stderr": STDERR_PHASE_DIR_TEMPLATE.format(self.out_dir, self.tool_name),
                    "project": self.cluster_project,
                },
            },
        )

        self.template_manager = TemplateManager.get_template_manager(
            jobmon_tool=self.jobmon_tool,
            tool_name=self.tool_name,
        )


    def _set_db_version_metadata(self) -> None:
        """Handles creation of GBD process versions and compare version (or
        reloads them from a file if resuming a previous run)
        """
        versions_file = os.path.join(
            self.out_dir, "gbd_processes.json")
        logger.info(f"Looking for process version file '{versions_file}'")
        if os.path.isfile(versions_file):
            # Preferentially load from a file if resuming a previous run
            logger.info(
                "    Reading from process version file")
            self.version_manager = VersionManager.from_file(versions_file)
            self.version_manager.validate_metadata(
                self.cod_object.metadata_type_id, self.cod_object.version_id)
            self.version_manager.validate_metadata(
                self.epi_object.metadata_type_id, self.epi_object.version_id)
            self.version_manager.validate_metadata(
                gbd.gbd_metadata_type.POPULATION, self.cache.population_run_id)
            if self.tool_name == "dalynator":
                self.version_manager.validate_metadata(
                    gbd.gbd_metadata_type.DALYNATOR, self.output_version)
            elif self.tool_name == "burdenator":
                self.version_manager.validate_metadata(
                    self.paf_object.metadata_type_id, self.paf_object.version_id)
                self.version_manager.validate_metadata(
                    gbd.gbd_metadata_type.BURDENATOR, self.output_version)
        else:
            # If the version file is not present in the output directory,
            # the assumption is this run needs to create brand new PVs and CV
            # in the database
            logger.info(
                "    No process version file")
            is_special = bool(
                self.tool_name == "burdenator"
                and self.paf_object.process_version_status_id
                == gbd.gbd_process_version_status["SPECIAL"]
            )
            self.version_manager = VersionManager(
                release_id=self.release_id,
                upload_to_test=self.upload_to_test,
                read_from_prod=self.read_from_prod,
                is_special=is_special,
            )

            # Set metadata for input machinery
            self.version_manager.set_tool_run_version(
                metadata_type_id=self.cod_object.metadata_type_id,
                value=self.cod_object.version_id,
                pvid=self.cod_object.process_version_id,
            )
            if self.epi_version:
                self.version_manager.set_tool_run_version(
                    metadata_type_id=self.epi_object.metadata_type_id,
                    value=self.epi_object.version_id,
                    pvid=self.epi_object.process_version_id,
                )
            self.version_manager.set_tool_run_version(
                metadata_type_id=gbd.gbd_metadata_type.POPULATION,
                value=self.cache.population_run_id,
            )
            if self.tool_name == "dalynator":
                self.version_manager.set_tool_run_version(
                    metadata_type_id=gbd.gbd_metadata_type.DALYNATOR,
                    value=self.output_version,
                )
            elif self.tool_name == "burdenator":
                self.version_manager.set_tool_run_version(
                    metadata_type_id=self.paf_object.metadata_type_id,
                    value=self.paf_object.version_id,
                    pvid=self.paf_object.process_version_id,
                )
                self.version_manager.set_tool_run_version(
                    metadata_type_id=gbd.gbd_metadata_type.BURDENATOR,
                    value=self.output_version,
                )

            # Set additional metadata about this run
            self.version_manager.add_gbd_metadata(
                {
                    gbd.gbd_metadata_type.N_DRAWS: self.n_draws,
                    gbd.gbd_metadata_type.MEASURE_IDS: self.measure_ids,
                    gbd.gbd_metadata_type.YEAR_IDS: self.all_year_ids,
                    gbd.gbd_metadata_type.PUBLIC_UPLOAD: self.public_upload,
                    gbd.gbd_metadata_type.TURN_OFF_NULL_AND_NAN_CHECK: (
                        self.turn_off_null_and_nan_check
                    ),
                    gbd.gbd_metadata_type.RAISE_ON_PAF_ERROR: self.raise_on_paf_error,
                    gbd.gbd_metadata_type.SKIP_CAUSE_AGGREGATION: self.skip_cause_agg,
                    gbd.gbd_metadata_type.THREE_FOUR_FIVE_RUN: (
                        self.tool_name == "burdenator"
                        and self.paf_object.process_version_metadata.get(
                            gbd.gbd_metadata_type.THREE_FOUR_FIVE_RUN, False
                        )
                    ),
                }
            )
            if self.start_year_ids:
                self.version_manager.add_gbd_metadata(
                    {
                        gbd.gbd_metadata_type.YEAR_START_IDS: self.start_year_ids,
                        gbd.gbd_metadata_type.YEAR_END_IDS: self.end_year_ids,
                    }
                )

            # Create process versions, write run metadata to a file
            self.version_manager.freeze(versions_file, self.full_location_ids)            


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

            self.create_workflow()

            # Run the core ___nator program
            if self.tool_name == "dalynator":
                self._loop_submit_xnator(self.full_location_ids)
            elif self.tool_name == "burdenator":
                self._loop_submit_xnator(self.most_detailed_location_ids)

            # Run pct change and upload for the dalynator
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
                self._loop_submit_upload(
                    gbd_process_version_ids=running_process_versions,
                    measure_ids=[gbd.measures.DALY],
                    location_ids=self.full_location_ids,
                )
                upload_measures = [gbd.measures.DALY]

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
                self._loop_submit_upload(
                    gbd_process_version_ids=running_process_versions,
                    measure_ids=self.measure_ids,
                    location_ids=self.full_location_ids,
                )
                upload_measures = self.measure_ids

            # If running public_upload, submit jobs for uploading to the public db:
            # one db sync job and a set of sorting and upload jobs
            if self.public_upload:
                self._submit_public_sync(running_process_versions)
                self._loop_submit_public_sort(
                    gbd_process_ids=self.version_manager.output_process_versions.keys(),
                    measure_ids=upload_measures,
                    location_ids=self.full_location_ids,
                )
                sorted_location_ids = copy.deepcopy(self.full_location_ids)
                sorted_location_ids.sort()
                self._loop_submit_public_upload(
                    gbd_process_ids=self.version_manager.output_process_versions.keys(),
                    measure_ids=upload_measures,
                    sorted_location_ids=sorted_location_ids,
                )

            makedirs_safely(
                STDERR_PHASE_DIR_TEMPLATE.format(self.out_dir, self.tool_name)
            )

            if self.do_not_execute:
                msg = "Did not execute."
                logger.info(msg)
                print(msg)
            else:
                wf_status = self.wf.run(
                    resume=True,
                    seconds_until_timeout=(60 * 60 * 24 * 12)
                )
                self.success = wf_status == WorkflowStatus.DONE
                if self.success:
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
                logger.info(null_inf_message)
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
        return "this is not a conda env"


    @property
    def _path_to_conda_bin(self) -> str:
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
                release_id=self.release_id).query(
                "location_type_id==6").location_id.unique()))
        return list(set(regions))

    def _existing_aggregation_files(self) -> List[str]:
        return glob(f"{self.out_dir}/loc_agg_draws")

    def _expected_aggregation_files(self, measure_ids: List[str]) -> List[str]:
        filelist = []
        for meas_id in measure_ids:
            for loc_id in self.aggregate_location_ids:
                d = f"{self.out_dir}/loc_agg_draws/"
                for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for rei_id in self.meta_info[sex_id][meas_id]['rei_ids']:
                        for year_id in self.all_year_ids:
                            f = f"{d}/{meas_id}_{year_id}_{loc_id}_{rei_id}_{sex_id}.h5"
                            filelist.append(f)
        return filelist

    def _missing_aggregation_files(self, measure_ids: List[str]) -> List[str]:
        existing = self._existing_aggregation_files()
        expected = self._expected_aggregation_files(
            measure_ids)
        missing = list(
            set(expected) - set(existing))
        logger.info(f"Missing {len(missing)} of {len(expected)} expected files")
        return missing

    def _write_start_of_run_log_messages(self) -> None:
        """Write log messages, no side effects."""
        logger.info(
            "Full set of arguments: ")
        for k, v in vars(self).items():
            logger.info(f"{k} == {v}")
        num_locs = len(self.full_location_ids)
        logger.info(f"Conda bin dir: {self._path_to_conda_bin}; Conda env: {self._conda_env}")
        if self.tool_name == "burdenator":
            logger.info(f"cod: {self.cod_object.version_id}; epi: {self.epi_version}; paf {self.paf_version}")
        else:
            logger.info(f"cod: {self.cod_object.version_id}; epi: {self.epi_version};")

        logger.info(f"{self.tool_name} Entering the job submission loop, number of locations "
                    f"= {num_locs}")

    def parse_file_path(self, fp: str, measures: Set[int],
                        process_types: Set[str]
                        ) -> Tuple[Set[int], Set[str]]:
        # remove filepath and file ending to get needed variables
        parsed = fp.split(
            "/")[-1].rstrip(".json").split("_")
        measures.add(int(parsed[2]))
        process_types.add(f"{parsed[5]}_{parsed[6]}")
        return measures, process_types

    def write_summarized_null_infs(self, counts: Dict[str, Dict[str, int]],
                                   new_fn: str) -> None:
        with open(new_fn, 'w') as f:
            for table_type in self.table_types:
                msg = (
                    f"{counts[table_type]['nulls']} nulls and {counts[table_type]['infs']} "
                    f"infs were found for {table_type} "
                )
                if counts[table_type]['nulls'] or counts[table_type]['infs']:
                    msg += (
                        f" for measures {counts[table_type]['measures']}, process_types "
                        f"{counts[table_type]['process_types']}"
                    )
                f.write(msg)

    def summarize_null_inf(self) -> str:
        counts: Dict = {
            'single_year': {'nulls': 0, 'infs': 0, 'measures': set(),
                            'process_types': set()},
            'multi_year': {'nulls': 0, 'infs': 0, 'measures': set(),
                           'process_types': set()},
            'num_files': 0}
        for table_type in self.table_types:
            file_pattern = f"{self.out_dir}/null_inf_*_{table_type}_*.json"
            count_files = glob(file_pattern)
            counts['num_files'] += len(count_files)
            if not count_files:
                logger.error(
                    f"no count files found from individual upload jobs for {table_type}"
                )
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
            self.out_dir, "null_inf_summary.txt")
        self.write_summarized_null_infs(
            counts, new_fn)
        return (f"{counts['single_year']['nulls']:,} nulls and {counts['single_year']['infs']:,} "
                f"infs for single_year and {counts['multi_year']['nulls']:,} nulls and "
                f"{counts['multi_year']['infs']:,} infs for multi_year were found. See {new_fn}")

    def _loop_submit_xnator(self, location_ids: List[int]
                            ) -> List[str]:

        if 'most_detailed' not in self.phases_to_run:
            logger.info(
                "Skipping most detailed phase")
            return []

        logger.info("============")
        logger.info(f"Submitting most_detailed {self.tool_name} jobs")

        most_detailed_template = self.template_manager.get_most_detailed_template()

        self.most_detailed_jobs_by_command = {}
        for location_id in location_ids:
            for year_id in self.all_year_ids:
                if self.tool_name == "burdenator":
                    task = most_detailed_template.create_task(
                        max_attempts=DalynatorJobSwarm.COMPUTATION_ATTEMPTS,
                        fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                        name=f"most_detailed_{location_id}_{year_id}",
                        script=f"run_{self.tool_name}_most_detailed",
                        location_id=location_id,
                        year_id=year_id,
                        input_data_root=self.input_data_root,
                        out_dir=self.out_dir,
                        tool_name=self.tool_name,
                        cod_type=self.cod_object._tool_name,
                        cod_version=self.cod_object.version_id,
                        epi=self.epi_version,
                        paf_version=self.paf_version,
                        n_draws=self.year_n_draws_map[year_id],
                        output_version=self.output_version,
                        release_id=self.release_id,
                        age_group_ids=" ".join(map(str, self.age_group_ids)),
                        age_group_set_id=self.age_group_set_id,
                        measure_ids=" ".join(map(str, self.measure_ids)),
                        verbose_flag="--verbose" if self.verbose else "",
                        paf_error_flag="--raise_on_paf_error"
                            if self.raise_on_paf_error else "",
                        skip_cause_agg_flag="--skip_cause_agg"
                            if self.skip_cause_agg else "",
                        null_nan_check_flag="--turn_off_null_and_nan_check"
                            if self.turn_off_null_and_nan_check else "",
                        star_ids_flag="--star_ids" if self.write_out_star_ids else "",
                    )
                elif self.tool_name == "dalynator":
                    task = most_detailed_template.create_task(
                        max_attempts=DalynatorJobSwarm.COMPUTATION_ATTEMPTS,
                        fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                        name=f"most_detailed_{location_id}_{year_id}",
                        script=f"run_{self.tool_name}_most_detailed",
                        location_id=location_id,
                        year_id=year_id,
                        input_data_root=self.input_data_root,
                        out_dir=self.out_dir,
                        tool_name=self.tool_name,
                        cod_type=self.cod_object._tool_name,
                        cod_version=self.cod_object.version_id,
                        epi=self.epi_version,
                        n_draws=self.year_n_draws_map[year_id],
                        output_version=self.output_version,
                        release_id=self.release_id,
                        age_group_ids=" ".join(map(str, self.age_group_ids)),
                        age_group_set_id=self.age_group_set_id,
                        verbose_flag="--verbose" if self.verbose else "",
                        null_nan_check_flag="--turn_off_null_and_nan_check"
                            if self.turn_off_null_and_nan_check else "",
                    )
                self.wf.add_task(task)
                self.most_detailed_jobs_by_command[task.name] = task

                logger.info(f"Created most-detailed job ({location_id}, {year_id})")

        return list(self.most_detailed_jobs_by_command.values())

    def _get_lsid_to_loc_map(self) -> Dict[int, List[int]]:
        lsid_to_loc_map = {}
        lts: List = []
        for loc_set_id in self.location_set_ids:
            loctree_list = dbtrees.loctree(location_set_id=loc_set_id,
                                           release_id=self.release_id,
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

        region_locs = self._get_region_locs()
        self.meta_info = self.cache.load_rei_restrictions(
            self.measure_ids)
        lsid_to_loc_map = self._get_lsid_to_loc_map()

        loc_agg_template = self.template_manager.get_loc_agg_template()
        loc_agg_resources = self.template_manager.get_loc_agg_resources(
            year_ids=self.all_year_ids
        )

        # Load up argument lists to submit
        self.loc_agg_jobs_by_command = {}
        sequential_num = 0
        for loc_set in self.location_set_ids:
            for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                for measure_id in measure_ids:
                    sex_measure_rei_ids = map(
                        int, self.meta_info[sex_id][measure_id]['rei_ids']
                    )
                    for rei_id in sex_measure_rei_ids:
                        sequential_num += 1
                        task = loc_agg_template.create_task(
                            compute_resources=loc_agg_resources,
                            max_attempts=DalynatorJobSwarm.COMPUTATION_ATTEMPTS,
                            name=f"loc_agg_{loc_set}_{sex_id}_{measure_id}_{rei_id}",
                            script=f"run_loc_agg",
                            years=" ".join(map(str, self.all_year_ids)),
                            sex_id=sex_id,
                            sequential_num=sequential_num,
                            measure_id=measure_id,
                            rei_id=rei_id,
                            data_root=self.out_dir,
                            location_set_id=loc_set,
                            release_id=self.release_id,
                            output_version=self.output_version,
                            n_draws=self.n_draws,
                            region_locs=" ".join(map(str, region_locs)),
                            verbose_flag="--verbose" if self.verbose else "",
                            star_ids_flag="--star_ids" if self.write_out_star_ids else "",
                        )
                        if "most_detailed" in self.phases_to_run:
                            for location_id in lsid_to_loc_map[loc_set]:
                                for year_id in self.all_year_ids:
                                    task.add_upstream(
                                        self.most_detailed_jobs_by_command[
                                            f"most_detailed_{location_id}_{year_id}"
                                        ]
                                    )
                        self.wf.add_task(task)
                        self.loc_agg_jobs_by_command[task.name] = task

                        logger.info(
                            f"Created LocAgg job ({loc_set}, {measure_id}, {sex_id}, {rei_id})"
                        )

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

        cleanup_template = self.template_manager.get_cleanup_template()
        cleanup_resources = self.template_manager.get_cleanup_resources(
            year_ids=self.all_year_ids
        )

        self.cleanup_jobs_by_command = {}

        for measure_id in measure_ids:
            for location_id in aggregate_location_ids:
                task = cleanup_template.create_task(
                    compute_resources=cleanup_resources,
                    max_attempts=DalynatorJobSwarm.COMPUTATION_ATTEMPTS,
                    fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                    name=f"cleanup_{measure_id}_{location_id}",
                    script=f"run_cleanup",
                    measure_id=measure_id,
                    location_id=location_id,
                    years=" ".join(map(str, self.all_year_ids)),
                    input_data_root=self.input_data_root,
                    out_dir=self.out_dir,
                    n_draws=self.year_n_draws_map[self.all_year_ids[0]],
                    cod_type=self.cod_object._tool_name,
                    cod_version=self.cod_object.version_id,
                    epi=self.epi_version,
                    output_version=self.output_version,
                    release_id=self.release_id,
                    age_group_ids=" ".join(map(str, self.age_group_ids)),
                    age_group_set_id=self.age_group_set_id,
                    verbose_flag="--verbose" if self.verbose else "",
                    null_nan_check_flag="--turn_off_null_and_nan_check"
                        if self.turn_off_null_and_nan_check else "",
                    star_ids_flag="--star_ids" if self.write_out_star_ids else "",
                )
                male_reis = map(
                    int, self.meta_info[gbd.sex.MALE][measure_id]['rei_ids']
                )
                female_reis = map(
                    int, self.meta_info[gbd.sex.FEMALE][measure_id]['rei_ids']
                )
                sex_to_sex_specific_reis = {gbd.sex.MALE: male_reis,
                                            gbd.sex.FEMALE: female_reis}
                if "loc_agg" in self.phases_to_run:
                    for loc_set in self.location_set_ids:
                        for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
                            for rei_id in sex_to_sex_specific_reis[sex_id]:
                                task.add_upstream(
                                    self.loc_agg_jobs_by_command[
                                        f"loc_agg_{loc_set}_{sex_id}_{measure_id}_{rei_id}"
                                    ]
                                )
                self.wf.add_task(task)
                self.cleanup_jobs_by_command[task.name] = task

                logger.info(
                    f"Created cleanup job ({measure_id}, {location_id})"
                )

        return list(self.cleanup_jobs_by_command.values())


    def _loop_submit_pct_change(self, tool_name: str, location_ids: List[int],
                                measure_ids: List[int]) -> List[str]:
        """For each measure id, compare start and end year pairs.
        Submit ALL the jobs and then wait for them
        """
        if 'pct_change' not in self.phases_to_run:
            logger.info(
                "Skipping pct_change phase")
            return []
        logger.info(f'Submitting pct change jobs for measures {measure_ids}')

        pct_change_template = self.template_manager.get_pct_change_template()

        self.pct_change_jobs_by_command = {}
        for measure_id in measure_ids:
            logger.debug(f'Pct change, specific measure {measure_id}')
            for start_year, end_year in zip(self.start_year_ids,
                                            self.end_year_ids):
                for location_id in location_ids:
                    task = pct_change_template.create_task(
                        max_attempts=DalynatorJobSwarm.COMPUTATION_ATTEMPTS,
                        fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                        name=f"pct_change_{measure_id}_{start_year}_{end_year}_{location_id}",
                        script=f"run_pct_change",
                        measure_id=measure_id,
                        start_year=start_year,
                        end_year=end_year,
                        location_id=location_id,
                        input_data_root=self.input_data_root,
                        out_dir=self.out_dir,
                        tool_name=self.tool_name,
                        release_id=self.release_id,
                        age_group_ids=" ".join(map(str, self.age_group_ids)),
                        output_version=self.output_version,
                        n_draws=self.n_draws,
                        cod_type=self.cod_object._tool_name,
                        cod_version=self.cod_object.version_id,
                        epi=self.epi_version,
                        verbose_flag="--verbose" if self.verbose else "",
                        star_ids_flag="--star_ids" if self.write_out_star_ids else "",
                    )
                    if self.tool_name == "dalynator":
                        # for dalynator, the pct_change upstreams are most_detailed jobs
                        if "most_detailed" in self.phases_to_run:
                            for year in [start_year, end_year]:
                                task.add_upstream(
                                    self.most_detailed_jobs_by_command[
                                        f"most_detailed_{location_id}_{year}"
                                    ]
                                )
                    else:
                        # for burdenator, upstreams are cleanup jobs for aggregate locs
                        # and most_detailed jobs otherwise
                        if location_id in self.aggregate_location_ids:
                            if "cleanup" in self.phases_to_run:
                                task.add_upstream(
                                    self.cleanup_jobs_by_command[
                                        f"cleanup_{measure_id}_{location_id}"
                                    ]
                                )
                        else:
                            if "most_detailed" in self.phases_to_run:
                                for year in [start_year, end_year]:
                                    task.add_upstream(
                                        self.most_detailed_jobs_by_command[
                                            f"most_detailed_{location_id}_{year}"
                                        ]
                                    )

                    self.wf.add_task(task)
                    self.pct_change_jobs_by_command[task.name] = task

                    logger.info(f"Created pct change job ({location_id}, {start_year}, {end_year})")

        return list(self.pct_change_jobs_by_command.values())


    def _loop_submit_upload(
        self,
        gbd_process_version_ids: List[int],
        measure_ids: List[int],
        location_ids: List[int],
    ) -> None:
        """ Submit jobs to upload to the internal database. We submit a job for each
                table_type (single_year or multi_year)
                gbd process (e.g. risk or etiology)
                measure (death, daly, yld, yll)
        """
        if 'upload' not in self.phases_to_run:
            logger.info("Skipping upload phase")
            return
        if self.tool_name == "dalynator" and (
            len(measure_ids) > 1 or measure_ids[0] != 2
        ):
            raise ValueError("Dalynator only takes measure_id 2, DALY")
        logger.info(f"Submitting {self.tool_name} internal upload jobs")
        all_templates = []
        for table_type in self.table_types:
            for gbd_process_version_id in gbd_process_version_ids:
                for measure_id in measure_ids:
                    upload_template = self.template_manager.get_upload_template(
                        table_type=table_type,
                        measure_id=measure_id,
                        gbd_process_version_id=gbd_process_version_id
                    )
                    all_templates.append(upload_template.template_name)
                    for location_id in sorted(location_ids):
                        task = upload_template.create_task(
                            max_attempts=DalynatorJobSwarm.UPLOAD_ATTEMPTS,
                            fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                            name=f"upload_{table_type}_{gbd_process_version_id}_{measure_id}_{location_id}",
                            script="run_upload",
                            gbd_process_version_id=gbd_process_version_id,
                            table_type=table_type,
                            out_dir=self.out_dir,
                            location_id=location_id,
                            measure_id=measure_id,
                            year_ids=" ".join(map(str, self.years)),
                            upload_to_test_flag="--upload_to_test" if self.upload_to_test else ""
                        )
                        # add upstreams from previous pipeline steps if applicable
                        if self.tool_name == "burdenator":
                            upstream_tasks = self._get_upstreams_burdenator_upload(
                                table_type=table_type,
                                location_id=location_id,
                                measure_id=measure_id,
                            )
                        else:
                            upstream_tasks = self._get_upstreams_dalynator_upload(
                                location_id=location_id,
                                measure_id=measure_id,
                            )
                        for upstream_task in upstream_tasks:
                            task.add_upstream(upstream_task)
                        self.wf.add_task(task)
                        logger.info(f"Created upload job ({task.name})")

        for tt in all_templates:
            self.wf.set_task_template_max_concurrency_limit(
                task_template_name=tt, limit=self.internal_upload_concurrency
        )


    def _get_upstreams_burdenator_upload(
        self,
        table_type: str,
        location_id: int,
        measure_id: int,
    ) -> List[Task]:
        """ Get the list of upstream tasks for Burdenator uploads. When
        running pct_change, the multi-year uploads depend on the pct_change
        tasks. The single_year uploads depend on the single-year terminal
        tasks. """

        upstream_tasks = []

        if table_type == "multi_year":
            # burdenator multi_year uploads depend on pct_change tasks
            upstream_tasks = self._get_pct_change_tasks(
                location_id, measure_id
            )
        else:
            # for burdenator single-year, the terminal tasks are either cleanup
            # (for aggregate locations) or most-detailed otherwise
            if location_id in self.aggregate_location_ids:
                if "cleanup" in self.phases_to_run:
                    upstream_tasks.append(
                        self.cleanup_jobs_by_command[
                            f"cleanup_{measure_id}_{location_id}"
                        ]
                    )
            else:
                if "most_detailed" in self.phases_to_run:
                    for year in self.all_year_ids:
                        upstream_tasks.append(
                            self.most_detailed_jobs_by_command[
                                f"most_detailed_{location_id}_{year}"
                            ]
                        )

        return upstream_tasks


    def _get_upstreams_dalynator_upload(
        self,
        location_id: int,
        measure_id: int,
    ) -> List[Task]:
        """ Get the list of upstream tasks for DALYnator uploads. For the
        DALYnator, all upload tasks will wait for the pct_change tasks if
        the pct_change phase was run. """

        upstream_tasks = []

        if self.start_year_ids:
            # if we ran multi-year, upstream tasks are pct_change tasks
            upstream_tasks = self._get_pct_change_tasks(
                location_id, measure_id
            )
        else:
            # if no pct_change, the terminal tasks are most-detailed
            if "most_detailed" in self.phases_to_run:
                for year in self.all_year_ids:
                    upstream_tasks.append(
                        self.most_detailed_jobs_by_command[
                            f"most_detailed_{location_id}_{year}"
                        ]
                    )

        return upstream_tasks


    def _get_pct_change_tasks(
        self,
        location_id: int,
        measure_id: int,
    ) -> List[Task]:
        """ Return the pct_change tasks by location and measure. If the
        pct_change phase was not run, no tasks will be returned. """

        tasks = []
        if "pct_change" in self.phases_to_run:
            for start_year, end_year in zip(
                self.start_year_ids, self.end_year_ids
            ):
                tasks.append(
                    self.pct_change_jobs_by_command[
                        f"pct_change_{measure_id}_{start_year}_{end_year}_{location_id}"
                    ]
                )
        return tasks


    def _submit_public_sync(
        self, gbd_process_versions: List[int],
        ) -> None:
        """
        Sync metadata from the internal gbd db to the public
        columnstore database for dual upload
        """
        if 'upload' not in self.phases_to_run:
            logger.info("Skipping public sync phase")
            return

        public_sync_template = self.template_manager.get_public_sync_template()

        task = public_sync_template.create_task(
            max_attempts=DalynatorJobSwarm.UPLOAD_ATTEMPTS,
            fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
            name="public_sync",
            script="run_public_sync",
            gbd_process_version_ids=" ".join(map(str, gbd_process_versions)),
            upload_to_test_flag="--upload_to_test" if self.upload_to_test else "",
        )

        self.wf.add_task(task)
        self.public_sync_job = task
    

    def _loop_submit_public_sort(
        self,
        gbd_process_ids: List[int],
        measure_ids: List[int],
        location_ids: List[int], 
    ) -> None:
        """ Submit jobs to sort summaries for upload to the public database. We submit a job
            for each:
                table_type (single_year or multi_year)
                gbd process (e.g. risk or etiology)
                measure
                location
        """
        if "upload" not in self.phases_to_run:
            logger.info("Skipping public sort phase")
            return

        public_sort_template = self.template_manager.get_public_sort_template()
        for table_type in self.table_types:
            for process_id in gbd_process_ids:
                for measure_id in measure_ids:
                    for location_id in location_ids:
                        # Upstreams for the sort tasks are the terminal computation tasks
                        # for the given location
                        sorter_upstream_tasks = []
                        if table_type == SINGLE_YEAR_TABLE_TYPE:
                            if location_id in self.aggregate_location_ids:
                                if "cleanup" in self.phases_to_run:
                                    command_str = (
                                        f"cleanup_{measure_id}_{location_id}"
                                    )
                                    sorter_upstream_tasks.append(
                                        self.cleanup_jobs_by_command[command_str]
                                    )
                            else:
                                if "most_detailed" in self.phases_to_run:
                                    for year_id in self.all_year_ids:
                                        command_str = f"most_detailed_{location_id}_{year_id}"
                                        sorter_upstream_tasks.append(
                                            self.most_detailed_jobs_by_command[command_str]
                                        )
                        elif (
                            table_type == MULTI_YEAR_TABLE_TYPE
                            and "pct_change" in self.phases_to_run
                        ):
                            for start_year, end_year in zip(
                                self.start_year_ids, self.end_year_ids
                            ):
                                command_str = (
                                    f"pct_change_{measure_id}_{start_year}_{end_year}_"
                                    f"{location_id}"
                                )
                                sorter_upstream_tasks.append(
                                    self.pct_change_jobs_by_command[command_str]
                                )
                        # Create the sort task
                        sort_task_name = (
                            f"public_sort_{table_type}_{process_id}_{measure_id}_"
                            f"{location_id}"
                        )
                        sort_task = public_sort_template.create_task(
                            max_attempts=DalynatorJobSwarm.UPLOAD_ATTEMPTS,
                            fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                            name=sort_task_name,
                            script="run_public_sort",
                            gbd_process_id=process_id,
                            table_type=table_type,
                            measure_id=measure_id,
                            location_id=location_id,
                            out_dir=self.out_dir,
                            upload_to_test_flag=(
                                "--upload_to_test" if self.upload_to_test else ""
                            ),
                        )
                        for upstream_task in sorter_upstream_tasks:
                            sort_task.add_upstream(upstream_task)
                        self.wf.add_task(sort_task)
                        self.public_sort_jobs_by_command[sort_task.name] = sort_task
                        logger.info(f"Created public sort job ({sort_task.name})")


    def _loop_submit_public_upload(
        self,
        gbd_process_ids: List[int],
        measure_ids: List[int],
        sorted_location_ids: List[int],
    ) -> None:
        """ Submit jobs to upload to the public columnstore database. We submit a job for each
                table_type (single_year or multi_year)
                gbd process (e.g. risk or etiology)
                measure
                location
        """
        if "upload" not in self.phases_to_run:
            logger.info("Skipping public upload phase")
            return

        public_upload_template = self.template_manager.get_public_upload_template()
        initial_tasks = []
        for table_type in self.table_types:
            for process_id in gbd_process_ids:
                process_version_id = self.version_manager.get_output_process_version_id(
                    process_id
                )
                for measure_id in measure_ids:
                    for index, location_id in enumerate(sorted_location_ids):
                        # Create the upload task
                        upload_task_name = (
                            f"upload_public_{table_type}_{process_id}_{measure_id}_"
                            f"{location_id}"
                        )
                        upload_task = public_upload_template.create_task(
                            max_attempts=DalynatorJobSwarm.UPLOAD_ATTEMPTS,
                            fallback_queues=DalynatorJobSwarm.FALLBACK_QUEUES,
                            name=upload_task_name,
                            script="run_public_upload",
                            gbd_process_id=process_id,
                            table_type=table_type,
                            measure_id=measure_id,
                            location_id=location_id,
                            gbd_process_version_id=process_version_id,
                            out_dir=self.out_dir,
                            upload_to_test_flag=(
                                "--upload_to_test" if self.upload_to_test else ""
                            ),
                        )
                        # Public upload jobs depend on their corresponding sort task, the
                        # upstream location upload task, and the single db sync task
                        upload_task.add_upstream(self.public_sync_job)
                        command_str = (
                            f"public_sort_{table_type}_{process_id}_{measure_id}_"
                            f"{location_id}"
                        )
                        upload_task.add_upstream(
                            self.public_sort_jobs_by_command[command_str]
                        )
                        if index == 0:
                            initial_tasks.append(upload_task)
                        else:
                            command_str = (
                                f"upload_public_{table_type}_{process_id}_{measure_id}_"
                                f"{sorted_location_ids[index - 1]}"
                            )
                            upload_task.add_upstream(
                                self.public_upload_jobs_by_command[command_str]
                            )
                            self.wf.add_task(upload_task)
                            logger.info(f"Created public upload job ({upload_task.name})")
                        self.public_upload_jobs_by_command[upload_task.name] = upload_task

        # Limit the number of simultaneous uploads
        for initial_task in initial_tasks[: DalynatorJobSwarm.N_PUBLIC_UPLOADS]:
            self.wf.add_task(initial_task)
        for index in range(DalynatorJobSwarm.N_PUBLIC_UPLOADS):
            this_group = initial_tasks[index :: DalynatorJobSwarm.N_PUBLIC_UPLOADS]
            for upstream_task, task in zip(this_group, this_group[1:]):
                task.add_upstream(upstream_task)
                self.wf.add_task(task)


    def get_num_causes(self) -> int:
        metadata_df = self.cache.load_cause_risk_metadata()
        return len(metadata_df.cause_id.unique())

    def get_num_risks(self) -> int:
        metadata_df = self.cache.load_cause_risk_metadata()
        return len(metadata_df.rei_id.unique())

    def get_num_age_groups(self) -> int:
        age_weights_df = get_age_weights(release_id=self.release_id)
        return len(age_weights_df.age_group_id.unique())

    def get_n_draws(self) -> int:
        if not self.mixed_draw_years:
            return self.n_draws
        return sum(self.mixed_draw_years.keys())
