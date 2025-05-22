import argparse
import getpass
import json
import logging
import os
import pathlib
import shutil
from typing import List, Union

import networkx as nx
import numpy as np
import pandas as pd

import gbd.constants as gbd_constants
import gbd.enums as gbd_enums
import gbd_outputs_versions
from db_queries import get_demographics, get_population
from gbd.estimation_years import estimation_years_from_release_id
from jobmon.client.api import Tool
from jobmon.client.task import Task
from jobmon.core import constants as jobmon_constants

from epic import __version__
from epic.legacy.maps.create_digraph import CombineMaps
from epic.legacy.util.constants import DAG, MEIDS, FilePaths, Params, Process
from epic.lib.maps.generators.generators import generate_maps
from epic.lib.tasks.calc_tmrel_task import TMRELTaskFactory
from epic.lib.tasks.causal_attribution_task import CATaskFactory
from epic.lib.tasks.ex_adjust_task import ExAdjustFactory
from epic.lib.tasks.modeled_split_task import ModeledSevSplitTaskFactory
from epic.lib.tasks.save_task import SaveFactory
from epic.lib.tasks.split_task import SevSplitTaskFactory
from epic.lib.tasks.super_squeeze_task import SuperSqueezeFactory
from epic.lib.tasks.upload_task import UploadFactory
from epic.lib.util import exceptions
from epic.lib.util.common import compile_all_mvid, get_upstreams, validate_release
from epic.lib.util.metadata import save_model_metadata

_THIS_DIR = pathlib.Path(__file__).resolve().parent


class EpicWorkFlow(object):
    """Sets up and runs the EPIC DAG and Jobmon Workflow.

    Args:
        version (int): EPIC version, set manually.
        mapbuilder (CombineMaps): CombineMaps instance, used for building the EPIC DAG.
        release_id (int): release ID to run with.
        year_ids (intlist): List of year IDs to run on.
        annual (bool): Whether or not to attempt to use annual years where available, using
            the passed year_ids argument as fallback years.
        n_draws(int): Number of draws to run with.
        anemia (bool): Whether or not to include anemia causal attribution (CA) in the EPIC
            run.
        best (bool): Whether or not output models should be bested.
        resume (bool): Whether the Jobmon workflow is a resume.
        test (bool): Whether the WF is being run in the test suite. If it is, create a dev DB
            PV and use the test EPIC path.
    """

    CODE_DIR = os.path.dirname(os.path.realpath(__file__))
    MAX_UPLOAD_TASKS = 25
    MAX_SS_TASKS = 800
    MAX_CA_TASKS = 1000
    DB_ENV = "ENV"

    def __init__(
        self,
        version: int,
        mapbuilder: CombineMaps,
        release_id: int,
        year_ids: List[int],
        annual: bool,
        n_draws: int,
        anemia: bool,
        best: bool,
        resume: bool,
        test: bool,
    ) -> None:
        validate_release("EPIC", release_id=release_id)

        self.anemia = anemia
        self.version = version
        self.test = test
        root_dir = FilePaths.TEST_DATA_DIR if self.test else FilePaths.DATA_DIR
        self.data_dir = os.path.join(root_dir, str(self.version))
        self.get_draws_diff_cache = os.path.join(
            "FILEPATH", str(self.version), FilePaths.GET_DRAWS_DIFF_CACHE
        )
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, FilePaths.INPUT_FILES_DIR), exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, FilePaths.STDERR), exist_ok=True)
        os.makedirs(os.path.join(self.data_dir, FilePaths.STDOUT), exist_ok=True)
        os.makedirs(
            os.path.join(self.data_dir, FilePaths.GET_DRAWS_DIFF_CACHE), exist_ok=True
        )
        os.makedirs(os.path.join(self.data_dir, FilePaths.Anemia.DIAGNOSTICS), exist_ok=True)
        os.makedirs(self.get_draws_diff_cache, exist_ok=True)

        self.release_id = release_id

        self.n_draws = n_draws
        self.year_ids = year_ids
        self.annual = annual
        self.best = best
        self.resume = resume

        self.pv_path = os.path.join(self.data_dir, FilePaths.PROCESS_VERSION_CACHE)
        self.pop_path = os.path.join(self.data_dir, FilePaths.POPULATION_CACHE)
        self.demo_path = os.path.join(self.data_dir, FilePaths.DEMOGRAPHICS_CACHE)
        if self.resume is False:
            self._pull_population()
            self._pull_demographics()
            self._generate_process_version()
        with open(self.pv_path) as pv_file:
            pv_content = json.load(pv_file)
        with open(self.demo_path) as demo_file:
            demo_content = json.load(demo_file)

        self.pv = pv_content[DAG.Cache.PROCESS_VERSION_KEY]
        self.location_ids = demo_content[gbd_constants.columns.LOCATION_ID]
        self.sex_ids = demo_content[gbd_constants.columns.SEX_ID]

        # instantiate the Jobmon tool and workflow
        self.tool = Tool(name=DAG.Workflow.TOOL)
        self.workflow = self.tool.create_workflow(
            name=DAG.Workflow.WORKFLOW_NAME.format(version=self.version),
            default_cluster_name=gbd_enums.Cluster.SLURM.value,
            default_compute_resources_set={
                gbd_enums.Cluster.SLURM.value: {
                    DAG.ArgNames.STDERR: os.path.join(self.data_dir, FilePaths.STDERR),
                    DAG.ArgNames.STDOUT: os.path.join(self.data_dir, FilePaths.STDOUT),
                    DAG.ArgNames.PROJECT: DAG.Workflow.PROJECT,
                    DAG.ArgNames.QUEUE: DAG.Workflow.QUEUE,
                }
            },
            workflow_args=DAG.Workflow.WORKFLOW_ARGS.format(version=self.version),
        )

        # instantiate the task templates
        self.task_registry = {}
        self.sev_split_fac = SevSplitTaskFactory(self.task_registry, self.tool)
        self.modeled_sev_split_fac = ModeledSevSplitTaskFactory(self.task_registry, self.tool)
        self.ex_adjust_fac = ExAdjustFactory(self.task_registry, self.tool)
        self.super_squeeze_fac = SuperSqueezeFactory(self.task_registry, self.tool)
        self.ca_fac = CATaskFactory(self.tool)
        self.tmrel_fac = TMRELTaskFactory(self.tool)
        self.save_fac = SaveFactory(self.tool)
        self.upload_fac = UploadFactory(self.tool)

        self.build_dag(mapbuilder=mapbuilder)

    def _pull_population(self) -> None:
        """Reads and saves population to disk."""
        pop_years = "all" if self.annual else self.year_ids
        non_birth_population = get_population(
            age_group_id="all",
            location_id="all",
            sex_id="all",
            year_id=pop_years,
            release_id=self.release_id,
        )
        birth_population = get_population(
            age_group_id=164,
            location_id="all",
            sex_id="all",
            year_id=pop_years,
            release_id=self.release_id,
        )
        if birth_population.empty:
            raise ValueError("No best birth (age_group_id 164) population.")
        population = pd.concat([non_birth_population, birth_population])
        # Cache population
        population.to_csv(self.pop_path, index=False)

    def _pull_demographics(self) -> None:
        """Reads and saves demographics to disk."""
        demo = get_demographics(gbd_team="epi", release_id=self.release_id)
        with open(self.demo_path, "w") as demo_file:
            json.dump(demo, demo_file)

    def _generate_process_version(self) -> None:
        """Generates EPIC process version and saves to disk."""
        # Generate process version
        gbd_metadata = {
            gbd_constants.gbd_metadata_type.YEAR_IDS: self.year_ids,
            gbd_constants.gbd_metadata_type.N_DRAWS: self.n_draws,
            gbd_constants.gbd_metadata_type.MEASURE_IDS: [
                gbd_constants.measures.PREVALENCE,
                gbd_constants.measures.INCIDENCE,
            ],
            gbd_constants.gbd_metadata_type.COMPARE_CONTEXT_ID: 1,  # Cause context
            gbd_constants.gbd_metadata_type.EPIC: self.version,
        }
        gbd_note = f"EPIC v{self.version}"
        if self.best is False:
            gbd_note += ", not bested"
        env = "ENV"
        if self.test:
            env = "ENV"
        process_version = gbd_outputs_versions.GBDProcessVersion.add_new_version(
            gbd_process_id=gbd_constants.gbd_process.EPIC,
            gbd_process_version_note=gbd_note,
            code_version=__version__,
            metadata=gbd_metadata,
            release_id=self.release_id,
            env=env,
            validate_env=env,
        )
        # Mark process version best
        if self.best is True:
            process_version.mark_best()
        # Cache process version
        pv_content = {DAG.Cache.PROCESS_VERSION_KEY: process_version.gbd_process_version_id}
        with open(self.pv_path, "w") as pv_file:
            json.dump(pv_content, pv_file)

    def build_dag(self, mapbuilder: CombineMaps) -> None:  # noqa: C901
        """Creates the EPIC DAG and builds the Jobmon workflow."""
        # create epic json map
        self.emap = mapbuilder.downstream_only(Process.COMO)

        if self.resume is False:
            # Save best input models as csv for posting to EPIC tracker HUB
            # page then separate into individual json files for use in
            # downstream scripts. Take care that downstream processes do not
            # pick up a model_verison_id from a previous run. Only
            # collect the best models once per run so we know exactly what
            # was a available at the start of the run and what was
            # consequently used in the rest of the workflow
            best_models = mapbuilder.best_models
            inputs = [int(me) for me in mapbuilder.inputs]
            best_models = best_models.loc[
                best_models[Params.MODELABLE_ENTITY_ID].isin(inputs)
            ]
            missing_best_models = set(inputs) - set(best_models[Params.MODELABLE_ENTITY_ID])
            if missing_best_models:
                raise exceptions.NoBestVersionError(
                    "The following input modelable entity IDs are missing best "
                    f"models: {missing_best_models}"
                )
            best_models.to_csv(
                os.path.join(
                    self.data_dir,
                    FilePaths.INPUT_FILES_DIR,
                    FilePaths.BEST_MODELS_FILE_PATTERN,
                ),
                index=False,
                encoding="utf8",
            )
            for row in best_models.itertuples():
                save_model_metadata(
                    self.data_dir,
                    row.modelable_entity_id,
                    row.model_version_id,
                    row.release_id,
                )

        # run every process in the pipeline regardless of whether or not
        # there is already a model saved
        self.pgraph = mapbuilder.P

        top_sort = nx.topological_sort(self.pgraph)

        # Note: this depends on the fact that task names do not overlap at all.
        # Keep this in mind when adding new epic tasks.
        for node in top_sort:
            # Need to be explicit here for bad networkx typing.
            node = str(node)
            if node == mapbuilder.start_node:
                pass
            elif DAG.Tasks.SPLIT in node:
                self._add_sev_split_task(node)
            elif DAG.Tasks.MODELED_PROPORTION in node:
                self._add_modeled_sev_split_task(node)
            elif DAG.Tasks.SUPER_SQUEEZE in node:
                self._add_super_squeeze_task(node)
            elif DAG.Tasks.CAUSAL_ATTRIBUTION in node:
                self._add_causal_attribution_task(node)
            elif DAG.Tasks.TMREL in node:
                self._add_tmrel_task(node)
            else:
                self._add_ex_adjust_task(node)

        # Set task-template level Jobmon throttling
        self.workflow.set_task_template_max_concurrency_limit(
            task_template_name=DAG.Tasks.SPLIT, limit=5
        )
        self.workflow.set_task_template_max_concurrency_limit(
            task_template_name=DAG.Tasks.SUPER_SQUEEZE, limit=self.MAX_SS_TASKS
        )
        self.workflow.set_task_template_max_concurrency_limit(
            task_template_name=DAG.Tasks.UPLOAD, limit=self.MAX_UPLOAD_TASKS
        )
        if self.anemia:
            self.workflow.set_task_template_max_concurrency_limit(
                task_template_name=DAG.Tasks.CAUSAL_ATTRIBUTION, limit=self.MAX_CA_TASKS
            )
        self.workflow.set_task_template_max_concurrency_limit(
            task_template_name=DAG.Tasks.MODELED_PROPORTION, limit=5
        )
        self.workflow.set_task_template_max_concurrency_limit(
            task_template_name=DAG.Tasks.EX_ADJUST, limit=5
        )

    def create_output_directories(self, meid_list: List[int]) -> None:
        """Generates all required output directories."""
        for fp in [str(meid) for meid in meid_list]:
            directory = os.path.join(self.data_dir, fp)

            if os.path.exists(directory) and not self.resume:
                shutil.rmtree(directory)
                os.makedirs(directory)
            elif os.path.exists(directory) and self.resume:
                logging.info(
                    f"Directory {directory} exists and resume is {self.resume}. Do not "
                    "delete anything. Continue workflow."
                )
            else:
                os.makedirs(directory)

    def _get_save_task(
        self,
        meid: int,
        input_file_pattern: str,
        description: str,
        measure_id: Union[int, List[int]],
        year_id: Union[int, List[int]],
        annual: bool,
        n_draws: int,
        sr_type: str = "epi",
    ) -> Task:
        """Gets an EPIC save tasks given an ME ID and associated args."""
        logging.info(f"Preparing {meid} save task")
        args = {
            Params.PARENT_DIR: self.data_dir,
            Params.INPUT_DIR: os.path.join(self.data_dir, str(meid)),
            Params.INPUT_FILE_PATTERN: input_file_pattern,
            Params.MODELABLE_ENTITY_ID: meid,
            Params.DESCRIPTION: description,
            Params.MEASURE_ID: measure_id,
            Params.YEAR_ID: year_id,
            Params.RELEASE_ID: self.release_id,
            Params.N_DRAWS: n_draws,
            Params.BEST: self.best,
            Params.POPULATION_PATH: self.pop_path,
            Params.ANNUAL: annual,
            Params.SR_TYPE: sr_type,
        }
        save_task = self.save_fac.get_task(**args)
        return save_task

    def _add_save_task(self, save_task: Task, upstream_task: Task, meid: int) -> None:
        """Adds a given EPIC save task instance to the Jobmon workflow."""
        logging.info(f"Adding {meid} save task")
        upstream_task = list(np.atleast_1d(upstream_task))
        for upt in upstream_task:
            save_task.add_upstream(upt)
        self.workflow.add_task(save_task)
        self.task_registry[SaveFactory.get_task_name(meid)] = save_task

    def _add_upload_task(self, upstream_task: Task, meid: int) -> None:
        """Creates and adds an EPIC upload task to the Jobmon workflow given an ME ID and
        upstream tasks.
        """
        logging.info(f"Preparing {meid} upload task")
        args = {
            Params.DB_ENV: self.DB_ENV,
            Params.OUTPUT_DIR: os.path.join(self.data_dir, str(meid)),
            Params.MODELABLE_ENTITY_ID: meid,
            Params.VERSION: self.version,
            Params.BEST: self.best,
            Params.TEST: self.test,
        }
        upload_task = self.upload_fac.get_task(**args)
        upstream_task = list(np.atleast_1d(upstream_task))
        for upt in upstream_task:
            upload_task.add_upstream(upt)
        self.workflow.add_task(upload_task)
        self.task_registry[UploadFactory.get_task_name(meid)] = upload_task

    def _prepare_ca_output_mes(self) -> pd.DataFrame:
        """Gets a list of CA output MEs to run SR on."""
        ca_map = pd.read_csv(str("FILEPATH"))
        outputs = [
            "mild_modelable_entity_id",
            "moderate_modelable_entity_id",
            "severe_modelable_entity_id",
            "without_modelable_entity_id",
            "prop_mild_modelable_entity_id",
            "prop_moderate_modelable_entity_id",
            "prop_severe_modelable_entity_id",
            "prop_without_modelable_entity_id",
        ]
        meids_to_save = pd.melt(
            ca_map,
            id_vars=["anemia_cause", "input_modelable_entity_id"],
            value_vars=outputs,
            var_name="meid_type",
            value_name="output_meid",
        ).drop_duplicates()  # Drop duplicates for hemo_shift sex-specificity
        meids_to_save = meids_to_save[meids_to_save["output_meid"].notna()]
        return meids_to_save

    def _add_causal_attribution_task(self, node: str) -> None:
        """Creates and adds EPIC causal attribution tasks to the Jobmon workflow.

        Also creates and adds related save and upload tasks.
        """
        # get upstream tasks before parallelizing since the
        # dependencies are the same for each parallelized demographic
        upstream_tasks = get_upstreams(node, self.pgraph, self.task_registry)
        logging.info(f"Adding {node} tasks")
        for location_id in self.location_ids:
            for year_id in self.year_ids:
                ca_task = self.ca_fac.get_task(
                    node=node,
                    location_id=location_id,
                    year_id=year_id,
                    release_id=self.release_id,
                    output_dir=self.data_dir,
                    n_draws=self.n_draws,
                    upstream_tasks=upstream_tasks,
                )
                self.workflow.add_task(ca_task)
                self.task_registry[
                    CATaskFactory.get_task_name(node, location_id, year_id)
                ] = ca_task

        meids_to_save = self._prepare_ca_output_mes()
        for row in meids_to_save.itertuples():
            # proportions are saved with measure_id 18
            if row.meid_type.startswith("prop"):
                if row.anemia_cause == "maternal_hem":
                    measures = [gbd_constants.measures.PROPORTION]
                else:
                    measures = [
                        gbd_constants.measures.PREVALENCE,
                        gbd_constants.measures.INCIDENCE,
                    ]
            else:
                if row.anemia_cause == "maternal_hem":
                    measures = [gbd_constants.measures.PREVALENCE]
                else:
                    measures = [
                        gbd_constants.measures.PREVALENCE,
                        gbd_constants.measures.INCIDENCE,
                    ]
            description = (
                f"CA_{int(row.output_meid)}_n_draws_{self.n_draws}_EPICv{self.version}"
            )
            save_task = self._get_save_task(
                meid=int(row.output_meid),
                input_file_pattern="{year_id}/{location_id}.csv",
                description=description,
                measure_id=measures,
                year_id=self.year_ids,
                annual=False,  # Anemia CA is never annual
                n_draws=self.n_draws,
            )
            ca_upstream = [
                self.task_registry[t]
                for t in list(self.task_registry.keys())
                if DAG.Tasks.CAUSAL_ATTRIBUTION in t
            ]
            self._add_save_task(
                save_task=save_task, upstream_task=ca_upstream, meid=int(row.output_meid)
            )
            self._add_upload_task(upstream_task=save_task, meid=int(row.output_meid))

    def _add_tmrel_task(self, node: str) -> None:
        """Creates and adds EPIC TMREL calculation tasks to the workflow."""
        logging.info(f"Adding {node} tasks")
        self.create_output_directories(meid_list=[MEIDS.IRON_DEF_TMREL])

        # get dependency_list before parallelizing since the
        # dependencies are the same for each parallelized demographic
        upstream_tasks = get_upstreams(node, self.pgraph, self.task_registry)

        for location_id in self.location_ids:
            tmrel_task = self.tmrel_fac.get_task(
                node=node,
                location_id=location_id,
                release_id=self.release_id,
                output_dir=self.data_dir,
                n_draws=self.n_draws,
                upstream_tasks=upstream_tasks,
            )
            self.workflow.add_task(tmrel_task)
            self.task_registry[TMRELTaskFactory.get_task_name(node, location_id)] = tmrel_task

        description = (
            f"TMREL_{MEIDS.IRON_DEF_TMREL}_n_draws_{self.n_draws}_EPICv{self.version}"
        )
        save_task = self._get_save_task(
            meid=MEIDS.IRON_DEF_TMREL,
            input_file_pattern="{location_id}.csv",
            description=description,
            sr_type="risk",
            measure_id=[-1],  # NOTE: measure not used for TMRELs
            year_id=self.year_ids,
            annual=True,  # NOTE: TMREL is always annual
            n_draws=self.n_draws,
        )
        tmrel_upstream = [
            self.task_registry[t]
            for t in list(self.task_registry.keys())
            if DAG.Tasks.TMREL in t
        ]
        self._add_save_task(
            save_task=save_task, upstream_task=tmrel_upstream, meid=MEIDS.IRON_DEF_TMREL
        )

    def _add_sev_split_task(self, node: str) -> None:
        """Creates and adds an EPIC standard sev split task to the Jobmon workflow.

        Also creates and adds related save and upload tasks.
        """
        logging.info(f"Adding {node} task")
        split_map = self.emap[node]
        split_version_id = int(split_map["kwargs"]["split_version_id"])
        children_meids = [int(child) for child in split_map["out"].keys()]

        # make output directories
        self.create_output_directories(children_meids)

        split_task = self.sev_split_fac.get_task(
            node=node,
            process_graph=self.pgraph,
            split_version_id=split_version_id,
            output_dir=self.data_dir,
            release_id=self.release_id,
            year_id=self.year_ids,
            annual=self.annual,
            n_draws=self.n_draws,
        )
        self.workflow.add_task(split_task)
        self.task_registry[SevSplitTaskFactory.get_task_name(node)] = split_task
        measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]

        for meid in children_meids:
            description = (
                f"Central_severity_split_{meid}_n_draws_{self.n_draws}_EPICv{self.version}"
            )
            save_task = self._get_save_task(
                meid=meid,
                input_file_pattern="{location_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                annual=self.annual,
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task=save_task, upstream_task=split_task, meid=meid)
            self._add_upload_task(upstream_task=save_task, meid=meid)

    def _add_modeled_sev_split_task(self, node: str) -> None:
        """Creates and adds an EPIC modeled sev split task to the Jobmon workflow.

        Also creates and adds related save and upload tasks.
        """
        logging.info(f"Adding {node} task")
        modeled_split_map = self.emap[node]
        parent_id = int(modeled_split_map["kwargs"]["parent"])
        measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]
        non_parent_keys = [
            key for key in modeled_split_map["kwargs"].keys() if key != "parent"
        ]
        for sev in non_parent_keys:
            child_id = int(modeled_split_map["kwargs"][sev]["child"])
            # make output directories
            self.create_output_directories(list(np.atleast_1d(child_id)))
            proportion_id = int(modeled_split_map["kwargs"][sev]["proportion"])
            modeled_split_task = self.modeled_sev_split_fac.get_task(
                node=node,
                process_graph=self.pgraph,
                parent_id=parent_id,
                child_id=child_id,
                proportion_id=proportion_id,
                output_dir=self.data_dir,
                release_id=self.release_id,
                year_id=self.year_ids,
                annual=self.annual,
                n_draws=self.n_draws,
            )
            self.workflow.add_task(modeled_split_task)
            self.task_registry[ModeledSevSplitTaskFactory.get_task_name(node)] = (
                modeled_split_task
            )

            description = (
                f"Central_modeled_severity_split_{child_id}_n_draws_{self.n_draws}_"
                f"EPICv{self.version}"
            )
            save_task = self._get_save_task(
                meid=child_id,
                input_file_pattern="{location_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                annual=self.annual,
                n_draws=self.n_draws,
            )
            self._add_save_task(
                save_task=save_task, upstream_task=modeled_split_task, meid=child_id
            )
            self._add_upload_task(upstream_task=save_task, meid=child_id)

    def _add_ex_adjust_task(self, node: str) -> None:
        """Creates and adds an EPIC exclusivity adjustment task to the Jobmon workflow.

        Also creates and adds related save and upload tasks.
        """
        logging.info(f"Adding {node} task")
        # compile submission arguments
        kwargs = self.emap[node]["kwargs"]
        try:
            kwargs.pop("copy_env_inc")
            measure_id = [gbd_constants.measures.PREVALENCE, gbd_constants.measures.INCIDENCE]
        except KeyError:
            measure_id = [gbd_constants.measures.PREVALENCE]

        # make output directories
        self.create_output_directories(self.pgraph.nodes[node]["outs"])

        ex_adj_task = self.ex_adjust_fac.get_task(
            node=node,
            process_graph=self.pgraph,
            output_dir=self.data_dir,
            release_id=self.release_id,
            year_id=self.year_ids,
            annual=self.annual,
            n_draws=self.n_draws,
        )
        self.workflow.add_task(ex_adj_task)
        self.task_registry[ExAdjustFactory.get_task_name(node)] = ex_adj_task

        description = f"Exclusivity_adjustment_n_draws_{self.n_draws}_EPICv{self.version}"
        for meid in self.pgraph.nodes[node]["outs"]:
            save_task = self._get_save_task(
                meid=meid,
                input_file_pattern="{location_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                annual=self.annual,
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task=save_task, upstream_task=ex_adj_task, meid=meid)
            self._add_upload_task(upstream_task=save_task, meid=meid)

    def _add_super_squeeze_task(self, node: str) -> None:
        """Creates and adds EPIC super squeeze tasks to the Jobmon workflow.

        Note that unlike other task creation, super squeeze creates multiple tasks per input
        node, since tasks are location/year/sex spcific.

        Also creates and adds related save and upload tasks.
        """
        logging.info(f"Adding {node} task")

        # make output directories
        self.create_output_directories(self.pgraph.nodes[node]["outs"])

        # get dependency_list before parallelizing since the
        # dependencies are the same for each parallelized demographic
        upstream_tasks = get_upstreams(node, self.pgraph, self.task_registry)

        for location_id in self.location_ids:
            for year_id in self.year_ids:
                for sex_id in self.sex_ids:
                    ss_task = self.super_squeeze_fac.get_task(
                        node=node,
                        output_dir=self.data_dir,
                        location_id=location_id,
                        year_id=year_id,
                        sex_id=sex_id,
                        release_id=self.release_id,
                        n_draws=self.n_draws,
                        upstream_tasks=upstream_tasks,
                    )
                    self.task_registry[
                        SuperSqueezeFactory.get_task_name(node, location_id, year_id, sex_id)
                    ] = ss_task
                    self.workflow.add_task(ss_task)

        ss_upstream = [
            self.task_registry[t]
            for t in list(self.task_registry.keys())
            if DAG.Tasks.SUPER_SQUEEZE in t
        ]
        description = f"Super_Squeeze_n_draws_{self.n_draws}_EPICv{self.version}"
        measure_id = [gbd_constants.measures.PREVALENCE]

        for meid in self.pgraph.nodes[node]["outs"]:
            save_task = self._get_save_task(
                meid=meid,
                input_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                annual=False,  # super squeeze is never run annually
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task=save_task, upstream_task=ss_upstream, meid=meid)
            self._add_upload_task(upstream_task=save_task, meid=meid)


def parse_arguments() -> argparse.Namespace:
    """Parse WF arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--version_id", required=True, type=int, help="version # of EPIC run")
    parser.add_argument("--release_id", required=True, type=int, help="ID of release")
    parser.add_argument(
        "--year_ids", required=False, type=int, nargs="+", help="years to run", default=None
    )
    parser.add_argument(
        "--annual",
        help="attempt to run computation on annual years where available",
        action="store_true",
    )
    parser.add_argument(
        "--n_draws", required=False, type=int, help="draws to run for", default=None
    )
    parser.add_argument(
        "--anemia",
        help="whether to include anemia causal attribution in the EPIC run",
        action="store_true",
    )
    parser.add_argument(
        "--best", help="whether models should be marked best", action="store_true"
    )
    parser.add_argument("--resume", help="resume flag", action="store_true")
    parser.add_argument("--test", help="test flag", action="store_true")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = parse_arguments()
    user = getpass.getuser()
    if user != Params.EPIC_USERNAME:
        logging.warning(
            f"EPIC workflow must be run by the {Params.EPIC_USERNAME} user in "
            "order to run save_results_epi, workflow is currently being run "
            f"with user: {user}"
        )
    root_dir = FilePaths.TEST_DATA_DIR if args.test else FilePaths.DATA_DIR
    versions = [int(v) for v in os.listdir(root_dir)]
    if not args.resume and args.version_id <= max(versions):
        raise RuntimeError(
            "Version number must be greater than the last EPIC version, was "
            f"given version={args.version_id} and the last EPIC run was "
            f"{max(versions)}"
        )

    # Set years to estimation years if not provided
    if not args.year_ids:
        logging.warning(
            "year_ids arg was not provided, defaulting to estimation years for"
            f"release ID={args.release_id}"
        )
    year_ids = (
        args.year_ids if args.year_ids else estimation_years_from_release_id(args.release_id)
    )
    # Set n_draws if not provided
    if not args.n_draws:
        logging.warning(
            f"n_draws was not provided, defaulting to {Params.DEFAULT_N_DRAWS} draws"
        )
    n_draws = args.n_draws if args.n_draws else Params.DEFAULT_N_DRAWS

    logging.info("Generating COMO, Severity Split, and Super Squeeze maps")
    generate_maps(release_id=args.release_id)
    logging.info("Maps created and stored in code directory!")
    logging.info("Combining all maps")
    cm = CombineMaps(
        release_id=args.release_id, include_anemia=args.anemia, include_como=False
    )
    ewf = EpicWorkFlow(
        version=args.version_id,
        mapbuilder=cm,
        release_id=args.release_id,
        year_ids=year_ids,
        annual=args.annual,
        n_draws=n_draws,
        anemia=args.anemia,
        best=args.best,
        resume=args.resume,
        test=args.test,
    )

    wf_status = ewf.workflow.run(
        resume=args.resume,
        reset_running_jobs=False,
        seconds_until_timeout=435600,  # ~5 days
        resume_timeout=900,  # 10 minutes
    )

    if wf_status != jobmon_constants.WorkflowRunStatus.DONE:
        workflow_errors = ewf.workflow.get_errors()
        error_json = os.path.join(
            FilePaths.STDERR.format(version=ewf.version), FilePaths.ERROR_JSON
        )
        logging.error(
            f"Workflow exited with status {wf_status}, saving first 1000 errors to "
            f"{error_json}"
        )
        with open(error_json, "w") as outfile:
            json.dump(workflow_errors, outfile)
        logging.error(f"Errors saved to {error_json}")

    logging.info("Compiling MEID+MVID map, post EPIC run")
    compile_all_mvid(ewf.data_dir)
