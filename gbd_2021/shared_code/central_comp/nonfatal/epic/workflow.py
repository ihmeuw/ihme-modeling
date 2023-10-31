import argparse
import getpass
import logging
import os
import shutil
import json
from typing import List, Union

import networkx as nx
import numpy as np

from db_queries import get_demographics
from gbd.estimation_years import estimation_years_from_gbd_round_id
from gbd_artifacts.severity_prop import SeverityPropMetadata
import gbd.constants as gbd
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.python_task import PythonTask

from epic.maps.create_digraph import CombineMaps
from epic.maps.generators.generators import generate_maps
from epic.tasks.ex_adjust_task import ExAdjustFactory
from epic.tasks.split_task import SevSplitTaskFactory
from epic.tasks.modeled_split_task import ModeledSevSplitTaskFactory
from epic.tasks.covid_scaling_task import CovidScalingTaskFactory
from epic.tasks.save_task import SaveFactory
from epic.tasks.upload_task import UploadFactory
from epic.tasks.super_squeeze_task import SuperSqueezeFactory
from epic.util.common import get_dependencies, validate_decomp_step, compile_all_mvid
from epic.util.constants import DAG, FilePaths, Params, Process


class EpicWorkFlow(object):

    CODE_DIR = os.path.dirname(os.path.realpath(__file__))
    LOG_DIR = os.path.join("FILEPATH", Params.EPIC_USERNAME)
    MAX_UPLOAD_TASKS = 25
    DB_ENV = "prod"

    def __init__(
        self,
        version: int,
        mapbuilder: CombineMaps,
        decomp_step: str,
        gbd_round_id: int,
        year_ids: List[int],
        n_draws: int,
        run_covid_scaling: bool,
        covid_scaling_year_ids: List[int],
        best: bool,
        resume: bool
    ) -> None:

        # validate decomp_step
        validate_decomp_step("EPIC", decomp_step, gbd_round_id)

        self.version = version
        self.data_dir = os.path.join(FilePaths.DATA_DIR, str(self.version))
        self.scalar_dir = os.path.join(FilePaths.SCALAR_DIR, str(self.version))
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            os.makedirs(
                os.path.join(self.data_dir, FilePaths.INPUT_FILES_DIR)
            )

        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        self.n_draws = n_draws
        self.year_ids = year_ids
        self.best = int(best)
        self.resume = resume
        self.run_covid_scaling = run_covid_scaling
        if covid_scaling_year_ids:
            self.covid_scaling_year_ids = covid_scaling_year_ids

        # create epic json map
        self.emap = mapbuilder.downstream_only(Process.COMO)
        
        # create a list to hold all MEs that get uploaded
        self.upload_mes = []

        # instantiate the factories
        self._task_registry = {}
        self._sev_split_fac = SevSplitTaskFactory(self._task_registry)
        self._modeled_sev_split_fac = ModeledSevSplitTaskFactory(self._task_registry)
        self._ex_adjust_fac = ExAdjustFactory(self._task_registry)
        self._super_squeeze_fac = SuperSqueezeFactory(self._task_registry)
        self._covid_scaling_fac = CovidScalingTaskFactory(self._task_registry)
        self._save_fac = SaveFactory()
        self._upload_fac = UploadFactory()

        self.workflow = Workflow(
            workflow_args=f"epic_v{self.version}",
            name="EPIC Central Machinery",
            project=DAG.Tasks.PROJECT,
            stdout=os.path.join(self.LOG_DIR, "output"),
            stderr=os.path.join(self.LOG_DIR, "errors"),
            resume=resume,
            reset_running_jobs=False,
            seconds_until_timeout=435600
        )

        if not resume:
            # Save best input models as csv for posting to EPIC tracker HUB
            # page then separate into individual json files for use in
            # downstream scripts. Take care that downstream processes do not
            # pick up a model_verison_id from a previous run. Only
            # collect the best models once per run so we know exactly what
            # was a available at the start of the run and what was
            # consequently used in the rest of the workflow
            best_models = mapbuilder.best_models
            inputs = [int(x) for x in mapbuilder.inputs]
            best_models = best_models.loc[
                best_models[Params.MODELABLE_ENTITY_ID].isin(inputs)
            ]
            best_models.to_csv(
                os.path.join(
                    self.data_dir,
                    FilePaths.INPUT_FILES_DIR,
                    FilePaths.BEST_MODELS_FILE_PATTERN
                ),
                index=False,
                encoding="utf8"
            )
            for index, row in best_models.iterrows():
                SaveFactory.save_model_metadata(
                    self.data_dir,
                    row.modelable_entity_id,
                    row.model_version_id,
                    row.decomp_step
                )

        self._task_map = {
            DAG.Tasks.SPLIT: self._add_sev_split_task,
            DAG.Tasks.MODELED_PROPORTION: self._add_modeled_sev_split_task,
            DAG.Tasks.SUPER_SQUEEZE: self._add_super_squeeze_task,
            DAG.Tasks.EX_ADJUST: self._add_ex_adjust_task,
            DAG.Tasks.COVID_SCALING: self._add_covid_scaling_task,
            DAG.Tasks.UPLOAD: self._add_modulo_upload_task
        }

        # run every process in the pipeline regardless of whether or not
        # there is already a model saved
        self.pgraph = mapbuilder.P

        top_sort = nx.topological_sort(self.pgraph)

        for node in top_sort:
            if node == mapbuilder.start_node:
                pass
            elif DAG.Tasks.SPLIT in node:
                self._task_map[DAG.Tasks.SPLIT](node)
            elif DAG.Tasks.MODELED_PROPORTION in node:
                self._task_map[DAG.Tasks.MODELED_PROPORTION](node)
            elif DAG.Tasks.COVID_SCALING in node:
                self._task_map[DAG.Tasks.COVID_SCALING](node)
            elif DAG.Tasks.SUPER_SQUEEZE in node:
                self._task_map[DAG.Tasks.SUPER_SQUEEZE](node)
            else:
                self._task_map[DAG.Tasks.EX_ADJUST](node)
        self._task_map[DAG.Tasks.UPLOAD]()


    def _create_output_directories(self, meid_list: List[int]) -> None:
        for meid in meid_list:
            directory = os.path.join(self.data_dir, str(meid))

            if os.path.exists(directory) and not self.resume:
                shutil.rmtree(directory)
                os.makedirs(directory)
            elif os.path.exists(directory) and self.resume:
                logging.info(
                    f"Directory exists for modelable_entity_id {meid} "
                    f"and resume is {self.resume}. Do not delete anything. "
                    f"Continue workflow."
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
        n_draws: int
    ) -> PythonTask:
        logging.info(f"Preparing {meid} save task")
        args = {
                Params.PARENT_DIR: self.data_dir,
                Params.INPUT_DIR: os.path.join(self.data_dir, str(meid)),
                Params.INPUT_FILE_PATTERN: input_file_pattern,
                Params.MODELABLE_ENTITY_ID: meid,
                Params.DESCRIPTION: description,
                Params.MEASURE_ID: measure_id,
                Params.YEAR_ID: year_id,
                Params.GBD_ROUND_ID: self.gbd_round_id,
                Params.DECOMP_STEP: self.decomp_step,
                Params.N_DRAWS: n_draws,
                Params.BEST: self.best,
                Params.UPLOAD_SUMMARIES: 0,
        }
        save_task = self._save_fac.get_task(**args)
        return save_task


    def _add_save_task(
        self,
        save_task: PythonTask,
        upstream_task: PythonTask,
        meid: int
    ) -> None:
        logging.info(f"Adding {meid} save task")
        upstream_task = list(np.atleast_1d(upstream_task))
        for upt in upstream_task:
            save_task.add_upstream(upt)
        self.workflow.add_task(save_task)
        self._task_registry[SaveFactory.get_task_name(meid)] = save_task


    def _prep_upload_task(
        self,
        upstream_task: PythonTask,
        meid: int
    ) -> None:
        logging.info(f"Preparing {meid} upload task")
        args = {
                Params.DB_ENV: self.DB_ENV,
                Params.OUTPUT_DIR: os.path.join(self.data_dir, str(meid)),
                Params.MODELABLE_ENTITY_ID: meid,
                Params.VERSION: self.version
        }
        upload_task = self._upload_fac.get_task(**args)
        upstream_task = list(np.atleast_1d(upstream_task))
        for upt in upstream_task:
            upload_task.add_upstream(upt)
        self._task_registry[UploadFactory.get_task_name(meid)] = upload_task
        self.upload_mes.append(meid)


    def _add_modulo_upload_task(self) -> None:
        logging.info(f"Adding upload tasks")
        all_upload_tasks = [
            self._task_registry[
                UploadFactory.get_task_name(meid)
            ] for meid in self.upload_mes
        ]        
        for initial_task in all_upload_tasks[:self.MAX_UPLOAD_TASKS]:
            self.workflow.add_task(initial_task)
        for index in range(self.MAX_UPLOAD_TASKS):
            this_group = all_upload_tasks[index::self.MAX_UPLOAD_TASKS]
            for upstream_task, task in zip(this_group, this_group[1:]):
                task.add_upstream(upstream_task)
                self.workflow.add_task(task)

    def _add_sev_split_task(self, node: str) -> None:
        logging.info(f"Adding {node} task")
        split_map = self.emap[node]
        split_id = int(split_map["kwargs"]["split_id"])
        split_meta = SeverityPropMetadata(split_id=split_id,
            decomp_step=self.decomp_step, gbd_round_id=self.gbd_round_id)
        split_version_id = split_meta.best_version
        meta_version = split_meta.get_metadata_version(split_version_id)
        children_meids = [int(x) for x in meta_version.child_meid().split(",")]

        # make output directories
        self._create_output_directories(children_meids)

        split_task = self._sev_split_fac.get_task(
                                node=node,
                                process_graph=self.pgraph,
                                split_version_id=split_version_id,
                                output_dir=self.data_dir,
                                gbd_round_id=self.gbd_round_id,
                                decomp_step=self.decomp_step,
                                year_id=self.year_ids,
                                n_draws=self.n_draws
        )
        self.workflow.add_task(split_task)
        self._task_registry[
            SevSplitTaskFactory.get_task_name(node)] = split_task
        measure_id = [gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]

        for meid in children_meids:
            description = (
                f"Central_severity_split_{meid}_"
                f"{Params.DESCRIPTION_MAP[self.n_draws]}_EPICv{self.version}"
            )
            save_task = self._get_save_task(
                meid=meid,
                input_file_pattern="{location_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task, split_task, meid)
            self._prep_upload_task(upstream_task=save_task, meid=meid)


    def _add_modeled_sev_split_task(self, node: str) -> None:
        logging.info(f"Adding {node} task")
        modeled_split_map = self.emap[node]
        parent_id = int(modeled_split_map["kwargs"]["parent"])
        measure_id = [gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]
        non_parent_keys = [
            key for key in modeled_split_map["kwargs"].keys() if key != "parent"
        ]
        for sev in non_parent_keys:
            child_id = int(modeled_split_map["kwargs"][sev]["child"])
            # make output directories
            self._create_output_directories(list(np.atleast_1d(child_id)))
            proportion_id = int(modeled_split_map["kwargs"][sev]["proportion"])
            modeled_split_task = self._modeled_sev_split_fac.get_task(
                node=node,
                process_graph=self.pgraph,
                parent_id=parent_id,
                child_id=child_id,
                proportion_id=proportion_id,
                output_dir=self.data_dir,
                gbd_round_id=self.gbd_round_id,
                decomp_step=self.decomp_step,
                year_id=self.year_ids,
                n_draws=self.n_draws
            )
            self.workflow.add_task(modeled_split_task)
            self._task_registry[
                ModeledSevSplitTaskFactory.get_task_name(node)
            ] = modeled_split_task
            
            description = (
                f"Central_modeled_severity_split_{child_id}_"
                f"{Params.DESCRIPTION_MAP[self.n_draws]}_EPICv{self.version}"
            )
            save_task = self._get_save_task(
                meid=child_id,
                input_file_pattern="{location_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task, modeled_split_task, child_id)
            self._prep_upload_task(upstream_task=save_task, meid=child_id)

    def _add_covid_scaling_task(self, node: str) -> None:
        logging.info(f"Adding {node} task")
        covid_scaling_map = self.emap[node]
        modelable_entity_id = int(covid_scaling_map["kwargs"]["in"])
        cause_id = int(covid_scaling_map["kwargs"]["cause"])
        output_modelable_entity_id = int(covid_scaling_map["kwargs"]["out"])
        measure_id = [gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]

        self._create_output_directories(list(np.atleast_1d(output_modelable_entity_id)))
        covid_scaling_task = self._covid_scaling_fac.get_task(
            node=node,
            process_graph=self.pgraph,
            modelable_entity_id=modelable_entity_id,
            output_modelable_entity_id=output_modelable_entity_id,
            output_dir=self.data_dir,
            decomp_step=self.decomp_step,
            cause_id=cause_id,
            scalar_dir=self.scalar_dir,
            year_id=self.covid_scaling_year_ids,
            gbd_round_id=self.gbd_round_id,
            n_draws=self.n_draws
        )

        self.workflow.add_task(covid_scaling_task)
        self._task_registry[
            ModeledSevSplitTaskFactory.get_task_name(node)
        ] = covid_scaling_task

        description = (
            f"Central_covid_scaled_{output_modelable_entity_id}_"
            f"{Params.DESCRIPTION_MAP[self.n_draws]}_EPICv{self.version}"
        )

        save_task = self._get_save_task(
            meid=output_modelable_entity_id,
            input_file_pattern="{location_id}.h5",
            description=description,
            measure_id=measure_id,
            year_id=self.year_ids,
            n_draws=self.n_draws,
        )
        self._add_save_task(save_task, covid_scaling_task, output_modelable_entity_id)
        self._prep_upload_task(upstream_task=save_task, meid=output_modelable_entity_id)


    def _add_ex_adjust_task(self, node: str) -> None:
        logging.info(f"Adding {node} task")
        # compile submission arguments
        kwargs = self.emap[node]["kwargs"]
        try:
            copy_env_inc = kwargs.pop("copy_env_inc")
            measure_id = [gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]
        except KeyError:
            copy_env_inc = False
            measure_id = [gbd.measures.PREVALENCE]

        # make output directories
        self._create_output_directories(self.pgraph.nodes[node]["outs"])

        ex_adj_task = self._ex_adjust_fac.get_task(
                                node=node,
                                process_graph=self.pgraph,
                                output_dir=self.data_dir,
                                gbd_round_id=self.gbd_round_id,
                                decomp_step=self.decomp_step,
                                year_id=self.year_ids,
                                n_draws=self.n_draws)
        self.workflow.add_task(ex_adj_task)
        self._task_registry[ExAdjustFactory.get_task_name(node)] = ex_adj_task

        description = (
            f"Exclusivity_adjustment_auto_mark_"
            f"{Params.DESCRIPTION_MAP[self.n_draws]}_EPICv{self.version}"
        )
        for meid in self.pgraph.nodes[node]["outs"]:
            save_task = self._get_save_task(
                meid=meid,
                input_file_pattern="{location_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task, ex_adj_task, meid)
            self._prep_upload_task(upstream_task=save_task, meid=meid)


    def _add_super_squeeze_task(self, node: str) -> None:
        logging.info(f"Adding {node} task")

        # make output directories
        self._create_output_directories(self.pgraph.nodes[node]["outs"])

        # get dependency_list before parallelizing since the
        # dependencies are the same for each parallelized demographic
        dep_list = get_dependencies(node, self.pgraph, self._task_registry)

        epi_demo = get_demographics("epi", gbd_round_id=self.gbd_round_id)
        for location_id in epi_demo[Params.LOCATION_ID]:
            for year_id in self.year_ids:
                for sex_id in epi_demo[Params.SEX_ID]:
                    ss_task = self._super_squeeze_fac.get_task(
                        node=node,
                        output_dir=self.data_dir,
                        location_id=location_id,
                        year_id=year_id,
                        sex_id=sex_id,
                        gbd_round_id=self.gbd_round_id,
                        decomp_step=self.decomp_step,
                        n_draws=self.n_draws,
                        dependency_list=dep_list
                    )
                    self.workflow.add_task(ss_task)
                    self._task_registry[
                        SuperSqueezeFactory.get_task_name(node, location_id, year_id, sex_id)
                    ] = ss_task

        ss_upstream = [
            self._task_registry[t] for t in list(
                self._task_registry.keys()
            ) if DAG.Tasks.SUPER_SQUEEZE in t
        ]
        description = (
            f"Super_Squeeze_auto_mark_{Params.DESCRIPTION_MAP[self.n_draws]}_"
            f"EPICv{self.version}"
        )
        measure_id = [gbd.measures.PREVALENCE]

        for meid in self.pgraph.nodes[node]["outs"]:
            save_task = self._get_save_task(
                meid=meid,
                input_file_pattern="{location_id}/{measure_id}_{year_id}_{sex_id}.h5",
                description=description,
                measure_id=measure_id,
                year_id=self.year_ids,
                n_draws=self.n_draws,
            )
            self._add_save_task(save_task, ss_upstream, meid)
            self._prep_upload_task(upstream_task=save_task, meid=meid)


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--version_id", required=True, type=int, help="version # of EPIC run"
    )
    parser.add_argument(
        "--decomp_step", required=True, type=str, help="decomp step of GBD round"
    )
    parser.add_argument(
        "--gbd_round_id", required=True, type=int, help="ID of GBD round"
    )
    parser.add_argument(
        "--year_ids", required=False, type=int, nargs="+", help="years to run",
        default=None
    )
    parser.add_argument(
        "--n_draws", required=False, type=int, help="draws to run for",
        default=None
    )
    parser.add_argument(
        "--best", help="whether models should be marked best", action="store_true"
    )
    parser.add_argument(
        "--resume", help="resume flag", action="store_true"
    )
    parser.add_argument(
        "--run_covid_scaling", help="resume flag", action="store_true"
    )
    parser.add_argument(
        "--covid_scaling_year_ids", required=False, type=int, nargs="+",
        help="years to scale", default=None
    )
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = parse_arguments()
    user = getpass.getuser()
    if user != Params.EPIC_USERNAME:
        raise RuntimeError(
            f"EPIC workflow must be run by the {Params.EPIC_USERNAME} user in "
            f"order to run save_results_epi, workflow is currently being run "
            f"with user: {user}"
        )
    if args.covid_scaling_year_ids and not args.run_covid_scaling:
            logging.warning(
                "Scaling years were provided but run_covid_scaling was set to "
                "False, ignoring years and running workflow without scaling"
            )
    if not args.covid_scaling_year_ids and args.run_covid_scaling:
        raise RuntimeError(
            f"EPIC Covid scaling must be provided with years to scale, "
            f"please provide a list of years to scale"
        )
    versions = [int(v) for v in os.listdir(FilePaths.DATA_DIR)]
    # Version number must be greater than the last version run when resume is True
    if not args.resume and args.version_id <= max(versions):
        raise RuntimeError(
            f"Version number must be greater than the last EPIC version, was "
            f"given version={args.version_id} and the last EPIC run was "
            f"{max(versions)}"
        )
    if args.gbd_round_id == 7 and args.decomp_step != gbd.decomp_step.ITERATIVE:
        raise RuntimeError(
            f"For GBD 2020, EPIC may only be run for the iterative decomp step, "
            f"was given decomp_step: {args.decomp_step}"
        )
    # Set years to estimation years if not provided
    if not args.year_ids:
        logging.warning(
            f"year_ids arg was not provided, defaulting to estimation years for"
            f"GBD round ID={args.gbd_round_id}"
        )
    year_ids = args.year_ids if args.year_ids else estimation_years_from_gbd_round_id(args.gbd_round_id)
    # Set n_draws if not provided
    if not args.n_draws:
        logging.warning(
            f"n_draws was not provided, defaulting to {Params.DEFAULT_N_DRAWS} "
            f"draws"
        )
    n_draws = args.n_draws if args.n_draws else Params.DEFAULT_N_DRAWS

    logging.info("Generating COMO, Severity Split, and Super Squeeze maps")
    generate_maps(decomp_step=args.decomp_step, gbd_round_id=args.gbd_round_id)
    logging.info("Maps created and stored in code directory!")
    logging.info("Combining all maps")
    cm = CombineMaps(
        decomp_step=args.decomp_step,
        gbd_round_id=args.gbd_round_id,
        include_como=False,
        run_covid_scaling=args.run_covid_scaling
    )
    ewf = EpicWorkFlow(
        version=args.version_id,
        mapbuilder=cm,
        decomp_step=args.decomp_step,
        gbd_round_id=args.gbd_round_id,
        year_ids=year_ids,
        n_draws=n_draws,
        best=args.best,
        resume=args.resume,
        run_covid_scaling=args.run_covid_scaling,
        covid_scaling_year_ids=args.covid_scaling_year_ids
    )
    success = ewf.workflow.run()
    logging.info("Compiling MEID+MVID map, post EPIC run")
    compile_all_mvid(ewf.data_dir)
