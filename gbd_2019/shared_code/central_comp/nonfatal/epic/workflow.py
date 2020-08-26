import os
import datetime
import getpass
import json
import logging
import shutil

import numpy as np
import networkx as nx
import pandas as pd

from dataframe_io.io_queue import RedisServer
from db_queries import get_demographics
from gbd_artifacts.severity_prop import SeverityPropMetadata
import gbd.constants as gbd

from hierarchies.dbtrees import loctree
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.python_task import PythonTask

from epic.maps.create_digraph import CombineMaps
from epic.maps.generators.generators import generate_maps
from epic.tasks.split_task import SevSplitTaskFactory
from epic.tasks.ex_adjust_task import ExAdjustFactory
from epic.tasks.super_squeeze_task import SuperSqueezeFactory
from epic.tasks.save_task import SaveFactory
from epic.util.common import get_dependencies, validate_decomp_step
from epic.util.constants import DAG, FilePaths, Params


class EpicWorkFlow(object):

    CODE_DIR = os.path.dirname(os.path.realpath(__file__))
    USERNAME = getpass.getuser()
    DATA_DIR = "FILEPATH"
    LOG_DIR = os.path.join('PATH', USERNAME)
    YEAR_IDS = [1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019]
    N_DRAWS = 1000

    def __init__(self, version, mapbuilder, decomp_step, gbd_round_id, resume):

        # validate decomp_step
        validate_decomp_step("EPIC", decomp_step, gbd_round_id)

        self.DATA_DIR = os.path.join(self.DATA_DIR, str(version))
        if not os.path.exists(self.DATA_DIR):
            os.makedirs(self.DATA_DIR)
            os.makedirs(
                os.path.join(self.DATA_DIR, FilePaths.INPUT_FILES_DIR)
            )

        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        self.resume = resume

        # create epic json map
        self.emap = mapbuilder.downstream_only("como")

        # instantiate the factories
        self._task_registry = {}
        self._sev_split_fac = SevSplitTaskFactory(self._task_registry)
        self._ex_adjust_fac = ExAdjustFactory(self._task_registry)
        self._super_squeeze_fac = SuperSqueezeFactory(self._task_registry)
        self._save_fac = SaveFactory()

        self.workflow  = Workflow(
            workflow_args="epic_v{version}".format(version=version),
            name="EPIC Central Machinery",
            project=DAG.Tasks.PROJECT,
            stdout=os.path.join(self.LOG_DIR, "output"),
            stderr=os.path.join(self.LOG_DIR, "errors"),
            resume=resume,
            seconds_until_timeout=435600
        )

        if not resume:
            # Save best input models as csv for posting to EPIC tracker HUB
            # page then separate into individual json files for use in
            # downstream scripts. Take care that downstream processes do not
            # pick up a model_version_id from a previous run. Only
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
                    self.DATA_DIR,
                    FilePaths.INPUT_FILES_DIR,
                    FilePaths.BEST_MODELS_FILE_PATTERN
                ),
                index=False,
                encoding="utf8"
            )
            for index, row in best_models.iterrows():
                SaveFactory.save_model_metadata(
                    self.DATA_DIR,
                    row.modelable_entity_id,
                    row.model_version_id,
                    row.decomp_step
                )

        self._task_map = {
            DAG.Tasks.SPLIT: self._add_sev_split_task,
            DAG.Tasks.SUPER_SQUEEZE: self._add_super_squeeze_task,
            DAG.Tasks.EX_ADJUST: self._add_ex_adjust_task
        }

        # run every process in the pipeline regardless of whether or not
        # there is already a model saved
        self.pgraph = mapbuilder.P

        # get process nodes and build out jobmon workflow
        # create a subgraph from the process nodes

        top_sort = nx.topological_sort(self.pgraph)

        for node in top_sort:
            if node == mapbuilder.start_node:
                pass
            elif DAG.Tasks.SPLIT in node:
                self._task_map[DAG.Tasks.SPLIT](node)
            elif DAG.Tasks.SUPER_SQUEEZE in node:
                self._task_map[DAG.Tasks.SUPER_SQUEEZE](node)
            else:
                self._task_map[DAG.Tasks.EX_ADJUST](node)


    def _create_output_directories(self, meid_list):
        for meid in meid_list:
            directory = os.path.join(self.DATA_DIR, str(meid))

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

    def _add_sev_split_task(self, node):
        logging.info(f"Adding {node} task")
        split_map = self.emap[node]
        split_id = int(split_map["kwargs"]["split_id"])
        split_meta = SeverityPropMetadata(split_id=split_id,
            decomp_step=self.decomp_step, gbd_round_id=self.gbd_round_id)
        split_version_id = split_meta.best_version
        meta_version = split_meta.get_metadata_version(split_version_id)
        parent_meid = int(meta_version.parent_meid())
        children_meids = [int(x) for x in meta_version.child_meid().split(",")]

        # make output directories
        self._create_output_directories(children_meids)

        split_task = self._sev_split_fac.get_task(
                                node=node,
                                process_graph=self.pgraph,
                                split_version_id=split_version_id,
                                output_dir=self.DATA_DIR,
                                decomp_step=self.decomp_step,
                                year_id=self.YEAR_IDS,
                                n_draws=self.N_DRAWS)
        self.workflow.add_task(split_task)
        self._task_registry[
            SevSplitTaskFactory.get_task_name(node)] = split_task

        description = (
            f"Central_severity_split_{Params.DESCRIPTION_MAP[self.N_DRAWS]}"
        )
        for meid in children_meids:
            measure_id = [gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]
            self._add_save_task(meid, "{location_id}.h5",
                                description, measure_id,
                                self.YEAR_IDS, self.N_DRAWS,
                                split_task)


    def _add_save_task(self, meid, input_file_pattern, description,
        measure_id, year_id, n_draws, upstream_task):
        logging.info(f"Adding {meid} save task")
        args = {
                Params.PARENT_DIR: self.DATA_DIR,
                Params.INPUT_DIR: os.path.join(self.DATA_DIR, str(meid)),
                Params.INPUT_FILE_PATTERN: input_file_pattern,
                Params.MODELABLE_ENTITY_ID: meid,
                Params.DESCRIPTION: description,
                Params.MEASURE_ID: measure_id,
                Params.YEAR_ID: year_id,
                Params.DECOMP_STEP: self.decomp_step,
                Params.N_DRAWS: n_draws
        }
        save_task = self._save_fac.get_task(**args)

        for upt in list(np.atleast_1d(upstream_task)):
            save_task.add_upstream(upt)

        self.workflow.add_task(save_task)
        self._task_registry[SaveFactory.get_task_name(meid)] = save_task


    def _add_ex_adjust_task(self, node):
        logging.info(f"Adding {node} task")
        # compile submission arguments
        kwargs = self.emap[node]["kwargs"]
        try:
            copy_env_inc = kwargs.pop("copy_env_inc")
            measure_id = [
                    gbd.measures.PREVALENCE, gbd.measures.INCIDENCE]
        except KeyError:
            copy_env_inc = False
            measure_id = [gbd.measures.PREVALENCE]

        # make output directories
        self._create_output_directories(self.pgraph.nodes[node]["outs"])

        ex_adj_task = self._ex_adjust_fac.get_task(
                                node=node,
                                process_graph=self.pgraph,
                                output_dir=self.DATA_DIR,
                                decomp_step=self.decomp_step,
                                year_id=self.YEAR_IDS,
                                n_draws=self.N_DRAWS)
        self.workflow.add_task(ex_adj_task)
        self._task_registry[ExAdjustFactory.get_task_name(node)] = ex_adj_task

        description = (
            f"Exclusivity_adjustment_auto_mark_"
            f"{Params.DESCRIPTION_MAP[self.N_DRAWS]}"
        )
        for meid in self.pgraph.nodes[node]["outs"]:
            self._add_save_task(meid, "{location_id}.h5",
                                description, measure_id,
                                self.YEAR_IDS, self.N_DRAWS,
                                ex_adj_task)


    def _add_super_squeeze_task(self, node):
        logging.info(f"Adding {node} task")

        # make output directories
        self._create_output_directories(self.pgraph.nodes[node]["outs"])

        # get dependency_list before parallelizing since the
        # dependencies are the same for each parallelized demographic
        dep_list = get_dependencies(node, self.pgraph, self._task_registry)

        epi_demo = get_demographics("epi", gbd_round_id=self.gbd_round_id)
        for location_id in epi_demo[Params.LOCATION_ID]:
            for year_id in self.YEAR_IDS:
                for sex_id in epi_demo[Params.SEX_ID]:
                    ss_task = self._super_squeeze_fac.get_task(
                                        node=node,
                                        output_dir=self.DATA_DIR,
                                        location_id=location_id,
                                        year_id=year_id,
                                        sex_id=sex_id,
                                        decomp_step=self.decomp_step,
                                        n_draws=self.N_DRAWS,
                                        dependency_list=dep_list)
                    self.workflow.add_task(ss_task)
                    self._task_registry[
                        SuperSqueezeFactory.get_task_name(
                            node, location_id, year_id, sex_id)] = ss_task

        ss_upstream = [
            self._task_registry[t] for t in list(
            self._task_registry.keys()) if DAG.Tasks.SUPER_SQUEEZE in t]
        description = (
            f"Super_Squeeze_auto_mark_{Params.DESCRIPTION_MAP[self.N_DRAWS]}"
        )
        measure_id = [gbd.measures.PREVALENCE]
        for meid in self.pgraph.nodes[node]["outs"]:
            self._add_save_task(
                meid, "{location_id}/{measure_id}_{year_id}_{sex_id}.h5",
                description, measure_id,
                self.YEAR_IDS, self.N_DRAWS,
                ss_upstream)


if __name__ == "__main__":
    # Manual creation of version for now.

    VERSION = 8
    DECOMP_STEP = "step4"
    GBDRID = 6
    RESUME = False
    logging.info("Generating COMO, Severity Split, and Super Squeeze maps")
    generate_maps(decomp_step=DECOMP_STEP, gbd_round_id=GBDRID)
    logging.info("Maps created and stored in code directory!")
    logging.info("Combining all maps")
    cm = CombineMaps(
        decomp_step=DECOMP_STEP,
        gbd_round_id=GBDRID,
        include_como=False
    )
    ewf = EpicWorkFlow(VERSION, cm, DECOMP_STEP, GBDRID, RESUME)
    success = ewf.workflow.run()
