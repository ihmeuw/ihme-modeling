import json
import os
import networkx as nx
import pandas as pd
from typing import Dict, Tuple

from epic.tasks.save_task import SaveFactory
from epic.util.constants import DAG, FilePaths, Params
from epic.util.exceptions import NoBestVersionError
from gbd.decomp_step import decomp_step_id_from_decomp_step
from get_draws.base.exceptions import DrawException
from get_draws.base.utils import ndraw_grouper
from ihme_dimensions.dfutils import resample
from rules.enums import ResearchAreas, Rules, Tools
from rules.RulesManager import RulesManager


def get_dependencies(node, process_graph, task_registry):
        ine = process_graph.in_edges(node)
        dep_list = []
        for u,v in ine:
            if u == Params.EPIC_START_NODE:
                continue
            # Use output meids from in_edges to add dependencies to jobmon
            # workflow. All dependencies are save jobs for the output meids
            # of the previous EPIC process.
            outs = process_graph.nodes[u]["outs"]
            for meid in outs:
                dep_name = SaveFactory.get_task_name(meid)
                dep = task_registry[dep_name]
                print(f"Adding dependency {dep}")
                dep_list.append(dep)
        return dep_list


def get_best_model_version_and_decomp_step(
        parent_dir: str,
        modelable_entity_id: int
) -> Tuple[int, int]:
    """
    Returns the model_version_id and decomp_step associated
    with a modelable_entity_id.

        Args:
            parent_dir (pd.DataFrame): dataframe containing
                modelable_entity_ids, model_version_ids, and decomp_steps
                pulled from the epi.model_version table.
            modelable_entity_id (int): the id to look for on the file system

        Raises:
            NoBestVersionError if a json for the modelable_entity_id
                does not exist in the input_files directory, or if
                the json does not contain the necessary dictionary keys

        Returns:
            Tuple

    """
    model_version_id, decomp_step = None, None
    try:
        metadata_dict = read_model_metadata(parent_dir, modelable_entity_id)
        model_version_id = int(metadata_dict[Params.MODEL_VERSION_ID])
        decomp_step = str(metadata_dict[Params.DECOMP_STEP])
    except (FileNotFoundError, KeyError) as e:
        raise NoBestVersionError(
            f"No model_version_id and/or decomp_step could be found for "
            f"modelable_entity_id {modelable_entity_id} in the collection of "
            f"best model json files.\n" + str(e)
        )
    return model_version_id, decomp_step


def group_and_downsample(df, n_draws, should_downsample=True):
    # if index is not unique, reset it
    if not df.index.is_unique:
        df = df.reset_index(drop=True)
    # Downsample different year groups if necessary
    groups = ndraw_grouper(df, "year_id")
    if len(groups) > 2:
        raise DrawException("More than two draw lengths in data: {}."
                            " Unable to downsample while retaining draw"
                            " correlation."
                            .format(list(groups)))
    elif len(groups) == 2 or n_draws is not None:
        print("Going into the downsample function")

        df_ndraws = list(groups)
        ndraws_min = min(df_ndraws)
        if n_draws is None:
            if not should_downsample:
                print("n_draws is None for some reason!")
                raise DrawException("Different draw numbers exist in data, {}, "
                                    "and downsample is set to False".format(
                                        df_ndraws))
            df_list = [(
                resample(groups[x], ndraws_min, "draw_")
                if x != ndraws_min else groups[x] for x in df_ndraws)
            ]
        else:
            if n_draws > ndraws_min:
                raise DrawException(
                    "n_draws {} is greater than the minimum number "
                    "of draws in the data, {}. Upsampling of draws "
                    "is not allowed.".format(n_draws, ndraws_min)
                )
            elif n_draws < ndraws_min and len(groups) > 1:
                raise DrawException(
                    "More than one draw length in data: {}. "
                    "Unable to downsample both to n_draws {} while "
                    "retaining draw correlation.".format(df_ndraws, n_draws)
                )
            print("Getting resampled")
            df_list = (
                [
                    resample(groups[x], n_draws, "draw_")
                    if x != n_draws else groups[x] for x in df_ndraws
                ]
            )
        df = pd.concat(df_list, sort=False)
        df_draws = [col for col in df.columns if "draw_" in col]
        if not (len(df_draws) == ndraws_min or len(df_draws) == n_draws):
            raise DrawException(
                "Error downsampling, incorrect number of "
                "draws in final dataframe, {}.".format(len(df_draws))
            )
        return df


def name_task(base_name, unique_params):
    unique_name = "_".join(
        [
            "{" + key + "}" for key in list(unique_params.keys())
        ]
    )
    return base_name + "_" + unique_name.format(**unique_params)


def read_model_metadata(
        parent_dir: str,
        modelable_entity_id: int
) -> Dict[str,int]:
    path = os.path.join(
        parent_dir,
        FilePaths.INPUT_FILES_DIR,
        f"{modelable_entity_id}.json"
    )
    with open(path) as json_file:
        metadata_dict = json.load(json_file)
    return metadata_dict


def validate_decomp_step(obj_name, decomp_step, gbd_round_id):
    # validate decomp_step
    decomp_step_id = decomp_step_id_from_decomp_step(
        decomp_step, gbd_round_id)
    rules_manager = RulesManager(ResearchAreas.EPI, Tools.SAVE_RESULTS,
        decomp_step_id=decomp_step_id)
    validate_step_viewable(obj_name, decomp_step, rules_manager)


def validate_step_viewable(obj_name, decomp_step, rm):
    if not rm.get_rule_value(Rules.STEP_VIEWABLE):
        raise ValueError(
            f'{obj_name} cannot currently be run for decomp step '
            f'{decomp_step}'
        )
