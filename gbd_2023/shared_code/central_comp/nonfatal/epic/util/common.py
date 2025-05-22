import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import networkx as nx
import numpy as np
import pandas as pd

from gbd.estimation_years import estimation_years_from_release_id
from get_draws.base.exceptions import DrawException
from get_draws.base.utils import ndraw_grouper
from ihme_cc_rules_client import ResearchAreas, Rules, RulesManager, Tools
from ihme_dimensions.dfutils import resample
from jobmon.client.task import Task

from epic.legacy.util.constants import DAG, FilePaths, Params
from epic.lib.util.exceptions import NoBestVersionError

ConfigDict = TypedDict(
    "ConfigDict",
    {
        "class": str,
        "in": Dict[str, str],
        "out": Dict[str, str],
        "args": List[str],
        "kwargs": Dict[str, Any],
    },
    total=False,
)


def get_annualized_year_set_by_release_id(release_id: int) -> List[int]:
    """Returns annual years based on release ID."""
    year_range = estimation_years_from_release_id(release_id)
    return list(range(min(year_range), max(year_range) + 1))


def get_save_task_name(modelable_entity_id: int) -> str:
    """Return save task name given ME ID."""
    return f"{DAG.Tasks.SAVE}_{modelable_entity_id}"


def get_upstreams(
    node: str, process_graph: nx.DiGraph, task_registry: Dict[str, Task]
) -> List[Task]:
    """Returns upstream tasks given a node and a process graph."""
    # The in_edges method is incorrectly typed and expects nbunch to be None
    # only. Ignore typing on this line.
    ine = process_graph.in_edges(nbunch=node)  # type: ignore
    dep_list: List[Task] = []
    for u, _ in ine:
        if u == Params.EPIC_START_NODE:
            continue
        # Use output meids from in_edges to add dependencies to jobmon
        # workflow. All dependencies are save jobs for the output meids
        # of the previous EPIC process.
        outs = process_graph.nodes[u]["outs"]
        for meid in outs:
            dep_name = get_save_task_name(meid)
            dep = task_registry[dep_name]
            print(f"Adding dependency {dep}")
            dep_list.append(dep)
    return dep_list


def get_best_model_version_and_release(
    parent_dir: str, modelable_entity_id: int
) -> Tuple[int, int]:
    """Returns the model_version_id and release_id associated with a modelable_entity_id.

    Args:
        parent_dir (str): versioned directory containing the json metadata files
        modelable_entity_id (int): the id to look for on the file system

    Raises:
        NoBestVersionError if a json for the modelable_entity_id does not exist in the
            input_files directory, or if the json does not contain the necessary
            dictionary keys.
    """
    model_version_id: Optional[int] = None
    release_id: Optional[int] = None
    try:
        metadata_dict = read_model_metadata(parent_dir, modelable_entity_id)
        model_version_id = int(metadata_dict[Params.MODEL_VERSION_ID])
        release_id = int(metadata_dict[Params.RELEASE_ID])
    except (FileNotFoundError, KeyError) as e:
        raise NoBestVersionError(
            f"No model_version_id could be found for "
            f"modelable_entity_id {modelable_entity_id} in the collection of "
            f"best model json files at {parent_dir}.\n" + str(e)
        ) from e
    return model_version_id, release_id


def group_and_downsample(
    df: pd.DataFrame, n_draws: int, should_downsample: bool = True
) -> pd.DataFrame:
    """From LINK"""
    # if index is not unique, reset it
    if not df.index.is_unique:
        df = df.reset_index(drop=True)
    # Downsample different year groups if necessary
    groups = ndraw_grouper(df, "year_id")
    if len(groups) > 2:
        raise DrawException(
            "More than two draw lengths in data: {}. "
            "Unable to downsample while retaining draw "
            "correlation.".format(list(groups))
        )
    elif len(groups) == 2 or n_draws is not None:
        print("Going into the downsample function")

        df_ndraws = list(groups)
        ndraws_min = min(df_ndraws)
        if n_draws is None:
            if not should_downsample:
                print("n_draws is None for some reason!")
                raise DrawException(
                    "Different draw numbers exist in data, {}, "
                    "and downsample is set to False".format(df_ndraws)
                )
            df_list = [
                (
                    resample(groups[x], ndraws_min, "draw_") if x != ndraws_min else groups[x]
                    for x in df_ndraws
                )
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
            df_list = [
                resample(groups[x], n_draws, "draw_") if x != n_draws else groups[x]
                for x in df_ndraws
            ]
        df = pd.concat(df_list, sort=False)
        df_draws = [col for col in df.columns if "draw_" in col]
        if not (len(df_draws) == ndraws_min or len(df_draws) == n_draws):
            raise DrawException(
                "Error downsampling, incorrect number of "
                "draws in final dataframe, {}.".format(len(df_draws))
            )
        return df


def name_task(base_name: str, unique_params: Dict[str, Any]) -> str:
    """Returns a task name given a base name and a dictionary of parameters."""
    unique_name = "_".join(["{" + key + "}" for key in list(unique_params.keys())])
    return base_name + "_" + unique_name.format(**unique_params)


def read_model_metadata(parent_dir: str, modelable_entity_id: int) -> Dict[str, int]:
    """Reads and returns model metadata given a parent directory and ME ID."""
    path = os.path.join(parent_dir, FilePaths.INPUT_FILES_DIR, f"{modelable_entity_id}.json")
    with open(path) as json_file:
        metadata_dict = json.load(json_file)
    return metadata_dict


def validate_release(obj_name: str, release_id: int) -> None:
    """Validates release is viewable for EPI SR."""
    rules_manager = RulesManager(
        research_area=ResearchAreas.EPI, tool=Tools.SAVE_RESULTS, release_id=release_id
    )
    if not rules_manager.get_rule_value(Rules.STEP_VIEWABLE):
        raise ValueError(f"{obj_name} cannot currently be run for release {release_id}")


def pull_mvid(parent_dir: str, modelable_entity_id: int) -> int:
    """Returns a model version ID given a parent directory and ME ID."""
    # read from disk
    json_path = os.path.join(
        parent_dir, FilePaths.INPUT_FILES_DIR, f"{modelable_entity_id}.json"
    )
    with open(json_path) as json_file:
        try:
            metadata = json.load(json_file, parse_int=int)
        except TypeError:
            with open(json_path) as json_file:
                metadata = json.load(json_file)
    mvid = int(metadata[Params.MODEL_VERSION_ID])
    return mvid


def compile_all_mvid(parent_dir: str) -> None:
    """Given a parent directory, collects and writes all MV IDs to a single CSV."""
    meid_mvid_dict: Dict[int, int] = {}
    # collect and read all ME json files from disk
    json_dir = os.path.join(parent_dir, FilePaths.INPUT_FILES_DIR)
    meid_files = [
        os.path.join(json_dir, file)
        for file in os.listdir(json_dir)
        if file.endswith(".json")
    ]
    for meid_json in meid_files:
        with open(meid_json) as json_file:
            try:
                metadata = json.load(json_file, parse_int=int)
            except TypeError:
                with open(meid_json) as json_file:
                    metadata = json.load(json_file)
        modelable_entity_id = int(metadata[Params.MODELABLE_ENTITY_ID])
        model_version_id = int(metadata[Params.MODEL_VERSION_ID])
        meid_mvid_dict[modelable_entity_id] = model_version_id
    meid_mvid_map = pd.DataFrame.from_dict(
        data=meid_mvid_dict, orient="index", columns=["model_version_id"]
    )
    meid_mvid_map = meid_mvid_map.reset_index().rename(
        columns={"index": "modelable_entity_id"}
    )
    output_path = os.path.join(parent_dir, "meid_mvid_map.csv")
    meid_mvid_map.to_csv(output_path, index=False)


def get_demographics(path: str) -> Dict[str, List[int]]:
    """Given a path to a directory containing a JSON with demographic info,
    reads in the json and returns a dictionary.
    """
    demographic_filepath = os.path.join(path, FilePaths.DEMOGRAPHICS_CACHE)
    with open(demographic_filepath, "r") as f:
        demo = json.load(f)
    return demo


def make_long(df: pd.DataFrame, new_col: str, id_cols: List[str]) -> pd.DataFrame:
    """Converts a dataframe with draw columns from wide to long."""
    draw_cols = [col for col in df.columns if "draw_" in col]
    df = pd.melt(
        df, id_vars=id_cols, value_vars=draw_cols, var_name="draw", value_name=new_col
    )
    df["draw"] = df["draw"].str.replace("draw_", "").astype(int)
    return df


def make_wide(df: pd.DataFrame, value_col: str, id_cols: List[str]) -> pd.DataFrame:
    """Converts a dataframe with a single long draw column from long to wide."""
    df["draw"] = "draw_" + df["draw"].astype(int).astype(str)
    df = pd.pivot_table(
        data=df, values=value_col, index=id_cols, columns="draw"
    ).reset_index()
    return df


def startup_jitter() -> None:
    """Sleeps a random interval within a constant-controlled window."""
    time.sleep(np.random.default_rng().uniform(low=0.0, high=60.0))
