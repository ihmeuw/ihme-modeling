"""
Rough helper functions to generate new CMS swarms

Example use for GBD 2021 first clinical version release with only Medicare data NR'd for GBD
df = create_new_swarm(cms_systems=['mdcr'], bundle_ids='all_bundles', estimate_ids=[17, 21],
                      deliverables=['gbd'], noise_reduce_dict={'deliverable': 'gbd'},
                      write_intermediate_data=True, write_final_data=True, run_id=30,
                      , , force_age_group_set_id=None)

save_swarm(df)
"""
import itertools
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions.pipeline import (
    get_map_version,
)
from crosscutting_functions.mapping import clinical_mapping_db
from db_connector import database
from db_tools.ezfuncs import query
from pydantic import validate_arguments

from cms.src.pipeline.lib import swarm_resource


def get_system_bundles(
    cms_system: str, map_version: int, filter_inactive_bundles: bool, estimate_ids: List[int]
) -> List[int]:
    """Gets valid bundles for a given cms system. Implements under 65 age restrictions
    on Medicare operations.

    Args:
        cms_system (str): CMS system abbreviation. ex: 'mdcr'
        map_version (int): Clinical bundle map version.
        filter_inactive_bundles (bool): True to limit to only active clinical bundle.
        estimate_ids (List[int]): Estimate IDs to create.

    Returns:
        List[int]: CMS system specific bundle id's.
    """

    db = database.Database()
    db.load_odbc(odbc_profile="SERVER")

    bundles = get_all_bundles(db_conn=db, map_version=map_version)

    if filter_inactive_bundles:
        bundles = active_bundles_only(
            bundle_list=bundles, map_version=map_version, estimate_ids=estimate_ids
        )

    if cms_system == "mdcr":
        u65_only_bundles = u65_bundles(map_version=map_version)
        bundles = [b for b in bundles if b not in u65_only_bundles]

    return bundles


def get_all_bundles(db_conn: database.Database, map_version: int) -> List[int]:
    """Get a list of all Clincal bundles from the database for a specific map version."""

    all_map_bundles = db_conn.query(
        "QUERY"
    )

    return all_map_bundles["bundle_id"].to_list()


def active_bundles_only(
    bundle_list: List[int], map_version: int, estimate_ids: List[int]
) -> List[int]:
    """Filter a bundle list to only active clinical bundles in a given map version.
    This will also limit bundles to only the bundles that are active and valid for
    the run's estimate_ids.

    Args:
        bundle_list (List[int]): Bundles in the 'map_version'.
        map_version (int): Bundle map version associated with the run.
        estimate_ids (List[int]): Estimate IDs to create.
    Returns:
        List[int]: bundle_list filtered to only active bundles that are valid for
            the input estimate_ids.
    """

    print("This swarm will now filter out all inactive bundles")

    bundle = "TABLE"
    mapv = "TABLE"
    clin_dtype = "TABLE"
    est = "TABLE"

    dql = "QUERY"

    active_bundles = query(dql, conn_def="SERVER")["bundle_id"].to_list()
    bundles = [b for b in bundle_list if b in active_bundles]

    return bundles


def get_next_swarm() -> int:
    """increment the highest existing swarm"""

    swarms = "FILEPATH"
    swarm_count = [int(Path(s).stem.replace("swarm_id_", "")) for s in swarms]

    return max(swarm_count) + 1


def u65_bundles(map_version: int) -> List[int]:
    """Get a list of bundle ids which which map to conditions
    that are ONLY found in under 65 populations."""

    restrictions = clinical_mapping_db.create_bundle_restrictions(map_version=map_version)
    restrictions = restrictions[restrictions["yld_age_end"] < 65]
    u65_only_bundles = restrictions["bundle_id"].to_list()

    return u65_only_bundles


def build_decision_table(
    cms_system: List[str],
    sys_bundles: List[int],
    estimate_ids: List[int],
    run_id: int,
    deliverables: List[str],
    write_intermediate_data: bool = False,
    write_final_data: bool = False,
    force_age_group_set_id: Optional[int] = None,
) -> pd.DataFrame:

    dt = pd.DataFrame(
        itertools.product(
            cms_system,
            sys_bundles,
            estimate_ids,
            deliverables,
        )
    )

    dt.columns = [
        "cms_system",
        "bundle_id",
        "estimate_id",
        "deliverable_name",
    ]

    dt["run_id"] = run_id
    dt["write_intermediate_data"] = write_intermediate_data
    dt["write_final_data"] = write_final_data
    dt["force_age_group_set_id"] = force_age_group_set_id

    # Will be augmented with noise_reduce_dict in create_new_swarm()
    dt["noise_reduce"] = False

    return dt


def save_swarm(df: pd.DataFrame, map_version: int) -> None:
    """Saves a given swarm, passed as df to disk and then creates a resource table"""

    swarm_id = df["swarm_id"].iloc[0]
    base = "FILEPATH"
    wp = "FILEPATH"
    resource_wp = "FILEPATH"

    if Path(wp).is_file():
        raise ValueError("This Swarm ID already exists. I won't overwrite a file")

    # write to disk and set as read-only
    df.to_csv(wp, index=False)
    os.chmod(wp, 0o555)

    # Once the swarm is set, create resource estimates
    rsrc = swarm_resource.allocate_bundle_tasks(
        mem_scalar=100, swarm_id=swarm_id, map_version=map_version
    )
    rsrc.to_csv(resource_wp, index=False)


@validate_arguments()
def create_new_swarm(
    run_id: int,
    cms_systems: List[str] = ["mdcr"],
    estimate_ids: List[int] = list(constants.est_id_dict["mdcr"].keys()),
    deliverables: List[str] = ["gbd"],
    noise_reduce_dict: Dict[str, Union[str, List[int]]] = {
        "mdcr": "active_bundles",
        "max": "active_bundles",
    },
    write_intermediate_data: bool = True,
    write_final_data: bool = True,
    bundles_to_run: Union[List[int], str] = "active_bundles",
    force_age_group_set_id: Optional[int] = None,
) -> pd.DataFrame:
    """Creates the task table for submission.

    Args:
        run_id (int): Clinical run_id. Out directories already configured.
        estimate_ids (List[int]): List of estimate_ids to include in swarm.
        cms_systems (List[str], optional): CMS systems to include in swarm.
            'mdcr' and 'max' are the 2 acceptable values. Defaults to ["mdcr"].
        deliverables (List[str], optional): List of deliverables to include.
            Currently 'gbd' and 'ushd' are acceptable but a correction factor deliverable
            may be developed in the future. Defaults to ["gbd"].
        noise_reduce_dict (Dict[str, Union[str, List[int]], optional): Use this dict to make
            system-bundle level adjustments to NR.
            Defaults to {"mdcr": "active_bundles", "max": "active_bundles"}
            which will apply ClinicalNR to all active bundles included in 'bundles_to_run'
            for the gbd deliverable.
        write_intermediate_data (bool, optional): Should intermediate data and plots be
            written to "FILEPATH" Defaults to False.
        write_final_data (bool, optional): Should final data be written to
            "FILEPATH" Defaults to True.
        bundles_to_run (Union[List[int], str], optional): List of bundles to process or
            'active_bundles' to pull everything in current CMS map.
            NOTE: inactive bundles will be removed unless spcified. Defaults to "active_bundles"
            which runs all active bundles in the current map.
        force_age_group_set_id (Optional[int], optional): Untested: Both mdcr and max default
            to clinical age group set 1 but this arg was meant to overwrite that.
            Defaults to None.

    Raises:
        ValueError: Not a list or expected string.
        RuntimeError: More than 1 run_id.
        RuntimeError: Duplicate tasks created in table.

    Returns:
        pd.DataFrame: Task table, each row unique.
    """

    map_version = get_map_version(run_id=run_id)

    df_list = []

    if bundles_to_run == "active_bundles":
        filter_inactive_bundles = True
    else:
        filter_inactive_bundles = False

    for cms_system in cms_systems:
        sys_bundles = get_system_bundles(
            cms_system=cms_system,
            map_version=map_version,
            filter_inactive_bundles=filter_inactive_bundles,
            estimate_ids=estimate_ids,
        )
        if isinstance(bundles_to_run, list):
            sys_bundles = [b for b in bundles_to_run if b in sys_bundles]
        elif bundles_to_run != "active_bundles":
            raise ValueError(
                "'bundles_to_run' must be list of bundle_id or string 'active_bundles'"
            )

        tmp = build_decision_table(
            cms_system=[cms_system],
            sys_bundles=sys_bundles,
            estimate_ids=estimate_ids,
            run_id=run_id,
            deliverables=deliverables,
            force_age_group_set_id=force_age_group_set_id,
            write_intermediate_data=write_intermediate_data,
            write_final_data=write_final_data,
        )
        df_list.append(tmp)

        if noise_reduce_dict[cms_system] == "active_bundles":
            noise_reduce_dict[cms_system] = sys_bundles

    # Concat and sort
    df = pd.concat(df_list, sort=False, ignore_index=True)
    df = df.sort_values(df.columns.tolist()).reset_index(drop=True)

    if df["run_id"].unique().size != 1:
        raise RuntimeError("There can't be more than a single run_id")

    if len(df) != len(df.drop_duplicates()):
        raise RuntimeError("There are duplicate values in df")

    for cms_system in cms_systems:
        df.loc[
            (df["cms_system"] == cms_system)
            & (df["bundle_id"].isin(noise_reduce_dict[cms_system])),
            "noise_reduce",
        ] = True

    swarm_id = get_next_swarm()
    df["swarm_id"] = swarm_id

    return df
