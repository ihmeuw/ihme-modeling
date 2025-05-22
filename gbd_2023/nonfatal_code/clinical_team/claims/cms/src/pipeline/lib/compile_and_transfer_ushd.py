"""
Transfer final CMS USHD data their team's subdir on LU CMS,
writing in their preferred output (CSV)

Example Use:
transfer_run_to_ushd(run_id=27)
"""

from pathlib import Path

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from db_tools.ezfuncs import query

from cms.utils import readers


def get_dirs(run_id):
    """Return the I/O directories for a given run ID"""
    output_dir = "FILEPATH"

    input_dir = "FILEPATH"
    return input_dir, output_dir


def swarm_runs(run_id):
    """Get swarm associated with a given CMS run ID"""
    swarms = list(
        (
            "FILEPATH"
        ).glob("swarm_id*.csv")
    )
    assert len(swarms) == 1, "Expect only a single swarm_id"
    swarm = pd.read_csv(swarms[0])
    swarm = swarm[swarm.deliverable == "ushd"]
    return swarm


def get_mart_swarm(swarm):
    """given a swarm table return the mart version id"""
    assert swarm.mart_version_id.unique().size == 1, "Expect only a single bundle mart version"
    return swarm.mart_version_id.iloc[0]


def swarm_map(mart_version):
    """Get map version associated with a given bundle mart version"""
    dql = (
        "QUERY"
    )
    res = query(dql, conn_def="SERVER")
    assert len(res) == 1, "Expect only a single row"
    return int(res.map_version.iloc[0])


def get_active_bundles(mapv):
    dql = (
        "QUERY"
    )
    actives = query(dql, conn_def="SERVER")
    assert len(actives) == len(actives.drop_duplicates())
    return actives


def transfer_run_to_ushd(run_id):
    """The USHD team does not have access to the clinical folder where all of our final data lives
    Transfer all of the active bundle/estimates to their team's folder for a given run
    """

    swarm = swarm_runs(run_id)
    mart_version_id = get_mart_swarm(swarm.copy())
    map_version = swarm_map(mart_version_id)

    active_bundles = get_active_bundles(map_version)

    input_dir, output_dir = get_dirs(run_id)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    failures = []

    counter = 0
    for idx in active_bundles.index:
        row = active_bundles.take([idx], axis=0)
        bundle_id = row.bundle_id.iloc[0]
        estimate_id = row.estimate_id.iloc[0]
        for cms_system in ["max", "mdcr"]:
            in_swarm = swarm.query(
                f"cms_system == '{cms_system}' and bundle_id == {bundle_id} and "
                f"estimate_id == {estimate_id}"
            ).copy()
            read_path = (
                "FILEPATH"
            )
            if len(in_swarm) != 0:
                try:
                    tmp = readers.read_from_parquet(read_path)

                    write_path = "FILEPATH"
                    tmp.to_csv(write_path, index=False)
                    counter += 1
                    del tmp
                except Exception as e:
                    failures.append(e)
            else:
                pass

    print(f"{counter} files have been copied to the USHD subdir on LU_CMS")
    if failures:
        print(f"The following transfers failed {failures}")
