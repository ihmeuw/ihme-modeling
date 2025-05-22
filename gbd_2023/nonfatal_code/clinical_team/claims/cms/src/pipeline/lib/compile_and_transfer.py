"""
Parse final outputs for a run swarm and transfer data to it's deliverable location
"""
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.src.pipeline.lib import compile_and_transfer_gbd, compile_and_transfer_ushd


def main(run_id: int) -> None:
    """Runner for compile_and_transfer_gbd and compile_and_transfer_ushd.

    Args:
        run_id (int): Clinical run_id found in DATABASE.

    Raises:
        NotImplementedError: 'correction_factors' deliverable passed.
        RuntimeError: No expected compilation method to run found.
    """

    # read in the swarm file for the run to get the deliverables we're transfering
    swarm = read_swarm(run_id)
    deliverables = swarm["deliverable_name"].unique().tolist()

    # transfer deliverable if it's present in the run deliverables
    if "gbd" in deliverables:
        compile_and_transfer_gbd.main(run_id)
    elif "ushd" in deliverables:
        compile_and_transfer_ushd.transfer_run_to_ushd(run_id)
    elif "correction_factors" in deliverables:
        raise NotImplementedError("correction_factors deliverable is not ready for CMS.")
    else:
        raise RuntimeError(
            "There aren't any deliverables to compile and transfer. Is this expected?"
        )


def read_swarm(run_id: int) -> pd.DataFrame:
    """Read in the swarm associated with a given run_id.

    Args:
        run_id (int): Clinical run_id found in DATABASE.

    Raises:
        RuntimeError: No swarm_id found.
        RuntimeError: Could not isolate a single swarm_id.

    Returns:
        pd.DataFrame: Swarm tasks read from disk.
    """

    read_path = (
        "FILEPATH"
    )

    read_path = [f for f in read_path.rglob("swarm_id_*.csv")]
    if len(read_path) == 0:
        raise RuntimeError(f"No swarm table associated with run_{run_id}")
    elif len(read_path) > 1:
        swarm_id = max([int(n.stem.split("_")[-1]) for n in read_path])
        read_path = [p for p in read_path if str(swarm_id) in p.stem]

    if len(read_path) != 1:
        raise RuntimeError("Could not identify a single swarm table.")

    swarm = pd.concat([pd.read_csv(f) for f in read_path], sort=False, ignore_index=True)

    return swarm
