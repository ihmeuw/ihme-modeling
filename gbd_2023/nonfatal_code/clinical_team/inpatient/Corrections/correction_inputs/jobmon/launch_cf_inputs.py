import glob
from pathlib import Path
from typing import List

import pandas as pd
from crosscutting_functions.ci_jobmon import ezjobmon

from inpatient.CorrectionsFactors.correction_inputs.sources import (
    nzl_cf_constants,
    phl_cf_constants,
)


def _create_run_year_df(run_id: int, years: List[int]) -> pd.DataFrame:
    run_ids = [run_id] * len(years)
    run_year_df = pd.DataFrame(list(zip(run_ids, years)), columns=["run_id", "year"])
    return run_year_df


def _create_nzl_tasks(run_id: int) -> pd.DataFrame:
    df = _create_run_year_df(run_id=run_id, years=nzl_cf_constants.YEARS)
    df["source"] = "NZL_NMDS"
    return df


def _create_phl_tasks(run_id: int) -> pd.DataFrame:
    df = _create_run_year_df(run_id=run_id, years=phl_cf_constants.YEARS)
    df["source"] = "PHL_HICC"
    return df


def _create_hcup_tasks(run_id: int) -> pd.DataFrame:
    filepaths = glob.glob(
        "FILEPATH/*.parquet"
    )
    run_ids = [run_id] * len(filepaths)

    df = pd.DataFrame(list(zip(run_ids, filepaths)), columns=["run_id", "filepath"])
    df["source"] = "USA_HCUP_SID"
    return df


def create_task_df(run_id: int, sources=List[str]):
    """Populate an ezjobmon task dataframe based on input run_id and sources."""
    task_df_list = []

    if "PHL_HICC" in sources:
        task_df_list.append(_create_phl_tasks(run_id=run_id))
    if "NZL_NMDS" in sources:
        task_df_list.append(_create_nzl_tasks(run_id=run_id))
    if "USA_HCUP_SID" in sources:
        task_df_list.append(_create_hcup_tasks(run_id=run_id))

    task_df = pd.concat(task_df_list, sort=False, ignore_index=True)
    task_df["year"] = task_df["year"].fillna(-1).astype(int)

    return task_df


def launch_workflow(
    run_id: int, sources: List[str], queue: str = "all.q", ncores: int = 2, ram: int = 32
) -> None:
    """Run the entire cf input workflow for select sources.

    Args:
        run_id: clinical main pipeline run_id.
        queue: Slurm partition to run on. Defaults to "all.q".
        ncores: Number of cores to give each task. Defaults to 2.
        ram: Amount of memory in GB to give each task. Defaults to 32.

    Raises:
        RuntimeError: Year worflow did not complete successfully.
    """
    task_df = create_task_df(run_id=run_id, sources=sources)

    script_path = str(Path(__file__).with_name("worker_distributor.py"))
    wf_status = ezjobmon.submit_batch(
        inputs=task_df,
        name="cf_inputs",
        script_path=script_path,
        queue=queue,
        ncores=ncores,
        ram=ram,
    )

    if wf_status != "D":
        raise RuntimeError(f"Formatting workflow completed with status {wf_status}")
    else:
        print("Workflow finished successfully!")
