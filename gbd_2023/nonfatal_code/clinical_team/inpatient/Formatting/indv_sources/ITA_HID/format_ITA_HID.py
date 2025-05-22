import os
from pathlib import Path

import pandas as pd
from crosscutting_functions.ci_jobmon import ezjobmon

from inpatient.Formatting.indv_sources.ITA_HID import constants_ITA_HID


def make_year_path_df(clinical_age_group_set_id: int = 2) -> pd.DataFrame:
    """Makes the ezjobmon task dataframe. All years.

    Args:
        clinical_age_group_set_id (int, optional): internal clinical age group 
        sets to identify which age groups to pull from shared/central tools.
        Defaults to 2.

    Returns:
        pd.Dataframe: Task df with year_id and its output filepath.
    """
    years = []
    clinical_age_group_set = []

    for year in constants_ITA_HID.NID_DICT.keys():
        years.append(year)
        clinical_age_group_set.append(clinical_age_group_set_id)

    year_path = pd.DataFrame(
        list(zip(years, clinical_age_group_set)),
        columns=["year_id", "clinical_age_group_set_id"],
    )

    return year_path


def launch_workflow(
    queue: str = "all.q",
    ncores: int = 2,
    ram: int = 75,
) -> None:
    """Run the entire workflow for ITA_HID. Concats all loc-year parquets
      together once workflow finishes successfully.

    Args:
        queue (str, optional): Slurm partition to run on. Defaults to "all.q".
        ncores (int, optional): Number of cores to give each task. Defaults to 2.
        ram (int, optional): Amount of memory in GB to give each task. Defaults to 100.

    Raises:
        RuntimeError: Workflow did not complete successfully.
    """
    script_path = Path(__file__).with_name("ita_worker.py")
    task_df = make_year_path_df()
    wf = ezjobmon.WorkflowBuilder(workflow_name="ita_formatting")

    wf.add_job(
        inputs=task_df,
        job_name="year_formatting",
        script_path=script_path,
        queue=queue,
        ncores=ncores,
        ram=ram,
    )

    wf.add_job(
        inputs=[""],
        job_name="ita_concat",
        script_path=Path(__file__).with_name("concat_worker.py"),
        queue=queue,
        ncores=2,
        ram=100,
        upstream=True,
    )

    wf_status = wf.run(fail_fast=True)

    if wf_status != "D":
        raise RuntimeError(f"Formatting workflow completed with status {wf_status}")

    all_files = (constants_ITA_HID.OUTPATH_DIR / "outpath_by_year").glob("*.parquet")
    for path in all_files:
        os.path.remove(path)


if __name__ == "__main__":

    launch_workflow()
