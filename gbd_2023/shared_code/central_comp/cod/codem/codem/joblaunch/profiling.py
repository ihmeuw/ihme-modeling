import ast
import glob
import logging
import os
import warnings
from datetime import datetime, timedelta
from typing import List, Optional

import numpy as np
import pandas as pd

from gbd import conn_defs
from gbd import constants as gbd_constants
from gbd.enums import Cluster

from codem.reference import db_connect

logger = logging.getLogger(__name__)

JOBMON_CONN_DEF = "privileged-jobmon-3"
JOBMON_DB_FILE = "FILEPATH"
# Default parameters to use for new cluster, if we don't have any previous
# runs saved for this set of demographics.
DEFAULT_PARAMS = {
    "cores_requested": [50],
    "ram_gb": [100],
    "ram_gb_requested": [100],
    "runtime_min": [60 * 60 * 5],
}
START_DATE = datetime(2019, 7, 9)


def get_jobs(
    start_date: Optional[datetime.date] = None,
    end_date: Optional[datetime.date] = None,
    hybridizer: bool = False,
) -> pd.DataFrame:
    """
    Get the codem jobs with resource usage either by reading in an existing hdf or pulling
    from the jobmon DB.

    :param start_date: a datetime object
    :param end_date: a datetime object
    :param hybridizer: whether or not to pull hybrid jobs instead of codem jobs
    :return: pd.DataFrame of jobs
    """
    start_date = start_date or START_DATE
    end_date = end_date or datetime.now()

    todays_jobmon_db_file = JOBMON_DB_FILE.format(d=datetime.now().strftime(r"%Y-%m-%d"))
    if os.path.exists(todays_jobmon_db_file):
        logger.info(f"Reading today's jobmon DB file {todays_jobmon_db_file}.")
        jobs = pd.read_hdf(todays_jobmon_db_file)
    else:
        last_jobmon_db_file = max(
            glob.glob(JOBMON_DB_FILE.format(d="*")), key=os.path.getctime
        )
        logger.info(
            f"Reading last jobmon DB file {last_jobmon_db_file} and pulling in new jobs."
        )
        jobs = pd.read_hdf(last_jobmon_db_file)
        # date of the last file generated minus a week, starting point for pulling new jobs
        # with buffer for potential delay in jobmon propagating resource usage to DB
        min_date = pd.to_datetime(jobs.submission_time).max() - timedelta(days=7)
        new_jobs = db_connect.execute_select(
            """
                SELECT
                   tool.name as tool_name,
                   ti.distributor_id AS job_number,
                   t.name AS job_name,
                   ti.wallclock,
                   ti.maxrss,
                   ti.submitted_date AS submission_time,
                   tr.requested_resources,
                   t.command AS submit_cmd,
                   ta.value AS job_attributes
                FROM docker.tool
                JOIN docker.tool_version tv ON tool.id = tv.tool_id
                JOIN docker.workflow wf ON tv.id = wf.tool_version_id
                JOIN docker.task t ON wf.id = t.workflow_id
                LEFT JOIN (
                    SELECT task_id, value
                    FROM task_attribute
                    WHERE task_attribute_type_id = :task_attribute_type_id
                ) ta ON t.id = ta.task_id
                JOIN docker.task_instance ti ON t.id = ti.task_id
                JOIN docker.task_resources tr ON ti.task_resources_id = tr.id
                WHERE
                    tool.id IN :tool_id AND
                    t.status = 'D' AND
                    ti.status = 'D' AND
                    ti.submitted_date >= :min_date AND
                    ti.maxrss >= 0 AND
                    tr.requested_resources IS NOT NULL
            """,
            parameters={
                "tool_id": [2540176, 2370867],  # CODEm and Hybridizer tool IDs
                "task_attribute_type_id": 1,  # INPUTS_INFO, the job attribute json
                "min_date": datetime.strftime(min_date, "%Y-%m-%d"),
            },
            conn_def=JOBMON_CONN_DEF,
        )
        new_jobs = (
            new_jobs[~new_jobs.job_name.str.contains("codem_sleep_|codem_diagnostics_")]
            .assign(
                cluster_type=Cluster.SLURM.value,  # as of 2024/03/18 jobmon DB only has slurm jobs
                runtime_min=lambda df: df["wallclock"].astype(float) / 60.0,
                cores_requested=lambda df: [
                    ast.literal_eval(x)["cores"] for x in df.requested_resources
                ],
                ram_gb=lambda df: df["maxrss"].astype(float) / 1024.0**3,
                ram_gb_requested=lambda df: [
                    ast.literal_eval(x)["memory"] for x in df.requested_resources
                ],
            )
            .drop(columns=["requested_resources", "wallclock", "maxrss"])
        )
        new_jobs["model_version_id"] = new_jobs.job_name.apply(lambda x: int(x.split("_")[1]))
        new_jobs = new_jobs.astype(jobs.dtypes.to_dict())
        jobs = (
            pd.concat([jobs, new_jobs])
            .drop_duplicates(["cluster_type", "job_number", "model_version_id"])
            .fillna({"job_attributes": np.nan})
            .reset_index(drop=True)
        )
        logger.info("Saving new jobmon DB file to hdf.")
        jobs.to_hdf(todays_jobmon_db_file, key="data", format="fixed")
    if hybridizer:
        jobs = jobs.loc[jobs.tool_name == "CODEm Hybridizer"]
    else:
        jobs = jobs.loc[jobs.tool_name == "CODEm Modeling"]
    jobs["submission_time"] = pd.to_datetime(jobs["submission_time"])
    return jobs.loc[(jobs.submission_time >= start_date) & (jobs.submission_time < end_date)]


def get_model_versions(
    cause_id: int, age_start: int, age_end: int, model_version_type_id: int
) -> Optional[List[int]]:
    """
    Get model versions for a given set of demographic params.

    :param cause_id: (int)
    :param age_start: (int)
    :param age_end: (int)
    :param model_version_type_id: (int)
    :return: list of model versions
    """
    model_versions = db_connect.execute_select(
        """
        SELECT model_version_id
        FROM cod.model_version
        WHERE cause_id = :cause_id
        AND age_start = :age_start
        AND age_end = :age_end
        AND model_version_type_id = :model_version_type_id
        AND release_id >= :release_id
        AND status = 1
        """,
        parameters={
            "cause_id": cause_id,
            "age_start": age_start,
            "age_end": age_end,
            "model_version_type_id": model_version_type_id,
            "release_id": gbd_constants.release.GBD_2019,
        },
        conn_def=conn_defs.CODEM,
    )["model_version_id"].tolist()
    return model_versions


def get_parameters(
    cause_id,
    age_start,
    age_end,
    model_version_type_id,
    hybridizer=False,
    start_date=None,
    end_date=None,
    jobs=None,
):
    """
    Pull the run parameters for a given cause ID, age range, and model version type.
    Add a little bit of padding for runtime and memory.

    :param cause_id: (int)
    :param age_start: (int)
    :param age_end: (int)
    :param model_version_type_id: (int) global or data rich
    :param hybridizer: (bool) hybrid jobs
    :param start_date: (datetime) starting date to look for jobs
    :param end_date: (datetime) end date to look for jobs
    :param jobs: pd.DataFrame of jobs already read in -- will use this to not read in the hdf
                 more often than necessary
    :return: dictionary with run parameters for cores, mem, and runtime
    """
    if model_version_type_id == 3 and not hybridizer:
        raise ValueError(
            "Cannot pull non-hybridizer model version type IDs when asked for hybridizer jobs."
        )
    if hybridizer:
        logger.info(
            "Getting hybrid parameters for cause_id {c}, age_start {s}, age_end {e}".format(
                c=cause_id, s=age_start, e=age_end
            )
        )
        model_versions = get_model_versions(
            cause_id, age_start, age_end, model_version_type_id
        )
        if jobs is None:
            jobs = get_jobs(start_date=start_date, end_date=end_date, hybridizer=hybridizer)
        jobs = jobs.loc[jobs.model_version_id.isin(model_versions)]
        if jobs.empty:
            jobs = pd.DataFrame.from_dict(DEFAULT_PARAMS, orient="columns")
            warnings.warn(
                f"Jobmon database does not contain any run information for these "
                f"model versions {', '.join([str(x) for x in model_versions])}.",
                RuntimeWarning,
            )
        jobs.loc[jobs.ram_gb < 0, "ram_gb"] = jobs.loc[jobs.ram_gb < 0]["ram_gb_requested"]
        parameters = (
            jobs[["cores_requested", "ram_gb", "ram_gb_requested", "runtime_min"]]
            .mean()
            .to_dict()
        )
    else:
        logger.info(
            "Setting non-hybrid parameters for cause_id {c}, age_start {s}, "
            "age_end {e}".format(c=cause_id, s=age_start, e=age_end)
        )
        parameters = {
            "cores_requested": 3,
            "ram_gb": 1,
            "ram_gb_requested": 1,
            "runtime_min": int(60 * 24 * 12),
        }
    logger.info(f"parameters: {parameters}")
    return parameters
