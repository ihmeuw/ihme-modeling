"""Module for storing filepaths and CLI for producing records of max runtime/gb per job/ME.

We run this periodically to keep stats up to date via a Jenkins job here:

For jobs missing from here, we default to the 95th percentile of all global jobs.
"""

import logging
import shutil
from datetime import datetime as dt
from functools import lru_cache
from pathlib import Path
from typing import Final, Tuple

import click
import pandas as pd

import db_tools_core

from cascade_ode.legacy import db
from cascade_ode.legacy.constants import ConnectionDefinitions
from cascade_ode.lib.cluster import slurm

MIN_MEMORY_GB: Final[str] = "5"
MIN_TIME_MIN: Final[str] = "10"

ROOT_PATH: Final[Path] = 
NODE_FILE: Final[Path] = 
VARN_GLOBAL_FILE: Final[Path] = 
ME_FACTORS_FILE: Final[Path] = 
GEN_DATA_FILE: Final[Path] = 
RAW_H5: Final[Path] = 

logger = logging.getLogger(__file__)


def gen_data() -> pd.DataFrame:
    """Generate dismod resource prediction data."""
    logger.info("Getting jobmon frames now.")

    df = _get_jobmon_frame()
    df["varnish"] = df.job_name.str.contains("varnish")
    df["driver"] = df.job_name.str.contains("driver")
    df["G0"] = df.job_name.str.contains("_G\d")  # noqa W605
    df["node"] = ~(df.varnish | df.driver | df.G0)
    df.to_csv(str(GEN_DATA_FILE), index=False)


def _get_jobmon_frame() -> pd.DataFrame:
    """Retrieve runtime data from jobmon db."""
    qry = f"""
        SELECT
            t.name as `job_name`,
            GREATEST(ti.maxpss / 1024 / 1024 / 1024, {MIN_MEMORY_GB}) AS `ram_gb_old`,
            GREATEST(ti.maxrss / 1024 / 1024 / 1024, {MIN_MEMORY_GB}) AS `ram_gb`,
            GREATEST((ti.wallclock) / 60, {MIN_TIME_MIN}) AS `runtime_min`
        FROM task_instance ti
        JOIN task t ON t.id = ti.task_id
        JOIN workflow_run wr ON wr.id = ti.workflow_run_id
        JOIN workflow w ON w.id = wr.workflow_id

        WHERE w.workflow_args like "dismod_%%_%%"
        AND ti.status in ("D", "Z");
    """  # nosec B608
    # NOTE: maxrss legacy data represented as negative, may still exist in DB,
    # but moot with current logic.
    with db_tools_core.session_scope(ConnectionDefinitions.JOBMON) as session:
        df = pd.read_sql(qry, session.connection())

    # Set null maxrss to maxpss.
    df["ram_gb"] = df["ram_gb"].fillna(df["ram_gb_old"])
    df = df.drop("ram_gb_old", axis=1)

    # Drop NAN values, shouldn't be more than about 1%
    na_mask = df.isna().any(1)
    drop_count = len(df[na_mask])
    logger.info(
        f"Dropping {drop_count} rows with NA values, which is "
        f"{round((drop_count / len(df)) * 100, 3)} percent of the rows."
    )
    df = df[~na_mask]

    return df


def read_data() -> pd.DataFrame:
    """Read CSV extracted from Jobmon DB."""
    return pd.read_csv(str(GEN_DATA_FILE))


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Split MVID and demos into columns. Subset to desired jobnames."""
    df = df[df.job_name.str.startswith("dm_")]
    df = df[~df.job_name.str.contains("drew")]
    df = df[~df.job_name.str.contains("redo")]
    df = df[~df.job_name.str.contains("P")]
    splits = df.job_name.str.split("_", expand=True)
    df["model_version_id"] = splits.get(1).astype(int)
    df.loc[df.node, "location_id"] = splits.loc[df.node].get(2).astype(int)
    df.loc[df.node, "sex"] = splits.loc[df.node].get(3)
    df.loc[df.node, "year"] = splits.loc[df.node].get(4)
    df.loc[df.node, "crossval"] = splits.loc[df.node].get(5)
    return df


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Inner join ME metadata from epi DB."""
    mes = db.execute_select(
        """
                select model_version_id, mv.modelable_entity_id,
                modelable_entity_name
                from epi.model_version mv join
                epi.modelable_entity using (modelable_entity_id)
                where
                model_version_id in :mvs""",
        params={"mvs": df.model_version_id.unique().tolist()},
        conn_def="epi",
    )
    # ok to drop rows -- some models in test db ran on prod cluster
    df = df.merge(mes, how="inner")
    return df


def compute_stats_node(df: pd.DataFrame) -> pd.DataFrame:
    """Get max runtime and RAM for each ME for each job."""
    df = df[df.node]
    grp = df.groupby(["modelable_entity_id", "location_id", "sex", "year"]).agg(
        {"runtime_min": "max", "ram_gb": "max"}
    )
    return grp


def compute_stats_varnish_global(df: pd.DataFrame) -> pd.DataFrame:
    """Get max runtime and RAM for each ME for each job."""
    grp_varn = (
        df[df.varnish]
        .groupby(["modelable_entity_id"])
        .agg({"runtime_min": "max", "ram_gb": "max"})
        .assign(varnish=True)
    )
    grp_global = (
        df[df.G0]
        .groupby(["modelable_entity_id"])
        .agg({"runtime_min": "max", "ram_gb": "max"})
        .assign(G0=True)
    )
    grp_global = grp_global.rename({"G0": "global"}, axis=1)

    return pd.concat([grp_varn, grp_global]).fillna(False)


def prune_job_stats(
    vg_df: pd.DataFrame, node_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lets make sure we stay within satisfiable bounds. A job's runtime cannot
    exceed long.q max runtime (ie this was a job from old cluster)
    """
    runtime_limit = slurm.get_sec(slurm.max_run_time_on_partition("long.q")) / 60
    vg_df.loc[vg_df.runtime_min > runtime_limit, "runtime_min"] = runtime_limit
    vg_df = vg_df.query("ram_gb > 0 and runtime_min > 0")
    node_df = node_df.query("ram_gb > 0 and runtime_min > 0")
    return vg_df, node_df


def save_for_production(
    df_w_meta: pd.DataFrame, node_stats: pd.DataFrame, vg_stats: pd.DataFrame
) -> None:
    """Backup old data and save new."""
    file_type_dataframe_map = {
        RAW_H5: df_w_meta,
        NODE_FILE: node_stats,
        VARN_GLOBAL_FILE: vg_stats,
    }
    for fp, df in file_type_dataframe_map.items():
        backup_job_stats_output(fp)
        df.to_hdf(str(fp), format="fixed", key="data", mode="w")


def backup_job_stats_output(filepath: Path) -> None:
    """Save old files to backup dir."""
    now = dt.now()
    backup_filepath = 
    if not backup_filepath.parent.exists():
        raise RuntimeError(f"{backup_filepath.parent} does not exist.")
    logger.info(f"Copying {str(filepath)} to {str(backup_filepath)}")
    shutil.copy(str(filepath), str(backup_filepath))


@lru_cache(maxsize=None)
def get_production_stats() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    We store a historical record of successful model runtimes in
    We use these to estimate how much
    RAM/runtime to allocate for new models.

    Returns:
        Stats for node jobs and stats for varnish and global jobs.
    """
    node_stats = pd.read_hdf(NODE_FILE)
    varn_global_stats = pd.read_hdf(VARN_GLOBAL_FILE)
    return node_stats, varn_global_stats


def main() -> None:
    """Read, format, aggregate, and save raw data on disk."""
    df = format_df(read_data())
    df = add_metadata(df)
    node_stats = compute_stats_node(df)
    vg_stats = compute_stats_varnish_global(df)
    vg_stats, node_stats = prune_job_stats(vg_df=vg_stats, node_df=node_stats)
    save_for_production(df, node_stats, vg_stats)


@click.command()
def cli() -> None:
    """Update job resource info at ."""
    gen_data()
    main()
