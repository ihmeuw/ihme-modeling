import argparse
import glob
import os
from getpass import getuser
from typing import List, Optional, Tuple

import pandas as pd
from crosscutting_functions.legacy_pipeline import drop_data
from db_tools.ezfuncs import query

GROUPBY_COLS = ["source", "year_start"]


def get_inpatient_runs() -> List[int]:
    """Pull a list of all inpatient runs stored in run_metadata."""
    run_metadata = query(
        QUERY
    )
    runs = run_metadata["run_id"].sort_values().unique().tolist()
    return runs


def create_splitting_report(runs: List[int], last_n_runs: Optional[int]) -> pd.DataFrame:
    """Main interface for creating the report table on the effects of
    age-sex splitting data.

    Args:
        runs: A set of run_ids to generate the report with.
        last_n_runs: An optional way to filter the `runs` argument. If you're passing an
        ad-hoc list of runs this isn't very useful, but if you're dynamically pulling runs
        via get_inpatient_runs then this can automatically filter the results to the last N
        most recent run_ids.

    Returns:
        A table summarizing admissions to split and total admissions lost by the splitting
        process. Negative values in the 'lost' column means admissions were added.
    """
    if last_n_runs:
        runs = runs[-last_n_runs:]
    all_summary = pd.DataFrame(columns=GROUPBY_COLS)
    for run_id in runs:
        pre_split, post_split = read_data(run_id)
        run_summary = review_split_data(pre_split=pre_split, post_split=post_split)
        if run_summary.empty:  # skip runs which have no data
            continue
        all_summary = merge_summary_data(all_summary=all_summary, run_summary=run_summary)

    all_summary = all_summary.sort_values(GROUPBY_COLS)
    return all_summary


def merge_summary_data(all_summary: pd.DataFrame, run_summary: pd.DataFrame) -> pd.DataFrame:
    """Merge the summary for a single run onto the all summary table.

    Args:
        all_summary: Counts of admissions which will be split with a column for each run.
        run_summary: Counts of admissions which will be split for a single run.

    Returns:
        Summary data for a specific run merged onto a table containing all
        prior runs.
    """
    all_summary = all_summary.merge(run_summary, how="outer", on=GROUPBY_COLS, validate="1:1")
    return all_summary


def review_split_data(pre_split: pd.DataFrame, post_split: pd.DataFrame) -> pd.DataFrame:
    """Identify admissions which were split and summarize them.

    Args:
        pre_split: Inpatient intermediate data before age-sex splitting.
        post_split: Inpatient intermediate data after age-sex splitting.

    Returns:
        pd.DataFrame: A small summary DF grouped and aggregated.
    """
    if pre_split.empty:
        return pd.DataFrame()

    to_split_df = pre_split.query(
        f"age_group_id not in {post_split['age_group_id'].unique().tolist()} or sex_id == 3"
    )

    agg_df = (
        to_split_df.groupby(GROUPBY_COLS)["val"].sum().reset_index().sort_values(GROUPBY_COLS)
    )
    agg_df["val"] = agg_df["val"].round(1)
    run_id = pre_split["run_id"].iloc[0]
    agg_df = agg_df.rename(columns={"val": f"run_{run_id}_admits_to_split"})

    pre_group = (
        pre_split.groupby(GROUPBY_COLS)["val"]
        .sum()
        .reset_index()
        .rename(columns={"val": "pre_val"})
    )
    post_group = (
        post_split.groupby(GROUPBY_COLS)["val"]
        .sum()
        .reset_index()
        .rename(columns={"val": "post_val"})
    )
    admits_lost = pre_group.merge(post_group, how="outer", on=GROUPBY_COLS, validate="1:1")
    admits_lost[f"run_{run_id}_admits_lost"] = admits_lost["pre_val"] - admits_lost["post_val"]

    agg_df = agg_df.merge(admits_lost, how="outer", validate="1:1").drop(
        ["pre_val", "post_val"], axis=1
    )

    return agg_df


def read_data(run_id: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read and lightly prep intermediate inpatient data.

    Args:
        run_id: Clinical run_id of interest to pull data from.

    Returns:
        Two dataframes, one containing pre split data and and another containing the results
        of the age-sex splittig process.
    """
    base = FILEPATH
    icd_mapping_file = glob.glob(FILEPATH)
    if len(icd_mapping_file) > 1:
        raise RuntimeError("Too many ICD mapping hdf files to read.")
    try:
        pre_split = pd.read_hdf(icd_mapping_file[0]).assign(run_id=run_id)
        pre_split = drop_data(pre_split)
        pre_split["source"] = pre_split["source"].astype(str)

        post_split = pd.read_hdf(FILEPATH).assign(
            run_id=run_id
        )
    except:  # noqa
        pre_split = pd.DataFrame()
        post_split = pd.DataFrame()

    return pre_split, post_split


def send_report(run_id: int, squeue: str) -> None:
    """Send out a job to create the report detailed in this module.

    Args:
        run_id: Standard clinical run_id.
        squeue: The slurm queue to dispatch this job to. Currently valid options are
        "all.q" or "long.q"
    """
    log_path = (FILEPATH
    )
    repo = FILEPATH

    sbatch = (ADDRESS
    )

    os.popen(sbatch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", type=int, required=True)
    args = parser.parse_args()

    df = create_splitting_report(get_inpatient_runs())
    df.to_csv(FILEPATH
    )
