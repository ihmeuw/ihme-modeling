import logging
from functools import lru_cache
from typing import Tuple

import pandas as pd

from cascade_ode.lib.cluster import job_stats, slurm

LOGGER = logging.getLogger(__name__)


def cluster_limits(
    jobtype, mvm, child_count=None, min_mins=60, min_gb=10, safety_factor=1.3, details=None
):
    """
    Return number of threads, GB of memory, and runtime in minutes to allocate
    on the fair cluster for a specific job.

    We fall back to our original method for all jobtypes except
    node/varnish/global. See `naive_cluster_limits` for the original method.

    For node/varnish/global, RAM and runtime allocation are calculated via the
    following method:

        Find the max RAM/runtime ever for the precise job (e.g., ME=1811 year=90
        sex=m location=102). If we don't have runtime stats for that particular
        job/ME combination, we use 90 percentile of ram/runtimes for that
        ME/location combination (assuming Node jobtype). If we don't have that
        stat, we use 90 percentile of jobs for that jobtype/ME. If we don't
        have that, we use 90 percentile of ram/runtimes for that jobtype across
        all MEs (if node jobtype, use 90 percentile for given location across
        all MEs).

    After we calculate the allocation based on historical data, we pad it by
    the provided safety factor.

    fthreads are calculated based on jobtype, see `naive_cluster_limits`

    Arguments:
        jobtype str: one of node/varnish/global/driver
        mvm [pd.DataFrame]: model version metadata dataframe with
            modelable_entity_id and measure_only columns
        child_count Opt[int]: number of child locations if Node jobtype
        min_mins [int]: lower bound of runtime in minutes. Function will
            never return below this value even if historical data is lower.
        min_gb [int]: lower bound of RAM in gb.
        safety_factor [float]: Historical runtime/ram is multiplied by this
            value to provide padding
        details Opt[Dict[str,int]]: For Node jobtype, we need to know the
        location/sex/year in order to match on historical data. If Node
        jobtype, details is required to be a dict with keys 'location_id',
        'year', 'sex'

    Returns:
        Tuple[int, int, int]: fthreads, GB of RAM, runtime in minutes
    """
    slots, mem = naive_cluster_limits(jobtype, mvm, child_count)
    naive_runtime = slurm.get_sec(slurm.max_run_time_on_partition("long.q"))

    if jobtype in ["node", "varnish", "global"]:
        runtime, mem = smart_cluster_limits(jobtype, mvm.modelable_entity_id.iat[0], details)
    else:
        runtime = naive_runtime

    runtime = max(min_mins, runtime)
    runtime = round(min(runtime * safety_factor, naive_runtime), 2)
    mem = max(min_gb, mem)
    mem = round(mem * safety_factor, 2)

    return slots, mem, runtime * 60


def smart_cluster_limits(
    jobtype, modelable_entity_id, details, stats_func=job_stats.get_production_stats
):
    """
    Try to consult known history of modelable entity job stats
    to get a good estimate of runtime and max ram usage

    Arguments:
        jobtype str: one of node/varnish/global/driver
        modelable_entity_id int: ME we're running
        details Opt[Dict[str,int]]: For Node jobtype, we need to know the
        location/sex/year in order to match on historical data. If Node
        jobtype, details is required to be a dict with keys 'location_id',
        'year', 'sex'

    Returns:
        Tuple[int, int]: GB of RAM, runtime in minutes
    """
    node_stats, vg_stats = stats_func()

    if jobtype == "node":
        runtime, mem = node_limits(modelable_entity_id, details, node_stats)
    elif jobtype in ["varnish", "global"]:
        runtime, mem = varn_global_limits(jobtype, modelable_entity_id, vg_stats)
    else:
        raise RuntimeError(f"don't know how to calculate {jobtype} stats")
    return runtime, mem


def node_limits(modelable_entity_id, details, all_stats_df):
    """
    Determine runtime/ram allocation for Node jobtype
    """
    assert "location_id" in details and "sex" in details and "year" in details
    location_id = details["location_id"]
    sex = details["sex"]
    year = details["year"]

    stats_df = pd.DataFrame()
    # compute stats based on precise node/job
    try:
        stats_df = all_stats_df.loc[
            pd.IndexSlice[[modelable_entity_id], location_id, sex, year], :
        ]
    except KeyError:
        pass
    # if no match, look for same ME/location stats
    if stats_df.empty:
        try:
            stats_df = all_stats_df.loc[
                pd.IndexSlice[[modelable_entity_id], location_id, :, :], :
            ]
        except KeyError:
            pass
    # if still no match, use same ME
    if stats_df.empty:
        try:
            stats_df = all_stats_df.loc[pd.IndexSlice[[modelable_entity_id], :, :, :], :]
        except KeyError:
            pass
    # if no ME, use all data for given location
    if stats_df.empty:
        try:
            stats_df = all_stats_df.loc[pd.IndexSlice[:, location_id, :, :], :]
        except KeyError:
            pass
    # this should only happen if we pass in a location with no runtime stats at
    # all (so we're on a new round and new ME, for example). This isn't ideal,
    # but we should at least return some value so use 90% of everything
    if stats_df.empty:
        stats_df = all_stats_df.copy()

    # if we didn't have a precise match, use 90th percentile of approximate
    # matches
    if len(stats_df) > 1:
        stats_df = stats_df.quantile(0.9)

    return float(stats_df["runtime_min"]), float(stats_df["ram_gb"])


def varn_global_limits(jobtype, modelable_entity_id, all_stats_df):
    """
    Determine runtime/ram allocation for varnish and global jobtypes
    """
    all_stats_df = all_stats_df[all_stats_df[jobtype]]

    stats_df = all_stats_df.query("modelable_entity_id == @modelable_entity_id")

    if stats_df.empty:
        stats_df = all_stats_df

    if len(stats_df) > 1:
        quantile = 0.9 if jobtype == "varnish" else 0.95
        stats_df = stats_df[["runtime_min", "ram_gb"]].quantile(quantile)

    runtime = float(stats_df["runtime_min"])

    return runtime, float(stats_df["ram_gb"])


def naive_cluster_limits(jobtype, mvm=None, child_count=None):
    """
    Returns memory and core count of different types of dismod jobs. Inputs
    into the calculation are type of job, model version metadata
    (for determining if it's a single-parameter model), and optionally
    child_count which specifies number of child locations for the 'node'
    jobtype.

    Allowable jobtypes:
        driver: jobmon driver that submits all jobs and oversees job graph
        global: initial global dismod job
        node: any of the sex/year/location specific jobs that run dismod for
            the children of the specified location
        varnish: final job that uploads all results/calls save-results

    Arguments:
        jobtype (str): One of 'driver', 'global', 'node', 'varnish'
        mvm (Opt[pd.DataFrame]): model version metadata object. Not necessary
            to specify for driver job
        child_count (Opt[int]): optional count of number of child nodes for
            the given location (only applicable for node jobtype)

    Returns:
        (slots, memory) tuple[int, int]: core count and memory usage (in gb)
    """
    assert jobtype in ["varnish", "node", "global", "driver"]

    if mvm is not None:
        assert jobtype != "driver"
        measure_only = mvm.measure_only.unique()[0]
        is_single_param = False if measure_only is None else True
    else:
        assert jobtype == "driver"
        # is_single_param isn't relevant for jobmon driver
        is_single_param = None

    single_param_allocs = {
        "varnish": (15, 30),
        "node": (10, 20),
        "global": (2, 20),
        "driver": (3, 1),
    }

    multi_param_allocs = {
        "varnish": (15, 175),
        "node": (10, 45),
        "global": (2, 70),
        "driver": (3, 1),
    }

    if is_single_param:
        return single_param_allocs[jobtype]
    else:
        return multi_param_allocs[jobtype]


def update_ME_specific_allocations(modelable_entity_id, memory, runtime):
    """
    For some MEs, we want to pad memory and runtime allocations even
    more than the default
    """
    mem_factor, rt_factor = get_factor_for_ME(modelable_entity_id)
    return memory * mem_factor, runtime * rt_factor


@lru_cache(maxsize=None)
def get_factor_for_ME(modelable_entity_id) -> Tuple[float, float]:
    df = pd.read_csv(job_stats.ME_FACTORS_FILE)
    df = df[df.modelable_entity_id == modelable_entity_id]
    if df.empty:
        return (1.0, 1.0)
    return df["mem_factor"].iat[0], df["rt_factor"].iat[0]
