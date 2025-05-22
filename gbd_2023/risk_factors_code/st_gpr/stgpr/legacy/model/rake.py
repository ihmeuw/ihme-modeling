#######################################################
# Run rake in python
#
# Date: March 18, 2018
#
#######################################################

import math
import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
import xarray as xr

import stgpr_helpers
import stgpr_schema
from stgpr_helpers import columns

from stgpr.legacy.st_gpr import helpers as hlp
from stgpr.lib import constants, utils

warnings.filterwarnings("ignore")

## Helper functions ##
##################################################################################################


def antijoin(a, b):
    return list(set(a).symmetric_difference(set(b)))


def intersection(a, b):
    return list(set(a) & set(b))


def invlogit(x):
    return np.exp(x) / (np.exp(x) + 1)


def logit(x):
    return np.log(x / (1 - x))


def drop_na(mylist):
    if "nan" in mylist:
        mylist = [x for x in mylist if str(x) != "nan"]
        mylist = [int(x) for x in mylist]
        if not mylist:
            mylist = [np.nan]
    else:
        mylist = [int(x) for x in mylist]
    return mylist


def get_levels(demographics, loc):
    all_lvls = [4, 5, 6]
    loc_lvls = demographics.level.unique()
    return intersection(all_lvls, loc_lvls)


def prep_demographics(locations, populations, loc):

    # keep countries and subnats
    locations = locations.query("level >=3")
    locations.drop(["region_id", "super_region_id"], axis=1, inplace=True)

    # merge on populations
    demographics = pd.merge(locations, populations, on="location_id", how="left")

    # keep only info on locations we're raking/aggregating
    demographics = demographics.query("level_3== {loc}".format(loc=loc))

    return demographics


## Logit raking functions ##
##################################################################################################

# Cutoffs used to determine whether to drop data very close to 0.0 or 1.0 in logit space.
# For logit raking to function we need to drop data that is very close to 0 or 1 and then
# add that data back on after raking. These cutoffs correspond to precision near 1e-12, so
# models that are raking in logit space will not have data dropped unless data is greater or
# less than 25 and -25 respectively, in logit space
_LOGIT_UPPER_BOUND = 25
_LOGIT_LOWER_BOUND = -25


def get_tolerance(demographics, level, round_to=5):
    max_pop = np.round(
        max(demographics.loc[demographics.level == (level - 1)].population), round_to
    )
    n = len(str(max_pop)) - 1  # subtract 1 for the decimal
    tol = 10 ** (-n)
    return tol


def eval_difference(df, p_N, vals, N_i):
    """Evaluate the difference between parent estimates and child guesses and in normal space.

    To compare child estimates vs. parent estimates, we take the population-weighted
    sum of the child estimates.

    Note:
        The estimate difference for all child locations for a given year/age/sex/draw
        will be the same as we're comparing the sum of the children.

        Positive differences indicate that the parent estimates are greater than the sum
        of child estimates while negative differences imply that parent estimates are
        less than the sum of child estimates.

    Args:
        df: logit raking dataframe
        p_N: column of parent estimates to rake p_i to, e.g. "est_parent"
        vals: estimate guesses column in logit space, e.g.  "guess_a"
        N_i: population column, e.g. "population"
    """
    df["new_est"] = (df[vals] * df[N_i]).sum(dim="location_id") / df[N_i].sum(
        dim="location_id"
    )
    out = invlogit(df[p_N]) - df["new_est"]
    return out


def new_p_i(k, vals):
    """Return new guess at raked estimates in normal space.

    Args:
        k: estimate "jump", to be added to current estimates in logit space
        vals: current estimates in logit space
    """
    return invlogit(vals + k)


def logit_rake(
    dataset,
    parent_id,
    p_i,
    p_N,
    N_i,
    rake_locations,
    non_rake_locations,
    tolerance,
    max_jump=12,
):
    """Find rake factor in logit space using the bisection method

    Step 1: Define number of iterations to reach specified tolerance
    Step 2: Take two guesses at rake factor, starting with +/- max_jump
    Step 3: See how far off your guesses are, and
    - if difference is lower than requested tolerance, return k (rake factor)
    - else, repeat with narrower window of difference between guesses to
    further approximate k

    Args:
        dataset: data to be raked, after trimming out data too close to 0/1
        parent_id: location id of the parent
        p_i: column of estimates to rake, e.g. "est"
        p_N: column of parent estimates to rake p_i to, e.g. "est_parent"
        N_i: population column, e.g. "population"
        rake_locations: locations to rake
        non_rake_locations: locations NOT to rake, i.e. locations that aren't child locations
            of the parent
        tolerance: raking tolerance, expected to be inversely proportional with population
        max_jump: max jump in range of guesses, in logit space. Defaults to 12, meaning first
            guesses are estimates +/- 12
    """
    # In the case that all data was screened out for being too close to 0/1,
    # simply return the empty dataset
    if dataset.to_dataframe().empty:
        return dataset

    # first, separate into to_rake and no_rake sections based on level
    to_rake = dataset.sel(location_id=rake_locations)
    no_rake = dataset.sel(location_id=non_rake_locations)

    # step 1
    num_iter = math.ceil(-np.log2(tolerance / max_jump))
    fun_range = np.arange(-max_jump, max_jump + 1)

    # repeat out a and b values by year, age, sex, draw
    # this was way more annoying than it needed to be
    a = float(fun_range[0])
    b = float(fun_range[-1])
    xa = xr.full_like(to_rake[p_N], fill_value=a).rename("a")
    xb = xr.full_like(to_rake[p_N], fill_value=b).rename("b")

    xvals = xr.merge([xa, xb])
    to_rake = xr.merge([to_rake, xvals])

    # step 2
    to_rake["guess_a"] = new_p_i(k=to_rake["a"], vals=to_rake[p_i])
    to_rake["guess_b"] = new_p_i(k=to_rake["b"], vals=to_rake[p_i])

    # step 3
    to_rake["F_a"] = eval_difference(df=to_rake, p_N=p_N, vals="guess_a", N_i=N_i)
    to_rake["F_b"] = eval_difference(df=to_rake, p_N=p_N, vals="guess_b", N_i=N_i)

    # parent estimate must be in between the two guesses to iteratively bring the two closer
    # to the parent. Therefore, we expect the guesses to have opposite signs
    to_rake["eval_product"] = to_rake["F_a"] * to_rake["F_b"]
    if to_rake["eval_product"].values.flatten().max() > 0:
        # In case of data being too far apart, clean up and present nicely
        drop_cols = [
            "a",
            "b",
            "guess_a",
            "guess_b",
            "new_est",
            "F_a",
            "F_b",
            "eval_product",
            "population",
        ]
        error_rows = (
            to_rake.to_dataframe()
            .reset_index()
            .query("eval_product > 0")
            .drop(columns=drop_cols)
        )
        # Convert GPR estimates to normal space
        error_rows[p_i] = invlogit(error_rows[p_i])
        error_rows[p_N] = invlogit(error_rows[p_N])

        num_years = len(error_rows["year_id"].unique())
        num_ages = len(error_rows["age_group_id"].unique())
        num_sexes = len(error_rows["sex_id"].unique())
        raise RuntimeError(
            "Your estimates are too far away from what you are raking them to "
            f"for {num_years} year(s), {num_ages} age group(s), and {num_sexes} sex(es) "
            f"for parent location id {parent_id}. Example rows:\n{error_rows}"
        )
    else:
        i = 1
        to_rake["c"] = (to_rake["a"] + to_rake["b"]) / 2
        to_rake["guess_c"] = new_p_i(k=to_rake["c"], vals=to_rake[p_i])
        to_rake["F_c"] = eval_difference(df=to_rake, p_N=p_N, vals="guess_c", N_i=N_i)
        success = (abs(to_rake["F_c"]) <= tolerance).values.all()
        while (not (success)) & (i < num_iter):

            # replace a, F_a or b, F_b with guess c, F_c to get closer to true value
            to_rake["a"] = xr.where(
                (np.sign(to_rake["F_a"]) == np.sign(to_rake["F_c"])),
                to_rake["c"],
                to_rake["a"],
            )
            to_rake["F_a"] = xr.where(
                (np.sign(to_rake["F_a"]) == np.sign(to_rake["F_c"])),
                to_rake["F_c"],
                to_rake["F_a"],
            )

            to_rake["b"] = xr.where(
                (np.sign(to_rake["F_a"]) != np.sign(to_rake["F_c"])),
                to_rake["c"],
                to_rake["b"],
            )
            to_rake["F_b"] = xr.where(
                (np.sign(to_rake["F_a"]) != np.sign(to_rake["F_c"])),
                to_rake["F_c"],
                to_rake["F_b"],
            )

            to_rake["c"] = (to_rake["a"] + to_rake["b"]) / 2
            to_rake["guess_c"] = new_p_i(k=to_rake["c"], vals=to_rake[p_i])
            to_rake["F_c"] = eval_difference(df=to_rake, p_N=p_N, vals="guess_c", N_i=N_i)
            success = (abs(to_rake["F_c"]) <= tolerance).values.all()
            i = i + 1

        print("Found rake factor after {} iterations".format(i))
        # apply successfully-approximated rake factor
        to_rake["est"] = to_rake["c"] + to_rake["est"]

        # recombine to_rake and no_rake
        drops = [
            "a",
            "b",
            "c",
            "guess_a",
            "guess_b",
            "guess_c",
            "F_a",
            "F_b",
            "F_c",
            "eval_product",
            "new_est",
            "population",
        ]
        to_rake = to_rake.drop(drops)
        no_rake = no_rake.drop(["population"])
        out = xr.merge([to_rake, no_rake])
        return out


## Rake and aggregate functions ##
##################################################################################################


def get_rake_status(params, loc, lvl):
    """Returns a list of parent location_ids with subnationals that
    require raking for the given location level, as specified by the user"""

    # set level names
    param_name = "agg_level_{child}_to_{parent}".format(child=lvl, parent=(lvl - 1))
    if not params[param_name].iat[0]:
        agglocs = []
    else:
        agglocs = hlp.separate_string_to_list(str(params[param_name].iat[0]), int)

    if loc in agglocs:
        status = "aggregate"
    else:
        status = "rake"

    return status


def get_rake_locs(demographics, loc, lvl, parent=None):
    """Returns a list of parent location_ids with subnationals that require
    raking for the given location level, as specified by the user"""

    lev = "level_{level}".format(level=lvl)

    if parent is not None:
        rakelocs = demographics.loc[demographics.parent_id == parent][lev].unique().tolist()
    else:
        rakelocs = demographics[lev].unique().tolist()
        rakelocs = [int(x) for x in rakelocs if str(x) != "nan"]

    return rakelocs


def get_parent(demographics, child_locations):
    """Find parent_id of given locations"""
    return list(
        set(demographics.loc[demographics.location_id.isin(child_locations)]["parent_id"])
    )


def rake_estimates(df, rakelocs, non_rakelocs):

    to_rake = df.sel(location_id=rakelocs)
    no_rake = df.sel(location_id=non_rakelocs)

    # calculate rake factor
    to_rake["subnat_est"] = (to_rake["est"] * to_rake["population"]).sum(dim="location_id")
    to_rake["parent_est"] = to_rake["est_parent"] * (
        to_rake["population"].sum(dim="location_id")
    )
    to_rake["k"] = to_rake["subnat_est"] / to_rake["parent_est"]
    # apply rake factor
    to_rake["est"] = to_rake["est"] / to_rake["k"]
    # drop unneeded cols
    to_rake = to_rake.drop(["est_parent", "subnat_est", "parent_est", "k"])
    no_rake = no_rake.drop(["est_parent", "population"])

    # rebind the no_rake cols on
    out = xr.merge([to_rake, no_rake])

    return out


def prep_validation(df, demographics):
    # merge in population by location_id
    demographics_tmp = demographics.copy()
    demographics_tmp = demographics_tmp.set_index(ids)
    xdemographics = demographics_tmp[["population"]].to_xarray()

    xfull = xr.merge([df, xdemographics])
    return xfull


def validate_rake_aggregate(dataset, lvl_locs, parent, status):
    """Validates that population-weighted summed subnational estimates equal parent est*pop"""

    child = dataset.sel(location_id=lvl_locs)
    par = dataset.sel(location_id=parent)

    child = np.round(((child["est"] * child["population"]).sum(dim="location_id")))
    par = np.round(par["est"] * (par["population"]))
    # allow rounding error of 1 because otherwise this gets ugly
    success = (abs(child.values - par.values) < 2).all()

    # check for equality
    if not success:
        yay = 0
        sys.exit("Estimates aren't actually {status}d!".format(status=status))
    else:
        yay = 1

    return yay


def aggregate_estimates(dataset, aggregate_locations, parent):
    """Calculates parent estimate using population-weighted subnational estimates"""

    children = dataset.sel(location_id=aggregate_locations)
    new_parent = ((children["est"] * children["population"]).sum(dim="location_id")) / (
        children["population"].sum(dim="location_id")
    )
    new_parent = new_parent.rename("est").to_dataset()

    old_parent = dataset.sel(location_id=parent)
    aggregated = old_parent.update(new_parent)

    dataset = dataset.drop(labels=parent, dim="location_id")
    dataset = xr.concat([dataset, aggregated], dim="location_id")
    dataset = dataset.drop(["population"])

    return dataset


## Xarray functions ##
##################################################################################################


def prep_xarray(df, vars):

    dt = df.copy()
    dt = dt[ids + vars]

    dt.set_index(ids, inplace=True)

    dt = dt.rename_axis("var", axis=1).stack()
    dt.rename("est", inplace=True)
    xdt = xr.DataArray.from_series(dt).to_dataset()

    return xdt


def prep_aggregate(xarray_df, demographics, aggregate_locations):

    # populations
    demographics_tmp = demographics.loc[
        demographics.location_id.isin(aggregate_locations)
    ].copy()
    demographics_tmp = demographics_tmp.set_index(ids)
    xdemographics = demographics_tmp[["population"]].to_xarray()

    xfull = xr.merge([xdf, xdemographics])

    return xfull


def prep_rake(xarray_df, demographics, rakelocs, parent=None):
    if "est_parent" in xarray_df:
        # Drop est_parent since it will get merged on again in this function.
        # If it exists, it's because it was carried over from a previous iteration.
        xarray_df = xarray_df.drop("est_parent")

    rk = xarray_df.copy()

    # populations
    demographics_tmp = demographics.loc[demographics.location_id.isin(rakelocs)].copy()
    demographics_tmp = demographics_tmp.set_index(ids)
    xdemographics = demographics_tmp[["population"]].to_xarray()

    # determine parents of rakelocs and assign them a different column for parent_est
    if parent is None:
        parents = list(set(demographics_tmp.parent_id.values))
        par = rk.sel(location_id=parents)
    else:
        par = rk.sel(location_id=parent)

    # set index and stack to make long by draw
    par = par.drop("location_id")
    par = par["est"].rename("est_parent")
    xpar = par.to_dataset()

    xfull = xr.merge([xarray_df, xdemographics, xpar])

    return xfull


def set_to_0_1(raked: xr.Dataset, dropped: xr.Dataset) -> xr.Dataset:
    """Combines raked dataset with dataset that was dropped for being close to 0/1.

    Before raking, data was dropped where raking target (parent) estimates were below
    invLogit(-10) or above invLogit(10).
    Here we add that data back into the raked dataset and set those values to 0 or 1.

    Overall algorithm:
        1. Before raking, drop indices where the parent estimates are close to 0/1.
        2. Do logit raking.
        3. (This function) Combine raked dataset with dropped dataset.
        4. (This function) Set indices where parent estimate is close to 0/1 to 0/1.
        5. (This function) Set indices where estimate is close to 0/1 to 0/1. This has the
            effect of setting the parent estimates to 0/1.

    Args:
        raked: The raked dataset.
        dropped: The dropped dataset.

    Returns:
        Combined dataset, with extreme values set to 0/1
    """
    merged = xr.merge([raked, dropped])
    merged = merged.update({"est": merged.where(merged.est_parent <= _LOGIT_UPPER_BOUND, 1).est})
    merged = merged.update({"est": merged.where(merged.est_parent >= _LOGIT_LOWER_BOUND, 0).est})
    merged = merged.update({"est": merged.where(merged.est <= _LOGIT_UPPER_BOUND, 1).est})
    merged = merged.update({"est": merged.where(merged.est >= _LOGIT_LOWER_BOUND, 0).est})
    return merged


## Run ##
##################################################################################################


if __name__ == "__main__":

    begin = time.time()

    run_id = int(sys.argv[1])
    holdout_num = int(sys.argv[2])
    draws = int(sys.argv[3])
    run_type = sys.argv[4]
    rake_logit = int(sys.argv[5])
    loc = int(sys.argv[6])

    for i in ["run_id", "holdout_num", "draws", "run_type", "rake_logit", "loc"]:
        print("{} : {}".format(i, eval(i)))

    # set ids
    ids = ["location_id", "year_id", "age_group_id", "sex_id"]

    # pull in params
    print("Getting model parameters.")
    params = hlp.model_load(run_id, "parameters", param_set=None)
    param_set = utils.get_best_parameter_set(run_id)

    # pull in locs and prep demographics table
    print("Getting locations for raking + aggregation and populations.")
    locs = hlp.model_load(run_id, "aggregation_location_hierarchy")
    pops = hlp.model_load(run_id, "populations")
    demographics = prep_demographics(locations=locs, populations=pops, loc=loc)

    # determine all locs needed for raking so we can pull in csvs named by loc
    all_locs = (
        locs.loc[locs[f"level_{constants.location.NATIONAL_LEVEL}"] == loc, "location_id"]
        .unique()
        .tolist()
    )

    # set vars
    if draws > 0:
        vars = ["draw_{}".format(x) for x in range(draws)]
    else:
        vars = ["gpr_mean", "gpr_lower", "gpr_upper"]

    # pull in data
    print("Loading data.")
    settings = stgpr_schema.get_settings()
    output_root = settings.output_root_format.format(stgpr_version_id=run_id)
    ls = []
    data_load_start = time.time()
    for l in all_locs:
        data_path = "{root}/rake_temp_{ko}/{p}/{loc}.csv".format(
            root=output_root, ko=holdout_num, p=param_set, loc=l
        )
        tmp = pd.read_csv(data_path)
        ls.append(tmp)
    df = pd.concat([x for x in ls]).reset_index()
    data_load_time = np.round(time.time() - data_load_start)

    # check for missings
    mi = antijoin(all_locs, df.location_id.unique())
    msg = (
        "The following locations are missing from " "{root}/rake_temp_{ko}/{p}: {mi}"
    ).format(root=output_root, ko=holdout_num, p=param_set, mi=mi)
    assert not mi, RuntimeError(msg)

    # convert to xarray for all operations
    start = time.time()
    xdf = prep_xarray(df, vars)
    print((time.time() - start))

    # backtransform outputs
    if rake_logit == 0:
        print("Transforming back to real world space.")
        xdf = stgpr_helpers.transform_data(xdf, params.data_transform.iat[0], reverse=True)

    # Run rake
    levels = get_levels(demographics, loc)
    dropped_1 = []
    dropped_0 = []
    statuses = []
    for lvl in levels:
        status = get_rake_status(params, loc, lvl)
        statuses.append(status)
        if status == "rake":
            rakelocs = get_rake_locs(demographics, loc, lvl)
            parents = get_parent(demographics, child_locations=rakelocs)
            for parent in parents:
                print("Running rake for parent_id {parent}".format(parent=parent))
                sub_rakelocs = get_rake_locs(demographics, loc, lvl, parent=parent)
                non_rakelocs = antijoin(xdf.location_id.values, sub_rakelocs)
                xrake = prep_rake(
                    xarray_df=xdf,
                    demographics=demographics,
                    rakelocs=sub_rakelocs,
                    parent=parent,
                )
                if rake_logit == 1:
                    start = time.time()
                    # Before raking in logit space, drop data that's really close to 0 or 1.
                    dropped_1.append(xrake.where(xrake.est_parent > _LOGIT_UPPER_BOUND, drop=True))
                    dropped_0.append(xrake.where(xrake.est_parent < _LOGIT_LOWER_BOUND, drop=True))
                    xrake_kept = xrake.where(xrake.est_parent <= _LOGIT_UPPER_BOUND, drop=True)
                    xrake_kept = xrake_kept.where(xrake_kept.est_parent >= _LOGIT_LOWER_BOUND, drop=True)
                    xdf = logit_rake(
                        xrake_kept,
                        parent_id=parent,
                        p_i="est",
                        p_N="est_parent",
                        N_i="population",
                        rake_locations=sub_rakelocs,
                        non_rake_locations=non_rakelocs,
                        tolerance=get_tolerance(demographics=demographics, level=lvl),
                    )
                    print((time.time() - start))
                else:
                    start = time.time()
                    xdf = rake_estimates(xrake, sub_rakelocs, non_rakelocs)
                    print((time.time() - start))

    if rake_logit == 1:
        print("Transforming variables to real world space before aggregation")
        xdf = stgpr_helpers.transform_data(xdf, params.data_transform.iat[0], reverse=True)

        # Only worry about this if we actually did logit raking.
        if "rake" in statuses:
            # Combine all of the data that was dropped for being close to 0/1.
            dropped = xr.merge(dropped_1 + dropped_0)

            # Now add back in demographics we dropped, and set them to 0/1.
            xdf = set_to_0_1(xdf, dropped)
            xdf = xdf.drop(["est_parent", "population"])

    # Run aggregation
    levels.reverse()
    for lvl in levels:
        status = get_rake_status(params, loc, lvl)
        if status == "aggregate":
            print("Aggregating level {level}".format(level=lvl))
            # identify subnationals to aggregate and their parent location
            agglocs = get_rake_locs(demographics, loc, lvl)
            parents = get_parent(demographics, child_locations=agglocs)
            # prep xdf with needed populations
            for parent in parents:
                start = time.time()
                sub_agglocs = get_rake_locs(demographics, loc, lvl, parent=parent)
                xagg = prep_aggregate(xdf, demographics, sub_agglocs)
                xdf = aggregate_estimates(xagg, sub_agglocs, parent)
                print((time.time() - start))

    # validate outputs
    print("Validating outputs...")
    start = time.time()
    xcheck = prep_validation(xdf, demographics)

    for lvl in levels:
        status = get_rake_status(params, loc, lvl)
        lvl_locs = get_rake_locs(demographics, loc, lvl)
        parents = get_parent(demographics, child_locations=lvl_locs)
        for parent in parents:
            sub_locs = get_rake_locs(demographics, loc, lvl, parent=parent)
            validate_rake_aggregate(xcheck, sub_locs, parent, status)
        print("Successfully {status}d level {level}!".format(status=status, level=lvl))
    print(("Validation time: {time}".format(time=(time.time() - start))))

    # Convert back to pandas. painfully.
    out = xdf.to_dataframe().reset_index()
    out = pd.pivot_table(
        out, values="est", index=columns.DEMOGRAPHICS, columns="var"
    ).reset_index()

    # Clean df for output
    out[ids] = out[ids].astype("int32")
    out[vars] = out[vars].astype("float64")

    # check for missings
    missing_locs = antijoin(out.location_id.unique(), demographics["location_id"].unique())

    if not missing_locs:
        print("No missing locations, neat.")
    else:
        sys.exit("Post-rake outputs are missing locations {mi}".format(mi=missing_locs))

    # write outputs
    sumdir = "{root}/rake_means_temp_{ko}".format(root=output_root, ko=holdout_num)
    sumpath = "{root}/{loc}.csv".format(root=sumdir, loc=loc)
    if not os.path.isdir(sumdir):
        os.system("mkdir -m 777 -p {}".format(sumdir))

    if draws == 0:
        out.sort_values(by=ids, inplace=True)
        out.to_csv(sumpath, index=False)
        print("Saved temporary rake outputs to {path}".format(path=sumpath))
    else:
        # save draws
        for loc_id in out.location_id.unique():
            tmp = out[out.location_id == loc_id]
            drawpath = "{root}/draws_temp_{ko}/{p}/{location_id}.csv".format(
                root=output_root, ko=holdout_num, p=param_set, location_id=loc_id
            )
            tmp.to_csv(drawpath, index=False)

        print(
            ("Draws saved by location_id to " "{root}/draws_temp_{ko}/{p}").format(
                root=output_root, ko=holdout_num, p=param_set
            )
        )
        # save draw mean, lower and upper by loc/yr/age/sex
        gpr_cols = ["gpr_mean", "gpr_lower", "gpr_upper"]
        out["gpr_mean"] = out[vars].mean(axis=1)
        out["gpr_lower"] = out[vars].quantile(q=constants.uncertainty.LOWER_QUANTILE, axis=1)
        out["gpr_upper"] = out[vars].quantile(q=constants.uncertainty.UPPER_QUANTILE, axis=1)

        # overwrite input temp data with gpr means for append and save
        out = out[ids + gpr_cols]
        out.sort_values(by=ids, inplace=True)
        out.to_csv(sumpath, index=False)
        print("Rake summaries temporarily saved to {path}!".format(path=sumpath))

    total_time = np.round(time.time() - begin)
    print("Total rake time: {total} seconds".format(total=total_time))
