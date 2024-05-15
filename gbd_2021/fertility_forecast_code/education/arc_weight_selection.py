"""Education weight-selection for the ARC method using predictive Validity

>>> python arc_weight_selection.py \
    --reference-scenario mean \
    --transform logit \
    --diff-over-mean \
    --truncate \
    --truncate-quantiles 0.15 0.85 \
    --max-weight 3 \
    --weight-step-size 0.25 \
    --pv-version 20181026_just_nats_capped_pv \
    --past-version 20181003_subnats_included \
    --gbd-round-id 5 \
    --years 1990:2008:2017 \
    all-weights

"""
import logging
import os
import subprocess
import sys

import numpy as np
import xarray as xr

from fbd_core import argparse
from fbd_core.file_interface import FBDPath, open_xr
from fbd_research.education.forecast_education import (TRANSFORMATIONS,
                                                       arc_forecast_education)

REFERENCE_SCENARIO = 0
LOGGER = logging.getLogger(__name__)


def calc_rmse(predicted, observed, years):
    predicted = predicted.sel(year_id=years.forecast_years)
    observed = observed.sel(year_id=years.forecast_years)

    rmse = np.sqrt(((predicted - observed) ** 2).mean())

    return rmse


def one_weight_main(reference_scenario, transform, diff_over_mean, truncate,
                    truncate_quantiles, replace_with_mean,
                    use_past_uncertainty, weight_exp, past_version, pv_version,
                    years, gbd_round_id, test_mode, **kwargs):
    """Predictive validity for one one weight of the range of weights"""

    LOGGER.debug("diff_over_mean:{}".format(diff_over_mean))
    LOGGER.debug("truncate:{}".format(truncate))
    LOGGER.debug("truncate_quantiles:{}".format(truncate_quantiles))
    LOGGER.debug("replace_with_mean:{}".format(replace_with_mean))
    LOGGER.debug("reference_scenario:{}".format(reference_scenario))
    LOGGER.debug("use_past_uncertainty:{}".format(use_past_uncertainty))

    LOGGER.debug("Reading in the past")
    past_path = FBDPath("".format())  # Path structure removed for security
    past = open_xr(past_path / "education.nc").data
    past = past.transpose(*list(past.coords))

    if not use_past_uncertainty:
        LOGGER.debug("Using past means for PV")
        past = past.mean("draw")
    else:
        LOGGER.debug("Using past draws for PV")

    if test_mode:
        past = past.sel(
            age_group_id=past["age_group_id"].values[:5],
            draw=past["draw"].values[:5],
            location_id=past["location_id"].values[:5])
    else:
        pass  # Use full data set.

    holdouts = past.sel(year_id=years.past_years)
    observed = past.sel(year_id=years.forecast_years)

    LOGGER.debug("Calculating RMSE for {}".format(weight_exp))
    predicted = arc_forecast_education(
        holdouts, gbd_round_id, transform, weight_exp, years,
        reference_scenario,
        diff_over_mean, truncate, truncate_quantiles, replace_with_mean)
    rmse = calc_rmse(predicted.sel(scenario=REFERENCE_SCENARIO, drop=True),
                     observed,
                     years)

    rmse_da = xr.DataArray(
        [rmse.values], [[weight_exp]], dims=["weight"])

    pv_path = FBDPath("".format())  # Path structure removed for security
    separate_weights_path = pv_path / "each_weight"
    separate_weights_path.mkdir(parents=True, exist_ok=True)
    rmse_da.to_netcdf(
        str(separate_weights_path / "{}_rmse.nc".format(weight_exp)))
    LOGGER.info("Saving RMSE for {}".format(weight_exp))


def merge_main(max_weight, weight_step_size, pv_version, gbd_round_id,
               **kwargs):
    """Merge RMSE values for all of the tested weights into one dataarray."""
    LOGGER.debug("Calculating RMSE for all weights")
    weights_to_test = np.arange(0, max_weight, weight_step_size)

    pv_path = FBDPath("".format())  # Path structure removed for security
    separate_weights_path = pv_path / "each_weight"

    rmse_results = []
    for weight_exp in weights_to_test:
        rmse_da = open_xr(
            separate_weights_path / "{}_rmse.nc".format(weight_exp)).data
        rmse_results.append(rmse_da)
    rmse_results = xr.concat(rmse_results, dim="weight")

    rmse_results.to_netcdf(str(pv_path / "education_arc_weight_rmse.nc"))
    LOGGER.info("RMSE is saved")


def parallelize_by_weight_main(reference_scenario, transform, diff_over_mean,
                               truncate, truncate_quantiles, replace_with_mean,
                               use_past_uncertainty, max_weight,
                               weight_step_size, past_version, pv_version,
                               years, test_mode, gbd_round_id, slots,
                               **kwargs):
    """Parallelize the script so Predictive validity is run for all weights."""
    LOGGER.debug("diff_over_mean:{}".format(diff_over_mean))
    LOGGER.debug("truncate:{}".format(truncate))
    LOGGER.debug("truncate_quantiles:{}".format(truncate_quantiles))
    LOGGER.debug("replace_with_mean:{}".format(replace_with_mean))
    LOGGER.debug("reference_scenario:{}".format(reference_scenario))
    LOGGER.debug("use_past_uncertainty:{}".format(use_past_uncertainty))

    script = os.path.abspath(os.path.realpath(__file__))
    LOGGER.debug(script)

    weights_to_test = np.arange(0, max_weight, weight_step_size)
    num_weights = len(weights_to_test)

    if test_mode:
        test_mode_call = "--test-mode"
    else:
        test_mode_call = ""  # Use full data set

    if truncate:
        truncate_call = "--truncate"
    else:
        truncate_call = ""

    if truncate_quantiles:
        truncate_quantiles_call = "--truncate-quantiles {}".format(
            " ".join([str(i) for i in truncate_quantiles]))
    else:
        truncate_quantiles_call = ""

    if replace_with_mean:
        replace_with_mean_call = "--replace-with-mean"
    else:
        replace_with_mean_call = ""

    if use_past_uncertainty:
        use_past_uncertainty_call = "--use-past-uncertainty"
    else:
        use_past_uncertainty_call = ""

    if diff_over_mean:
        diff_over_mean_call = "--diff-over-mean"
    else:
        diff_over_mean_call = ""

    one_weight_qsub = (
        "qsub -N 'edu_arc_pv' "
        "-pe multi_slot {slots} "
        "-t 1:{N} "
        "-b y {which_python} "
        "{script} "
        "--reference-scenario {reference_scenario} "
        "--transform {transform} "
        "--max-weight {max_weight} "
        "--weight-step-size {weight_step_size} "
        "--pv-version {pv_version} "
        "--past-version {past_version} "
        "--years {years} "
        "--gbd-round-id {gbd_round_id} "
        "{truncate_call} "
        "{truncate_quantiles_call} "
        "{replace_with_mean_call} "
        "{use_past_uncertainty_call} "
        "{diff_over_mean_call} "
        "{test_mode_call} "
        "one-weight"
    ).format(
        slots=slots,
        N=num_weights,
        which_python=sys.executable,
        script=script,
        reference_scenario=reference_scenario,
        transform=transform,
        max_weight=max_weight,
        weight_step_size=weight_step_size,
        pv_version=pv_version,
        past_version=past_version,
        years=years,
        gbd_round_id=gbd_round_id,
        truncate_call=truncate_call,
        truncate_quantiles_call=truncate_quantiles_call,
        replace_with_mean_call=replace_with_mean_call,
        use_past_uncertainty_call=use_past_uncertainty_call,
        diff_over_mean_call=diff_over_mean_call,
        test_mode_call=test_mode_call)

    LOGGER.debug(one_weight_qsub)
    one_weight_qsub_proc = subprocess.Popen(
        one_weight_qsub,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    one_weight_qsub_out, _ = one_weight_qsub_proc.communicate()
    if one_weight_qsub_proc.returncode:
        one_weight_qsub_err_msg = "One-weight qsub failed."
        LOGGER.error(one_weight_qsub_err_msg)
        raise RuntimeError(one_weight_qsub_err_msg)
    LOGGER.debug(one_weight_qsub_out)

    hold_jid = [
        int(word)
        for word in str(
            one_weight_qsub_out).split(".")[0].split(" ")
        if word.isdigit()
    ][0]
    LOGGER.debug(hold_jid)

    merge_slots = 5
    merge_qsub = (
        "qsub -N 'edu_arc_pv' "
        "-hold_jid {hold_jid} "
        "-pe multi_slot {slots} "
        "-b y {which_python} "
        "{script} "
        "--reference-scenario {reference_scenario} "
        "--transform {transform} "
        "--max-weight {max_weight} "
        "--weight-step-size {weight_step_size} "
        "--pv-version {pv_version} "
        "--past-version {past_version} "
        "--gbd-round-id {gbd_round_id} "
        "--years {years} "
        "{truncate_call} "
        "{truncate_quantiles_call} "
        "{replace_with_mean_call} "
        "{use_past_uncertainty_call} "
        "{diff_over_mean_call} "
        "{test_mode_call} "
        "merge"
    ).format(hold_jid=hold_jid,
             slots=merge_slots,
             which_python=sys.executable,
             script=script,
             reference_scenario=reference_scenario,
             transform=transform,
             max_weight=max_weight,
             weight_step_size=weight_step_size,
             pv_version=pv_version,
             past_version=past_version,
             years=years,
             gbd_round_id=gbd_round_id,
             truncate_call=truncate_call,
             truncate_quantiles_call=truncate_quantiles_call,
             replace_with_mean_call=replace_with_mean_call,
             use_past_uncertainty_call=use_past_uncertainty_call,
             diff_over_mean_call=diff_over_mean_call,
             test_mode_call=test_mode_call)
    LOGGER.debug(merge_qsub)
    merge_qsub_proc = subprocess.Popen(
        merge_qsub,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    merge_qsub_out, _ = merge_qsub_proc.communicate()
    if merge_qsub_proc.returncode:
        merge_err_msg = "Merge qsub failed."
        LOGGER.error(merge_err_msg)
        raise RuntimeError(merge_err_msg)


def all_weights_main(reference_scenario, diff_over_mean, truncate,
                     truncate_quantiles, replace_with_mean,
                     use_past_uncertainty, transform, max_weight,
                     weight_step_size, past_version, pv_version, years,
                     gbd_round_id, test_mode, **kwargs):
    """Predictive validity for one weight of the range of weights at a time."""
    LOGGER.debug("diff_over_mean:{}".format(diff_over_mean))
    LOGGER.debug("truncate:{}".format(truncate))
    LOGGER.debug("truncate_quantiles:{}".format(truncate_quantiles))
    LOGGER.debug("replace_with_mean:{}".format(replace_with_mean))
    LOGGER.debug("reference_scenario:{}".format(reference_scenario))
    LOGGER.debug("use_past_uncertainty:{}".format(use_past_uncertainty))

    LOGGER.debug("Reading in the past")
    past_path = FBDPath("".format())
    past = open_xr(past_path / "education.nc").data
    past = past.transpose(*list(past.coords))

    if not use_past_uncertainty:
        LOGGER.debug("Using past means for PV")
        past = past.mean("draw")
    else:
        LOGGER.debug("Using past draws for PV")

    if test_mode:
        past = past.sel(
            age_group_id=past["age_group_id"].values[:5],
            draw=past["draw"].values[:5],
            location_id=past["location_id"].values[:5])
    else:
        pass  # Use full data set.

    holdouts = past.sel(year_id=years.past_years)
    observed = past.sel(year_id=years.forecast_years)

    LOGGER.debug("Calculating RMSE for all weights")
    weights_to_test = np.arange(0, max_weight, weight_step_size)
    rmse_results = []
    for weight_exp in weights_to_test:
        predicted = arc_forecast_education(
            holdouts, gbd_round_id, transform, weight_exp, years,
            reference_scenario,
            diff_over_mean, truncate, truncate_quantiles, replace_with_mean)
        rmse = calc_rmse(predicted.sel(scenario=REFERENCE_SCENARIO, drop=True),
                         observed,
                         years)

        rmse_da = xr.DataArray(
            [rmse.values], [[weight_exp]], dims=["weight"])
        rmse_results.append(rmse_da)
    rmse_results = xr.concat(rmse_results, dim="weight")

    pv_path = FBDPath("".format())
    pv_path.mkdir(parents=True, exist_ok=True)
    rmse_results.to_netcdf(str(pv_path / "education_arc_weight_rmse.nc"))
    LOGGER.info("RMSE is saved")


if __name__ == "__main__":

    def get_weight_from_jid(weight_arg, max_weight, weight_step_size):
        sge_task_id = os.environ.get("SGE_TASK_ID")
        if weight_arg is not None:
            return weight_arg
        elif sge_task_id:
            weights_to_test = np.arange(0, max_weight, weight_step_size)
            sge_task_id = int(sge_task_id) - 1
            LOGGER.debug("SGE_TASK_ID: {}".format(sge_task_id))

            weight = weights_to_test[sge_task_id]
        else:
            err_msg = "`weight` and `SGE_TASK_ID` can not all be NoneType."
            LOGGER.error(err_msg)
            raise ValueError(err_msg)
        return weight

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--reference-scenario", type=str, choices=["median", "mean"],
        help=("If 'median' then the reference scenarios is made using the "
              "weighted median of past annualized rate-of-change across all "
              "past years, 'mean' then it is made using the weighted mean of "
              "past annualized rate-of-change across all past years."))
    parser.add_argument(
        "--diff-over-mean", action="store_true",
        help=("If True, then take annual differences for means-of-draws, "
              "instead of draws."))
    parser.add_argument(
        "--truncate", action="store_true",
        help=("If True, then truncates the dataarray over the given "
              "dimensions."))
    parser.add_argument(
        "--truncate-quantiles", type=float, nargs="+",
        help="The tuple of two floats representing the quantiles to take.")
    parser.add_argument(
        "--replace-with-mean", action="store_true",
        help=("If True and `truncate` is True, then replace values outside of "
              "the upper and lower quantiles taken across `location_id` and "
              "`year_id` and with the mean across `year_id`, if False, then "
              "replace with the upper and lower bounds themselves."))
    parser.add_argument(
        "--use-past-uncertainty", action="store_true",
        help=("Use past draws in PV forecasts. **Note** if you're running "
              "with `diff_over_mean=True`, then it doesn't make much sense "
              "use past draws at all, in fact, it will only make the run "
              "slower without adding any statistical significance."))
    parser.add_argument(
        "--transform", type=str,
        choices=list(sorted(TRANSFORMATIONS.keys())),
        help="Space to transform education to for forecasting.")
    parser.add_argument(
        "--max-weight", type=float, required=True,
        help="The maximum weight to try. Current convention is 3.")
    parser.add_argument(
        "--weight-step-size", type=float, required=True,
        help="The step size of weights to try between 0 and `max_weight`."
             "Current convention is 2.5.")
    parser.add_argument(
        "--pv-version", type=str, required=True,
        help="Version of education weight selection")
    parser.add_argument(
        "--past-version", type=str, required=True,
        help="Version of past education")
    parser.add_argument(
        "--gbd-round-id", type=int, required=True)
    parser.add_argument(
        "--test-mode", action="store_true",
        help="Run on smaller test data set.")
    parser.add_arg_years(required=True)

    subparsers = parser.add_subparsers()

    # Run script for only one weight
    one_weight = subparsers.add_parser(
        "one-weight", help=one_weight_main.__doc__)
    one_weight.add_argument(
        "--weight-exp", type=float, help="The weight to do PV for")
    one_weight.set_defaults(func=one_weight_main)

    # Merge the outputs of the parallelized jobs
    merge = subparsers.add_parser("merge", help=merge_main.__doc__)
    merge.set_defaults(func=merge_main)

    # Run script for all weights in parallel.
    parallelize_by_weight = subparsers.add_parser(
        "parallelize-by-weight", help=parallelize_by_weight_main.__doc__)
    parallelize_by_weight.add_argument(
        "--slots", type=int, default=35,
        help="Number of slots required for each job.")
    parallelize_by_weight.set_defaults(func=parallelize_by_weight_main)

    # Run script all weights.
    all_weights = subparsers.add_parser(
        "all-weights", help=all_weights_main.__doc__)
    all_weights.set_defaults(func=all_weights_main)

    args = parser.parse_args()

    if args.func == one_weight_main:
        args.weight_exp = get_weight_from_jid(
            args.weight_exp, args.max_weight, args.weight_step_size)
    else:
        # `args.weight_exp` only needs to be defined for `one_weight_main`.
        pass

    args.func(**args.__dict__)
