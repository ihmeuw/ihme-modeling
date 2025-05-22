"""Post raking: aggregate raked results by location and summarize for upload."""

import logging
import sys

import stgpr_helpers
from stgpr_helpers import columns, parameters

from stgpr.legacy.st_gpr import helpers
from stgpr.lib import constants, expansion, location_aggregation, utils

logger = logging.getLogger(__name__)


def run_post_raking(stgpr_version_id: int) -> None:
    """Run post raking: location aggregate, save draws, summarize, and save for upload.

    If the run included draws, saves draws to their permanent location. Summaries are
    also saved to their permanent location for holdout 0 and the best parameter set
    """
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)
    params = file_utility.read_parameters()
    best_param_set = utils.get_best_parameter_set(stgpr_version_id)

    logger.info(f"Reading post-raking data for best parameter set: {best_param_set}")
    data = (
        file_utility.read_draws(parameter_set_number=best_param_set)
        if params[parameters.GPR_DRAWS] > 0
        else file_utility.read_final_estimates_temp(best_param_set)
    )

    # Only run location aggregation if metric id is non-null
    if params[parameters.METRIC_ID]:
        logger.info("Running location aggregation")
        data = location_aggregation.aggregate_locations(
            stgpr_version_id, data, data_in_model_space=False
        )

    data = expansion.expand_results(data=data, params=params)

    # Save draws and create summaries if we have draws.
    # Otherwise, set upper and lower as equal to mean
    if params[parameters.GPR_DRAWS] > 0:
        logger.info("Saving and summarizing draws")
        file_utility.cache_final_draws(data)

        draw_cols = [f"draw_{i}" for i in range(params[parameters.GPR_DRAWS])]
        data["gpr_mean"] = data[draw_cols].mean(axis=1)
        data["gpr_lower"] = data[draw_cols].quantile(
            q=constants.uncertainty.LOWER_QUANTILE, axis=1
        )
        data["gpr_upper"] = data[draw_cols].quantile(
            q=constants.uncertainty.UPPER_QUANTILE, axis=1
        )
    else:
        data["gpr_lower"] = data["gpr_mean"]
        data["gpr_upper"] = data["gpr_mean"]

    logger.info(f"Saving summaries for holdout 0, parameter set {best_param_set}")
    helpers.model_save(
        data[columns.DEMOGRAPHICS + ["gpr_mean", "gpr_lower", "gpr_upper"]],
        stgpr_version_id,
        "raked",
        holdout=0,
        param_set=best_param_set,
    )


if __name__ == "__main__":
    stgpr_helpers.configure_logging()

    stgpr_version_id = int(sys.argv[1])
    run_post_raking(stgpr_version_id)
