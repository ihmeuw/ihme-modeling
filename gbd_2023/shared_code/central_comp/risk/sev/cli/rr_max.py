import click
import numpy as np
import pandas as pd

from ihme_cc_sev_calculator.lib import constants, io, lbwsg, logging_utils, parameters, rr_max

logger = logging_utils.module_logger(__name__)

# Set a random seed for consistency, particularly for functions with randomness we can't
# directly control the seed of, like get_draws
np.random.seed(constants.SEED)


@click.command
@click.option("rei_id", "--rei_id", type=int, help="REI ID to calculate RR max for.")
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def calculate_rr_max(rei_id: int, version_id: int) -> None:
    """Calculate RR max for the given REI ID."""
    params = parameters.Parameters.read_from_cache(version_id)
    rei_metadata = params.read_rei_metadata()
    logger.info(
        "Starting RR max calculation for "
        f"'{rei_metadata.query(f'rei_id == {rei_id}')['rei_name'].iat[0]}', REI ID {rei_id}"
    )

    if rei_id in constants.CUSTOM_RR_MAX_REI_IDS:
        rr_max_df = io.read_custom_rr_max_draws(
            rei_id, rei_metadata, params.release_id, params.n_draws
        )

        # For air pollution risks that are mediated through LBW/SG, add in RRmax for
        # LBW/SG outcomes if they're not already modeled
        if rei_id in constants.AIR_PM_MEDIATED_BY_LBWSG_REI_IDS:
            lbwsg_rr_max_df = lbwsg.calculate_rr_max_for_lbwsg(
                constants.LBWSGA_REI_ID, params
            )
            rr_max_df = pd.concat([rr_max_df, lbwsg_rr_max_df])

            # If any LBW/SG outcomes are already in custom RRmax, keep directly modeled RRmax
            rr_max_df = rr_max_df.drop_duplicates(subset=rr_max.INDEX_COLS, keep="first")
    elif rei_id in constants.LBW_PRETERM_REI_IDS:
        rr_max_df = lbwsg.calculate_rr_max_for_lbwsg(rei_id, params)
    else:
        # Otherwise... proceed as normal
        rr_max_df = rr_max.calculate_rr_max(rei_id, params, rei_metadata)

        # Average across sub-risks for drugs_illicit_suicide.
        rr_max_df = rr_max.average_rr_max_across_drug_subrisks(
            rei_id=rei_id, rr_max=rr_max_df
        )

        # Two-stage mediation handling - adjust RRmax when a cause has multiple mediator risks
        rr_max_df = rr_max.adjust_rrmax_for_multiple_mediators(rr_max_df, params.draw_cols)

    # Validate RR max
    rr_max.validate_rr_max(rei_id, rr_max_df, params)

    logger.info("RRmax calculation complete. Saving draws.")
    io.save_rr_max_draws(rr_max_df, rei_id, params.draw_cols, params.output_dir)


if __name__ == "__main__":
    calculate_rr_max()
