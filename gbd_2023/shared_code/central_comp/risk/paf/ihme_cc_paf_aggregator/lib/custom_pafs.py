import pandas as pd

import ihme_cc_paf_calculator

from ihme_cc_paf_aggregator.lib import constants, logging_utils

logger = logging_utils.module_logger(__name__)


def calculate_and_append_custom_pafs(paf_df: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """
    Prior to aggregation we want to add on custom PAFs.

    Currently implemented:
        * Custom PAFs for injuries related to smoking (REI ID 99), which replace
            PAFs for fracture causes
        * Custom PAFS for HIV due to injected drug use (REI ID 138)

    Arguments:
        paf_df: input PAFs prior to aggregation
        release_id: used to determine age/sex restrictions to apply

    Returns:
        paf_df with custom PAFs appended
    """
    # Calculate injuries PAFs, catching error if we have no fracture PAFs for smoking
    try:
        injury_pafs = ihme_cc_paf_calculator.calculate_injury_pafs(
            paf_df, constants.SMOKING_REI_ID, release_id
        )
        paf_df = pd.concat(
            [paf_df[~paf_df.cause_id.isin(constants.FRACTURE_CAUSE_IDS)], injury_pafs],
            ignore_index=True,
        )
    except ValueError as e:
        if "No PAFs found for causes" in str(e):
            logger.info(str(e))
        else:
            raise e

    # Calculate HIV due to drug use PAFs if there are already injected drug use PAFs
    if constants.INJECTED_DRUG_USE_REI_ID in paf_df[constants.REI_ID].unique():
        hiv_pafs = ihme_cc_paf_calculator.calculate_hiv_due_to_drug_use_pafs(
            location_id=paf_df[constants.LOCATION_ID].iloc[
                0
            ],  # only 1 location expected in df
            year_ids=paf_df[constants.YEAR_ID].unique().tolist(),
            release_id=release_id,
            n_draws=len([col for col in paf_df.columns if col.startswith("draw_")]),
        )
        paf_df = pd.concat([paf_df, hiv_pafs], ignore_index=True)
    else:
        logger.info(
            "No PAFs found for injected drug use (REI id "
            f"{constants.INJECTED_DRUG_USE_REI_ID}). Not appending PAFs for HIV due to "
            "drug use."
        )

    return paf_df
