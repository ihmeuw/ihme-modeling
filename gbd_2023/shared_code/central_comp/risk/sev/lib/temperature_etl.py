import pandas as pd

from gbd.constants import cause

from ihme_cc_sev_calculator.lib import constants


def validate_temperature_sevs(sev_df: pd.DataFrame, cause_metadata: pd.DataFrame) -> None:
    """Validate temperature SEVs."""
    # Should only include temperature risks
    if not sev_df["rei_id"].isin(constants.TEMPERATURE_REI_IDS).all():
        raise ValueError("Found draws for non-temperature risks")

    # Should include all temperature risks
    missing_rei_ids = set(constants.TEMPERATURE_REI_IDS).difference(set(sev_df["rei_id"]))
    if len(missing_rei_ids) > 0:
        raise ValueError(f"Input draws are missing temperature REI ID(s) {missing_rei_ids}")

    # Should only include most-detailed causes and all-cause
    most_detailed_cause_ids = cause_metadata.query("most_detailed == 1")["cause_id"].tolist()
    if not sev_df["cause_id"].isin(most_detailed_cause_ids + [cause.ALL_CAUSE]).all():
        raise ValueError("Found draws for aggregate causes")

    # Must include both all-cause SEVs and most-detailed-cause SEVs
    if sev_df.query(f"cause_id == {cause.ALL_CAUSE}").empty:
        raise ValueError("Input draws are missing all-cause SEVs")

    if sev_df.query(f"cause_id != {cause.ALL_CAUSE}").empty:
        raise ValueError("Input draws are missing most-detailed causes")
