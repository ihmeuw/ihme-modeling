"""Custom PAF calculation for intimate partner violence (IPV)."""

import pathlib

import pandas as pd

from ihme_cc_paf_calculator.lib import constants, io_utils

# Parent cause for interpersonal violence (not the same as intimate partner violence
# although both abbreviations are IPV)
INTERPERSONAL_VIOLENCE_ID: int = 724


def calculate_abuse_ipv_paf(
    root_dir: pathlib.Path, location_id: int, settings: constants.PafCalculatorSettings
) -> None:
    """Calculates custom PAFs for intimate partner violence, abuse_ipv_paf, REI ID 168.

    IPV has direct PAFs (no RRs). We pull draws for the associated "exposure" modelable
    entity interpreted as the PAF, duplicate out to all most-detailed homicide causes,
    both measures, keep only ages 15+, and save.

    Intended only for use within the PAF Calculator.
    """
    cause_metadata = io_utils.get(root_dir, constants.CacheContents.CAUSE_METADATA)
    age_metadata = io_utils.get(root_dir, constants.CacheContents.AGE_METADATA)

    if settings.rei_id != constants.IPV_REI_ID:
        raise RuntimeError(
            "Internal error: function can only be run for IPV, REI ID "
            f"{constants.IPV_REI_ID}. Got rei_id {settings.rei_id}"
        )

    cause_ids = cause_metadata.query(f"parent_id == {INTERPERSONAL_VIOLENCE_ID}")[
        "cause_id"
    ].tolist()

    exposure = (
        io_utils.get_location_specific(
            root_dir, constants.LocationCacheContents.EXPOSURE, location_id=location_id
        )
        .drop(columns="parameter")
        .assign(rei_id=constants.IPV_REI_ID)
    )

    # Copy exposure draws for all causes and mortality/morbidity - interpreted as PAFs
    pafs = pd.merge(exposure, pd.DataFrame({"cause_id": cause_ids}), how="cross").merge(
        pd.DataFrame({"mortality": [1, 0], "morbidity": [0, 1]}), how="cross"
    )

    # Age-restrict to 15+
    age_group_ids_15_plus = age_metadata.query("age_group_years_start >= 15")[
        "age_group_id"
    ].tolist()
    pafs = pafs.query(f"age_group_id.isin({age_group_ids_15_plus})")

    # Save PAFs: unlike normal PAFs that are calculated by cause, here all causes
    # have been 'calculated' at once. Save separately by cause to follow expected pattern
    io_utils.write_paf(pafs, root_dir, location_id)
