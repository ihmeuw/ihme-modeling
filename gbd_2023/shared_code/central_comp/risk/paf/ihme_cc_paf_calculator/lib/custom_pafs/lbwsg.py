import pathlib
from typing import List

import pandas as pd

import ihme_cc_risk_utils
from gbd import gbd_round, release
from ihme_cc_risk_utils.lib.lbwsg import (
    LBW_TMREL,
    LBWSGA_LBW_TMREL,
    LBWSGA_SG_TMREL,
    PARAMETER_MAP,
    SG_TMREL,
)

from ihme_cc_paf_calculator.lib import constants, io_utils, math
from ihme_cc_paf_calculator.lib.custom_pafs import subcause_split_utils

GLOBAL_LOCATION_ID: int = 1


def get_exposure_draws(
    release_id: int, location_id: int, year_id: List[int], n_draws: int
) -> pd.DataFrame:
    """Get exposure draws for calculating LBW/SG PAFs.

    Pulls location-specific exposure for all requested years and global exposure
    for a single year (the year corresponding to the GBD round).

    In both cases, if exposure across all categories exceeds 1, exposure is rescaled to sum
    to exactly 1. The residual category (40+ weeks, 4000+ g) is recalculated as 1 - the sum
    of all exposure categories. As follows, in the rescale case, the residual category is 0.
    This all happens within the function ihme_cc_risk_utils.get_exposure_draws.
    """
    exposure = ihme_cc_risk_utils.get_exposure_draws(
        rei_id=constants.LBWSGA_REI_ID,
        release_id=release_id,
        location_id=location_id,
        year_id=year_id,
        n_draws=n_draws,
    )

    global_year_id = int(
        gbd_round.gbd_round_from_gbd_round_id(
            release.get_gbd_round_id_from_release(release_id)
        )
    )
    global_exposure = ihme_cc_risk_utils.get_exposure_draws(
        rei_id=constants.LBWSGA_REI_ID,
        release_id=release_id,
        location_id=GLOBAL_LOCATION_ID,
        year_id=[global_year_id],
        n_draws=n_draws,
    )

    return pd.concat([exposure, global_exposure]).reset_index(drop=True)


def calculate_nutrition_lbw_preterm_paf(
    root_dir: pathlib.Path, location_id: int, settings: constants.PafCalculatorSettings
) -> None:
    """Calculate custom PAFs for joint risk LBW/SGA, nutrition_lbw_preterm.

    Includes PAF calculation for its two child risks, low birth weight and short gestation.

    PAFs are calculated for LBW/SGA as a normal categorical risk using two-dimensional
    grids (low birth weight as one dimension and short gestation as the other), scaled to 1,
    with the residual category, 40+ weeks and 4000+ g as the TMREL.

    For its child risks, exposure and RR are collapsed from a two-dimensionsal grid to one
    dimension. Collapsed exposure is the sum across the collapsed dimension. Collapsed RR is
    the global exposure-weighted average.

    The TMREL is set to 38 weeks for preterm and 3500 g for low birth weight.

    All RRs are rescaled by dividing by the largest RR in the TMREL categories. Any RRs under
    1 after rescaling are set to 1. This is a way to force all TMREL RRs to 1, readjusting
    others accordingly.

    Child risk PAFs are calculated using the standard categorical equation.

    Finally, PAFs for all three risks are split into PAFs for the subcauses.
    """
    if settings.rei_id != constants.LBWSGA_REI_ID:
        raise RuntimeError(
            "Internal error: function can only be run for joint risk LBW/SGA, REI ID "
            f"{constants.LBWSGA_REI_ID}. Got rei_id {settings.rei_id}"
        )

    cause_metadata = io_utils.get(root_dir, constants.CacheContents.CAUSE_METADATA)

    exposure = io_utils.get_location_specific(
        root_dir, constants.LocationCacheContents.EXPOSURE, location_id=location_id
    )
    global_exposure = exposure.query(f"location_id == {GLOBAL_LOCATION_ID}")
    exposure = exposure.query(f"location_id != {GLOBAL_LOCATION_ID}")

    # Pull RR
    me_ids = io_utils.get(root_dir, constants.CacheContents.MODEL_VERSIONS)
    rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA)

    # Assumption check: LBW/SG has one cause in it's RR metadata
    if len(rr_metadata) != 1:
        raise RuntimeError(
            f"Expected exactly one row in rr_metadata for LBW/SG, got {len(rr_metadata)}:\n"
            f"{rr_metadata}"
        )

    rr_model_version_id = me_ids.query("draw_type == 'rr'")["model_version_id"].iat[0]
    rr_is_year_specific = (rr_metadata["year_specific"] == 1).bool()
    rr = ihme_cc_risk_utils.get_rr_draws(
        rei_id=settings.rei_id,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
        year_id=settings.year_id if rr_is_year_specific else None,
        location_id=location_id,
        model_version_id=rr_model_version_id,
    ).drop(
        columns=[
            "model_version_id",
            "metric_id",
            "modelable_entity_id",
            "location_id",
            "exposure",
        ]
    )

    # Child risk PAFs
    lbw_paf = _calculate_child_paf(
        constants.LOW_BIRTH_WEIGHT_REI_ID, rr, exposure, global_exposure, settings.n_draws
    )
    sg_paf = _calculate_child_paf(
        constants.SHORT_GESTATION_REI_ID, rr, exposure, global_exposure, settings.n_draws
    )

    # Calculate joint risk PAF as categorical PAF
    rr = _add_tmrel_indicator(settings.rei_id, rr)
    joint_paf = math.calculate_categorical_paf(rr, exposure)

    # Split PAFs for all three risks into child causes, keeping parent cause
    lbw_paf = subcause_split_utils.split_and_append_subcauses(
        settings=settings,
        paf_df=lbw_paf,
        mediator_rei_id=settings.rei_id,
        cause_metadata=cause_metadata,
    ).assign(rei_id=constants.LOW_BIRTH_WEIGHT_REI_ID)
    sg_paf = subcause_split_utils.split_and_append_subcauses(
        settings=settings,
        paf_df=sg_paf,
        mediator_rei_id=settings.rei_id,
        cause_metadata=cause_metadata,
    ).assign(rei_id=constants.SHORT_GESTATION_REI_ID)
    joint_paf = subcause_split_utils.split_and_append_subcauses(
        settings=settings,
        paf_df=joint_paf,
        mediator_rei_id=settings.rei_id,
        cause_metadata=cause_metadata,
    )

    # Save
    io_utils.write_paf_for_lbwsg_child_risk(
        df=lbw_paf,
        root_dir=root_dir,
        location_id=location_id,
        rei_id=constants.LOW_BIRTH_WEIGHT_REI_ID,
    )
    io_utils.write_paf_for_lbwsg_child_risk(
        df=sg_paf,
        root_dir=root_dir,
        location_id=location_id,
        rei_id=constants.SHORT_GESTATION_REI_ID,
    )
    io_utils.write_paf(df=joint_paf, root_dir=root_dir, location_id=location_id)


def _calculate_child_paf(
    rei_id: int,
    rr: pd.DataFrame,
    exposure: pd.DataFrame,
    global_exposure: pd.DataFrame,
    n_draws: int,
) -> pd.DataFrame:
    """Calculate child PAFs for LBW/SG."""
    collapsed_rr, collapsed_exp = ihme_cc_risk_utils.collapse_lbwsg_rr_and_exposure(
        rei_id=rei_id,
        rr=rr,
        exposure=exposure,
        global_exposure=global_exposure,
        n_draws=n_draws,
    )
    collapsed_rr = _add_tmrel_indicator(rei_id, collapsed_rr)
    return math.calculate_categorical_paf(collapsed_rr, collapsed_exp)


def _add_tmrel_indicator(rei_id: int, df: pd.DataFrame) -> pd.DataFrame:
    """Adds TMREL indicator column to df dependent on rei_id.

    TMRELs:
        * LBW/SG: 40+ weeks and 4000 g
        * LBW: 3500 g
        * SG: 38 weeks

    df is expected to have a 'parameter' column. For LBW/SG, the original parameter
    categories are expected. For LBW and SG, 'parameter' is expected to represent a
    category mapped to the relevant dimension. Ie. 3500 for LBW.

    Note:
        Technically, for LBW and SG, the TMRELs are 3500+ and 38+ (including 4000 g and 40
        weeks). However, the RR for both categories for all demographics is guaranteed to
        be 1, so they're equivalent, and we only expect a single category marked as the TMREL
        within the categorical PAF calculation.
    """
    df = df.assign(tmrel=0)

    if rei_id == constants.LBWSGA_REI_ID:
        tmrel_category = PARAMETER_MAP.query(
            f"preterm == {LBWSGA_SG_TMREL} & lbw == {LBWSGA_LBW_TMREL}"
        )["parameter"].iat[0]
        df.loc[df["parameter"] == tmrel_category, "tmrel"] = 1
    elif rei_id == constants.LOW_BIRTH_WEIGHT_REI_ID:
        df.loc[df["parameter"] == LBW_TMREL, "tmrel"] = 1
    elif rei_id == constants.SHORT_GESTATION_REI_ID:
        df.loc[df["parameter"] == SG_TMREL, "tmrel"] = 1
    else:
        raise ValueError(f"Cannot add TMREL indicator for unknown rei_id: {rei_id}")

    return df
