"""Custom PAF calculation for Occupational Noise."""

from typing import List

import numpy as np
import pandas as pd

from db_queries import get_sequela_metadata
from gbd.constants import columns, measures
from get_draws.api import get_draws

from ihme_cc_paf_calculator.lib import constants, data_utils

_SEQUELA_SET_ID = 2


def aggregate_hearing_sequela_to_cause(
    sequela_pafs: pd.DataFrame,
    location_id: int,
    year_id: List[int],
    release_id: int,
    n_draws: int,
    como_version_id: int,
) -> pd.DataFrame:
    """Aggregate sequela-level PAFs to the cause level, replacing PAFs for hearing
    sequelae with PAFs for the hearing loss cause.

    RRs for occupational noise are modeled for sequelae. To aggregate these to cause PAFs,
    we pull both sequela and cause YLDs from COMO. We proportionally scale the sequela
    YLDs so their total is equal to the cause YLD total. This ensures that the YLDs sum
    to the cause even when there is some drift due to downsampling. Then we calculate the
    cause PAF as the YLD-weighted average of the sequela PAFs by multiplying by the
    scaled sequela YLDs, summing across sequelae, and dividing back by the cause YLDs.

    Arguments:
        sequela PAFs: a dataframe of PAFs at the sequela level, where the cause_id column
            represents a sequela_id. With columns
            rei_id/cause_id/measure_id/location_id/year_id/age_group_id/sex_id/draw_*
        location_id: the location for this job
        year_id: the list of years in this PAF calculator run
        release_id: the release of the run
        n_draws: number of draws for the run
        como_version_id: the version of COMO to use for pulling YLDs

    Returns:
        dataframe of PAFs at the cause level with columns
            rei_id/cause_id/measure_id/location_id/year_id/age_group_id/sex_id/draw_*
    """
    _, draw_cols = data_utils.get_index_draw_columns(sequela_pafs)

    sequela_pafs_long = sequela_pafs.melt(
        id_vars=[columns.CAUSE_ID] + constants.DEMOGRAPHIC_COLS,
        value_vars=draw_cols,
        var_name="draw",
        value_name="paf",
    ).rename(columns={columns.CAUSE_ID: columns.SEQUELA_ID})

    # Pull COMO YLDs for hearing sequela and find the sum across sequela
    sequela_ids = sequela_pafs[columns.CAUSE_ID].unique().tolist()
    sequela_ylds = get_draws(
        gbd_id_type=[columns.SEQUELA_ID] * len(sequela_ids),
        gbd_id=sequela_ids,
        source="como",
        location_id=location_id,
        year_id=year_id,
        age_group_id=sequela_pafs[columns.AGE_GROUP_ID].unique().tolist(),
        measure_id=measures.YLD,
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
        version_id=como_version_id,
    )
    sequela_ylds_long = sequela_ylds.melt(
        id_vars=[columns.SEQUELA_ID] + constants.DEMOGRAPHIC_COLS,
        value_vars=draw_cols,
        var_name="draw",
        value_name="sequela_yld",
    )
    sequela_ylds_long["sequela_total"] = sequela_ylds_long.groupby(
        constants.DEMOGRAPHIC_COLS + ["draw"]
    )["sequela_yld"].transform("sum")

    # Pull COMO YLDs for the hearing cause
    cause_ylds = get_draws(
        gbd_id_type="cause_id",
        gbd_id=constants.HEARING_CAUSE_ID,
        source="como",
        location_id=location_id,
        year_id=year_id,
        age_group_id=sequela_pafs[columns.AGE_GROUP_ID].unique().tolist(),
        measure_id=measures.YLD,
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
        version_id=como_version_id,
    )
    cause_ylds_long = cause_ylds.melt(
        id_vars=[columns.CAUSE_ID] + constants.DEMOGRAPHIC_COLS,
        value_vars=draw_cols,
        var_name="draw",
        value_name="cause_yld",
    )

    # Merge PAFs, sequela YLDs, and cause YLDs
    yld_df = sequela_pafs_long.merge(
        sequela_ylds_long, on=[columns.SEQUELA_ID] + constants.DEMOGRAPHIC_COLS + ["draw"]
    )
    yld_df = yld_df.merge(cause_ylds_long, on=constants.DEMOGRAPHIC_COLS + ["draw"])

    # Scale PAF by sequela/cause proportion to protect against the sequela and cause YLDs
    # drifting apart due to downsampling, which could lead to calculating PAFs outside of
    # the allowed range of (-Inf, 1]
    yld_df["sequela_yld"] = np.where(
        yld_df["sequela_total"] == 0,
        0,
        yld_df["cause_yld"] * yld_df["sequela_yld"] / yld_df["sequela_total"],
    )
    yld_df["paf"] = yld_df["paf"] * yld_df["sequela_yld"]

    # Collapse and sum PAFs over sequela
    yld_df = (
        yld_df.groupby(constants.DEMOGRAPHIC_COLS + ["draw", "cause_id", "cause_yld"])
        .sum()
        .reset_index()
    )

    # Divide by original YLDs by cause
    yld_df["paf"] = np.where(yld_df["cause_yld"] == 0, 0, yld_df["paf"] / yld_df["cause_yld"])

    # Transform wide by draw
    paf_wide = pd.pivot(
        yld_df,
        index=constants.DEMOGRAPHIC_COLS + [columns.CAUSE_ID],
        columns="draw",
        values="paf",
    ).reset_index()

    # Add back rei_id and set to YLD only
    paf_wide = paf_wide.assign(
        rei_id=constants.OCCUPATIONAL_NOISE_REI_ID, measure_id=measures.YLD
    )

    return paf_wide


def validate_hearing_sequelae(release_id: int, rr_metadata: pd.DataFrame) -> None:
    """Pull the list of most-detailed sequelae associated with the hearing loss cause.
    The "causes" in the RR model should match these sequelae. If they don't, that
    means either the hierarchy has changed or the RR model has changed, and the run
    will fail fast.
    """
    hearing_sequela_metadata = get_sequela_metadata(
        sequela_set_id=_SEQUELA_SET_ID, release_id=release_id
    ).query(f"cause_id == {constants.HEARING_CAUSE_ID} & most_detailed == 1")

    rr_sequelae = set(rr_metadata["cause_id"].tolist())
    hierarchy_sequelae = set(hearing_sequela_metadata["sequela_id"].tolist())
    if hierarchy_sequelae != rr_sequelae:
        raise ValueError(
            "The hearing loss sequelae present in the relative risk model don't match "
            "those in the sequela hierarchy.\nRelative risk contains sequelae "
            f"{sorted(rr_sequelae)}.\nHierarchy contains {sorted(hierarchy_sequelae)}.\n"
            "Please submit a ticket to Central Comp."
        )
