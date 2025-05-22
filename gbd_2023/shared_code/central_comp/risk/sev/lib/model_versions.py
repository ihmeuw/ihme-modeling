from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import orm

import db_queries
import db_tools_core
import ihme_cc_risk_utils
from gbd import conn_defs

from ihme_cc_sev_calculator.lib import constants

RELEVANT_DRAW_TYPES: List[str] = ["exposure", "exposure_sd", "rr", "tmrel"]


def get_rei_mes_and_rr_metadata(
    rei_ids: List[int], release_id: int
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get REI MEs and RR metadata for all requested rei_ids.

    Returned REI MEs dataframe mimics the one returned by
    ihme_cc_risk_utils.get_rei_me_ids with two differences. It has a med_id column,
    like rr_metadata, that represents the mediator REI ID. This med_id column is the same
    as the rei_id column in get_rei_me_ids. The returned rei_id column allows filtering
    to all rows relevant to a risk, including two-stage mediation inputs.

    This function is intended to be called at the beginning of a SEV Calculator run.
    Functions that pull exposure means and draws do not accept model_version_id as an
    argument. Therefore, exposure models pulled during the run cannot be specified to
    as specific version. Instead, the best model versions will be pulled at runtime. This
    introduces an unlikely race condition that if a modeler updates their best exposure model
    between the start of a SEV run and said exposure model being pulled, we will have recorded
    the wrong exposure model version.

    I am ok with this race condition for the time being. In the future, we could consider
    storing the exposure model versions that are pulled during the run and updating this
    metadata if any exposure model versions change.

    Returns:
        Two dataframes, 1) ME metadata (me_ids) with columns: rei_id, med_id,
        modelable_entity_id, modelable_entity_name, model_type, draw_type, exp_categ,
        model_version_id; 2) RR metadata (rr_metadata) with columns: rei_id, med_id,
        cause_id, source, relative_risk_type_id, year_specific, model_version_id.
    """
    rei_ids_without_metadata = (
        # Risks w/o RR shouldn't be passed in but just in case
        constants.NO_RR_REI_IDS
        # We don't use RR/exposure/TMREL models for risks with custom RRmax
        + constants.CUSTOM_RR_MAX_REI_IDS
        # LBW/SG chid risks don't have ME metadata; they use the parent risk's
        + [constants.SHORT_GESTATION_REI_ID, constants.LOW_BIRTH_WEIGHT_REI_ID]
    )
    rei_ids_with_metadata = [
        rei_id for rei_id in rei_ids if rei_id not in rei_ids_without_metadata
    ]
    # We require LBW/SGA RR metadata if any AIR_PM_MEDIATED_BY_LBWSG_REI_IDS are being run.
    if (
        len(set(constants.AIR_PM_MEDIATED_BY_LBWSG_REI_IDS).intersection(rei_ids)) > 0
        and constants.LBWSGA_REI_ID not in rei_ids_with_metadata
    ):
        rei_ids_with_metadata += [constants.LBWSGA_REI_ID]

    all_me_ids = []
    all_rr_metadata = []
    with db_tools_core.session_scope(conn_defs.EPI) as session:
        for rei_id in rei_ids_with_metadata:
            me_ids = ihme_cc_risk_utils.get_rei_me_ids(
                rei_id=rei_id, release_id=release_id, session=session
            ).query(f"draw_type.isin({RELEVANT_DRAW_TYPES})")

            if me_ids.empty:
                raise RuntimeError(f"No MEs found for rei_id {rei_id}.")

            # All relevant MEs should have best model versions
            no_best_model_versions = me_ids.query("model_version_id.isna()")
            if not no_best_model_versions.empty:
                raise RuntimeError(
                    f"rei_id {rei_id} is missing best model versions:\n"
                    f"{no_best_model_versions}"
                )

            all_rr_metadata.append(
                ihme_cc_risk_utils.get_relative_risk_metadata(
                    rei_id=rei_id, release_id=release_id, me_ids=me_ids, session=session
                )
            )

            # Add on relevant anemia prevalence MEs, used in SEVs for PAFs of 1
            if rei_id == constants.IRON_DEFICIENCY_REI_ID:
                me_ids = pd.concat(
                    [me_ids, _get_anemia_prevalence_me_ids(release_id, session)]
                )

            # Edit me_ids to follow rei_id/med_id pattern like rr_metadata.
            # This way we can find all relevant rows for a risk after combining them together
            me_ids = me_ids.rename(columns={"rei_id": "med_id"}).assign(rei_id=rei_id)
            all_me_ids.append(me_ids)

    all_me_ids = (
        pd.concat(all_me_ids)
        .sort_values(by=["rei_id", "med_id", "draw_type"])
        .reset_index(drop=True)
    )[
        [
            "rei_id",
            "med_id",
            "modelable_entity_id",
            "modelable_entity_name",
            "model_type",
            "draw_type",
            "exp_categ",
            "model_version_id",
        ]
    ]
    all_rr_metadata = (
        pd.concat(all_rr_metadata)
        .sort_values(by=["rei_id", "med_id", "cause_id"])
        .reset_index(drop=True)
    )

    return (all_me_ids, all_rr_metadata)


def _get_anemia_prevalence_me_ids(release_id: int, session: orm.Session) -> pd.DataFrame:
    """Get relevant anemia prevalence ME IDs, formatted like get_rei_me_ids result.

    Returns:
        Anemia prevalence ME metadata with columns: rei_id, modelable_entity_id,
        modelable_entity_name, model_type, draw_type, exp_categ, model_version_id
    """
    anemia_me_ids = db_queries.get_best_model_versions(
        entity="modelable_entity",
        ids=[constants.MODERATE_ANEMIA_ME_ID, constants.SEVERE_ANEMIA_ME_ID],
        release_id=release_id,
        session=session,
    )

    if len(anemia_me_ids) != 2:
        raise RuntimeError(
            "Expected exactly two rows of best anemia model versions. Received "
            f"{len(anemia_me_ids)}:\n{anemia_me_ids}"
        )

    # Adapt get_best_model_versions dataframe to get_rei_me_ids result
    me_ids_cols = [
        "rei_id",
        "modelable_entity_id",
        "modelable_entity_name",
        "model_type",
        "draw_type",
        "exp_categ",
        "model_version_id",
    ]
    return anemia_me_ids.assign(
        rei_id=constants.IRON_DEFICIENCY_REI_ID,
        model_type="epi",
        draw_type="prevalence",
        exp_categ=np.NaN,
    )[me_ids_cols]
