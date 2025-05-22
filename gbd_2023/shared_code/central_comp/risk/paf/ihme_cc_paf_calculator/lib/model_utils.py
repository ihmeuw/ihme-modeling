from dataclasses import dataclass
from typing import Optional

import pandas as pd

import ihme_cc_averted_burden
from ihme_cc_risk_utils import get_rei_me_ids
from save_results.api.internal import StagedResult, create_model_version_id

from ihme_cc_paf_calculator.lib import constants, logging_utils

logger = logging_utils.module_logger(__name__)


@dataclass
class PafModelableEntities:
    """Convenience class for named ME IDs."""

    paf: int
    paf_unmediated: Optional[int]


@dataclass
class PafStagedModelVersions:
    """Convenience class for named MV IDs."""

    paf: StagedResult
    paf_unmediated: StagedResult

    # Extra MVID slots for LBW/SG
    paf_lbw: StagedResult
    paf_sg: StagedResult

    # Extra MVID slot for unavertable
    paf_unavertable: StagedResult


def get_paf_modelable_entities(best_model_versions: pd.DataFrame) -> PafModelableEntities:
    """Get ME IDs for paf and paf_unmediated, allowing None for the latter."""
    paf_me = best_model_versions.query("draw_type == 'paf'")["modelable_entity_id"]
    if len(paf_me) != 1:
        extra = f":{paf_me.values}" if len(paf_me) > 1 else ""
        raise ValueError(
            "Expected one and only one modelable_entity_id for paf "
            f"but found {len(paf_me)}{extra}."
        )
    paf_me = int(paf_me.values[0])
    paf_unmediated_me = best_model_versions.query("draw_type == 'paf_unmediated'")[
        "modelable_entity_id"
    ]
    if len(paf_unmediated_me) > 1:
        raise ValueError(
            "Expected one or zero modelable_entity_id for paf_unmediated "
            f"but found {len(paf_unmediated_me)}: {paf_unmediated_me.values}."
        )
    if len(paf_unmediated_me) == 0:
        paf_unmediated_me = None
    else:
        paf_unmediated_me = int(paf_unmediated_me.values[0])
    return PafModelableEntities(paf=paf_me, paf_unmediated=paf_unmediated_me)


def stage_model_versions(
    modelable_entities: PafModelableEntities,
    rei_id: int,
    rei_set_id: int,
    release_id: int,
    db_env: str,
    extra_description: Optional[str],
) -> PafStagedModelVersions:
    """Create model_version_ids that will eventually be used for save_results_risk_staged.

    LBW/SG has special handling: modelers only run PAFs for the parent risk (LBW/SG), which
    also creates PAFs for both child risks (LBW, SG) as well for a total of three MVIDs
    (no mediation). Avertable REIs in the Averted Burden REI set have special handling:
    running PAFs for the Avertable counterfacutal also creates a PAF for the Unavertable
    counterfactual, a total of two MVIDs (no mediation).
    """
    extra_description = " " + extra_description if extra_description else ""

    paf: StagedResult = create_model_version_id(
        modelable_entity_id=modelable_entities.paf,
        description=constants.MODEL_DESCRIPTION.format(
            unmediated="", rei_id=rei_id, extra_description=extra_description
        ),
        release_id=release_id,
        db_env=db_env,
    )

    if modelable_entities.paf_unmediated is None:
        paf_unmediated = StagedResult(
            modelable_entity_id=None,
            model_version_id=None,
            description=f"unused PAF unmediated for rei_id {rei_id}.",
            release_id=release_id,
            db_env=db_env,
        )
    else:
        paf_unmediated: StagedResult = create_model_version_id(
            modelable_entity_id=modelable_entities.paf_unmediated,
            description=constants.MODEL_DESCRIPTION.format(
                unmediated=" unmediated", rei_id=rei_id, extra_description=extra_description
            ),
            release_id=release_id,
            db_env=db_env,
        )

    if rei_id == constants.LBWSGA_REI_ID:
        paf_lbw = create_model_version_id(
            modelable_entity_id=constants.LOW_BIRTH_WEIGHT_ME_ID,
            description=constants.MODEL_DESCRIPTION.format(
                unmediated="",
                rei_id=constants.LOW_BIRTH_WEIGHT_REI_ID,
                extra_description=extra_description,
            ),
            release_id=release_id,
            db_env=db_env,
        )
        paf_sg = create_model_version_id(
            modelable_entity_id=constants.SHORT_GESTATION_ME_ID,
            description=constants.MODEL_DESCRIPTION.format(
                unmediated="",
                rei_id=constants.SHORT_GESTATION_REI_ID,
                extra_description=extra_description,
            ),
            release_id=release_id,
            db_env=db_env,
        )
        logger.info(
            f"Created two additional model version ids: {paf_lbw.model_version_id} for low "
            f"birth weight and {paf_sg.model_version_id} for short gestation"
        )
    else:
        paf_lbw = StagedResult(
            modelable_entity_id=None,
            model_version_id=None,
            description="Unused LBW PAF",
            release_id=release_id,
            db_env=db_env,
        )
        paf_sg = StagedResult(
            modelable_entity_id=None,
            model_version_id=None,
            description="Unused SG PAF",
            release_id=release_id,
            db_env=db_env,
        )

    paf_unavertable = None
    if rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
        drug_rei_ids = ihme_cc_averted_burden.get_all_rei_ids_for_drug(rei_id)
        if rei_id == drug_rei_ids.avertable_rei_id:
            paf_unavertable_me = int(
                get_rei_me_ids(rei_id=drug_rei_ids.unavertable_rei_id, release_id=release_id)
                .query("draw_type == 'paf'")["modelable_entity_id"]
                .iat[0]
            )
            paf_unavertable = create_model_version_id(
                modelable_entity_id=paf_unavertable_me,
                description=constants.MODEL_DESCRIPTION.format(
                    unmediated="",
                    rei_id=drug_rei_ids.unavertable_rei_id,
                    extra_description=extra_description,
                ),
                release_id=release_id,
                db_env=db_env,
            )
            logger.info(
                f"Created additional model version id: {paf_unavertable.model_version_id} for "
                f"unavertable burden"
            )
    if paf_unavertable is None:
        paf_unavertable = StagedResult(
            modelable_entity_id=None,
            model_version_id=None,
            description="Unused Unavertable PAF",
            release_id=release_id,
            db_env=db_env,
        )

    return PafStagedModelVersions(
        paf=paf,
        paf_unmediated=paf_unmediated,
        paf_lbw=paf_lbw,
        paf_sg=paf_sg,
        paf_unavertable=paf_unavertable,
    )
