from typing import Any, List, Optional
import pandas as pd
import numpy as np

from db_tools import ezfuncs
from gbd.constants import decomp_step
from gbd.decomp_step import decomp_step_from_decomp_step_id, decomp_step_id_from_decomp_step
from rules import ResearchAreas, Rules, RulesManager, Tools


def get_best_model_versions(
    gbd_round_id: int, decomp_step_id: Optional[int] = None
) -> pd.DataFrame:
    """
    Returns list of best model versions. Pre-GBD 2019 not supported.

    Raises:
        ValueError:
            - if round is less than GBD 2019
    """
    if gbd_round_id == 6:
        return _get_best_mvid_2019(gbd_round_id, decomp_step_id)
    elif gbd_round_id < 6:
        raise ValueError("gbd_round_id is pre-2019 and is no longer maintained.")
    else:
        return _get_best_mvid(gbd_round_id, decomp_step_id)


def _get_best_mvid(gbd_round_id: int, decomp_step_id: int) -> pd.DataFrame:
    """
    Returns list of best model versions for GBD 2020- runs.
    """
    new_cause_ids = decomp_exempt_cause_ids = []
    mvid_list = _get_combined_mvid_list(gbd_round_id, decomp_step_id)

    _validate_model_versions(
        new_cause_ids, decomp_exempt_cause_ids, decomp_step_id, mvid_list
    )

    mvid_list = mvid_list.loc[
        (mvid_list.cause_id.isin(new_cause_ids + decomp_exempt_cause_ids))
        | (mvid_list.decomp_step_id == decomp_step_id)
    ]

    return mvid_list[["modelable_entity_id", "model_version_id", "decomp_step_id"]]


def _get_best_mvid_2019(gbd_round_id: int, decomp_step_id: int) -> pd.DataFrame:
    """
    Returns list of best model versions for GBD2019 runs.
    """
    mvid_list = _get_combined_mvid_list(gbd_round_id, decomp_step_id)

    all_ntd_cause = [
        346,
        347,
        348,
        349,
        350,
        351,
        352,
        353,
        354,
        355,
        356,
        357,
        358,
        359,
        360,
        361,
        362,
        363,
        364,
        365,
        405,
        843,
        935,
        936,
    ]

    ntd_decomp_me = [
        1500,
        1503,
        10402,
        1513,
        1514,
        1515,
        2999,
        3109,
        20265,
        1516,
        1517,
        1518,
        3001,
        3110,
        20266,
        1519,
        1520,
        1521,
        3000,
        3139,
        3111,
        20009,
        2797,
        1474,
        1469,
        2965,
        1475,
        10524,
        10525,
        1476,
        2966,
        1470,
        1471,
        10480,
        1477,
        10537,
        1472,
        1468,
        1466,
        1473,
        1465,
        16393,
        1478,
    ]

    new_gbd_2019_cause = [
        1004,
        1005,
        1006,
        1007,
        1008,
        1009,
        1010,
        1011,
        1012,
        1013,
        1014,
        1015,
        1016,
        1017,
        628,
    ]

    has_decomp_version = mvid_list.loc[
        mvid_list.decomp_step_id == decomp_step_id
    ].modelable_entity_id.tolist()
    use_iterative = mvid_list.loc[
        ~mvid_list.modelable_entity_id.isin(ntd_decomp_me)
        & mvid_list.cause_id.isin(all_ntd_cause + new_gbd_2019_cause)
        & ~mvid_list.modelable_entity_id.isin(has_decomp_version)
    ].modelable_entity_id.tolist()
    mvid_list = mvid_list.loc[
        (mvid_list.decomp_step_id == decomp_step_id)
        | (mvid_list.modelable_entity_id.isin(use_iterative))
    ]
    return mvid_list[["modelable_entity_id", "model_version_id", "decomp_step_id"]]


def _get_combined_mvid_list(gbd_round_id: int, decomp_step_id: int) -> pd.DataFrame:
    """
    Returns the best model entities for the round for both iterative & current
    decomp step.
    """
    mvid_list = ezfuncs.query(
        """
            SELECT
                cause.cause_id, mv.modelable_entity_id,
                mv.model_version_id, mv.decomp_step_id
            FROM epi.model_version mv
            LEFT JOIN epi.modelable_entity_cause cause
                ON mv.modelable_entity_id = cause.modelable_entity_id
            WHERE
                mv.model_version_status_id = :best
                and mv.gbd_round_id = :gbd_round_id
                and mv.decomp_step_id IN (:decomp_step_id, :iterative)
            """,
        conn_def="epi",
        parameters={
            "best": 1,
            "gbd_round_id": gbd_round_id,
            "decomp_step_id": decomp_step_id,
            "iterative": decomp_step_id_from_decomp_step(
                step=decomp_step.ITERATIVE, gbd_round_id=gbd_round_id
            ),
        },
    )
    return mvid_list


def _get_new_cause_ids(decomp_step_id: int) -> List[int]:
    """
    Gets the new cause IDs for the round.
    """
    rules_manager = RulesManager(
        research_area=ResearchAreas.EPI, tool=Tools.DISMOD_MR, decomp_step_id=decomp_step_id
    )
    return rules_manager.get_rule_value(rule=Rules.NEW_CAUSES_TO_ROUND) or []


def _get_decomp_exempt_cause_ids(decomp_step_id: int) -> List[int]:
    """
    Gets the decomp exempt cause IDs for the round.
    """
    rules_manager = RulesManager(
        research_area=ResearchAreas.EPI, tool=Tools.DISMOD_MR, decomp_step_id=decomp_step_id
    )
    return rules_manager.get_rule_value(rule=Rules.EXEMPT_CAUSES_IN_ROUND) or []


def _validate_model_versions(
    new_cause_ids: List[int],
    decomp_exempt_cause_ids: List[int],
    decomp_step_id: int,
    mvid_list: pd.DataFrame,
) -> None:
    """
    Confirm new and exempt causes don't have model versions for self.decomp_step_id, only for iterative.
    """
    new_cause_models = mvid_list[mvid_list.cause_id.isin(new_cause_ids)]
    decomp_exempt_models = mvid_list[mvid_list.cause_id.isin(decomp_exempt_cause_ids)]
    decomp_step = decomp_step_from_decomp_step_id(decomp_step_id)

    if decomp_step_id in new_cause_models.decomp_step_id.tolist():
        raise RuntimeError(
            f"""A {decomp_step} model was run for a new cause, this
                should not be possible and requires investigation. New
                causes with {decomp_step} models are:
                {new_cause_models[
                    new_cause_models.decomp_step_id == decomp_step_id
                ].cause_id.tolist()}"""
        )

    if decomp_step_id in decomp_exempt_models.decomp_step_id.tolist():
        raise RuntimeError(
            f"""A {decomp_step} model was run for a decomp exempt 
                cause, this should not be possible and requires investigation. 
                Decomp exempt causes with {decomp_step} models are:
                {decomp_exempt_models[
                    decomp_exempt_models.decomp_step_id == decomp_step_id
                ].cause_id.tolist()}"""
        )
