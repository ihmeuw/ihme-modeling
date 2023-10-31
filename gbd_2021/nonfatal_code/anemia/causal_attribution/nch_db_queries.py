from typing import List, Optional, Union
import pandas as pd
import numpy as np

from db_tools.ezfuncs import query
from gbd import constants as gbd
from gbd import gbd_round as gbd_round_helpers
from gbd.decomp_step import (validate_decomp_step,
                             decomp_step_id_from_decomp_step)
from gbd.gbd_round import validate_gbd_round_id
from rules.enums import ResearchAreas, Tools
from test_support.profile_support import profile

from db_queries.lib.validation import general as general_validation
from db_queries.lib.validation import rules as rules_validation

def get_best_model_information_modelable_entity(me_ids: Union[int, List[int]],
                                                gbd_round_id:int = gbd.GBD_ROUND_ID,
                                               decomp_step:str = None) -> pd.DataFrame:
    """
    Used to get a dataset of the best model version with its associated crosswalk version
    and bundle id for a given list of modelable entities. If there is no best model for
    the given id(s), gbd_round, decomp_step, and status, an empty data frame will be returned.

    Args:
        me_ids: list of modelable entity ids to be queried.  Will accept a single int or
            list of ints.
        gbd_round_id: Defaults to current GBD round.
        decomp_step: Decomposition step. Allowed values are None,
            'iterative', 'step1', 'step2', 'step3', 'step4', 'step5',
            or 'usa_re'. Defaults to None, but must not be None if
            gbd_round_id >= 6.

    Returns:
        Dataframe of metadata including model version id, date bested, and
        model description with one row for each id passed.

    Raises:
        ValueError:
            User must pass a valid gbd_round_id.
            User must pass a valid decomp_step for the given GBD round.    """
    gbd_round_helpers.validate_gbd_round_id(gbd_round_id=gbd_round_id)
    research_area = ResearchAreas.EPI
    rules_validation.validate_step_viewable(
        research_area, Tools.SHARED_FUNCTIONS, decomp_step, gbd_round_id
    )
    id_list = list(np.atleast_1d(me_ids))
    if not id_list:
        raise ValueError(
            "Must pass at least one modelable entity id.")

    query_str= """
        SELECT
            mv.modelable_entity_id, mv.decomp_step_id, mv.model_version_id,
            mv.crosswalk_version_id, mv.bundle_id
        FROM
            epi.model_version mv
        WHERE
            mv.modelable_entity_id in :id_list
            AND
            mv.gbd_round_id = :gbd_round_id
            AND
            mv.decomp_step_id = :decomp_step_id
            AND
            mv.model_version_status_id =1
        ;
        """
    results = query(query_str, conn_def='epi', dispose=True,
               parameters={'id_list': id_list,
                           'decomp_step_id': decomp_step_id_from_decomp_step(
                                decomp_step, gbd_round_id=gbd_round_id),
                           'gbd_round_id': gbd_round_id})
    return results

def get_bundle_xwalk_version_for_best_model(me_id: int,
        gbd_round_id:int = gbd.GBD_ROUND_ID, decomp_step:str = None):
    """
    Used to get a tulpe of the bundle id and crosswalk version id associated
    with the bested model for a given modelable entity id for a gbd round
    and decomp step.

    Args:
        me_id: modelable entity id to be queried.
        gbd_round_id: Defaults to current GBD round.
        decomp_step: Decomposition step. Allowed values are None,
            'iterative', 'step1', 'step2', 'step3', 'step4', 'step5',
            or 'usa_re'. Defaults to None, but must not be None if
            gbd_round_id >= 6.

    Returns:
        Tuple of bundle_id and crosswalk_version_id

    Raises:
        ValueError:
            User must pass a valid gbd_round_id.
            User must pass a valid decomp_step for the given GBD round."""
    model_information = get_best_model_information_modelable_entity(me_id,
                            gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    if len(model_information) < 1:
        raise LookupError(f"Model doesn't have a bested model for parameters")

    model_information = model_information.squeeze()
    return model_information['bundle_id'], model_information['crosswalk_version_id']
    
