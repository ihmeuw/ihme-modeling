import collections
from typing import List

import fhs_lib_database_interface.lib.query.risk as query_risk
import pandas as pd
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseRiskPairConstants,
    DimensionConstants,
    FHSDBConstants,
    SexConstants,
)
from fhs_lib_database_interface.lib.fhs_lru_cache import fhs_lru_cache
from fhs_lib_database_interface.lib.query import cause, entity
from fhs_lib_database_interface.lib.query.age import get_ages
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_database_interface.lib.strategy_set import strategy
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()


@fhs_lru_cache(1)
def demographic_coords(gbd_round_id: int, years: YearRange) -> collections.OrderedDict:
    """Create and cache an OrderedDict of demographic indices.

    Args:
        gbd_round_id (int): gbd round id.
        years (YearRange): [past_start, forecast_start, forecast_end] years.

    Returns:
        OrderedDict: ordered dict of all non-draw dimensions and their
            coordinates.
    """
    location_ids = get_location_set(gbd_round_id=gbd_round_id).location_id.values.tolist()

    age_group_ids = get_ages()[DimensionConstants.AGE_GROUP_ID].unique().tolist()

    return collections.OrderedDict(
        [
            (DimensionConstants.LOCATION_ID, location_ids),
            (DimensionConstants.AGE_GROUP_ID, age_group_ids),
            (DimensionConstants.SEX_ID, list(SexConstants.SEX_IDS)),
            (DimensionConstants.YEAR_ID, years.years),
        ]
    )


@fhs_lru_cache(3)
def _get_precalculated_pafs(gbd_round_id: int, strategy_id: int) -> pd.DataFrame:
    """Get cause-risk pairs that have directly-modeled PAFs.

    Args:
        gbd_round_id (int): gbd round id.
        strategy_id (int): strategy ID for this particular cause-risk pair.

    Returns:
        (pd.DataFrame): spreadsheeto of the precalcualted PAF cause-risk pairs.
    """
    if gbd_round_id == 4:
        raise ValueError("GBD round ID 4 is no longer supported.")

    with db_session.create_db_session(FHSDBConstants.FORECASTING_DB_NAME) as session:
        result = strategy.get_cause_risk_pair_set(
            session=session,
            strategy_id=strategy_id,
            gbd_round_id=gbd_round_id,
        )

    # Attach acause, rei (to match the cause_id, rei_id).
    acause_cause_id_map = cause.get_acauses(result["cause_id"].unique())
    rei_rei_id_map = query_risk.get_reis(result["rei_id"].unique())
    result = result.merge(acause_cause_id_map, how="left").merge(rei_rei_id_map, how="left")

    entity.assert_cause_risk_pairs_are_named(result)

    return result


def is_pre_calculated(acause: str, rei: str, gbd_round_id: int, strategy_id: int) -> bool:
    """Return true if the cause-risk pair has a directly modeled PAF.

    Args:
        acause (str): acause.
        rei (str): risk.
        gbd_round_id (int): gbd round id.
        strategy_id (int): strategy ID for this particular cause-risk pair.

    Returns:
        (bool): whether this cause-risk pair is pre-calculated.
    """
    dm_pafs = _get_precalculated_pafs(gbd_round_id, strategy_id)
    return not dm_pafs.query("acause == @acause and rei == @rei").empty


def get_most_detailed_acause_related_risks(acause: str, gbd_round_id: int) -> List[str]:
    """Return a list of most-detailed risks contributing to certain acause.

    Args:
        acause (str): analytical cause.
        gbd_round_id (int): gbd round id.

    Returns:
        (List[str]): a list of risks associated with this acause,
                     ignoring the ones that are in_scalar = 0.
    """
    if acause in ["rotavirus"]:
        risks = ["rota"]
    else:
        df_acause_risk = entity.get_scalars_most_detailed_cause_risk_pairs(gbd_round_id)
        risks = list(df_acause_risk.query("acause == @acause")["rei"].unique())
    return risks


@fhs_lru_cache(1)
def _get_maybe_negative_paf_pairs(gbd_round_id: int) -> pd.DataFrame:
    """Get cause-risk pairs that *can* have negative PAFs.

    Args:
        gbd_round_id (int): gbd round id.

    Returns:
        (pd.DataFrame): spreadsheet of cause-risk pairs that could have
                        negative PAF.
    """
    if gbd_round_id == 4:
        raise ValueError("GBD round ID 4 is no longer supported.")

    with db_session.create_db_session(FHSDBConstants.FORECASTING_DB_NAME) as session:
        result = strategy.get_cause_risk_pair_set(
            session=session,
            strategy_id=CauseRiskPairConstants.MAYBE_NEGATIVE_PAF_SET_ID,
            gbd_round_id=gbd_round_id,
        )

    # Set only has cause_ids and rei_ids, so get acauses
    acause_cause_id_map = cause.get_acauses(result["cause_id"].unique())
    rei_rei_id_map = query_risk.get_reis(result["rei_id"].unique())
    result = result.merge(acause_cause_id_map, how="left").merge(rei_rei_id_map, how="left")

    entity.assert_cause_risk_pairs_are_named(result)

    return result


def is_maybe_negative_paf(acause: str, rei: str, gbd_round_id: int) -> bool:
    """Return true if the cause-risk pair is maybe a negative PAF.

    Args:
        acause (str): acause.
        rei (str): risk.
        gbd_round_id (int): gbd round id.

    Returns:
        (bool): whether this cause-risk pair might be protective.
    """
    maybe_negative_pafs = _get_maybe_negative_paf_pairs(gbd_round_id)
    return not maybe_negative_pafs.query("acause == @acause and rei == @rei").empty


@fhs_lru_cache(1)
def get_detailed_directly_modeled_pafs(gbd_round_id: int) -> pd.DataFrame:
    """Get the directly modeled PAFs, as cause_risk pairs.

    Args:
        gbd_round_id (int): gbd round id.

    Returns:
        (pd.DataFrame): dataframe of directly modeled acause and rei.
    """
    all_detailed_pafs = entity.get_scalars_most_detailed_cause_risk_pairs(gbd_round_id)[
        ["acause", "rei"]
    ]
    directly_modeled_pafs = _get_precalculated_pafs(
        gbd_round_id, CauseRiskPairConstants.DIRECTLY_MODELED_STRATEGY_ID
    )  # has aggregates
    detailed_dm_pafs = all_detailed_pafs.merge(directly_modeled_pafs, on=["acause", "rei"])

    return detailed_dm_pafs


def get_modeling_causes(gbd_round_id: int) -> List[str]:
    """Return the causes we are modeling.

    Args:
        gbd_round_id (int): gbd round id.

    Returns:
        (List[str]): list of all acauses for the scalars pipeline.
    """
    df_acause_risk = entity.get_scalars_most_detailed_cause_risk_pairs(gbd_round_id)
    acauses = list(df_acause_risk.acause.unique())
    return acauses
