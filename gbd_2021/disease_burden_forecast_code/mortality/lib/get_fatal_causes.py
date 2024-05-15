import pandas as pd
from fhs_lib_database_interface.lib import db_session
from fhs_lib_database_interface.lib.constants import (
    CauseConstants,
    DimensionConstants,
    FHSDBConstants,
)
from fhs_lib_database_interface.lib.query import cause
from fhs_lib_database_interface.lib.strategy_set import strategy

from fhs_pipeline_mortality.lib.make_hierarchies import (
    include_up_hierarchy,
    make_hierarchy_tree,
)

ROOT_CAUSE_ID = 294
CAUSE_COLUMNS_OF_INTEREST = [
    DimensionConstants.ACAUSE,
    DimensionConstants.CAUSE_ID,
    DimensionConstants.PARENT_ID_COL,
    DimensionConstants.LEVEL_COL,
]


def get_fatal_causes_df(gbd_round_id: int) -> pd.DataFrame:
    """Get the fatal cause dataframe for the input ``gbd_round_id``.

    Returns:
        pd.DataFrame: A dataframe containing only fatal causes and the following columns:
            'acause', 'cause_id', 'parent_id', 'level'
    """
    cause_hierarchy = cause.get_cause_hierarchy(gbd_round_id=gbd_round_id)
    all_causes = cause_hierarchy.copy(deep=True)[CAUSE_COLUMNS_OF_INTEREST]

    fatal_subset = _get_fatal_subset(
        gbd_round_id=gbd_round_id,
        cause_hierarchy=cause_hierarchy,
        all_causes=all_causes,
    )

    # Subset the all-causes dataframe to just the fatal cause IDs
    fatal_causes_df = all_causes[all_causes.cause_id.isin(fatal_subset)]

    _recode_cause_parents(fatal_causes_df)

    return fatal_causes_df


def _get_fatal_subset(
    gbd_round_id: int,
    cause_hierarchy: pd.DataFrame,
    all_causes: pd.DataFrame,
) -> list[int]:
    with db_session.create_db_session(FHSDBConstants.FORECASTING_DB_NAME) as session:
        # Pull fatal cause IDs
        fatal_cause_ids = strategy.get_cause_set(
            session=session,
            strategy_id=CauseConstants.FATAL_GK_STRATEGY_ID,
            cause_set_id=CauseConstants.FHS_CAUSE_SET_ID,
            gbd_round_id=gbd_round_id,
        )[DimensionConstants.CAUSE_ID].values

    # Get the subset of fatal cause IDs
    cause_tree, node_map = make_hierarchy_tree(cause_hierarchy, ROOT_CAUSE_ID, "cause_id")
    fatal_subset = include_up_hierarchy(cause_tree, node_map, fatal_cause_ids)

    _add_special_causes_to_fatal_subset(fatal_subset, all_causes)

    return fatal_subset


def _add_special_causes_to_fatal_subset(
    fatal_subset: list[int], all_causes: pd.DataFrame
) -> None:
    """In-place update ``fatal_subset`` with special fatal causes."""
    # Isolate the fatal maternal cause IDs and add them to the running list of fatal causes
    maternal_subset = all_causes[all_causes.acause.str.startswith("maternal_")][
        "cause_id"
    ].tolist()
    fatal_subset += maternal_subset

    # Isolate the fatal CKD cause IDs and add them to the running list of fatal causes
    ckd_subset = all_causes[all_causes.acause.str.startswith("ckd_")]["cause_id"].tolist()
    fatal_subset += ckd_subset


def _recode_cause_parents(fatal_causes_df: pd.DataFrame) -> None:
    """In-place update ``fatal_causes_df`` with custom cause parent remappings."""
    # Update malaria to level 2 cause (malaria is a lot larger than the other
    # children of _ntd), will change it back to lvl3 cause as a child of _ntd
    # in stage 5.
    fatal_causes_df.loc[fatal_causes_df.acause == "malaria", "level"] = 2
    fatal_causes_df.loc[fatal_causes_df.acause == "malaria", "parent_id"] = 295
