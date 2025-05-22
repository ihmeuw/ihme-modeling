"""Functions related to location aggregation for ST-GPR."""

import logging
from typing import List

import numpy as np
import pandas as pd
from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema
from aggregator import aggregators, operators
from draw_sources import draw_sources, io
from gbd.constants import metrics
from hierarchies import dbtrees
from hierarchies import tree as hierarchies_tree
from stgpr_helpers import columns, parameters

from stgpr.lib import utils
from stgpr.lib.constants import location


def aggregate_locations(
    stgpr_version_id: int, data: pd.DataFrame, data_in_model_space: bool
) -> pd.DataFrame:
    """Aggregate ST-GPR estimates from the country level up to global.

    For counts, country-level estimates are summed up, applying regional scalars to region
    totals. For rates, sums are population-weighted.

    Args:
        stgpr_version_id: ID of the ST-GPR version
        data: data to aggregate locations for
        data_in_model_space: True if the data is in modeling space and should be
            converted to normal space from aggregation and back into modeling space
            afterword. False if data is already in normal space and should stay in
            that space.
    """
    # Avoid mutating original data set
    data = data.copy()

    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)
    params = file_utility.read_parameters()
    # Pull the hierarchy used for location aggregation
    location_hierarchy = file_utility.read_aggregation_location_hierarchy()

    index_cols = [col for col in columns.DEMOGRAPHICS if col != columns.LOCATION_ID]
    value_cols = utils.get_value_cols(data=data, params=params)

    # Transform out of modeling space if needed
    if data_in_model_space:
        data = _transform_data(data, stgpr_version_id, reverse=True, value_cols=value_cols)

    # Set up in-memory draw source and sink. Cache needs to be defined separately as both
    # source and sink point to same object. Otherwise, draw_sink is essentially ignored
    cache = {"data": data}
    draw_source = draw_sources.DrawSource(
        {"draw_dict": cache, "name": "data"}, read_func=io.mem_read_func
    )
    draw_sink = draw_sources.DrawSink(
        {"draw_dict": cache, "name": "data"}, write_func=io.mem_write_func
    )

    # For counts, sum. For rates, weighted sum using population
    if params[parameters.METRIC_ID] == metrics.NUMBER:
        operator = operators.Sum(index_cols=index_cols, value_cols=value_cols)

        settings = stgpr_schema.get_settings()
        with db_tools_core.session_scope(settings.mortality_db_conn_def) as session:
            regional_scalars = _get_all_regional_scalars(
                location_hierarchy, params[parameters.RELEASE_ID], session
            )

        # Regional scalars MUST be applied within aggregation as aggregation
        # works from the bottom of the location hierarchy to the top, applying
        # the sink transforms to each level as we go. This is the only way
        # the scaled region data correctly goes into super region and global estimates
        draw_sink.add_transform(_apply_regional_scalars, regional_scalars, value_cols)
    else:
        operator = operators.WtdSum(
            index_cols=index_cols,
            value_cols=value_cols,
            weight_df=file_utility.read_population(),
            weight_name=columns.POPULATION,
            merge_cols=columns.DEMOGRAPHICS,
        )

    agg = aggregators.AggSynchronous(
        draw_source, draw_sink, index_cols, columns.LOCATION_ID, operator, chunksize=2
    )

    trees = dbtrees.loctree(
        location_set_id=params[parameters.LOCATION_SET_ID],
        release_id=params[parameters.RELEASE_ID],
        return_many=params[parameters.LOCATION_SET_ID] == location.SDI_LOCATION_SET_ID,
    )

    # Pull country location ids to prune out subnationals
    country_ids = location_hierarchy.loc[
        location_hierarchy[columns.LEVEL] == location.NATIONAL_LEVEL, columns.LOCATION_ID
    ]

    logging.info(
        f"Aggregating locations, location_set_id: {params[parameters.LOCATION_SET_ID]}"
    )
    for tree in np.atleast_1d(trees):
        # Prune out subnationals. trees.Tree objects have similar methods `prune` and
        # `remove_child_nodes` but they do not properly clean up after themselves
        for location_id in country_ids:
            _prune(tree, tree.get_node_by_id(location_id))

        agg.run(tree, draw_filters={})

    # Transform back into model space if needed and return
    if data_in_model_space:
        return _transform_data(
            draw_source.content(), stgpr_version_id, reverse=False, value_cols=value_cols
        )
    else:
        return draw_source.content()


def _prune(
    tree: hierarchies_tree.Tree, node: hierarchies_tree.Node, only_prune_children: bool = True
) -> None:
    """Prune (delete) a node from the tree.

    We need to recurse to bottom of tree to delete all child nodes and then we delete
    the node itself (if not skipped). If only_prune_children is True, the children field
    of the given node is set to an empty list.

    Arguments:
        tree: a hierarchies.tree.Tree object
        node: a hierarchies.tree.Node object to delete from the tree
        only_prune_children: True if only the children of the given node should be deleted,
            not the node itself. Defaults to False
    """
    # First, delete children
    for child in node.children:
        _prune(tree, child, only_prune_children=False)

    # Then delete node itself from tree (if not skipped)
    if not only_prune_children:
        tree.nodes = [n for n in tree.nodes if n.id != node.id]

        # Delete note from list of its parent's children (if it has a parent)
        if node.parent:
            node.parent.children = [n for n in node.parent.children if n.id != node.id]
    else:
        # Set children to empty, representing no children
        node.children = []


def _transform_data(
    data: pd.DataFrame, stgpr_version_id: int, reverse: bool, value_cols: List[str]
) -> pd.DataFrame:
    """Transform data into or out of modeling space.

    Original dataset is mutated.
    """
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as session:
        transform = db_stgpr.get_stgpr_version(
            stgpr_version_id, session, fields=[columns.TRANSFORM_TYPE], epi_best=False
        )[columns.TRANSFORM_TYPE]

    for col in value_cols:
        data[col] = stgpr_helpers.transform_data(data[col], transform, reverse=reverse)

    return data


def _get_all_regional_scalars(
    hierarchy: pd.DataFrame, release_id: int, session: orm.Session
) -> pd.DataFrame:
    """Fetch all regional scalars from the mortality database.

    Arguments:
        hierarchy: location hierarchy
        release_id: Release ID for the ST-GPR run.
        session: Session with the mortality database.

    Returns:
        DataFrame containing year_id (int), location_id (int), and mean (float)
    """
    regional_scalars = db_tools_core.query_2_df(
        """
        SELECT
            run_id,
            year_id,
            location_id,
            mean as scalar
        FROM
            mortality.upload_population_scalar_estimate
        WHERE
            run_id = (
                SELECT run_id
                FROM mortality.process_version pv
                JOIN mortality.release_process_version rpv USING(proc_version_id)
                WHERE pv.process_id = 23  -- population scalar estimate
                AND rpv.is_best = 1  -- marked best
                AND rpv.release_id = :release_id
            )
        """,
        session=session,
        parameters={parameters.RELEASE_ID: release_id},
    )

    if regional_scalars.empty:
        raise RuntimeError(
            f"No best regional population scalars were found for release {release_id}. "
            "Cannot produce count-space location aggregations. Please submit a help desk "
            "ticket."
        )

    # TODO: consider saving this information in stgpr.stgpr_version
    logging.info(f"Using regional population run id {regional_scalars['run_id'].iat[0]}")

    # Filter to regions in location hierarchy
    region_ids = hierarchy.loc[hierarchy[columns.LEVEL] == 2, columns.LOCATION_ID]

    return regional_scalars[regional_scalars[columns.LOCATION_ID].isin(region_ids)].drop(
        "run_id", axis=1
    )


def _apply_regional_scalars(
    data: pd.DataFrame, regional_scalars: pd.DataFrame, value_cols: List[str]
) -> pd.DataFrame:
    """
    Transform to apply regional scalars to draws.

    Apply scalars where applicable, multiply by 1.0 when scalars aren't present.
    """
    return (
        pd.merge(
            data, regional_scalars, how="left", on=[columns.YEAR_ID, columns.LOCATION_ID]
        )
        .fillna(1.0)
        .pipe(_apply_regional_scalars_helper, value_cols=value_cols)
        .drop("scalar", axis=1)
    )


def _apply_regional_scalars_helper(data: pd.DataFrame, value_cols: List[str]) -> pd.DataFrame:
    """Helper function that multiplies draw columns by a regional scalar."""
    for col in value_cols:
        data[col] = data[col].mul(data["scalar"], axis=0)

    return data
