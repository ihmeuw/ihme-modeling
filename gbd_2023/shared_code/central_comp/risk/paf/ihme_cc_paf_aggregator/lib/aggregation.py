from typing import Dict, List, Set

import networkx as nx
import numpy as np
import pandas as pd

from ihme_cc_paf_aggregator.lib import constants, utils


def aggregate_pafs(
    total_paf_df: pd.DataFrame,
    risk_hierarchy_df: pd.DataFrame,
    mediation_matrix: pd.DataFrame,
    unmediated_paf_df: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregates PAFs up a risk hierarchy.

    Invariants:
        Input data shouldn't have aggregate REIs
        Input data must contain all children REIs
        rei_id column present in input data
        at least one column present in input data that starts with 'draw'

    Arguments:
        total_paf_df: dataframe of input draws. Total PAFs are the standard output of the
            PAF calculator, produced for every risk-cause pair and uploaded using
            save_results. These PAFs represent the total estimate of direct and
            indirect risk.
        risk_hierarchy_df: dataframe of the risk hierarchy to aggregate
        mediation_matrix: dataframe that defines what risk-cause pairs are mediated
        unmediated_paf_df: dataframe of unmediated pafs. When mediation
            applies to a risk-cause pair, the PAF calculator outputs a second set of
            estimates, the unmediated PAFs, representing the portion that is due to
            exposure to the risk itself rather than mediated through exposure to
            another risk.

    Returns:
        dataframe with same schema as total_paf_df, but contains aggregate REI ids
        instead of children REI ids.
    """
    _validate_input_draws(total_paf_df, risk_hierarchy_df)
    _validate_unmediated_draws(
        unmediated_paf_df=unmediated_paf_df, mediation_matrix=mediation_matrix
    )

    id_cols, draw_cols = utils.get_index_draw_columns(total_paf_df)
    parent_child_map = _parents_and_child_leaves_of(risk_hierarchy_df)

    results = []
    for parent_rei_id, child_ids in parent_child_map.items():
        subdf = total_paf_df[total_paf_df[constants.REI_ID].isin(child_ids)]
        if subdf.empty:
            continue
        to_aggregate = _swap_total_with_unmediated_pafs(
            subdf, unmediated_paf_df, mediation_matrix, child_ids, id_cols, draw_cols
        )

        output = _aggregate_pafs(to_aggregate, parent_rei_id, id_cols, draw_cols)
        results.append(output)

    results = pd.concat(results).reset_index()[id_cols + draw_cols]
    return results


def _aggregate_pafs(
    df: pd.DataFrame, parent_rei_id: int, id_cols: List[str], draw_cols: List[str]
) -> pd.DataFrame:
    result = (
        df.groupby([c for c in id_cols if c != constants.REI_ID])
        .apply(lambda x: 1 - np.prod(1 - x[draw_cols]))
        .assign(rei_id=parent_rei_id)
    )
    return result


def _validate_input_draws(
    total_paf_df: pd.DataFrame, risk_hierarchy_df: pd.DataFrame
) -> None:
    """Validate inputs invariants.

    Shouldn't have aggregate reis in inputs.
    Shouldn't have missing children (not necessarily square though).
    """
    parent_child_map = _parents_and_child_leaves_of(risk_hierarchy_df)
    present_reis = set(total_paf_df[constants.REI_ID].unique())
    present_parents = set(parent_child_map.keys()).intersection(present_reis)
    if present_parents:
        raise RuntimeError(
            f"Input data contained parent REIs: {present_parents}.\n"
            "This can happen if the risk hierarchy doesn't treat "
            "an aggregate REI that has a PAF model as most-detailed."
        )

    child_reis = set().union(*parent_child_map.values())
    missing_children = child_reis.difference(present_reis)
    if missing_children:
        raise RuntimeError(f"Input data missing child REIs: {missing_children}")


def _validate_unmediated_draws(
    mediation_matrix: pd.DataFrame, unmediated_paf_df: pd.DataFrame
) -> None:
    """Validate unmediated invariants.

    Any risk-cause in the mediation matrix with mean_mediation = 1
        should not exist in unmediated draws
    """
    must_be_absent = set(
        mediation_matrix.query("mean_mediation == 1")
        .groupby([constants.REI_ID, constants.CAUSE_ID])
        .groups
    )
    is_present = set(unmediated_paf_df.groupby([constants.REI_ID, constants.CAUSE_ID]).groups)

    incorrectly_present = is_present.intersection(must_be_absent)
    if incorrectly_present:
        raise RuntimeError(
            f"Unmediated PAFs contain risk-causes with 100% mediation: {incorrectly_present}"
        )


def _parents_and_child_leaves_of(df: pd.DataFrame) -> Dict[int, Set[int]]:
    """Returns a map of parent_id: [child_id_0, child_id_1, ...].

    Note that children are always leaves of the tree, in this case.
    PAF aggregation is performed with most-detailed inputs. The tree
    is collapsed to 2 levels: parent-to-most-detailed-children.
    """
    parents = set(df.parent_id)
    leaves = set(df.loc[~df[constants.REI_ID].isin(parents), constants.REI_ID])
    graph = nx.from_pandas_edgelist(
        df, constants.REI_ID, constants.PARENT_ID, create_using=nx.DiGraph
    )

    parents_to_leaves = {}
    for parent_id in parents:
        children = nx.ancestors(graph, parent_id)
        leaf_children = {int(c) for c in children if c in leaves}
        parents_to_leaves[parent_id] = leaf_children

    return parents_to_leaves


def _swap_total_with_unmediated_pafs(
    total_paf_df: pd.DataFrame,
    unmediated_paf_df: pd.DataFrame,
    mediation_matrix: pd.DataFrame,
    child_ids: Set[int],
    id_cols: List[str],
    draw_cols: List[str],
) -> pd.DataFrame:
    """
    Replace total PAF with unmediated PAF for a given child risk iff it's mediated
    by another risk that is a sibling (ie child of same parent).

    Some risks in the mediation matrix may be missing from unmediated_paf_df because they
    are definitionally 0. So we fill with 0 for those risks.

    We need to apply this logic within each demographic and cause grouping.
    """
    mediation = mediation_matrix.query("rei_id in @child_ids and med_id in @child_ids")
    if mediation.empty:
        return total_paf_df
    mediators = (
        mediation.groupby([constants.CAUSE_ID, constants.REI_ID])[constants.MED_ID]
        .apply(set)
        .to_dict()
    )

    id_df = total_paf_df[id_cols].reset_index(drop=True)
    group_cols = [
        constants.MEASURE_ID,
        constants.SEX_ID,
        constants.AGE_GROUP_ID,
        constants.LOCATION_ID,
        constants.YEAR_ID,
        constants.CAUSE_ID,
    ]
    all_risks = id_df.groupby(group_cols)[constants.REI_ID].apply(set).rename("all_risks")
    id_df = id_df.merge(all_risks.reset_index(), how="left")
    id_df["should_swap"] = False
    for (cause_id, rei_id), mediator_ids in mediators.items():
        cause_mask = id_df[constants.CAUSE_ID] == cause_id
        rei_mask = id_df[constants.REI_ID] == rei_id
        mediator_mask = id_df.all_risks.map(
            lambda x: any(x.intersection(mediator_ids))  # noqa: B023
        )
        id_df.loc[cause_mask & rei_mask & mediator_mask, "should_swap"] = True

    should_swap = id_df.reset_index().set_index(id_cols)["should_swap"]
    stays_the_same = ~should_swap

    total_paf_df = total_paf_df.set_index(id_cols)
    unmediated_paf_df = unmediated_paf_df.set_index(id_cols)
    combined = total_paf_df.where(stays_the_same, unmediated_paf_df)[draw_cols].fillna(0)
    return combined
