"""Functions used by Mortality stage 2 and stage 3.

to set up the aggregation hierarchy and the
cause set for ARIMA to operate on.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import pandas as pd


class EntityNode(object):
    """Initialize an EntityNode object.

    Args:
        entity_id (int): id for the node to be associated with
        children (Optional): Child EntityNodes or None if the node is a leaf

    """

    def __init__(self, entity_id: int, children: Optional[List[EntityNode]] = None) -> None:
        """Initialize an EntityNode object."""
        self.entity_id = entity_id
        self.children = children or []


def make_hierarchy_tree(
    entity_hierarchy: pd.DataFrame, root_id: int, id_col_name: str
) -> Tuple[EntityNode, Dict]:
    """Converts a pandas dataframe hierarchy into a tree.

    Args:
        entity_hierarchy (pd.DataFrame): the pandas dataframe hierarchy.
        root_id (int): id of the root in the hierarchy.
        id_col_name (str): name of the column in the dataframe that the hierarchy is being set
            up on.

    Returns:
        Tuple: root node and dictionary representation of the tree.
    """
    hier_indexed = entity_hierarchy.set_index("parent_id").sort_index()
    root = EntityNode(root_id)
    node_queue = [root]
    node_map = dict(root_id=root)
    while node_queue:
        curr = node_queue.pop()
        try:
            children = pd.Series(hier_indexed.loc[curr.entity_id][id_col_name]).values
        except KeyError:
            continue  # leaf node!

        for child in children:
            if child == curr.entity_id:
                continue
            child_node = EntityNode(child)
            curr.children.append(child_node)
            node_map[child] = child_node
            node_queue.append(child_node)
    return root, node_map


def include_up_hierarchy(root: EntityNode, node_map: Dict, entity_ids: List[int]) -> List:
    """Filter the hierarchy tree.

    Include node if any of its descendents are in the entity ids.

    Generally speaking, we would like to include the filtering by [include/exclude] if
    [any/all] [ancestors/descendents] are in the entity ids.

    For now this does include if any descendents are in the strategy set.

    Args:
        root (EntityNode): An entity node, with some children or maybe no children.
        node_map (Dict): Dictionary representation of the tree
        entity_ids (List[int]): List of entity ids to filter with

    Returns:
        List: List of entity ids

    """
    for entity_id in entity_ids:
        node_map[entity_id].mark = True
    subset = []
    _include_up_hierarchy(root, subset)
    return subset


def _include_up_hierarchy(node: EntityNode, subset: List) -> bool:
    """Mark nodes or recurse on children.

    Mark nodes True, if they are markable, otherwise recurse on children to add them to the
    hierarchy before adding self to the hierarchy.

    Args:
        node (EntityNode): Entity node to include.
        subset (List): The subset of enity ids in the hierarchy.

    Returns:
        bool: Whether the node has been included or not.

    """
    if hasattr(node, "mark"):
        subset.append(node.entity_id)
        return True

    else:
        include = False
        for child in node.children:
            if _include_up_hierarchy(child, subset):
                include = True
        if include:
            node.mark = True
            subset.append(node.entity_id)
        return include
