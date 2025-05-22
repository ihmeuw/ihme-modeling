import pandas as pd
from typing import Dict, List, Optional

import db_queries
from hierarchies.dbtrees import loctree
import hierarchies.tree

from codcorrect.legacy.utils import constants


class LocationParameters:
    """
    Location parameters represent data structures that contain the relevant
    location metadata from the shared database that are used in a CoDCorrect run.

    Properties
    ----------

        location_ids (List[int]): a list of all location_ids in the provided
            location_set_id.

        most_detailed_ids (List[int]): a list of all the most-detailed
            location_ids.

        hierarchy (pd.DataFrame): a representation of the location hierarchy
            from the location_hierarchy_history table. Useful for grabbing the
            official location_name, level, most_detailed, and parent_id of the
            location hierarchy.

        tree (hierarchies.tree.Tree): the tree representation of the location
            hierarchy for use in location aggregation.
    """
    def __init__(
            self,
            release_id: int,
            location_set_id: int
    ):
        """
        Create an instance of Location Parameters for a CoDCorrect run.

        Arguments:
            location_set_id (int): informs which group of locations this
                parameter details.
            release_id (int): defaults to the current release
        """

        self.set_id: int = location_set_id
        self.release_id: int = release_id

        self.conn_def: str = constants.ConnectionDefinitions.COD

        # Initialize attributes derived from input arguments
        self._set_version_id: Optional[int] = None
        self._hierarchy: Optional[pd.DataFrame] = None
        self._location_ids: Optional[List[int]] = None
        self._most_detailed_ids: Optional[List[int]] = None
        self._tree: Optional[hierarchies.tree.Tree] = None

    @property
    def set_version_id(self) -> int:
        if self._set_version_id is None:
            self._set_version_id = self._get_set_version_id()
        return self._set_version_id

    @property
    def hierarchy(self) -> pd.DataFrame:
        if self._hierarchy is None:
            self._hierarchy = self._get_hierarchy()
        return self._hierarchy

    @property
    def tree(self) -> hierarchies.tree.Tree:
        if self._tree is None:
            self._tree = self._get_tree()
        return self._tree

    @property
    def location_ids(self) -> List[int]:
        if self._location_ids is None:
            self._location_ids = self.hierarchy.location_id.tolist()
        return self._location_ids

    @property
    def most_detailed_ids(self) -> List[int]:
        if self._most_detailed_ids is None:
            self._most_detailed_ids = self.hierarchy.query(
                'most_detailed == 1'
            ).location_id.tolist()
        return self._most_detailed_ids

    def _get_set_version_id(self) -> int:
        """
        Returns the location_set_version_id for the given location_set_id and
        release_id.

        Returns:
            location_set_version_id (int)
        """
        location_set_version_id = db_queries.api.internal.get_active_location_set_version(
            location_set_id=self.set_id, release_id=self.release_id
        )
        return location_set_version_id

    def _get_hierarchy(self) -> pd.DataFrame:
        hierarchy_cols: List[str] = [
            constants.Columns.LOCATION_ID, constants.Columns.LOCATION_NAME,
            constants.Columns.LEVEL, constants.Columns.PARENT_ID,
            constants.Columns.SORT_ORDER, constants.Columns.MOST_DETAILED
        ]
        hierarchy: pd.DataFrame = db_queries.get_location_metadata(
            location_set_id=self.set_id,
            location_set_version_id=self.set_version_id,
            release_id=self.release_id
        )
        return hierarchy[hierarchy_cols]

    def _get_tree(self) -> hierarchies.tree.Tree:
        return loctree(
            location_set_version_id=self.set_version_id,
            release_id=self.release_id
        )
