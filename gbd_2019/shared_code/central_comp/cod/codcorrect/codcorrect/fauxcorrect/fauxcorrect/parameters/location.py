import pandas as pd
from typing import Dict, List, Optional

from db_queries import get_location_metadata
from db_tools.ezfuncs import query
import gbd.constants as gbd
from hierarchies.dbtrees import loctree
import hierarchies.tree

from fauxcorrect.queries.queries import Location
from fauxcorrect.utils import constants
from fauxcorrect.validations.queries import one_row_returned


class LocationParameters:
    """
    Location parameters represent data structures that contain the relevant
    location metadata from the shared database that are used in a CoD- or
    FauxCorrect run.

    Location parameters are specific to the provided location_set_id. Two common
    location_set_ids are used in CoDCorrect to validate that all of the most-
    detailed locations are present in the input draws and for use in location
    aggregation: id 35 'Model Results' and id 40 'SDI'.


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
            location_set_id: int=constants.LocationSetId.OUTPUTS,
            gbd_round_id: int=gbd.GBD_ROUND_ID
    ):
        """
        Create an instance of Location Parameters for a CoD or Faux-Correct run.

        Arguments:
            location_set_id (int): informs which group of locations this
                parameter details.
            gbd_round_id (int): defaults to the current GBD round
        """

        self.set_id: int = location_set_id
        self.gbd_round_id: int = gbd_round_id

        # Define our connection to shared
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
        gbd_round_id.

        Returns:
            location_set_version_id (int)

        Raises:
            RuntimeError - if the query returns more or less than a single row.
        """

        params: Dict[str, int] = {
            'location_set_id': self.set_id, 'gbd_round_id': self.gbd_round_id
        }
        results: pd.DataFrame = query(
            Location.GET_VERSION_ID,
            conn_def=self.conn_def,
            parameters=params
        )
        if not one_row_returned(results):
            raise RuntimeError(
                "Query for location_set_version_id with location_set_id: "
                f"{self.set_id} and gbd_round_id: {self.gbd_round_id} "
                "returned more than one location_set_version_id or no "
                "location_set_version_id. Returned: "
                f"{results.location_set_version_id.tolist()}."
            )
        return results.at[0, constants.Columns.LOCATION_SET_VERSION_ID]

    def _get_hierarchy(self) -> pd.DataFrame:
        hierarchy_cols: List[str] = [
            constants.Columns.LOCATION_ID, constants.Columns.LOCATION_NAME,
            constants.Columns.LEVEL, constants.Columns.PARENT_ID,
            constants.Columns.SORT_ORDER, constants.Columns.MOST_DETAILED
        ]
        hierarchy: pd.DataFrame = get_location_metadata(
            location_set_version_id=self.set_version_id,
            gbd_round_id=self.gbd_round_id
        )
        return hierarchy[hierarchy_cols]

    def _get_tree(self) -> hierarchies.tree.Tree:
        return loctree(
            location_set_version_id=self.set_version_id,
            gbd_round_id=self.gbd_round_id
        )
