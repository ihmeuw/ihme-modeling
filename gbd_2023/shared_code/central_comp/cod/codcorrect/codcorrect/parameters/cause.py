import logging
import pandas as pd
import networkx as nx
from typing import Dict, List, Optional, Union
import warnings

import db_queries
import db_tools_core
import gbd.constants as gbd
from hierarchies.dbtrees import causetree
import hierarchies.tree

from codcorrect.lib.db.queries import Cause
from codcorrect.legacy.utils import constants
from codcorrect.lib.validations import data_validations

logger = logging.getLogger(__name__)


class CauseParameters:
    """
    Cause parameters represent data structures that contain the relevant cause
    metadata from the shared database that are used in a CoDCorrect
    run.

    Cause parameters are cause_set specific for the provided release_id.
    Two cause_set_ids are used in CoDCorrect: id 1 'CoDCorrect', used for
    rescaling down the cause hierarchy to fit the All Cause Mortality Envelope,
    and id 3 'Reporting', used to aggregate back up the cause hierarchy to
    produce.


    Properties
    ----------

        cause_ids (List[int]): a list of all cause_ids associated with the
            cause_set_id.

        hierarchy (pd.DataFrame): a representation of the cause hierarchy from
            the cause_hierarchy_history table. Useful for grabbing the official
            acause, level, most_detailed, and parent_id of the cause hierarchy.

        metadata (pd.DataFrame): a representation of the cause_metadata_history
            table for the provided cause_set and release_id.

        metadata_dict (dict): a record of cause-specific metadata.
            Key: cause_id, value: dict(Keys: sex_id, age_start, age_end).

        most_detailed_ids (List[int]): a list of all most detailed cause_ids
            associated with this parameters configuration.

        tree (hierarchies.tree.Tree): the tree representation of the cause
            hierarchy for use in cause aggregation.


    Methods
    -------

        get_cause_specific_metadata - returns a record of cause metadata--valid
            sex_ids, age_group_start, and age_group_end ids--from the provided
            cause_id as a dictionary.

    """

    def __init__(
            self,
            cause_set_id: int,
            release_id: int,
    ):
        """
        Create an instance of Cause Parameters for a CoDCorrect run.

        Arguments:
            cause_set_id (int): informs which group of causes this parameter
                details.
            release_id (int): The release ID for the run
        """
        self.set_id: int = cause_set_id
        self.release_id: int = release_id

        # Define our connection to cod
        self.conn_def: str = constants.ConnectionDefinitions.COD

        # Initialize attributes derived from input arguments
        self._set_version_id: Optional[int] = None
        self._hierarchy: Optional[pd.DataFrame] = None
        self._cause_ids: Optional[List[int]] = None
        self._metadata_version_id: Optional[int] = None
        self._metadata: Optional[pd.DataFrame] = None
        self._metadata_dataframe: Optional[pd.DataFrame] = None
        self._metadata_dict: Optional[dict] = None
        self._most_detailed_ids: Optional[List[int]] = None
        self._tree: Optional[hierarchies.tree.Tree] = None

        # Fields that aren't private
        self.correction_exclusion_map: pd.DataFrame = self._get_correction_exclusion_map()

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
    def cause_ids(self) -> List[int]:
        if self._cause_ids is None:
            self._cause_ids = self.hierarchy.cause_id.unique().tolist()
        return self._cause_ids

    @property
    def metadata_version_id(self) -> int:
        if self._metadata_version_id is None:
            self._metadata_version_id = self._get_metadata_version_id()
        return self._metadata_version_id

    @property
    def metadata(self) -> pd.DataFrame:
        if self._metadata is None:
            self._metadata = self._get_metadata()
        return self._metadata

    @property
    def metadata_dataframe(self) -> pd.DataFrame:
        if self._metadata_dataframe is None:
            self._metadata_dataframe = self._get_metadata_dataframe()
        return self._metadata_dataframe

    @property
    def metadata_dict(self) -> dict:
        if self._metadata_dict is None:
            self._metadata_dict = self._get_metadata_dict()
        return self._metadata_dict

    @property
    def most_detailed_ids(self) -> List[int]:
        if self._most_detailed_ids is None:
            self._most_detailed_ids = self.hierarchy.query(
                'most_detailed == 1'
            ).cause_id.tolist()
        return self._most_detailed_ids

    @property
    def tree(self) -> hierarchies.tree.Tree:
        if self._tree is None:
            self._tree = causetree(
                cause_set_version_id=self.set_version_id,
                release_id=self.release_id
            )
        return self._tree

    def _get_set_version_id(self) -> int:
        """
        Returns the cause_set_version_id for the given cause_set_id and
        release_id.

        Returns:
            cause_set_version_id (int)
        """
        cause_set_version_id = db_queries.api.internal.get_active_cause_set_version(
            cause_set_id=self.set_id, release_id=self.release_id
        )
        return cause_set_version_id

    def _get_metadata_version_id(self) -> int:
        """
        Returns the cause_metadata_version_id for the given
        cause_set_version_id.

        Returns:
            cause_metadata_version_id (int)

        Raises:
            RuntimeError - if the query returns more or less than a single row.
        """
        params = {'cause_set_version_id': self.set_version_id}
        with db_tools_core.session_scope(self.conn_def) as session:
            results = db_tools_core.query_2_df(
                Cause.GET_METADATA_VERSION_ID,
                parameters=params,
                session=session,
            )

        if not data_validations.one_row_returned(results):
            raise RuntimeError(
                "Query for cause_metadata_version using cause_set_version_id: "
                f"{self.set_version_id} returned more than one "
                "cause_metadata_version_id or no cause_metadata_version_id. "
                f"Returned: {results.cause_metadata_version_id.tolist()}."
            )
        return int(results.at[0, constants.Columns.CAUSE_METADATA_VERSION_ID])

    def _get_metadata(self) -> pd.DataFrame:
        """
        Returns the cause_metadata_history as a dataframe.
        """
        params = {
            'cause_metadata_version_id': self.metadata_version_id,
            'valid_cause_ids': self.cause_ids
        }
        with db_tools_core.session_scope(self.conn_def) as session:
            return db_tools_core.query_2_df(
                Cause.GET_METADATA,
                parameters=params,
                session=session,
            )

    def _get_metadata_dict(self) -> Dict[int, Dict[str, int]]:
        """
        Convert metadata dataframe to dictionary with cause_id's as keys and
        sex_id, age_start, and age_end as the fields of each cause's sub-dict.
        """
        _cols_to_keys = {
            'female': constants.Columns.SEX_ID,
            'male': constants.Columns.SEX_ID,
            'yll_age_group_id_start': constants.Columns.AGE_START,
            'yll_age_group_id_end': constants.Columns.AGE_END,
            'yld_only': constants.Columns.YLD_ONLY
        }
        # initialize metadata dictionary
        result_dict = {
            cause_id: {} for cause_id in
            self.metadata.cause_id.unique()
        }
        # iterate over each metadata row
        for _, row in self.metadata.iterrows():
            row_dict = row.to_dict()
            cause_id = row_dict[constants.Columns.CAUSE_ID]
            metadata_type = row_dict[constants.Columns.CAUSE_METADATA_TYPE]
            metadata_value = row_dict[constants.Columns.CAUSE_METADATA_VALUE]
            # if the row is relevant to validation, process it
            if metadata_type in _cols_to_keys:
                metadata_key = _cols_to_keys[metadata_type]
                # standardize metadata values - find the appropriate sex_id for
                # male and female columns or convert age_group columns to int.
                metadata_value = _standardize_metadata_value(
                    metadata_type=metadata_type,
                    metadata_value=metadata_value
                )
                # add metadata key/value to cause metadata dictionary
                if metadata_key not in result_dict[cause_id]:
                    result_dict[cause_id][metadata_key] = metadata_value
                else:  # only sex_id could already exist in the dictionary
                    result_dict[cause_id][metadata_key].extend(metadata_value)
                    # prune sex-specific metadata
                    result_dict[cause_id][metadata_key] = [
                        value for value in result_dict[cause_id][metadata_key]
                        if value > 0
                    ]
        return result_dict

    def _get_hierarchy(self) -> pd.DataFrame:
        hierarchy_cols = [
            constants.Columns.CAUSE_ID, constants.Columns.ACAUSE,
            constants.Columns.LEVEL, constants.Columns.PARENT_ID,
            constants.Columns.SORT_ORDER, constants.Columns.MOST_DETAILED,
            constants.Columns.IS_ESTIMATE_COD, constants.Columns.SHOCK_CAUSE,
            constants.Columns.EXCLUDED_FROM_CORRECTION,
        ]
        hierarchy = db_queries.get_cause_metadata(
            release_id=self.release_id,
            cause_set_id=self.set_id,
            cause_set_version_id=self.set_version_id
        )
        self._validate_hierachy(hierarchy)
        return hierarchy[hierarchy_cols]

    def get_cause_specific_metadata(self, cause_id: int) -> Dict[str, int]:
        """
        Returns the metadata record for the provided cause_id.

        Arguments:
            cause_id (int): the id with which to grab the metadata record.

        Returns:
            The record of the provided cause's valid sex_ids, age_start, and
            age_end.

        Raises:
            RuntimeError if the provided cause is not found in the collection
            of this cause parameter's metadata.
        """
        if cause_id not in self.cause_ids:
            raise RuntimeError(
                f"Cause_id: {cause_id} was not found in our list of cause_ids "
                f"for this parameter's cause_set: {self.set_id}."
            )
        return self.metadata_dict.get(cause_id)

    def _get_metadata_dataframe(self) -> pd.DataFrame:
        """
        Converts metadata dictionary to dataframe with cause_id, sex_id,
        cause_age_start, and cause_age_end columns. All-cause cause_id
        is removed from the dataframe.
        """
        metadata_df = pd.DataFrame()
        for key, value in self.metadata_dict.items():
            temp = pd.DataFrame(value)
            temp.loc[:, constants.Columns.CAUSE_ID] = key
            metadata_df = pd.concat(
                [metadata_df, temp], ignore_index=True, sort=False
            )
        metadata_df = metadata_df.rename(
            columns={
                constants.Columns.AGE_START: constants.Columns.CAUSE_AGE_START,
                constants.Columns.AGE_END: constants.Columns.CAUSE_AGE_END
            }
        )
        hierarchy = self.hierarchy
        zero = hierarchy.loc[hierarchy.level == 0][
            constants.Columns.CAUSE_ID].tolist()
        metadata_df = metadata_df.loc[~metadata_df.cause_id.isin(zero)]
        return metadata_df

    def _get_correction_exclusion_map(self) -> pd.DataFrame:
        """Get the correction exclusion map.

        Used in the correction (scaling) step to determine which cause - location pairs
        are to be excluded from correction.

        Raises:
            RuntimeError iff causes not marked as 'excluded_from_correction' have
                cause-location pairs in the exclusion map
        """
        with db_tools_core.session_scope(self.conn_def) as session:
            exclusion_map = db_tools_core.query_2_df(
                Cause.GET_CORRECTION_EXCLUSION_MAP,
                 parameters={
                    "release_id": self.release_id,
                },
                session=session,
            )

        if exclusion_map.empty:
            warnings.warn(
                f"No correction exclusions found for release id {self.release_id}"
            )

        # Add on 'excluded_from_correction' metadata expected within correction
        exclusion_map[constants.Columns.EXCLUDED_FROM_CORRECTION] = 1

        # Subset exclusion map to only causes in the hierarchy
        exclusion_map = exclusion_map[
            exclusion_map[constants.Columns.CAUSE_ID].isin(
                self.hierarchy[constants.Columns.CAUSE_ID]
            )
        ]

        # If causes are in the exclusion cause/location list but not marked
        # as 'excluded_from_correction' in the hierarchy, raise an error
        excluded_cause_ids = self.hierarchy[
            self.hierarchy[constants.Columns.EXCLUDED_FROM_CORRECTION] == 1
        ][constants.Columns.CAUSE_ID].unique().tolist()
        exclusion_map_cause_ids = exclusion_map[constants.Columns.CAUSE_ID].unique().tolist()

        unexpected_excluded_cause_ids = set(exclusion_map_cause_ids) - set(excluded_cause_ids)
        if unexpected_excluded_cause_ids:
            raise RuntimeError(
                f"Found {len(unexpected_excluded_cause_ids)} unexpected cause(s) in the "
                f"correction exclusion cause/loc map: {unexpected_excluded_cause_ids}.\n"
                f"Exclusion cause/loc map: {exclusion_map_cause_ids}\n"
                f"Excluded causes: {excluded_cause_ids}"
            )

        return exclusion_map

    def _validate_hierachy(self, hierarchy) -> None:
        """
        Validate active hierarchy returned for the cause_set_id.
        """
        G = nx.DiGraph()
        errors = ""
        for index, row in hierarchy.iterrows():
            if row.cause_id == row.parent_id and row.level == 0:
                continue
            parent_df = (
                hierarchy.loc[
                    hierarchy.cause_id == row.parent_id]
            )
            if len(parent_df) > 1 or G.has_edge(row.parent_id, row.cause_id):
                errors += (
                    f'Duplicates found for cause_id {row.cause_id}, '
                    f'parent_id {row.parent_id}.\n'
                )
            elif parent_df.empty:
                errors += (
                    f'The hierarchy is missing information for parent_id '
                    f'{row.parent_id}.\n'
                )
            else:
                parent_level = parent_df.level.iat[0]
                edge_weight = row.level - parent_level
                G.add_edge(
                    u_of_edge=row.parent_id,
                    v_of_edge=row.cause_id,
                    weight=edge_weight
                )
                if edge_weight != 1:
                    errors += (
                        f'A child cause should be 1 level below its parent. '
                        f'parent_id {row.parent_id} has level {parent_level}. '
                        f'cause_id {row.cause_id} has level {row.level}.\n'
                    )
        if (
            self.set_id != constants.CauseSetId.REPORTING_AGGREGATES
            and
            not nx.is_tree(G)
        ):
            errors += (
                'The hierarchy is not a tree. nx.is_tree() '
                'returned False\n'
            )

        if errors:
            msg = (
                f"There are issues with the hierarchy for cause_set_id "
                f"{self.set_id}, cause_set_version_id "
                f"{self.set_version_id}\n{errors}"
            )
            logger.error(msg)
            raise RuntimeError(msg)


def _standardize_metadata_value(
        metadata_type: str,
        metadata_value: str
) -> Union[int, List[int]]:
    """
    Convert metadata values into the appropriate integer value.

    If the metadata_type is a sex_id, it returns a one element list of the
    sex's id value. If it is any other type of metadata, it returns that value
    as an integer.
    """
    metadata_value = int(metadata_value)
    if metadata_type in ['female', 'male']:
        if metadata_value == 1:
            return [gbd.sex.__getattribute__(metadata_type.upper())]
        # if the cause is sex-specific and this metadata type is restricted
        return [-1]
    return metadata_value
