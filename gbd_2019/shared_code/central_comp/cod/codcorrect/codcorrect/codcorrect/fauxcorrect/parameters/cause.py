import logging
import pandas as pd
import networkx as nx
from typing import Dict, List, Optional, Union

from db_queries import get_cause_metadata
from db_tools.ezfuncs import query
import gbd.constants as gbd
from hierarchies.dbtrees import causetree
import hierarchies.tree

from fauxcorrect.queries.queries import Cause, HIV
from fauxcorrect.utils import constants
from fauxcorrect.validations.queries import one_row_returned


class CauseParameters:
    """
    Cause parameters represent data structures that contain the relevant cause
    metadata from the shared database that are used in a CoD- or FauxCorrect
    run.

    Cause parameters are cause_set specific for the provided gbd_round_id.
    Two cause_set_ids are used in CoDCorrect: id 1 'CoDCorrect', used for
    rescaling down the cause hierarchy to fit the All Cause Mortality Envelope,
    and id 3 'Reporting', used to aggregate back up the cause hierarchy to
    produce.

    Because FauxCorrect does not aggregate causes, it only uses cause set id 1.


    Properties
    ----------

        cause_ids (List[int]): a list of all cause_ids associated with the
            cause_set_id.

        decomp_cause_ids (List[int]): a list of all valid cause_ids for the
            provided decomp_step_id.

        hierarchy (pd.DataFrame): a representation of the cause hierarchy from
            the cause_hierarchy_history table. Useful for grabbing the official
            acause, level, most_detailed, and parent_id of the cause hierarchy.

        metadata (pd.DataFrame): a representation of the cause_metadata_history
            table for the provided cause_set and gbd_round_id.

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
            cause_set_id: int=constants.CauseSetId.FAUXCORRECT,
            gbd_round_id: int=gbd.GBD_ROUND_ID,
            decomp_step_id: int=gbd.decomp_step.ONE,
    ):
        """
        Create an instance of Cause Parameters for a CoD or Faux-Correct run.

        Arguments:
            cause_set_id (int): informs which group of causes this parameter
                details.
            gbd_round_id (int): defaults to the current GBD round
            decomp_step_id (int): the specific step in GBD decomposition that
                the model versions are associated.
        """
        self.set_id: int = cause_set_id
        self.gbd_round_id: int = gbd_round_id
        self.decomp_step_id: int = decomp_step_id

        # Define our connection to shared
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

        # list of valid cause ids for a given decomp step should come from
        # rules manager.
        self._decomp_cause_ids: Optional[List[int]] = None
        self._hiv_cause_ids: List[int] = self._get_hiv_cause_ids()

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
                gbd_round_id=self.gbd_round_id
            )
        return self._tree

    @property
    def hiv_cause_ids(self) -> List[int]:
        if self._hiv_cause_ids is None:
            self._hiv_cause_ids = self._get_hiv_cause_ids()[
            'cause_id'].to_list()
        return self._hiv_cause_ids

    def _get_set_version_id(self) -> int:
        """
        Returns the cause_set_version_id for the given cause_set_id and
        gbd_round_id.

        Returns:
            cause_set_version_id (int)

        Raises:
            RuntimeError - if the query returns more or less than a single row.
        """
        params: Dict[str, int] = {
            'cause_set_id': self.set_id,
            'gbd_round_id': self.gbd_round_id
        }
        results = query(
            Cause.GET_VERSION_ID,
            conn_def=self.conn_def,
            parameters=params
        )
        if not one_row_returned(results):
            raise RuntimeError(
                "Query for cause_set_version_id with cause_set_id: "
                f"{self.set_id} and gbd_round_id: {self.gbd_round_id} "
                "returned more than one cause_set_version_id or no "
                "cause_set_version_id. Returned: "
                f"{results.cause_set_version_id.tolist()}."
            )
        return int(results.at[0, constants.Columns.CAUSE_SET_VERSION_ID])

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
        results = query(
            Cause.GET_METADATA_VERSION_ID,
            conn_def=self.conn_def,
            parameters=params
        )
        if not one_row_returned(results):
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
        return query(
            Cause.GET_METADATA,
            conn_def=self.conn_def,
            parameters=params
        )

    def _get_metadata_dict(self) -> dict:
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
            constants.Columns.IS_ESTIMATE
        ]
        hierarchy = get_cause_metadata(
            cause_set_version_id=self.set_version_id,
            gbd_round_id=self.gbd_round_id
        )
        self._validate_hierachy(hierarchy)
        return hierarchy[hierarchy_cols]

    def get_cause_specific_metadata(self, cause_id: int) -> dict:
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
        zero = hierarchy.loc[hierarchy.level==0][
            constants.Columns.CAUSE_ID].tolist()
        metadata_df = metadata_df.loc[~metadata_df.cause_id.isin(zero)]
        return metadata_df

    def _get_hiv_cause_ids(self) -> pd.DataFrame:
        params = {
            'cause_set_id': self.set_id,
            constants.Columns.GBD_ROUND_ID: self.gbd_round_id}
        return query(HIV.GET_HIV_CAUSES,
                     conn_def=self.conn_def,
                     parameters=params)[
                     constants.Columns.CAUSE_ID].tolist()

    def _validate_hierachy(self, hierarchy) -> None:
        """
        Validate active hierarchy returned for the cause_set_id.

        Some of our cause hierarchies are not true trees.
        They can have more than one root node and each node may have more
        than one parent. For these reasons we can't use traditional methods
        for tree recognition and validation for all the hierarchies and 
        must create our own suite of validations to confirm the fitness of 
        the hierarchies used in the machinery run.

        This method creates a networkx digraph from the cause hierarchy
        dataframe and uses it to check for duplicates and validate tree
        structure for hierarchies other than the reporting aggregates
        hierarchy.

        The digraph may become more useful as validations are added or we can
        get rid of it entirely.
        """
        G = nx.DiGraph()
        errors = ""
        for index, row in hierarchy.iterrows():
            if row.cause_id==row.parent_id and row.level==0:
                continue
            parent_df = (
                hierarchy.loc[
                    hierarchy.cause_id==row.parent_id]
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
            logging.error(msg)
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
