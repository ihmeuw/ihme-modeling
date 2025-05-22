"""Class to attach and validate icd_group_ids."""

import functools
from pathlib import Path
from typing import Tuple, Union

import db_tools_core
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.icd_group_id import TRUNCATION_LEN
from crosscutting_functions.formatting-functions import formatting

from crosscutting_functions.mapping import clinical_mapping_db


class UseIcdGroups:
    """Class to attach and validate icd_group_ids."""

    def _validate_zero_group(
        self, icd_mart_df: pd.DataFrame, threshold: float = 0.005
    ) -> None:
        """The icd_group_id 0 is a placeholder for all unexpected icd_group_code values which
        should allow the ICD group lookup to be versionless which means we don't need to
        re-create ICD-mart data each time a new map is created. In order to support this we
        place all edge-case ICD codes into group id 0. This validation ensures that these
        0 codes are not too common

        Args:
            icd_mart_df: A dataframe to validate
            threshold: failure threshold, eg the rate of 0 coding above which
                to fail. Defaults to 0.005.

        Raises:
            RuntimeError: If the rate of icd_group_id == 0 is above the threshold.
        """

        icd_mart_df = self.attach_icd_group(icd_mart_df=icd_mart_df)
        group_zero = len(icd_mart_df.query("icd_group_id == 0"))

        zero_rate = group_zero / len(icd_mart_df)
        if zero_rate > threshold:
            raise RuntimeError("The rate of icd_group_id == 0 is occuring too frequently.")

    def _fill_missing_ids(self, icd_mart_df: pd.DataFrame) -> pd.DataFrame:
        """Impute unmapped group codes to 0 in case they're ever added to the map.

        Arguments:
            icd_mart_df: The dataframe with the ICD codes and code systems.

        Returns:
            A dataframe with the icd_group_id.
        """
        icd_mart_df["icd_group_id"] = icd_mart_df["icd_group_id"].fillna(0).astype(int)
        return icd_mart_df

    def _create_icd_group_code(self, icd_mart_df: pd.DataFrame) -> pd.DataFrame:
        """Slices/truncates the cause code column down to three digits.

        Arguments:
            icd_mart_df: The dataframe with the ICD codes and code systems.

        Returns:
            A dataframe with the icd_group_code.
        """
        icd_mart_df["icd_group_code"] = icd_mart_df["cause_code"].str[0:TRUNCATION_LEN]
        return icd_mart_df

    @functools.lru_cache(maxsize=1)
    def get_icd_group_lookup(self) -> None:
        """Pull a DataFrame of ICD groups stored in the DATABASE production database."""
        with db_tools_core.session_scope("DATABASE") as scoped_session:
            self.icd_groups = db_tools_core.query_2_df(
                query="QUERY", session=scoped_session
            )

    # pull the cause code map and cache it
    @functools.lru_cache(maxsize=1)
    def _pull_icg_map(self, map_version: int) -> None:
        """Pull the ICD-ICG map.

        Arguments:
            map_version: The version of the clinical mapping to use.
        """
        # pull the ICD-ICG map, used in both cases.
        self.icg_icd = clinical_mapping_db.get_clinical_process_data(
            "cause_code_icg", map_version
        )
        self.icg_icd = self.icg_icd[self.icg_icd["code_system_id"].isin([1, 2])]

        # just in case
        self.icg_icd["cause_code"] = formatting.sanitize_diagnoses(self.icg_icd["cause_code"])
        self.icg_icd["cause_code"] = self.icg_icd["cause_code"].str.upper()

    # pull the bundle map and cache it
    @functools.lru_cache(maxsize=1)
    def _pull_bundle_map(self, map_version: int) -> None:
        """Pull the bundle-ICD map."""
        self.full_bundle_map = clinical_mapping_db.get_clinical_process_data(
            "icg_bundle", map_version
        )

    # pull the bundle map and cache it
    @functools.lru_cache(maxsize=1)
    def _pull_clinical_map(
        self, etiology: str, etiology_id: int, map_version: int
    ) -> pd.DataFrame:
        """Returns either a mapping from ICD to ICG or ICD to Bundle for a single ID.

        Arguments:
            etiology: Either "bundle" or "icg".
            etiology_id: The ID of the etiology.
            map_version: The version of the clinical mapping to use.

        Raises:
            ValueError: If the etiology is not "bundle" or "icg".

        Returns:
            A dataframe with the ICD codes and code systems.
        """

        # pull the ICD-ICG map, used in both cases.
        self._pull_icg_map(map_version)

        if etiology == "bundle":
            # make the bundle-ICD map
            self._pull_bundle_map(map_version)
            bundle_map = self.full_bundle_map.query(f"bundle_id == {etiology_id}")
            bundle_icd = bundle_map.merge(self.icg_icd, how="left")
            return bundle_icd
        elif etiology == "icg":
            return self.icg_icd.copy()
        else:
            raise ValueError("Only bundle and ICG etiologies are supported.")

    def _remove_helper_cols(
        self, icd_mart_df: pd.DataFrame, col_to_attach: str
    ) -> pd.DataFrame:
        """Removes the columns that were used to attach the icd_group_id or code_system_id.

        Three columns present in the lookup table and generally we only need to propogate one.

        Arguments:
            icd_mart_df: The dataframe with the ICD codes and code systems.
            col_to_attach: The column to attach to the dataframe.

        Returns:
            A dataframe with the icd_group_id or code_system_id.
        """

        remove_cols = ["icd_group_code"]
        if col_to_attach == "icd_group_id":
            remove_cols.append("code_system_id")
        elif col_to_attach == "code_system_id":
            remove_cols.append("icd_group_id")
        return icd_mart_df.drop(remove_cols, axis=1)

    def _get_icd_group_id(self, etiology_icd: pd.DataFrame) -> Tuple[int, ...]:
        """Get the ICD group IDs for a df mapping icg_id or bundle_id to three-digit ICD code.

        Arguments:
            etiology_icd: A dataframe with the ICD codes and code systems.

        Returns:
            A tuple of the ICD group IDs.
        """

        self.get_icd_group_lookup()
        etiology_icd = etiology_icd.merge(
            self.icd_groups,
            how="left",
            on=["icd_group_code", "code_system_id"],
            validate="1:1",
        )

        return tuple(etiology_icd.icd_group_id.sort_values().unique())

    def get_bundle_group_ids(self, bundle_id: int, map_version: int) -> Tuple[int, ...]:
        """Get all three-digit icd_group_ids mapped to a bundle.

        Arguments:
            bundle_id: The bundle ID.
            map_version: The version of the clinical mapping to use.

        Returns:
            A tuple of the ICD group IDs.
        """

        bundle_icd = self._pull_clinical_map(
            etiology="bundle", etiology_id=bundle_id, map_version=map_version
        )

        bundle_icd = self._create_icd_group_code(bundle_icd)

        bundle_icd = (
            bundle_icd[["icd_group_code", "code_system_id", "bundle_id"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        return self._get_icd_group_id(bundle_icd)

    def attach_icd_group(
        self, icd_mart_df: pd.DataFrame, remove_helper_cols: bool = True
    ) -> pd.DataFrame:
        """Attach an icd_group_id column.

        Use the icd_group lookup table to attach an icd_group_id column onto the data using
        three-digit icd_group_code (created by this method) and code system id. This can be
        applied to new data sources in order to attach the icd_group_id col for partitioning.

        Arguments:
            icd_mart_df: The dataframe with the ICD codes and code systems.
            remove_helper_cols: Whether to remove the helper columns. Defaults to True.

        Returns:
            A dataframe with the icd_group_id.
        """

        icd_mart_df = self._create_icd_group_code(icd_mart_df)

        # get a map to ICD group id
        self.get_icd_group_lookup()

        icd_mart_df = icd_mart_df.merge(
            self.icd_groups,
            how="left",
            on=["icd_group_code", "code_system_id"],
            validate="m:1",
        )

        icd_mart_df = self._fill_missing_ids(icd_mart_df)

        if remove_helper_cols:
            pass
        return icd_mart_df

    def _validate_icd_group_id_merge(self, icd_mart_df: pd.DataFrame) -> None:
        """Ensure the first three digits of the cause code and the group code match for all
        non-null ICD group ids.

        Arguments:
            icd_mart_df: The dataframe with the ICD codes and code systems.

        Raises:
            ValueError: If the first three digits of the cause code and the group code do not
                match.
        """
        not_null_cond = "icd_mart_df['icd_group_id'].notnull()"
        if not (
            icd_mart_df[eval(not_null_cond)]["cause_code"].str[0:TRUNCATION_LEN]
            == icd_mart_df[eval(not_null_cond)]["icd_group_code"]
        ).all():
            raise ValueError(
                "All three-digit truncated ICDs must match b/w data and group codes."
            )

    def attach_code_system(
        self, icd_mart_df: pd.DataFrame, remove_helper_cols: bool = True
    ) -> pd.DataFrame:
        """Attach an icd_group_code column and code_system_id column.

        Sort of the inverse of attach_icd_group. Use the icd_group lookup table to attach
        an icd_group_code column and code_system_id column to the data.

        Arguments:
            icd_mart_df: The dataframe with the ICD codes and code systems.
            remove_helper_cols: Whether to remove the helper columns. Defaults to True.

        Returns:
            A dataframe with the icd_group_code and code_system_id.
        """

        self.get_icd_group_lookup()

        icd_mart_df = icd_mart_df.merge(self.icd_groups, how="left", validate="m:1")
        self._validate_icd_group_id_merge(icd_mart_df)
        icd_mart_df = self._fill_missing_ids(icd_mart_df)

        if remove_helper_cols:
            icd_mart_df = self._remove_helper_cols(icd_mart_df, "code_system_id")

        return icd_mart_df

    def get_icg_group_ids(self, icg_id: int, map_version: int) -> Tuple[int, ...]:
        """Get all three-digit icd_group_ids mapped to the ICG.

        Arguments:
            icg_id: The ICG ID.
            map_version: The version of the clinical mapping to use.

        Returns:
            A tuple of the ICD group IDs.
        """

        icg_icd = self._pull_clinical_map(
            etiology="icg", etiology_id=icg_id, map_version=map_version
        ).query(f"icg_id == {icg_id}")

        icg_icd = self._create_icd_group_code(icg_icd)

        icg_icd = (
            icg_icd[["icd_group_code", "code_system_id"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        return self._get_icd_group_id(icg_icd)

    def get_icd_mart_data(
        self, file_path: str, etiology: str, etiology_id: int, map_version: int
    ) -> Union[pd.DataFrame, str]:
        """Pull ICD-mart data from single level of partitioning.

        Path file_path must be a directory with subdirs of partitioned ICD-mart data.

        Note: After the data is pulled using this, you would still need to map and filter it
        to subset to just what's relevant for a given ICG or bundle.

        Arguments:
            file_path: Parent file path for a set of ICD-mart data.
            etiology: Disease type of results to pull, either "icg" or "bundle".
            etiology_id: ID corresponding to the selected etiology.
            map_version: A map version to determine which ICG ids to pull.

        Returns:
            A dataframe of ICD-mart data corresponding to a bundle/ICG and map version.
        """

        if etiology == "bundle":
            icd_group_ids_to_pull = self.get_bundle_group_ids(
                bundle_id=etiology_id, map_version=map_version
            )
        elif etiology == "icg":
            icd_group_ids_to_pull = self.get_icg_group_ids(
                icg_id=etiology_id, map_version=map_version
            )

        df_list = []
        for icd_group_id in icd_group_ids_to_pull:
            icd_files = "FILEPATH"
            for file in icd_files:
                tmp = pd.read_parquet(file)
                tmp["icd_group_id"] = icd_group_id
                df_list.append(tmp)
        if len(df_list) == 0:
            return "Nothing is returned."
        else:
            icd_mart_df = pd.concat(df_list, sort=False, ignore_index=True)
            icd_mart_df = self.attach_code_system(icd_mart_df)
            return icd_mart_df
