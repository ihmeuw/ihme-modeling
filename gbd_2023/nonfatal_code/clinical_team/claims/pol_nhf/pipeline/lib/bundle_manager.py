from typing import List

import pandas as pd
from crosscutting_functions.clinical_constants.shared.mapping import CODE_SYSTEMS
from crosscutting_functions.clinical_db_tools.db_connector.database import Database
from crosscutting_functions.mapping import clinical_mapping_db


def get_single_code_bundles(code_system_ids: List[int], map_version: int) -> List[int]:
    """
    Fetch bundles that map to one code system id defined by the client
    """
    single_code_bundles = single_code_system_bundles(map_version)
    return single_code_bundles[
        single_code_bundles.code_system_id.isin(code_system_ids)
    ].bundle_id.tolist()


def single_code_system_bundles(map_version: int) -> pd.DataFrame:
    """
    Fetch bundles that map to one code system id
    """
    icg_bundle = clinical_mapping_db.get_clinical_process_data(
        table="icg_bundle", map_version=map_version, prod=True
    )
    cause_code_bundle = clinical_mapping_db.get_clinical_process_data(
        table="cause_code_icg", map_version=map_version, prod=True
    )
    bundle_map = icg_bundle.merge(cause_code_bundle, on=["icg_id", "map_version"])
    bundle_map = bundle_map[bundle_map.code_system_id.isin(list(CODE_SYSTEMS.values()))]
    bundle_code_sys_df = (
        bundle_map.groupby(["bundle_id"])["code_system_id"].nunique().reset_index(name="vals")
    )
    single_code_bundles = bundle_code_sys_df[bundle_code_sys_df.vals == 1].bundle_id.tolist()
    bundle_map = (
        bundle_map[bundle_map.bundle_id.isin(single_code_bundles)][
            ["bundle_id", "code_system_id"]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return bundle_map


def get_active_bundles(map_version: int) -> List[int]:
    """
    Pull active bundles for a given map_version
    """
    active_bundle_metadata = clinical_mapping_db.get_active_bundles(map_version)
    bundles = active_bundle_metadata.bundle_id.unique().tolist()
    return bundles


def get_all_bundles(map_version: int) -> List[int]:
    """
    Pull all bundles from clinical_mapping db
    """
    icg_bundle = clinical_mapping_db.get_clinical_process_data(
        table="icg_bundle", map_version=map_version, prod=True
    )
    bundles = icg_bundle.bundle_id.unique().tolist()
    return bundles


def get_exclusion_bundles(map_version: int) -> List[int]:
    """Some bundles are active and in the map, but are not processed. Instead,
    thay have a database relationship to another bundle. Bundles such as these will
    be found via get_active_bundles() but break pipeline validations due to not having
    any icd_groups associated with them.
    """
    db = Database()
    db.load_odbc(odbc_profile="SERVER")

    query_string = f"""QUERY"""
    active_bundles = get_active_bundles(map_version=map_version)
    process_bundles = db.query(query_string).bundle_id.to_list()

    inactive_bundles = set(process_bundles) - set(active_bundles)
    active_process_bundles = set(process_bundles) - set(inactive_bundles)
    exclude_bundles = set(active_bundles) - set(active_process_bundles)

    return list(exclude_bundles)
