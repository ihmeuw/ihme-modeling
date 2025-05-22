"""
Module full of helper functions related to building lookup tables. These lookups
will be stored in a lower level dictionary form in the DIRECTORY
folder which matches the ResDac documentation

This module relates to transforming those ResDac lookups into the tables we'll
use in the CMS Schema. Review the `Lookup Tables` section of the ERD.
"""
from typing import Dict, List

import numpy as np
import pandas as pd

from cms.lookups.inputs import enrollment_type, location_type, race_ethnicity, sex_type
from cms.validations import lookup_tests


def build_cms_facility_map(fac_lookup: Dict[str, Dict[int, str]]) -> pd.DataFrame:
    """Build the cms_facility_id lookup table using the facility mappings
    outlined in the facility_type_lookup file"""

    cms_facility_list = []
    cms_facility_id = 1
    for fac_type, fac_values in fac_lookup.items():
        for key, value in fac_values.items():
            tmp = pd.DataFrame(
                {
                    "cms_facility_id": cms_facility_id,
                    "facility_code_type": fac_type,
                    "facility_code": key,
                    "facility_name": value,
                },
                index=[0],
            )

            cms_facility_list.append(tmp)
            cms_facility_id += 1

    # concat and return full table
    cms_facility_df = pd.concat(cms_facility_list, sort=False, ignore_index=True)
    if len(cms_facility_df) != cms_facility_df.cms_facility_id.unique().size:
        raise RuntimeError("Ther is a mismatch in facility count.")

    lookup_tests.validate_cms_facility_id(cms_facility_df)

    return cms_facility_df


def build_state_map(loc_lookup: Dict[int, location_type.Codes]) -> pd.DataFrame:
    """
    Build the location id table

    Parameters:
        loc_lookup (dict): location dict from lookups/
    """

    cms_locs = pd.DataFrame(
        {
            "location_id": [e for e in loc_lookup.keys()],
            "MDCR_STATE_CD": [loc_lookup[e].STATE_CD for e in loc_lookup.keys()],
            "STATE_CNTY_FIPS": [loc_lookup[e].STATE_CNTY_FIPS for e in loc_lookup.keys()],
            "MAX_STATE_CD": [loc_lookup[e].MAX_STATE_CD for e in loc_lookup.keys()],
        }
    )

    # transform from wide to long
    cms_locs = (
        pd.melt(
            cms_locs,
            id_vars="location_id",
            value_vars=["MDCR_STATE_CD", "STATE_CNTY_FIPS", "MAX_STATE_CD"],
            var_name="code_type",
            value_name="code",
        )
        .sort_values(by="location_id")
        .reset_index(drop=True)
    )

    return cms_locs


def state_map_wide(loc_lookup: Dict[int, tuple]) -> pd.DataFrame:
    loc = build_state_map(loc_lookup)
    loc = loc.pivot(index="location_id", columns="code_type", values="code")
    loc.reset_index(inplace=True)
    loc.columns.name = None

    return loc


def build_mdcr_enrollment_reason(enroll: enrollment_type = enrollment_type) -> pd.DataFrame:
    """
    Create the MDCR enrollment reason lookup table

    Parameters:
        enroll (module): enrollment_type module from lookups/
    """
    df = pd.DataFrame(
        {
            "code": [e for e in enroll.ENTLMT_RSN.keys()],
            "enrollment_type": [e for e in enroll.ENTLMT_RSN.values()],
        }
    )

    df_codes = sorted(df.code.unique().tolist())
    dict_codes = sorted(list(enroll.ENTLMT_RSN.keys()))

    if df_codes != dict_codes:
        raise ValueError("Mismatch in enrollment codes found.")

    return df


def build_max_enrollment_reason(enroll: enrollment_type = enrollment_type) -> pd.DataFrame:
    """
    Create the MAX enrollment reason lookup table

    Parameters:
        enroll (module): enrollment_type module from lookups/
    """
    size = len(enroll.MAX_ELGBLTY_CD_LTST.keys())

    df = pd.DataFrame(
        {
            "code": [e for e in np.arange(0, size)],
            "resdac_code": [e for e in enroll.MAX_ELGBLTY_CD_LTST.keys()],
            "enrollment_type": [e for e in enroll.MAX_ELGBLTY_CD_LTST.values()],
        }
    )

    return df


def build_sex_table(sex: sex_type) -> pd.DataFrame:
    """
    Create the sex variable lookup table

    Parameters:
        sex (module): sex_type module from lookups/
    """

    df = pd.DataFrame(
        {
            "sex_id": [e for e in sex.sex_lookup.keys()],
            "SEX_IDENT_CD": [sex.sex_lookup[e].SEX_IDENT_CD for e in sex.sex_lookup.keys()],
            "STATE_CNTY_FIPS": [sex.sex_lookup[e].EL_SEX_CD for e in sex.sex_lookup.keys()],
        }
    )

    return df


def build_race_map() -> pd.DataFrame:
    """Combine all race/ethnicity coding sets into 1 lookup DataFrame"""
    race_dict: Dict[str, Dict[int, str]] = race_ethnicity.race_lookup

    df_list = []
    for col_name, lookup_vals in race_dict.items():
        tmp = pd.DataFrame.from_dict(lookup_vals, orient="index").reset_index()
        tmp.columns = ["race_cd", "race_name"]
        tmp["column_name"] = col_name
        df_list.append(tmp)

    df = pd.concat(df_list, sort=False, ignore_index=True)
    return df


def create_file_source_type(df: pd.DataFrame) -> pd.DataFrame:
    """Append on a plain language identifier we'll use to categorize the
    reporting source for CMS processing"""

    fs_type_dict = {
        "bcarrier": "mdcr_outpatient",
        "hha_base": "mdcr_hha",
        "hospice_": "mdcr_hospice",
        "inpatien": "mdcr_inpatient",
        "maxdata_": None,
        "mbsf_ab_": "mdcr_summary",
        "mbsf_abc": "mdcr_summary",
        "outpatie": "mdcr_outpatient",
    }
    max_dict = {
        "_ot_": "max_outpatient",
        "_ip_": "max_inpatient",
        "_ps_": "max_summary",
        "max_data_all_states": "max_inpatient",
    }

    df["file_source_type"] = df["file_source_name"].str[0:8].map(fs_type_dict)
    for key, value in max_dict.items():
        df.loc[df["file_source_name"].str.contains(key), "file_source_type"] = value

    return df


def build_file_source_map(fs_list: List[str]) -> pd.DataFrame:
    """Create the list lookup from CI generated file source id to file name."""

    if not isinstance(fs_list, list):
        raise TypeError(f"expected fs_list to be a list. Found '{type(fs_list)}'")
    "wrong data type"

    fs_list = sorted(fs_list)
    df_list = []
    for file_source_id in range(1, len(fs_list) + 1, 1):
        i = file_source_id - 1
        tmp = pd.DataFrame(
            {"file_source_id": file_source_id, "file_source_name": fs_list[i]},
            index=[0],
        )
        df_list.append(tmp)
        del tmp

    df = pd.concat(df_list, sort=False, ignore_index=True)
    df = create_file_source_type(df)

    if len(df) != len(df.drop_duplicates()):
        raise RuntimeError(f"{len(df) - len(df.drop_duplicates())} duplicates are present")

    lookup_tests.validate_file_source_id(df)

    return df
