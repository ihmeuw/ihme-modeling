from dataclasses import dataclass
from typing import Dict, Final

import pandas as pd
from gbd.constants import measures

from crosscutting_functions.clinical_constants.shared.database import Tables
from crosscutting_functions.clinical_constants.shared.values import ClinicalDataTypes, Estimates

LIVE_BIRTH_CODES: Final[pd.DataFrame] = pd.DataFrame(
    columns=["cause_code", "icd_vers"],
    data=[
        ["V30", "ICD9_detail"],
        ["V300", "ICD9_detail"],
        ["V3000", "ICD9_detail"],
        ["V3001", "ICD9_detail"],
        ["V301", "ICD9_detail"],
        ["V302", "ICD9_detail"],
        ["V31", "ICD9_detail"],
        ["V310", "ICD9_detail"],
        ["V3100", "ICD9_detail"],
        ["V3101", "ICD9_detail"],
        ["V311", "ICD9_detail"],
        ["V312", "ICD9_detail"],
        ["V32", "ICD9_detail"],
        ["V320", "ICD9_detail"],
        ["V3200", "ICD9_detail"],
        ["V3201", "ICD9_detail"],
        ["V321", "ICD9_detail"],
        ["V322", "ICD9_detail"],
        ["V33", "ICD9_detail"],
        ["V330", "ICD9_detail"],
        ["V3300", "ICD9_detail"],
        ["V3301", "ICD9_detail"],
        ["V331", "ICD9_detail"],
        ["V332", "ICD9_detail"],
        ["V34", "ICD9_detail"],
        ["V340", "ICD9_detail"],
        ["V3400", "ICD9_detail"],
        ["V3401", "ICD9_detail"],
        ["V341", "ICD9_detail"],
        ["V342", "ICD9_detail"],
        ["V36", "ICD9_detail"],
        ["V360", "ICD9_detail"],
        ["V3600", "ICD9_detail"],
        ["V3601", "ICD9_detail"],
        ["V361", "ICD9_detail"],
        ["V362", "ICD9_detail"],
        ["V37", "ICD9_detail"],
        ["V370", "ICD9_detail"],
        ["V3700", "ICD9_detail"],
        ["V3701", "ICD9_detail"],
        ["V371", "ICD9_detail"],
        ["V372", "ICD9_detail"],
        ["V39", "ICD9_detail"],
        ["V390", "ICD9_detail"],
        ["V3900", "ICD9_detail"],
        ["V3901", "ICD9_detail"],
        ["V391", "ICD9_detail"],
        ["V392", "ICD9_detail"],
        ["Z38", "ICD10"],
        ["Z380", "ICD10"],
        ["Z3800", "ICD10"],
        ["Z3801", "ICD10"],
        ["Z381", "ICD10"],
        ["Z382", "ICD10"],
        ["Z383", "ICD10"],
        ["Z3830", "ICD10"],
        ["Z3831", "ICD10"],
        ["Z384", "ICD10"],
        ["Z385", "ICD10"],
        ["Z386", "ICD10"],
        ["Z3861", "ICD10"],
        ["Z3862", "ICD10"],
        ["Z3863", "ICD10"],
        ["Z3864", "ICD10"],
        ["Z3865", "ICD10"],
        ["Z3866", "ICD10"],
        ["Z3868", "ICD10"],
        ["Z3869", "ICD10"],
        ["Z387", "ICD10"],
        ["Z388", "ICD10"],
    ],
)

CODE_SYSTEMS: Dict[str, int] = {"ICD9": 1, "ICD10": 2, "Vietnam": 3, "Indonesia": 4}

US_CODE_SYSTEM_YEARS = {
    CODE_SYSTEMS["ICD9"]: [2000, 2010, 2014],
    CODE_SYSTEMS["ICD10"]: [2016],
}

# Prefix values by code system for injury codes
ECODE_HEADERS = {CODE_SYSTEMS["ICD9"]: ["E"], CODE_SYSTEMS["ICD10"]: ["V", "W", "X", "Y"]}
NCODE_HEADERS = {CODE_SYSTEMS["ICD9"]: ["8", "9"], CODE_SYSTEMS["ICD10"]: ["S", "T"]}

# Sourced from the most common age sets by estimate_id in active_bundle_metadata
DEFAULT_AGE_SETS = {"inpatient": 3, "outpatient": 5, "claims": 3, "claims - flagged": 3}

# If a bundle gets the estimate_id stored in the key, it should also get the flagged estimate
# stored in the dict value
CLAIMS_FLAGGED_BINDS = {
    Estimates.claims_any_inp_union_require2_otp_indv: Estimates.claims_any_inp_union_otp_adm,
    Estimates.claims_any_inp_indv: Estimates.claims_any_inp_claims,
}

DEFAULT_PIPELINE_ESTIMATES = {"inpatient": 4, "claims": 21}

# Sourced from the clinical.estimate table. This maps estimate_id to the pipeline used to
# create them. This is analogous to clinical_data_type.
ESTIMATE_TYPES = {
    "inpatient": (
        Estimates.inp_primary_unadj,
        Estimates.inp_primary_cf1_modeled,
        Estimates.inp_primary_cf2_modeled,
        Estimates.inp_primary_cf3_modeled,
        Estimates.inp_primary_live_births_unadj,
        Estimates.inp_primary_live_births_cf1_modeled,
        Estimates.inp_primary_live_births_cf2_modeled,
        Estimates.inp_primary_live_births_cf3_modeled,
        Estimates.inp_primary_inj_cf_en_prop,
    ),
    "claims": (
        Estimates.claims_primary_inp_claims,
        Estimates.claims_primary_inp_indv,
        Estimates.claims_any_inp_claims,
        Estimates.claims_any_inp_indv,
        Estimates.claims_any_inp_union_require2_otp_indv,
        Estimates.inp_pri_claims_5_percent,
        Estimates.claims_primary_inp_indv_5_percent_claims,
        Estimates.claims_any_inp_indv_5_percent_claims,
    ),
    "outpatient": (
        Estimates.otp_any_unadj,
        Estimates.otp_any_otp_cf,
        Estimates.otp_any_otp_inj_cf,
        Estimates.otp_any_icpc,
    ),
}


@dataclass
class Measure:
    PREVALENCE_STR: str = "prev"
    INCIDENCE_STR: str = "inc"


MEASURE_LOOKUP = {
    Measure.PREVALENCE_STR: measures.PREVALENCE,
    Measure.INCIDENCE_STR: measures.INCIDENCE,
}


# Constants below are used in the new map creation process
@dataclass(init=True, frozen=True)
class InitialOperations:
    ADD_ICD: str = "add_icd_to_bundle"
    REMOVE_ICD: str = "remove_icd_from_bundle"
    CREATE_BUNDLE: str = "create_new_clinical_bundle_mapping"
    REMOVE_BUNDLE: str = "remove_entire_bundle_from_map"


@dataclass(init=True, frozen=True)
class UpdateOperations:
    SWAP: str = "swap_cause_icg"
    SPLIT: str = "split_cause_from_icg"
    ADD_ICG: str = "add_icg_to_bundle"
    REMOVE_ICG: str = "remove_icg_from_bundle"
    CREATE_ICG_IN_MAP: str = "create_new_icg"
    CREATE_CAUSE_CODE_IN_MAP: str = "create_new_cause_code"
    UPDATE_PROPERTY: str = "update_property"


# These tables do not contain a "map_version" column
UNVERSIONED = [Tables.ICG, Tables.BUNDLE, Tables.CODE_SYSTEM]


# set widths for types of columns in the initial Excel report
@dataclass
class ReportColumnWidths:
    UPDATE_REQUESTED: int = 22
    CAUSE_CODE: int = 10
    CODE_SYSTEM_ID: int = 13
    CODE_SYSTEM_NAME: int = 17
    ENTITY_ID: int = 12
    ENTITY_NAME: int = 30


# Set columns and widths for the cause codes/ICGs in the initial Excel report
REQUEST_COLS = {
    "cause_code": ReportColumnWidths.CAUSE_CODE,
    "code_system_id": ReportColumnWidths.CODE_SYSTEM_ID,
    "code_system_name": ReportColumnWidths.CODE_SYSTEM_NAME,
    "icg_id": ReportColumnWidths.ENTITY_ID,
    "icg_name": ReportColumnWidths.ENTITY_NAME,
    "update_requested": ReportColumnWidths.UPDATE_REQUESTED,
    "bundle_id_1": ReportColumnWidths.ENTITY_ID,
    "bundle_name_1": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_2": ReportColumnWidths.ENTITY_ID,
    "bundle_name_2": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_3": ReportColumnWidths.ENTITY_ID,
    "bundle_name_3": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_4": ReportColumnWidths.ENTITY_ID,
    "bundle_name_4": ReportColumnWidths.ENTITY_NAME,
}

# Set columns and widths for the current bundle mapping in the initial Excel report
BUNDLE_COLS = {
    "cause_code": ReportColumnWidths.CAUSE_CODE,
    "code_system_id": ReportColumnWidths.CODE_SYSTEM_ID,
    "code_system_name": ReportColumnWidths.CODE_SYSTEM_NAME,
    "icg_id": ReportColumnWidths.ENTITY_ID,
    "icg_name": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_1": ReportColumnWidths.ENTITY_ID,
    "bundle_name_1": ReportColumnWidths.ENTITY_NAME,
    "cause_id_1": ReportColumnWidths.ENTITY_ID,
    "cause_name_1": ReportColumnWidths.ENTITY_NAME,
    "rei_id_1": ReportColumnWidths.ENTITY_ID,
    "rei_name_1": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_2": ReportColumnWidths.ENTITY_ID,
    "bundle_name_2": ReportColumnWidths.ENTITY_NAME,
    "cause_id_2": ReportColumnWidths.ENTITY_ID,
    "cause_name_2": ReportColumnWidths.ENTITY_NAME,
    "rei_id_2": ReportColumnWidths.ENTITY_ID,
    "rei_name_2": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_3": ReportColumnWidths.ENTITY_ID,
    "bundle_name_3": ReportColumnWidths.ENTITY_NAME,
    "cause_id_3": ReportColumnWidths.ENTITY_ID,
    "cause_name_3": ReportColumnWidths.ENTITY_NAME,
    "rei_id_3": ReportColumnWidths.ENTITY_ID,
    "rei_name_3": ReportColumnWidths.ENTITY_NAME,
    "bundle_id_4": ReportColumnWidths.ENTITY_ID,
    "bundle_name_4": ReportColumnWidths.ENTITY_NAME,
    "cause_id_4": ReportColumnWidths.ENTITY_ID,
    "cause_name_4": ReportColumnWidths.ENTITY_NAME,
    "rei_id_4": ReportColumnWidths.ENTITY_ID,
    "rei_name_4": ReportColumnWidths.ENTITY_NAME,
}


clinical_data_type_dict = {
    "inpatient": ClinicalDataTypes.inpatient,
    "outpatient": ClinicalDataTypes.outpatient,
    "claims": ClinicalDataTypes.claims,
    "claims - flagged": ClinicalDataTypes.claims_flagged,
}