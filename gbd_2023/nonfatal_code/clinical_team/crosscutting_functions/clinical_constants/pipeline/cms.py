import itertools
from typing import Dict

from clinical_constants.shared.values import Estimates

cms_systems = ["mdcr", "max"]

# Map from our common short-hand for CMS systems to their full program name
full_source_name = {"max": "medicaid", "mdcr": "medicare"}

# Researcher ID for different datasets
research_ids = [52318, 56340]

# Researcher IDs and the request IDs they contain.
res_req_ids = {
    52318: [12193, 12208, 12209, 12210, 12214, 8140, 8153, 9437, 9439],
    56340: [10448],
}

# Request IDs for Medicare
mdcr_req_ids = [8140, 9437]

# Request IDs for Medicaid
max_req_ids = [8153, 9439]

# years available in Medicare data
mdcr_years = [2000, 2010, 2014, 2015, 2016]

# source facility types available in Medicare
mdcr_fac_types = ["bcarrier", "hha", "hospice", "inpatient", "outpatient"]
# source facility types available in Medicaid
max_fac_types = ["ip", "ot"]

# patterns in raw data parquet file paths to identify CMS system
mdcr_claim_pattern = "claims_k"
max_claim_pattern = "maxdata"

all_processing_types = {
    "max": ["inpatient", "outpatient"],
    "mdcr": ["inpatient", "outpatient", "hha", "hospice"],
}

# years available in Medicaid data
max_years = [2000, 2010, 2014]

# states present in the PS and OT Medicaid staging tables
max_states = [
    "AK",
    "AL",
    "AR",
    "AZ",
    "CA",
    "CO",
    "CT",
    "DC",
    "DE",
    "FL",
    "GA",
    "HI",
    "IA",
    "ID",
    "IL",
    "IN",
    "KS",
    "KY",
    "LA",
    "MA",
    "MD",
    "ME",
    "MI",
    "MN",
    "MO",
    "MS",
    "MT",
    "NC",
    "ND",
    "NE",
    "NH",
    "NJ",
    "NM",
    "NV",
    "NY",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VA",
    "VT",
    "WA",
    "WI",
    "WV",
    "WY",
]

max_state_years = {
    2000: max_states,  # tied to var above for all states
    2010: [s for s in max_states if s not in ["KS", "ME"]],  # remove 2 states with no claims
    2014: [
        "CA",
        "GA",
        "IA",
        "ID",
        "LA",
        "MI",
        "MN",
        "MO",
        "MS",
        "NJ",
        "PA",
        "SD",
        "TN",
        "UT",
        "VT",
        "WV",
        "WY",
    ],
}

# Key is a data source, Value is the set of ICD related columns
cms_system_icd_lookup = {
    "mdcr_bcarrier": "12_with_vers",
    "mdcr_hha": "25_no_vers",
    "mdcr_hospice": "25_no_vers",
    "mdcr_inpatient": "25_no_vers",
    "mdcr_outpatient": "25_no_vers",
    "max_all_states_ip": "9_no_vers",
    "max_ot": "2_with_vers",
}

# Key is an ICD col set, Value is the list of raw column names
icd_col_names = {
    "12_with_vers": list(
        itertools.chain.from_iterable(
            [[f"ICD_DGNS_CD{i} as dx_{i}", f"ICD_DGNS_VRSN_CD{i}"] for i in range(1, 13)]
        )
    ),
    "25_no_vers": [f"ICD_DGNS_CD{i} as dx_{i}" for i in range(1, 26)],
    "9_no_vers": [f"DIAG_CD_{i} as dx_{i}" for i in range(1, 10)],
    "2_with_vers": [f"DIAG_CD_{i} as dx_{i}" for i in range(1, 3)] + ["DIAG_VRSN_CD_1"],

    "e_codes": [
        "CLM_ID as claim_id",
        "CLM_THRU_DT as clm_thru_dt",
        "CLM_FROM_DT as clm_from_dt",
    ]
    + [f"ICD_DGNS_E_CD{i} as dx_{i}" for i in range(1, 13)],
}

# nested dictionary of cms NIDs
nid_year = {
    "max": {2000: 471320, 2010: 471319, 2014: 471318},
    "mdcr": {2000: 471277, 2010: 471267, 2014: 471258, 2015: 470991, 2016: 470990},
}

# file_source_type processing dictionary
fst_proc_dict = {
    "mdcr": {
        "inpatient": ("Medicare inpatient_base_claims",),
        "outpatient": ("Medicare Carrier", "Medicare outpatient_base_claims"),
        "hospice": ("Medicare hospice_base_claims",),
        "hha": ("Medicare hha_base_claims",),
    },
    "max": {
        "inpatient": ("Medicaid inpatient_base_claims",),
        "outpatient": ("Medicaid outpatient_base_claims",),
    },
}

# Define mapping from plain language inp/otp designation to file_source_type.
# this is used to add a 1 or 0 to each row of bundle mart data's is_otp column
is_otp_dict = {"inpatient": (3, 5), "outpatient": (1, 2, 4, 6, 7)}

# convert estimate id controller conditional blocks to dict using a human
# readable nested dictionary here. New estimate types are added here with
# appropriate key/val settings then they must be extended onto the
# convert_mart_to_estimate method.
est_id_dict = {
    "max": {
        Estimates.claims_primary_inp_claims: {"proc_type": ["inpatient"]},
        Estimates.claims_primary_inp_indv: {"proc_type": ["inpatient"]},
        Estimates.claims_any_inp_indv: {"proc_type": ["inpatient"]},
        Estimates.claims_any_inp_union_require2_otp_indv: {
            "proc_type": ["inpatient", "outpatient"]
        },
    },
    "mdcr": {
        Estimates.claims_primary_inp_claims: {
            "proc_type": ["inpatient"],
            "sample_status": "in_and_out",
            "entitlements": [1, 3],
        },
        Estimates.claims_primary_inp_indv: {
            "proc_type": ["inpatient"],
            "sample_status": "in_and_out",
            "entitlements": [1, 3],
        },
        Estimates.claims_any_inp_indv: {
            "proc_type": ["inpatient"],
            "sample_status": "in_and_out",
            "entitlements": [1, 3],
        },
        Estimates.claims_any_inp_union_require2_otp_indv: {
            "proc_type": ["inpatient", "outpatient", "hha", "hospice"],
            "sample_status": "basic_five_only",
            "entitlements": [3],
        },
        Estimates.inp_pri_claims_5_percent: {
            "proc_type": ["inpatient"],
            "sample_status": "basic_five_only",
            "entitlements": [3],
        },
        Estimates.claims_primary_inp_indv_5_percent_claims: {
            "proc_type": ["inpatient"],
            "sample_status": "basic_five_only",
            "entitlements": [3],
        },
        Estimates.claims_any_inp_indv_5_percent_claims: {
            "proc_type": ["inpatient"],
            "sample_status": "basic_five_only",
            "entitlements": [3],
        },
    },
}

max_denom_types = {"full_count": ["1", "7"], "full_with_pregnancy": ["1", "7", "4"]}


def bene_int_tbl_type(cms_system: str) -> Dict[str, Dict[str, type]]:
    """Dictionary of expected data types for intermediate tables.

    Args:
        cms_system (str): CMS system name abbreviation.
            Ex: 'mdcr'

    Returns:
        Dict[str, Dict[str, type]]: Table specific column dtypes.
            key1=table, key2=column, value=dtype
    """
    col_type = {
        "base": {"bene_id": str, "year_id": int},
        "demo": {
            "location_id_gbd": str,
            "location_id_cnty": str,
            "sex_id": int,
            "dob": str,
            "dod": str,
        },
        "age": {"age": float, "month_id": int, "days": float},
    }

    if cms_system == "mdcr":
        col_type.update(
            {
                "elig": {
                    "month_id": int,
                    "eligibility_reason": str,
                    "entitlement": int,
                    "part_c": bool,
                },
                "sample": {"five_percent_sample": bool, "eh_five_percent_sample": bool},
            }
        )
    else:
        col_type.update(
            {
                "elig": {
                    "month_id": int,
                    "eligibility_reason": object,
                    "restricted_benefit": str,
                    "managed_care": int,
                }
            }
        )

    return col_type


# group by columns used in the creation of numerator and denominator for rate estimates
cms_system_deliverable_gb_dict = {
    "demo": ["age", "sex_id", "year_id"],
    "cit": ["nid"],
    "est": ["bundle_id", "estimate_id", "is_identifiable"],
}


sys_deliverable_ages = {
    "mdcr": {
        "ushd": {"min_age": 65, "max_age": 110},
        "gbd": {"min_age": 65, "max_age": 110},
        "correction_factors": {"min_age": 65, "max_age": 110},
    },
    "max": {
        "ushd": {"min_age": 0, "max_age": 110},
        "gbd": {"min_age": 0, "max_age": 110},
        "correction_factors": {"min_age": 0, "max_age": 110},
    },
}


plotting_cols = {
    0: ["bene_id", "claim_id", "year_id", "month_id", "cause_code"],
    1: ["bene_id", "claim_id", "year_id", "month_id", "cause_code"],
    2: [],  # size is dramatically reduced during step 2, OK to copy full df
    3: [],
    4: [],
    5: [],
}


# current list of restricted benefit values that are captured in
# the creation of the denom and numerator.
max_rst_bnft_vals = {"max_full_count": ["1"], "preg_rst_count": ["4"]}


# race code dict to convert el_race in max to rti values
max_race_convert_dict = {1: 1, 2: 2, 3: 6, 4: 4, 5: 5, 6: 4, 7: 5, 8: 3, 9: 0}


# Used by both systems/deliverables in the CMS ICD-Mart
general_cols = [
    "bene_id",
    "file_source_id",
    "month_id",
    "service_start",
    "dob",
    "dod",
    "sex_id",
    "year_id",
    "claim_id",
    "diagnosis_id",
]

# from cms icd claims in helpers
ICD_ECODE_COLUMNS = ["cause_code", "code_system_id", "diagnosis_id", "claim_id", "year_id"]

COL_TYPES = {  
    "cause_code": str,
    "diagnosis_id": int,
    "claim_id": str,
    "code_system_id": int,
    "year_id": int,
}
SORT_ORDER = ["code_system_id", "cause_code", "diagnosis_id", "claim_id", "year_id"]
COL_ORDER = ["claim_id", "diagnosis_id", "cause_code", "code_system_id", "year_id"]

# Keys are present in raw CMS data
RENAME_DICT = {
    "max": {
        "BENE_ID": "bene_id",
        "clm_id": "claim_id",
        "PLC_OF_SRVC_CD": "cms_facility_id",
        "SRVC_END_DT": "service_end",  # inpatient (discharge date not present), outpatient
        "ADMSN_DT": "service_start",  # inpatient
        "SRVC_BGN_DT": "service_start",  # outpatient
        "PATIENT_STATUS_CD": "patient_status_cd",  # not present in outpatient data
    },
    "mdcr": {
        "BENE_ID": "bene_id",
        "CLM_ID": "claim_id",
        "CLM_SRVC_FAC_ZIP_CD": "clm_srvc_fac_zip_cd",
        "CLM_FAC_TYPE_CD": "cms_facility_id",
        "NCH_BENE_DSCHRG_DT": "service_end",  # inpatient, hospice
        "CLM_THRU_DT": "service_end",  # outpatient, hha, carrier
        "CLM_ADMSN_DT": "service_start",  # inpatient, hha
        "CLM_FROM_DT": "service_start",  # outpatient, carrier
        "CLM_HOSPC_START_DT_ID": "service_start",  # hospice
        "PTNT_DSCHRG_STUS_CD": "ptnt_dschrg_stus_cd",  # not present in carrier
    },
}

OUTCOME_COL_NAMES = {"max": "patient_status_cd", "mdcr": "ptnt_dschrg_stus_cd"}
NO_OUTCOME_COL_TABLE_PATTERN = {"max": "_ot_", "mdcr": "carrier"}
PIPELINE_CONFIG_NAME = "cms-pipeline-config"