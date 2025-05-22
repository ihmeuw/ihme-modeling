from typing import Any, Dict, Final, List

import numpy as np

from crosscutting_functions.clinical_constants.shared.values import Estimates

# FROM pipeline constants
BASE_GROUPBY_COLS: List[str] = ["bundle_id", "estimate_id", "sex_id", "year_id", "location_id"]

DELIVERABLE: Dict[str, Any] = {
    "correction_factors": {
        "estimate_ids": [
            Estimates.claims_primary_inp_claims,
            Estimates.claims_primary_inp_indv,
            Estimates.claims_any_inp_claims,
            Estimates.claims_any_inp_indv,
            Estimates.claims_any_otp_claims,
            Estimates.claims_any_otp_indv,
            Estimates.claims_any_inp_union_require2_otp_indv,
        ],
        "apply_maternal_adjustment": False,
        "groupby_cols": BASE_GROUPBY_COLS + ["age"],
        "sum_cols": ["val"],
    },
    "gbd": {
        "estimate_ids": [
            Estimates.claims_any_inp_indv,
            Estimates.claims_any_inp_union_require2_otp_indv,
        ],
        "apply_maternal_adjustment": True,
        "groupby_cols": BASE_GROUPBY_COLS + ["age_group_id", "age_start", "age_end"],
        "sum_cols": ["val"],
    },
}
NID_DICT: Final[Dict[int, int]] = {
    2015: 397812,
    2016: 397813,
    2017: 397814,
    2018: 431674,
    2019: 466458,
    2020: 466459,
    2021: 509155,
    2022: 539438,
}
FACILITY_MAP: Dict[str, tuple] = {"inp": (0,), "inp_otp": (0, 1)}
PIPELINE_MEM_FLOOR: float = 5.0
PIPELINE_MEM_SCALE: Dict[str, float] = {"memory": 0.75, "runtime": 0.75}

READ_TO_LATIN_VOIVODESHIP: Dict[str, str] = {
    "DOLNO\x8cLĽSKIE": "DOLNOŚLĄSKIE",
    "KUJAWSKO-POMORSKIE": "KUJAWSKO-POMORSKIE",
    "LUBELSKIE": "LUBELSKIE",
    "LUBUSKIE": "LUBUSKIE",
    "MAZOWIECKIE": "MAZOWIECKIE",
    "MAŁOPOLSKIE": "MAŁOPOLSKIE",
    "OPOLSKIE": "OPOLSKIE",
    "PODKARPACKIE": "PODKARPACKIE",
    "PODLASKIE": "PODLASKIE",
    "POMORSKIE": "POMORSKIE",
    "WARMIŃSKO-MAZURSKIE": "WARMIŃSKO-MAZURSKIE",
    "WIELKOPOLSKIE": "WIELKOPOLSKIE",
    "ZACHODNIOPOMORSKIE": "ZACHODNIOPOMORSKIE",
    "\x8cLĽSKIE": "ŚLĄSKIE",
    "\x8cWIĘTOKRZYSKIE": "ŚWIĘTOKRZYSKIE",
    "ŁÓDZKIE": "ŁÓDZKIE",
}

READ_TO_LATIN_POVIATS: Dict[Any, Any] = {}
LATIN_2_GBD_MAP: Dict[str, str] = {
    "DOLNOŚLĄSKIE": "Dolnoslaskie",
    "KUJAWSKO-POMORSKIE": "Kujawsko-Pomorskie",
    "LUBELSKIE": "Lubelskie",
    "LUBUSKIE": "Lubuskie",
    "MAZOWIECKIE": "Mazowieckie",
    "MAŁOPOLSKIE": "Malopolskie",
    "OPOLSKIE": "Opolskie",
    "PODKARPACKIE": "Podkarpackie",
    "PODLASKIE": "Podlaskie",
    "POMORSKIE": "Pomorskie",
    "WARMIŃSKO-MAZURSKIE": "Warminsko-Mazurskie",
    "WIELKOPOLSKIE": "Wielkopolskie",
    "ZACHODNIOPOMORSKIE": "Zachodniopomorskie",
    "ŚLĄSKIE": "Slaskie",
    "ŚWIĘTOKRZYSKIE": "Swietokrzyskie",
    "ŁÓDZKIE": "Lodzkie",
}
READ_DTYPES = {
    "id": int,
    "visit_year": int,
    "teryt": str,
    "voivodeship": str,
    "poviats": str,
    "id_provider": str,
    "place_of_visit_id": int,
    "place_of_visit": str,
    "gender": str,
    "age": int,
    "first_visit_date": str,
    "visit_number": int,
    "start_date": str,
    "end_date": str,
    "icd10_main": str,
    "icd10_coex1": str,
    "icd10_coex2": str,
    "icd10_coex3": str,
    "previous_visit_date": str,
}
READ_KWARGS: Dict[str, Any] = {"header": 0, "sep": "\t", "dtype": READ_DTYPES}
# {file_source_id: int, year-encoding: Dict[int, str]}
RAW_YEAR_ENCODING: Final[Dict[int, Dict[int, str]]] = {
    4: {
        2015: "ISO-8859-2",
        2016: "ISO-8859-2",
        2017: "ISO-8859-2",
        2018: "ISO-8859-2",
        2019: "ISO-8859-2",
        2020: "ISO-8859-2",
        2021: "UTF-8",
        2022: "ISO-8859-2",
    }
}
FACILITIES: List[str] = ["INPATIENT", "OUTPATIENT", "OTHER"]
MAKE_LATIN_2_COLS: List[str] = ["voivodeship"]
DATE_COLS: List[str] = ["first_visit_date", "start_date", "end_date", "previous_visit_date"]
PARTITION_COLS: List[str] = ["visit_year", "place_of_visit"]

POL_GBD_LOC_ID: int = 51
YEARS = np.arange(2015, 2023, 1).tolist()

SEX_ID: Dict[str, int] = {"MALE": 1, "FEMALE": 2}

DEFAULT_CLUSTER: str = "slurm"

PARTITION: List[str] = ["year_id", "file_source_id", "is_dup", "is_otp", "icd_group_id"]

RAW_READ_COLS: List[str] = [
    "id",
    "gender",
    "age",
    "first_visit_date",
    "visit_number",
    "start_date",
    "end_date",
    "icd10_main",
    "icd10_coex1",
    "icd10_coex2",
    "icd10_coex3",
    "voivodeship",
    "place_of_visit_id",
]
COL_RENAME: Dict[str, str] = {
    "gender": "sex_id",
    "id": "bene_id",
    "visit_year": "year_id",
    "start_date": "admission_date",
    "end_date": "discharge_date",
    "voivodeship": "location_id",
    "place_of_visit_id": "facility_id",
    "icd10_main": "dx_1",
    "icd10_coex1": "dx_2",
    "icd10_coex2": "dx_3",
    "icd10_coex3": "dx_4",
}
ORDERED_ICD_MART_COLS: List[str] = [
    "bene_id",
    "age",
    "sex_id",
    "location_id",
    "year_id",
    "is_otp",
    "facility_id",
    "first_visit_date",
    "visit_number",
    "admission_date",
    "discharge_date",
    "diagnosis_id",
    "code_system_id",
    "cause_code",
    "is_dup",
    "icd_group_id",
    "file_source_id",
    "primary_dx_only",
]
PIPELINE_CONFIG_NAME = "poland-pipeline-config"