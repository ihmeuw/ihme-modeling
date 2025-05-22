from typing import Dict, Final, List

from crosscutting_functions.clinical_constants.shared.mapping import CODE_SYSTEMS

AGE_GROUPBY_COLS: Final[List[str]] = [
    "bundle_id",
    "estimate_id",
    "age",
    "sex_id",
    "location_id",
    "year_id",
]

DENOMINATOR_GROUPBY_COLS: Final[List[str]] = [
    "bundle_id",
    "estimate_id",
    "age_start",
    "age_end",
    "sex_id",
    "location_id",
    "year_id",
    "nid",
]

DENOMINATOR_MERGE_COLS: Final[List[str]] = [
    "age",
    "sex_id",
    "location_id",
    "year_id",
    "bundle_id",
    "estimate_id",
]

# otp lab and pharma facility_ids
INCIDENCE_FACILITY_REMOVALS: Final[List[int]] = [1, 81, 98]

# Apply this in rate space to match Stata behavior
PREVALENCE_CAP: int = 1

# ICD 9 to 10 switch happened on Oct 1st 2015 so neither code system is full for this year
FULL_CODE_SYSTEM_YEARS: Dict[int, List[int]] = {
    CODE_SYSTEMS["ICD9"]: [year for year in range(1980, 2015)],
    CODE_SYSTEMS["ICD10"]: [year for year in range(2016, 2023)],
}

YEAR_NID_DICT: Dict[int, int] = {
    2000: 244369,
    2010: 244370,
    2012: 244371,
    2011: 336850,
    2013: 336849,
    2014: 336848,
    2015: 336847,
    2016: 408680,
    2017: 433114,
    2018: 494351,
    2019: 494352,
    2020: 507351,
    2021: 548974,
    2022: 548975,
}

PARENT_LOCATION_ID: int = 102

COLUMN_TO_NOISE_REDUCE = "marketscan_mean"


# below is from schema.constants
IS_OTPS: Final[List[int]] = [0, 1]
IN_STD_SAMPLES: Final[List[int]] = [0, 1]
IS_DUPLICATES: Final[List[int]] = [0, 1]

COMP: Final[str] = "zstd"

MS_DATA_DIR: Final[str] = "FILEPATH"
PARQUET_DIR: Final[str] = "FILEPATH"
WRITE_DIR: Final[str] = "FILEPATH"
YEAR_TRACKER: Final[str] = "FILEPATH"

E2E_SAMPLE_DIR: Final[str] = "FILEPATH"

# 2 distinct sources of MS data, private CCAE and public supplemental MDCR
SOURCES: Final[List[str]] = ["ccae", "mdcr"]

# The Marketscan raw data uses the values here to identify different tables in their schema
MS_TABLE_LABEL = {
    "inpatient_admissions": "i",
    "facility_header": "f",
    "inpatient_services": "s",
    "outpatient_services": "o",
    "outpatient_drug_claims": "d",
    "populations": "p",
    "annual_summary_enrollment": "a",
    "detail_enrollment": "t",
}

TABLE_TYPES: Final[Dict[str, List[str]]] = {
    "claims": [MS_TABLE_LABEL["inpatient_admissions"], MS_TABLE_LABEL["outpatient_services"]],
    "denominator": [MS_TABLE_LABEL["detail_enrollment"]],
}

# columns of interest BY DATASETS
INP_READ_COLS: Final[List[str]] = ["admdate", "enrolid", "dstatus"] + [
    f"dx{i}" for i in range(1, 16)
]
OTP_READ_COLS: Final[List[str]] = ["svcdate", "stdplac", "enrolid"] + [
    f"dx{i}" for i in range(1, 5)
]
ENROL_READ_COLS: Final[List[str]] = ["enrolid", "dtstart", "age", "egeoloc", "sex"]


# columns for the reshape
IDX: Final[List[str]] = [
    "bene_id",
    "service_start",
    "dstatus",
    "code_system_id",
    "is_otp",
    "is_duplicate",
    "year",
    "facility_id",
]

# columns and order to use when partitioning the data
PART_COLS: Final[List[str]] = [
    "year",
    "is_duplicate",
    "in_std_sample",
    "is_otp",
    "icd_group_id",
]

# determines the final sort order of the data before it gets written
SORT_COLS: Final[List[str]] = [
    "year",
    "is_duplicate",
    "in_std_sample",
    "is_otp",
    "icd_group_id",
    "bene_id",
    "age",
    "sex_id",
    "location_id",
    "mhsacovg",
    "cause_code",
    "service_start",
    "facility_id",
    "diagnosis_id",
]

YEAR_NAMING_SCHEME: Final[Dict[int, str]] = {
    2000: "002",
    2010: "103",
    2011: "113",
    2012: "121",
    2013: "133",
    2014: "142",
    2015: "151",
    2016: "161",
    2017: "171",
    2018: "181",
    2019: "192_a",
    2020: "201_a",
    2021: "211_A",
    2022: "221_A",
}

SAMPLE_SIZE_DEMO: Final[List[str]] = ["age", "sex_id", "location_id", "year"]

DEATH_CODES: Final[List[int]] = [20, 40, 41, 42]

DX_VERS: Final[Dict[str, int]] = {"ICD9": 9, "ICD10": 0}

# Mapping from MS's dxver to Clinical's code system id
DXVER_TO_CODE_SYS: Final[Dict[int, int]] = {
    DX_VERS["ICD9"]: CODE_SYSTEMS["ICD9"],
    DX_VERS["ICD10"]: CODE_SYSTEMS["ICD10"],
}

PIPELINE_CONFIG_NAME = "marketscan-pipeline-config"

SAS_SPARK_JARS = "FILEPATH"
JAR_PARCKAGE = "saurfang:spark-sas7bdat:3.0.0-s_2.12"
SAS_SPARK_FORMAT_TEMPLATE = "com.github.saurfang.sas.spark"