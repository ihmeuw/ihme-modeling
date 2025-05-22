from pathlib import Path

FILE_DIR = "FILEPATH"
OUTPATH_DIR = Path("FILEPATH")
FINAL_OUTPATH = f"{OUTPATH_DIR}/formatted_GEO_COL.H5"
PREV_DATA = f"{OUTPATH_DIR}/_archive/2022_04_28_formatted_GEO_COL.H5"

ICD_VERS = 10

PATH_DICT = {
    2014: f"{FILE_DIR}/2014/GEO_HOSPITAL_DISCHARGE_DATA_2014_Y2015M06D29.XLSX",
    2015: f"{FILE_DIR}/2015/GEO_2015_NCDCPH_HOSPITAL_DISCHARGES_Y2024M04D03.XLSX",
    2016: f"{FILE_DIR}/2016/GEO_2016_NCDCPH_HOSPITAL_DISCHARGES.XLSX",
    2017: f"{FILE_DIR}/2017/GEO_2017_NCDCPH_HOSPITAL_DISCHARGES.XLSX",
    2018: f"{FILE_DIR}/2018/GEO_2018_NCDCPH_HOSPITAL_DISCHARGES_Y2024M04D03.XLSX",
    2019: f"{FILE_DIR}/2019/GEO_2019_NCDCPH_HOSPITAL_DISCHARGES_Y2024M04D03.XLSX",
    2020: f"{FILE_DIR}/2020/GEO_2020_NCDCPH_HOSPITAL_DISCHARGES_Y2024M04D03.XLSX",
    2021: f"{FILE_DIR}/2021/GEO_2021_NCDCPH_HOSPITAL_DISCHARGES_Y2023M12D13.XLSX",
    2022: f"{FILE_DIR}/2022/GEO_2022_NCDCPH_HOSPITAL_DISCHARGES_Y2023M11D22.XLSX",
}

GROUP_B_COL_DICT = {
    "Discharge Date": "discharge_date",
    "Beddays": "los",
    "Sex": "sex_id",
    "Age (years)": "age_years",
    "Age (months)": "age_months",
    "Age (days)": "age_days",
    "age_unit": "age_unit",
    "year_start": "year_start",
    "year_end": "year_end",
    "Disharge status": "outcome_id",
    "Main Diagnosis (ICD10)": "dx_1",
    "External causes (ICD10)": "dx_2",
    "Complication (ICD 10)": "dx_3",
    "Comorbidity (ICD 10)": "dx_4",
}

GROUP_A_COL_DICT = {
    "Discharge Date": "discharge_date",
    "Beddays": "los",
    "Cender": "sex_id",
    "Age (Years)": "age_years",
    "Age (Months)": "age_months",
    "Age (Days)": "age_days",
    "age_unit": "age_unit",
    "year_start": "year_start",
    "year_end": "year_end",
    "Disharge status": "outcome_id",
    "Main Diagnosis (ICD10)": "dx_1",
    "External causes (ICD10)": "dx_2",
    "Complication (ICD 10)": "dx_3",
    "Comorbidity (ICD 10)": "dx_4",
}

GROUP_YEAR = {
    2014: GROUP_B_COL_DICT,
    2015: GROUP_B_COL_DICT,
    2016: GROUP_B_COL_DICT,
    2017: GROUP_B_COL_DICT,
    2018: GROUP_B_COL_DICT,
    2019: GROUP_B_COL_DICT,
    2020: GROUP_B_COL_DICT,
    2021: GROUP_A_COL_DICT,
    2022: GROUP_A_COL_DICT,
}

FILL_COLS = {
    "location_id": 35,
    "source": "GEO_COL",
    "code_system_id": 2,
    "representative_id": 1,
    "metric_id": 1,
    "age_group_unit": 1,
    # inpatient only, inherited, unknown from datasets
    "facility_id": "hospital",
}

KEEP = [
    "year_start",
    "year_end",
    "sex_id",
    "age_start",
    "age_end",
    "outcome_id",
    "dx_1",
    "dx_2",
    "dx_3",
    "dx_4",
    "los",
]

NIDS = {
    2014: 212493,
    2015: 464534,
    2016: 319414,
    2017: 354240,
    2018: 464535,
    2019: 464097,
    2020: 464098,
    2021: 540608,
    2022: 539809,
}

OUTCOME_VALS = [
    "death",
    "died",
]

GEO_CHARS = [
    "უცნობი",
]

INT_COLS = [
    "location_id",
    "nid",
    "age_group_unit",
    "sex_id",
    "year_start",
    "year_end",
    "code_system_id",
    "diagnosis_id",
]

GROUP_VARS = [
    "diagnosis_id",
    "cause_code",
    "year_start",
    "year_end",
    "sex_id",
    "age_start",
    "age_end",
    "age_group_unit",
    "outcome_id",
    "location_id",
    "source",
    "code_system_id",
    "facility_id",
    "metric_id",
    "representative_id",
    "nid",
]

HOSP_FRMAT_FEAT = [
    "age_group_unit",
    "age_start",
    "age_end",
    "year_start",
    "year_end",
    "location_id",
    "representative_id",
    "sex_id",
    "diagnosis_id",
    "metric_id",
    "outcome_id",
    "val",
    "source",
    "nid",
    "facility_id",
    "code_system_id",
    "cause_code",
]
