from pathlib import Path

import pandas as pd

PROCESS_DICT = {
    "year_start": 2005,
    "year_end": 2023,
    # years contain admission and discharge dates
    "admit_date": list(range(2005, 2017)),
    # years which do not contain admit and discharge dates
    "no_admit_date": list(range(2017, 2023)),
}

BASE_DIR = "FILEPATH"

SPECIAL_PROCESS = {
    2007: {
        "fpath": (f"{BASE_DIR}/gbd 2005-2016 sas7bdat files/gbd_2007.sas7bdat"),
        "read_method": "read_sas",
    },
    2019: {
        "fpath": (f"{BASE_DIR}/2019/ITA_HOSPITAL_INPATIENT_DISCHARGES_2019_Y2021M02D22.DTA"),
        "read_method": "read_stata",
    },
    2020: {
        "fpath": (f"{BASE_DIR}/2020/ITA_HOSPITAL_INPATIENT_DISCHARGES_2020_Y2021M12D29.DTA"),
        "read_method": "read_stata",
    },
    2021: {
        "fpath": (f"{BASE_DIR}/2021/ITA_HOSPITAL_INPATIENT_DISCHARGES_2021_Y2022M12D15.DTA"),
        "read_method": "read_stata",
    },
    2022: {
        "fpath": (f"{BASE_DIR}/2022/ITA_HOSPITAL_INPATIENT_DISCHARGES_2022_Y2023M11D21.TXT"),
        "read_method": "read_csv",
    },
}

OUTPATH_DIR = Path("/home/j/WORK/06_hospital/01_inputs/sources/ITA_HID/data/intermediate")

FINAL_OUTPATH = f"{OUTPATH_DIR}/formatted_ITA_HID.H5"

PREV_DATA = f"{OUTPATH_DIR}/_archive/2023_11_29_formatted_ITA_HID.H5"

FILL_COLS = {
    "source": "ITA_HID",
    "representative_id": 1,
    # code_system_id: 1 for ICD-9
    "code_system_id": 1,
    # metric_id: 1 -> val column consists of counts
    "metric_id": 1,
    # this is hospital discharge data
    "facility_id": "hospital",
}

IDEAL_KEEP = [
    "sesso",
    "eta",
    "eta_gg",
    "età",
    "etÃ\xa0",
    "età_gg",
    "etÃ\xa0_gg",
    "reg_ric",
    "regric",
    "gg_deg",
    "mod_dim",
    "mot_dh",
    "tiporic",
    "dpr",
    "dsec1",
    "dsec2",
    "dsec3",
    "dsec4",
    "dsec5",
    "causa_ext",
    "tip_ist2",
    "data_ric",
    "data_dim",
    "data_ricA",
    "data_dimA",
    "cod_reg",
]

COLS_2007 = [
    "sex_id",
    "dx_1",
    "dx_2",
    "dx_3",
    "dx_4",
    "dx_5",
    "dx_6",
    "adm_date",
    "dis_date",
]

HOSP_WIDE_FEAT = {
    "nid": "nid",
    "representative_id": "representative_id",
    "year_start": "year_start",
    "year_end": "year_end",
    "sesso": "sex_id",
    "eta": "age",
    "età": "age",
    "etÃ\xa0": "age",
    "eta_gg": "age_days",
    "età_gg": "age_days",
    "etÃ\xa0_gg": "age_days",
    "age_group_unit": "age_group_unit",
    "code_system_id": "code_system_id",
    "data_dim": "dis_date",
    "data_dimA": "dis_date",
    "data_ric": "adm_date",
    "data_ricA": "adm_date",
    "gg_deg": "los",
    "mod_dim": "outcome_id",
    "tip_ist2": "facility_id",
    "dpr": "dx_1",
    "dsec1": "dx_2",
    "dsec2": "dx_3",
    "dsec3": "dx_4",
    "dsec4": "dx_5",
    "dsec5": "dx_6",
    "causa_ext": "ecode_1",
    "reg_ric": "day_case",
    "regric": "day_case",
}

LOC_DF = pd.DataFrame(
    columns=["cod_reg", "location_name", "location_id"],
    data=[
        ("010", "Piemonte", 35494),
        ("020", "Valle d'Aosta", 35495),
        ("030", "Lombardia", 35497),
        ("041", "Provincia autonoma di Bolzano", 35498),
        ("042", "Provincia autonoma di Trento", 35499),
        ("050", "Veneto", 35500),
        ("060", "Friuli-Venezia Giulia", 35501),
        ("070", "Liguria", 35496),
        ("080", "Emilia-Romagna", 35502),
        ("090", "Toscana", 35503),
        ("100", "Umbria", 35504),
        ("110", "Marche", 35505),
        ("120", "Lazio", 35506),
        ("130", "Abruzzo", 35507),
        ("140", "Molise", 35508),
        ("150", "Campania", 35509),
        ("160", "Puglia", 35510),
        ("170", "Basilicata", 35511),
        ("180", "Calabria", 35512),
        ("190", "Sicilia", 35513),
        ("200", "Sardegna", 35514),
    ],
)

NID_DICT = {
    2005: 331137,
    2006: 331138,
    2007: 331139,
    2008: 331140,
    2009: 331141,
    2010: 331142,
    2011: 331143,
    2012: 331144,
    2013: 331145,
    2014: 331146,
    2015: 331147,
    2016: 331148,
    2017: 421046,
    2018: 421047,
    2019: 468421,
    2020: 494049,
    2021: 512053,
    2022: 540895,
}

INT_COLS = [
    "location_id",
    "year_start",
    "year_end",
    "age_group_unit",
    "age",
    "sex_id",
    "nid",
    "representative_id",
    "metric_id",
]

STR_COLS = ["source", "facility_id", "outcome_id"]

GROUP_VARS = [
    "cause_code",
    "diagnosis_id",
    "sex_id",
    "age_start",
    "age_end",
    "year_start",
    "year_end",
    "location_id",
    "nid",
    "age_group_unit",
    "source",
    "facility_id",
    "code_system_id",
    "outcome_id",
    "representative_id",
    "metric_id",
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
