
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class Deliverable:
    name: str
    race_deliverable: Optional[str]
    # zeroeth icd mart col must be the location column
    icd_mart_cols: List[str]
    location_col: str
    groupby_cols: List[str]
    rename_loc_dict: Dict[str, str]
    col_reqs_drop_cols: List[str]
    apply_maternal_adjustment: bool
    zfill_fips: bool
    rm_locs: List[int]
    table_deliverable: str
    denom_filter_prefix: Dict[str, str]


GbdDeliverable = Deliverable(
    name="gbd",
    race_deliverable=None,
    icd_mart_cols=["location_id_gbd"],
    location_col="location_id",
    groupby_cols=["location_id"],
    rename_loc_dict={"location_id_gbd": "location_id"},
    col_reqs_drop_cols=[
        "year_id",
        "metric_id",
        "val",
        "facility_id",
    ],
    apply_maternal_adjustment=True,
    zfill_fips=False,
    rm_locs=[298, 351, 376, 385, 422],
    table_deliverable="gbd",
    denom_filter_prefix={
        "where_clause": "location_id is not NULL and ",
        "df_cond": "(d_df.location_id_gbd.notnull())",
    },
)


CfDeliverable = Deliverable(
    name="correction_factors",
    race_deliverable=None,
    icd_mart_cols=["location_id_gbd", "cms_facility_id"],
    location_col="location_id",
    groupby_cols=["location_id", "facility_id"],
    rename_loc_dict={"location_id_gbd": "location_id", "cms_facility_id": "facility_id"},
    col_reqs_drop_cols=["year_id", "metric_id"],
    apply_maternal_adjustment=True,
    zfill_fips=False,
    rm_locs=[],
    table_deliverable="gbd",
    denom_filter_prefix={
        "where_clause": "location_id is not NULL and ",
        "df_cond": "(d_df.location_id_gbd.notnull())",
    },
)


UshdDeliverable = Deliverable(
    name="ushd",
    race_deliverable="rti_race_cd",
    icd_mart_cols=["location_id_cnty", "rti_race_cd"],
    location_col="fips_6_4_code",
    groupby_cols=["fips_6_4_code", "rti_race_cd"],
    rename_loc_dict={"location_id_cnty": "fips_6_4_code"},
    col_reqs_drop_cols=[
        "year_start",
        "year_end",
        "val",
        "facility_id",
    ],
    apply_maternal_adjustment=False,
    zfill_fips=False,
    rm_locs=[],
    table_deliverable="cnty",
    denom_filter_prefix={"where_clause": "", "df_cond": ""},
)
