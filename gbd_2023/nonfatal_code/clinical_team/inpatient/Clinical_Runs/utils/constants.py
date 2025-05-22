from dataclasses import dataclass
from typing import Tuple


@dataclass
class RunDBSettings:
    iw_profile: str = "clinical"
    ow_profile: str = "clinical"
    cw_profile: str = "clinical"
    db_profile: str = "clinical"


@dataclass
class InpRunSettings:
    clinical_age_group_set_id: int = 2
    GBD_START_YEAR: int = 1990


@dataclass
class InpRunConstants:
    make_weights: bool = False
    remove_live_births: bool = True
    bin_years: bool = True
    que: str = "long.q"
    cf_agg_stat: str = "median"
    write_results: bool = True
    bundle_level_cfs: bool = True
    draws: int = 1000
    run_tmp_unc: bool = True
    weight_squaring_method: str = "bundle_source_specific"


@dataclass
class ClaimsRunSettings:
    clinical_age_group_set_id: int = 1
    flagged_age_group_set_id: int = 7
    flagged_estimates: Tuple[int] = (16, 18, 27)
    clinical_data_type_ids: Tuple[int] = (3, 5)
