"""
Controls the removal of data along eligibility criteria
"""
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants


def filter_elig_vals(
    df: pd.DataFrame, cms_system: str, estimate_id: int, max_denom_type: str
) -> pd.DataFrame:
    """Remove rows of bundle mart data that fall outside of our eligibility requirements
    Currently this means part b or part_ab enrollees for Medicare (depends on estimate_id)
    and restricted benefit enrollees for Medicaid (depends on bundle_id which is used to create
    the max_denom_type, eg full_count, full_with_pregnancies, etc)
    """

    pre_rstr = len(df)
    # system based filters
    if cms_system == "mdcr":
        assert df.entitlement.apply(
            lambda x: isinstance(x, int)
        ).all(), "There are mixed data types, all int expected"

        keep_codes = constants.est_id_dict[cms_system][estimate_id]["entitlements"]
        df = df[df.entitlement.isin(keep_codes)]
    elif cms_system == "max":
        assert df.restricted_benefit.apply(
            lambda x: isinstance(x, str)
        ).all(), "There are mixed data types, all str expected"

        keep_codes = constants.max_denom_types[max_denom_type]
        df = df[df.restricted_benefit.isin(keep_codes)]
    else:
        raise ValueError(f"cms system must be either mdcr or max not {cms_system}")

    post_rstr = len(df)
    print(f"There were {pre_rstr - post_rstr} rows removed due to eligibility restrictions")

    if len(df) == 0:
        raise ValueError(
            "All rows of data have been removed. Something went wrong, probably related to data types"
        )
    return df
