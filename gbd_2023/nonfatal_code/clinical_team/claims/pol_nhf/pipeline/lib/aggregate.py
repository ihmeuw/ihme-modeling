import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import poland as constants


def create_estimate_id_values(df: pd.DataFrame, deliverable: str) -> pd.DataFrame:
    """
    Create count value

    Aggregate data by deliverable pre defined requirments
    """
    if "val" not in df.columns:
        df["val"] = 1

    sum_cols = constants.DELIVERABLE[deliverable]["sum_cols"]
    gb_cols = constants.DELIVERABLE[deliverable]["groupby_cols"]
    post_agg_check = {col: df[col].sum() for col in sum_cols}

    df = df.groupby(gb_cols).agg({col: "sum" for col in sum_cols}).reset_index()

    validataion = {key: df[key].sum() == post_agg_check[key] for key in post_agg_check.keys()}

    if not all(validataion.values()):
        raise ValueError("Lost rows when performing groupby.")
    return df


def create_national_values(df: pd.DataFrame, deliverable: str) -> pd.DataFrame:
    """
    Create national level estimates

    ICD Mart only contains subnational level data
    """
    pre_val = df["val"].sum()
    national_df = df.copy()
    national_df["location_id"] = constants.POL_GBD_LOC_ID
    national_df = create_estimate_id_values(national_df, deliverable)

    if national_df["val"].sum() != pre_val:
        raise ValueError("Lost rows aggregating to national level")
    return pd.concat([df, national_df], sort=False)
