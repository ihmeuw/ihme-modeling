import pandas as pd
from crosscutting_functions import demographic, legacy_pipeline

from pol_nhf.pipeline.lib import mapping
from pol_nhf.utils.settings import PipelineAgeSettings


def prep_for_squaring(df: pd.DataFrame) -> pd.DataFrame:
    """Reshape data to meet hosp prep squaring method requirments."""
    df["source"] = "POL_NHF"
    df["year_start"] = df["year_id"]
    df["year_end"] = df["year_id"]

    df = df.drop(["age_start", "age_end"], axis=1)
    return df


def clean_squared_data(df: pd.DataFrame) -> pd.DataFrame:
    """Refill column values and drop columns need for squaring."""
    df = df.drop(["source", "year_id", "year_end"], axis=1)
    df = df.rename({"year_start": "year_id"}, axis=1)
    estimate_id = df[df.estimate_id.notnull()]["estimate_id"].iloc[0]
    df["estimate_id"] = int(estimate_id)
    df["val"] = df["val"].astype(int)

    df = demographic.group_id_start_end_switcher(
        df=df,
        clinical_age_group_set_id=PipelineAgeSettings.clinical_age_group_id,
        remove_cols=False,
    )
    return df


def create_square_data(df: pd.DataFrame, map_version: int) -> pd.DataFrame:
    """Square the data by creating zeros for age / sexes that should be present in the data."""
    age_df = demographic.get_hospital_age_groups(
        clinical_age_group_set_id=PipelineAgeSettings.clinical_age_group_id
    )
    ages = age_df.age_group_id.tolist()
    df = prep_for_squaring(df)
    df = legacy_pipeline.make_zeros(
        df=df, cols_to_square=["val"], ages=ages, etiology="bundle_id"
    )
    df = mapping.apply_bundle_age_sex_restrictions(df=df, map_version=map_version)
    df = clean_squared_data(df)
    return df
