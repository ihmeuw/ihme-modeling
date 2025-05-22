import pandas as pd
from crosscutting_functions.pipeline import get_map_version

from inpatient.CorrectionsFactors.correction_inputs import estimate_indv
from inpatient.CorrectionsFactors.correction_inputs.sources import hcup_cf_constants


def format_hcup_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add/remove columns as required and cast to expected data types."""
    # create adm_date feature using amonth and ayear
    df["aday"] = 1
    # fill missing years with data year
    df.loc[df["adm_year"].isnull(), "adm_year"] = df.loc[df["adm_year"].isnull(), "year_start"]
    # fill missing months with january
    df.loc[df["adm_month"].isnull(), "adm_month"] = 1

    # convert to type int for pd.to_datetime func to work properly
    df["adm_month"] = df["adm_month"].astype(int)
    df["adm_year"] = df["adm_year"].astype(int)

    df["adm_date_str"] = (
        df["adm_year"].astype(str)
        + "/"
        + df["adm_month"].astype(str)
        + "/"
        + df["aday"].astype(str)
    )
    df["adm_date"] = pd.to_datetime(df["adm_date_str"])

    # drop the cols we used to make adm_date
    df = df.drop(["adm_date_str", "adm_month", "adm_year", "aday"], axis=1)
    df["is_otp"] = 0

    return df


def main(run_id: int, filepath: str) -> pd.DataFrame:
    """Process a single input file from the HCUP CDW for use in the correction factor
    modeling process."""
    df = pd.read_parquet(filepath, columns=hcup_cf_constants.COLS)

    if not all(df["patient_id"].isnull()) and not all(df["age"].isnull()):
        df = format_hcup_df(df)
        # deduplicate and compile for all estimate ids
        map_version = get_map_version(run_id=run_id)
        df = estimate_indv.main(df, map_version, hcup_cf_constants.CLINICAL_AGE_GROUP_SET_ID)

        year = int(df["year_start"].unique()[0])
        loc = int(df["location_id"].unique()[0])
        write_path = (
            "FILEPATH/bundle_USA_HCUP_SID_{year}_{loc}.csv"
        )

        df.to_csv(write_path, index=False)
        print(f"{loc}_{year} done!")
    else:
        print(f"This file ({filepath}) does not have valid patient id and/or age.")
        del df
