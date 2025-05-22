import pandas as pd
from crosscutting_functions.pipeline import get_map_version

from inpatient.CorrectionsFactors.correction_inputs import estimate_indv
from inpatient.CorrectionsFactors.correction_inputs.sources import nzl_cf_constants


def format_nzl_df(year: int) -> pd.DataFrame:
    path = f"{nzl_cf_constants.BASE_DIR}/nzl_nmds_{year}_stage_1.parquet"
    df = pd.read_parquet(path, columns=nzl_cf_constants.COLS)

    # keep only inpatient
    df = df.loc[df["facility_type_id"] == 1]
    df = df.drop("facility_type_id", axis=1)

    # rename cols to fit our schema
    df = df.rename(columns={"admission_date": "adm_date"})
    df["adm_date"] = pd.to_datetime(df["adm_date"])

    # remove same day cases
    df = df[df["los"] >= 1]
    df = df.drop("los", axis=1)

    df["is_otp"] = 0

    return df


def main(run_id: int, year: int) -> None:
    # output path
    write_path = (
        "FILEPATH/bundle_NZL_NMDS_{year}.csv"
    )
    df = format_nzl_df(year)

    if not all(df["patient_id"].isnull()) and not all(df["age"].isnull()):
        map_version = get_map_version(run_id=run_id)
        df = estimate_indv.main(df, map_version, nzl_cf_constants.CLINICAL_AGE_GROUP_SET_ID)
        # write output
        df.to_csv(write_path, index=False)
        print(f"{year} NZL done.")
    else:
        print(f"This {year} of NZL df does not have valid patient id and/or age.")
        del df
