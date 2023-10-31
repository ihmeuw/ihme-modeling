import pandas as pd
import os
import warnings

# load our functions
from clinical_info.Functions import hosp_prep

# Environment:
ROOT = "FILEPATH"


def read_raw_greece_data():
    """Reads in the rawest data from FILEPATH"""
    files = [f"{ROOT}/{f}" for f in os.listdir(ROOT) if ".XLSX" in f]

    df = pd.read_excel(files[0], header=1)
    df_list = [df]
    for f in files[1:]:
        df = pd.concat(df_list, ignore_index=True, sort=False)
        temp = pd.read_excel(f, header=1)
        assert not set(temp.columns).symmetric_difference(set(df.columns))
        df_list.append(temp)

    df = pd.concat(df_list, ignore_index=True, sort=False)

    return df


def clean_raw_data(df):
    """
    Cleans raw data; Makes column names lowercase and replaces spaces with
    underscores
    """
    df = df.rename(
        columns=dict(
            zip(
                df.columns.tolist(),
                [col.replace(" ", "_").lower() for col in df.columns.tolist()],
            )
        )
    )
    return df


def drop_unneeded_columns(df):
    """
    Removes columns such as 'Unnamed: 9', 'description of diagnosis',
    'station', and 'days of hospitalization'
    """
    assert set(df["Unnamed: 9"].unique().tolist()) == set(
        [" ", pd.np.nan]
    ), "This column should only have ' ' and NaN."
    return df.drop(
        [
            "Unnamed: 9",
            "description of diagnosis",
            "station",
            "days of hospitalization",
        ],
        axis=1,
    )


def convert_dates(df, col):
    """Casts columns with date information from strings to datetime"""
    df.loc[:, col] = pd.to_datetime(df.loc[:, col], dayfirst=True)

    return df


def make_year(df):
    """
    Extracts year from the dates in the discharge date column to make
    year_start and year_end columns
    """
    df = convert_dates(df, "discharge_date")
    df.loc[:, "year_start"] = df.loc[:, "discharge_date"].dt.year
    assert df["year_start"].notnull().all(), "Shouldn't be any nulls in year_start"
    df.loc[:, "year_end"] = df.loc[:, "year_start"]
    return df


def remove_day_cases(df):
    """
    Removes day cases by keeping rows where date of discharge does not equal
    date of admission.
    """
    # days_of_hospitalization is not accurate, often it is Null, or incorrect.
    df = df[df.admission_date != df.discharge_date]
    df = df.drop(["admission_date", "discharge_date"], axis=1)
    return df


def make_sex(df):
    """Converts sex values to sex_id"""
    sex_dict = {"MALE": 1, "FEMALE": 2}

    assert set(df.gender.unique()) == {
        "MALE",
        "FEMALE",
        pd.np.nan,
    }, "There are unaccounted for sex values"

    df["sex_id"] = df.gender.map(sex_dict)
    # map null gender to "both" sex, so that there is an associated population
    # for age sex splitting. There's no population for "unknown" sex.
    df.loc[df.gender.isnull(), "sex_id"] = 3

    assert df.sex_id.notnull().all(), "There are missing sex values"

    df = df.drop("gender", axis=1)

    return df


def make_age(df):
    """Bins ages into age_start and age_end"""
    df = hosp_prep.age_binning(
        df=df, drop_age=True, terminal_age_in_data=True, under1_age_detail=False
    )

    assert df.age_start.notnull().all(), "There are missing values in age_start"
    assert df.age_end.notnull().all(), "There are missing values in age_end"

    return df


def make_cause_code(df):
    """Removes null/missing ICD codes, and cleans ICD codes."""
    mask = df.icd_code.notnull()
    warning_msg = f"""There are {df[~mask].shape[0]} rows out of {df.shape[0]}
    with null ICD codes that are being dropped."""
    warnings.warn(warning_msg)
    df = df.loc[mask, :]  # drop rows where icd_code is null
    df.loc[:, "icd_code"] = df.loc[:, "icd_code"].astype(str)
    df.loc[:, "icd_code"] = hosp_prep.sanitize_diagnoses(df.loc[:, "icd_code"])
    df = df.rename(columns={"icd_code": "cause_code"})

    return df


def make_outcome_id(df):
    """
    Drops rows where outcome is misisng, and converts them to the outcome
    labels we use.
    """
    mask = df.outcome.isnull()
    warning_msg = f"""There are {df[mask].shape[0]} rows out of {df.shape[0]}
    with null outcomes that are being dropped."""
    warnings.warn(warning_msg)
    df = df.loc[df.outcome.notnull(), :]

    # our system only allows for, death, discharge, or "case" for when
    # discharges and deaths cannot be distinguished. So, for these values,
    # if it is not a death, then it is a discharge.
    outcome_dict = {
        "IMPROVEMENT": "discharge",
        "UNCHANGED": "discharge",
        "DEATH": "death",
        "CURE": "discharge",
        "DETERIORATION": "discharge",
    }

    df["outcome_id"] = df.outcome.map(outcome_dict)
    assert df.outcome_id.notnull().all(), f"""There are outcomes that are not
        accounted for: {df[df.outcome_id.isnull()].outcome.unique()}"""
    df = df.drop("outcome", axis=1)

    return df


def make_nid(df):
    """Attaches NID."""
    nid_dict = {
        2012: 432298,
        2013: 432299,
        2014: 432300,
        2015: 432301,
        2016: 432304,
        2017: 432305,
        2018: 432306,
        2019: 432307,
    }

    df["nid"] = df.year_start.map(nid_dict)

    assert df.nid.notnull().all(), f"""There are missing values of NID for
        years {df.loc[df.nid.isnull(), 'year_start'].unique()}"""

    return df


def finalize_columns(df):
    """Fill out the remaining columns and check that they're all present"""

    hosp_frmat_feat = [
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

    df["age_group_unit"] = 1
    df["location_id"] = 82
    df["representative_id"] = 3
    df["diagnosis_id"] = 1
    df["metric_id"] = 1
    df["source"] = "GRC_UTH"
    df["facility_id"] = "inpatient unknown"
    df["code_system_id"] = 2

    # This data is not already tabulated
    df["val"] = 1

    missing_columns = set(hosp_frmat_feat) - set(df.columns)
    assert not missing_columns, f"The columns {missing_columns} are missing"

    # Using this to drop anything that shouldn't be here
    df = df.loc[:, hosp_frmat_feat]

    return df


def collapse_formatted_data(df):
    """Final collapse of data, converting it to aggregated tabulations."""
    group_vars = [
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
    assert (
        df[group_vars + ["val"]].notnull().any().any()
    ), f"There cannot be nulls in any of these columns:{group_vars + ['val']}"
    df = df.groupby(group_vars).agg({"val": "sum"}).reset_index()

    return df


def check_formatted_data(df):
    """Checks and tests copied from the template script."""
    # check data types
    for i in df.drop(
        ["cause_code", "source", "facility_id", "outcome_id"], axis=1, inplace=False
    ).columns:
        # assert that everything but cause_code, source, measure_id (for now)
        # are NOT object
        assert df[i].dtype != object, "%s should not be of type object" % (i)

    # check number of unique feature levels
    # assert len(df['year_start'].unique()) == len(df['nid'].unique()),\
    #     "number of feature levels of years and nid should match number"
    assert (
        len(df["diagnosis_id"].unique()) <= 2
    ), "diagnosis_id should have 2 or less feature levels"
    assert not set(df["sex_id"].unique()).symmetric_difference(
        [1, 2, 3]
    ), "There should only be three feature levels to sex_id"
    assert (
        len(df["code_system_id"].unique()) <= 2
    ), "code_system_id should have 2 or less feature levels"
    assert len(df["source"].unique()) == 1, "source should only have one feature level"

    assert (df.val >= 0).all(), "for some reason there are negative case counts"


def save_formated_GRC_UTH(df):
    """Saves data for inpatient pipeline."""
    # Saving the file
    write_path = "FILEPATH"

    hosp_prep.write_hosp_file(df, write_path, backup=True)


def greece_main():
    """Formats Greece inpatient data."""
    df = read_raw_greece_data()
    df = drop_unneeded_columns(df)
    df = clean_raw_data(df)
    df = make_year(df)
    df = remove_day_cases(df)
    df = make_age(df)
    df = make_sex(df)
    df = make_cause_code(df)
    df = make_outcome_id(df)
    df = make_nid(df)
    df = finalize_columns(df)
    df = collapse_formatted_data(df)
    check_formatted_data(df)
    save_formated_GRC_UTH(df)


if __name__ == "__main__":
    greece_main()
