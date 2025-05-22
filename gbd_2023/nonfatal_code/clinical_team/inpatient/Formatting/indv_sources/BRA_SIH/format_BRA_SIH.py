"""
Formatting CI inpatient source BRA_SIH. This is a worker script.

ADDRESS

Notes:
- There is location info available that is more detailed than state. For
example, the year 1997 has 4959 unique locations

Data lives in these folders:
FILEPATH
├── RD_1992_1999
├── RD_2000_2007
├── RD_2004_2013
└── RD_2008_2014

Specific lists of files to use are read from
FILEPATH

Outputs are saved to
FILEPATH

"""

import pickle
import sys
import warnings

import numpy as np
import pandas as pd
from simpledbf import Dbf5

# load our functions
from crosscutting_functions import *


def read_pickled_list(year):
    """
    Reads a pickled python list that contains files that should be read in for
    one year

    Parameters
    ----------
    year: int
        year to read file list for

    Returns
    -------
    list of files
    """
    print("Reading in pickled files")

    with open(
        f"FILEPATH/file_list_{year}.pickle",
        "rb",
    ) as fp:
        files_list = pickle.load(fp)
    return files_list


def read_brazil_data(files_list, year):
    """
    Reads in several raw data files for one year of data, specified by
    files_list, and keeps only the need columns. Data is not collapsed.

    Parameters
    ----------
        files_list (list): list of full filepaths

    Returns
    -------
        Pandas DataFrame for single year, according to files_list
    """
    assert len(files_list) > 0, "No files found."

    year = int(year)

    df_list = []
    for f in files_list:
        print(f"  Reading in {f}")
        df = Dbf5(f, codec="latin")
        df = df.to_dataframe()

        # Keep both municipality columns inside loop and select later
        keep = [
            "MUNIC_MOV",
            "MUNIC_RES",
            "DIAG_PRINC",
            "DIAG_SECUN",
            "IDADE",
            "COD_IDADE",
            "SEXO",
            "MORTE",
            "DT_INTER",
            "DT_SAIDA",
        ]
        existing_cols = df.columns.tolist()
        # safe way to drop only columns that surely exist
        drop_cols = list(set(existing_cols) - set(keep))
        df = df.drop(drop_cols, axis=1)

        df_list.append(df)

    print("  Appending together data")
    df = pd.concat(df_list, ignore_index=True, sort=False)

    # Select municipality column outside of for loop to prevent each loop
    # from having different choices.
    print("  Selecing municipality column")
    possible_munic_cols = df.filter(regex="MUNIC").columns.tolist()
    munic_col = select_municipality_column(df=df, year=year)
    if len(possible_munic_cols) > 1:
        munic_col_drops = list(set(possible_munic_cols) - set([munic_col]))
        df = df.drop(munic_col_drops, axis=1)

    df[munic_col] = df[munic_col].astype(str)

    return df


def select_municipality_column(df, year):

    year = int(year)

    munic_col = None
    # MUNIC_RES holds municipality of residence
    # MUNIC_MOV holds municipality of hospital
    if "MUNIC_MOV" in df.columns:
        munic_col = "MUNIC_MOV"
        print(f"MUNIC_MOV is in the columns, munic_col is {munic_col}.")
    if "MUNIC_RES" in df.columns:
        munic_col = "MUNIC_RES"
        print(f"MUNIC_RES is in the columns, munic_col is {munic_col}.")
    if year == 1994:
        # in 1994 91% of MUNIC_RES is missing, so use MUNIC_MOV
        munic_col = "MUNIC_MOV"
        print(f"year is 1994 is the year, munic_col is {munic_col}.")
    if munic_col is None:
        raise ValueError("Location column wasn't identified")

    return munic_col


def make_location_id(df, munic_col):
    """
    Adds location_id column to dataframe. NOTE that munic_col represents
    municipality, which is more geographically detailed than State; the first
    two digits of the code represent the state.

    Parameters
    ----------
    df: Pandas DataFrame
        data without location_id column

    Returns
    -------
    Pandas DataFrame with location_id column added
    """

    location_dict = {
        "12": 4750,  # Acre
        "27": 4751,  # Alagoas
        "13": 4752,  # Amazonas
        "16": 4753,  # Amapa
        "29": 4754,  # Bahia
        "23": 4755,  # Ceara
        "53": 4756,  # Distrito Federal
        "32": 4757,  # Espirito Santo
        "52": 4758,  # Goias
        "21": 4759,  # Maranhao
        "31": 4760,  # Minas Gerais
        "50": 4761,  # Mato Grosso do Sul
        "51": 4762,  # Mato Grosso
        "15": 4763,  # Para
        "25": 4764,  # Paraiba
        "41": 4765,  # Parana
        "26": 4766,  # Pernambuco
        "20": 4766,  # Pernambuco
        "22": 4767,  # Piaui
        "33": 4768,  # Rio de Janeiro
        "24": 4769,  # Rio de Janeiro do Norte
        "11": 4770,  # Rondonia
        "14": 4771,  # Roraima
        "43": 4772,  # Rio Grande do Sul
        "42": 4773,  # Santa Catarina
        "28": 4774,  # Sergipe
        "35": 4775,  # Sao Paulo
        "17": 4776,  # Tocantins
    }

    df = df[df[munic_col].notnull()]

    df[munic_col] = df[munic_col].astype(str)

    df["MUNIC_RES_substr"] = df[munic_col].str[0:2].copy()

    df["location_id"] = df.MUNIC_RES_substr.map(location_dict)

    print("  Handling missing locations")
    # We don't know what any of these locations are
    missing_location_mask = df.MUNIC_RES_substr.isin(
        ["00", "02", "03", "60", "61", "na"]
    )  # Added 0. and na
    missing_location_values = (
        df[missing_location_mask].MUNIC_RES_substr.unique().tolist()
    )

    if missing_location_mask.any():
        warnings.warn(
            "We don't know what these values of MUNIC_RES_substr are: "
            f"{missing_location_values}. "
            "They're being dropped from this data."
        )
        df = df[~missing_location_mask]

    # "99" in MUNIC_RES_substr means unknown municipality or residence
    df = df[df.MUNIC_RES_substr != "99"]

    null_location_ids = df[df.location_id.isnull()].MUNIC_RES_substr.unique().tolist()
    assert_msg = (
        "There are null location_ids for these values of "
        f"MUNIC_RES_substr: {null_location_ids}"
    )
    assert df.location_id.notnull().all(), assert_msg

    df["location_id"] = df.location_id.astype(int)

    df = df.drop([munic_col, "MUNIC_RES_substr"], axis=1)

    return df


def make_age_groups(df):
    """
    Adds age_start and age_end to dataframe

    Parameters
    ----------
    df: Pandas DataFrame
        data without age columns

    Returns
    -------
    Pandas DataFrame with age_start and age_end column added
    """

    age_unit_dict = {
        "0": "ignored",
        "1": "unknown",  # Literally do not know, it's not in the codebook.
        "2": "days",
        "3": "months",
        "4": "years",
        "5": "centuries",
    }

    df["COD_IDADE"] = df.COD_IDADE.astype(str)

    df["age_unit"] = df.COD_IDADE.map(age_unit_dict)

    print("  Handling missing age units")
    null_age_units = df[df.age_unit.isnull()].COD_IDADE.unique().tolist()
    null_ages = df[df.age_unit.isnull()].IDADE.unique().tolist()
    assert_msg = (
        "There are null age_units for these values of "
        f"COD_IDADE: {null_age_units}, "
        f"and these values of IDADE: {null_ages}"
    )
    assert df.age_unit.notnull().all(), assert_msg

    df["age"] = np.nan

    # Nulls will get set to 0 - 125, which will ultimately be split
    df.loc[df.age_unit == "ignored", "age"] = np.nan

    # Unknown will get set to 0 - 125, which will ultimately be split
    # This only appeared in some of the earliers, no later than 1996
    df.loc[df.age_unit == "unknown", "age"] = np.nan

    df.loc[df.age_unit == "days", "age"] = df.loc[df.age_unit == "days", "IDADE"] / 365

    df.loc[df.age_unit == "months", "age"] = (
        df.loc[df.age_unit == "months", "IDADE"] / 12
    )

    df.loc[df.age_unit == "years", "age"] = df.loc[df.age_unit == "years", "IDADE"]

    # The values in IDADE look like years past 100 years, e.g., 4 (100 + 4)
    df.loc[df.age_unit == "centuries", "age"] = (
        df.loc[df.age_unit == "centuries", "IDADE"] + 100
    )

    print("  Binning ages")
    df = hosp_prep.age_binning(df, clinical_age_group_set_id=2)
    assert df.age_start.notnull().all()
    assert df.age_end.notnull().all()

    df = df.drop(["age", "IDADE", "COD_IDADE", "age_unit"], axis=1)

    return df


def make_sex(df):
    """
    Adds sex_id to the dataframe

    Parameters
    ----------
    df: Pandas DataFrame
        data without sex_id column

    Returns
    -------
    Pandas DataFrame with sex_id column added
    """

    # Original Stata code:
    # NOTE, we code sex to 3 (both) for unknown)
    # replace sex = 9 if sex != 1 & sex != 3
    # replace sex = 2 if sex == 3

    df["SEXO"] = df.SEXO.astype(int)

    df["sex_id"] = np.nan

    df.loc[(df.SEXO != 1) & (df.SEXO != 3), "sex_id"] = 3

    df.loc[df.SEXO == 3, "sex_id"] = 2

    df.loc[df.SEXO == 1, "sex_id"] = 1

    df = df.drop("SEXO", axis=1)

    df["sex_id"] = df.sex_id.astype(int)

    return df


def remove_day_cases(df):
    """
    Drops rows where date of admittance and discharge are the same

    Parameters
    ----------
    df: Pandas DataFrame

    Returns
    -------
    Pandas DataFrame with day cases dropped
    """

    df = df[df.DT_SAIDA != df.DT_INTER]

    df = df.drop(["DT_INTER", "DT_SAIDA"], axis=1)

    return df


def create_outcome_id(df):
    """
    Adds outcome_id column to dataframe, with values "discharge" and "death"

    Parameters
    ----------
    df: Pandas DataFrame
        data without outcome_id column

    Returns
    -------
    Pandas DataFrame with outcome_id column added
    """

    df["MORTE"] = df.MORTE.astype(int)

    df["outcome_id"] = df.MORTE.map({0: "discharge", 1: "death"})

    df = df.drop(["MORTE"], axis=1)

    return df


def make_dx_cols(df):
    """
    adds columns dx_1 and dx_2 to the dataframe, which contain ICD codes.

    Parameters
    ----------
    df: Pandas DataFrame
        data without dx_1 and dx_2 columns

    Returns
    -------
    Pandas DataFrame with dx_1 and dx_2 columns added
    """

    df = df.rename(columns={"DIAG_PRINC": "dx_1", "DIAG_SECUN": "dx_2"})

    # drop null dx_1, if there are any
    df = df[df.dx_1.notnull()]

    # fill nulls in dx_2, handle them in master script:
    if "dx_2" in df.columns:
        df.loc[df["dx_2"].isnull(), "dx_2"] = "NULL"

    return df


def collapse_data(df):
    """
    Aggregates the data, converting it from one representing an admission, to
    one row representing a tabluated total of admissions for a
    demographic + ICD code.

    Parameters
    ----------
    df: Pandas DataFrame
        uncollapsed data

    Returns
    -------
    Pandas DataFrame collapsed by all columns besided value column
    """

    group_cols = df.columns.tolist()

    df["val"] = 1

    null_values = df[group_cols].isnull().sum()
    assert_msg = f"Any rows with nulls will be dropped.\n{null_values}"

    assert df[group_cols].notnull().all().all(), assert_msg

    df = df.groupby(by=group_cols, as_index=False).val.sum()

    return df


def save_single_year(df, year):
    """
    Saves the output of this script

    Parameters
    ----------
    df: Pandas DataFrame
        data
    year: int
        year of data to save

    Returns
    -------
    None
    """
    save_filepath = (
        "FILEPATH"
        f"FILEPATH/{year}.csv"
    )
    df.to_csv(save_filepath, index=False)

    print(f"Saved to {save_filepath}")


def main(year):
    """
    Processes a single year of data.

    Parameters
    ----------
    year: int
        year of data to save

    Returns
    -------
    None
    """
    year = int(year)
    print(f"Formatting data for {year}")

    files_list = read_pickled_list(year)
    print("Reading in Brazil data")
    df = read_brazil_data(files_list=files_list, year=year)

    print("Selecting municipality column again")
    munic_col = select_municipality_column(df=df, year=year)

    print("Adding location IDs")
    df = make_location_id(df, munic_col)  # Error likely before here
    print("Adding age group IDs")
    df = make_age_groups(df)
    print("Adding sex IDs")
    df = make_sex(df)
    print("Removing day cases")
    df = remove_day_cases(df)
    print("Adding outcome IDs")
    df = create_outcome_id(df)
    print("Making dx columns")
    df = make_dx_cols(df)
    print("Collapsing data")
    df = collapse_data(df)

    df["year_id"] = year

    print("Saving single-year data")
    save_single_year(df, year)
    return df


if __name__ == "__main__":
    year = sys.argv[1]
    main(year)
