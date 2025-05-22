"""check location mapping worker script"""

import pickle
import sys
import warnings

import pandas as pd
from db_queries import get_location_metadata
from simpledbf import Dbf5

# load our functions
from clinical_info.Functions import hosp_prep


def check_loc_read_pickled_list(year):
    """
    Reads a pickled python list that contains files that should be read in for
    one year

    Parameters
    ----------
    year: int

    Returns
    -------
    file_list
    """
    with open(
        f"FILEPATH/file_list_{year}.pickle",
        "rb",
    ) as fp:
        files_list = pickle.load(fp)
    return files_list


def check_loc_read_brazil_data(files_list):
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

    df_list = []
    for f in files_list:

        df = Dbf5(f, codec="latin")
        df = df.to_dataframe()

        # get state_name
        state_name = f.split("/")[8]
        df["state_name"] = state_name

        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True, sort=False)

    return df


def check_loc_make_location_id(df, munic_col):
    """
    Adds location_id column to dataframe

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

    df["MUNIC_RES_substr"] = df[munic_col].str[0:2].copy()

    df["location_id"] = df[munic_col].map(location_dict)

    locs = get_location_metadata(location_set_id=35)

    locs = locs[["location_id", "location_ascii_name"]]
    locs = locs.rename(columns={"location_ascii_name": "location_name"})

    df = df.merge(locs, how="left", on="location_id")

    return df


def check_loc_main(year):
    """
    Processes a single year of data.

    Parameters
    ----------
    year: int

    Returns
    -------
    None
    """
    files_list = check_loc_read_pickled_list(year)
    df = check_loc_read_brazil_data(files_list)

    if "MUNIC_RES" in df.columns:
        munic_col = "MUNIC_RES"
    elif "MUNIC_MOV" in df.columns:
        munic_col = "MUNIC_MOV"
    else:
        raise ValueError("Location column wasn't identified")

    df = df[[munic_col, "state_name"]].copy()
    df = check_loc_make_location_id(df, munic_col)

    df = df[["MUNIC_RES_substr", "location_id", "state_name", "location_name"]].copy()

    df = df[df.MUNIC_RES_substr != "00"]

    df["year"] = year

    df.to_csv(
        f"FILEPATH/{year}_location_mapping.csv",
        index=False,
    )


if __name__ == "__main__":
    year = sys.argv[1]
    check_loc_main(year)
