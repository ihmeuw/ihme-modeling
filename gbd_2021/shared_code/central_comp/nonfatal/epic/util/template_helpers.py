import argparse
import json

import pandas as pd
from typing import Dict, List
from sqlalchemy import orm

from db_tools import ezfuncs, query_tools
import gbd.conn_defs as gbd_conn_defs

import epic.util.queries as epic_queries

_DEPRECATE_SHEET_NAME = "Deprecate"
_UPDATE_SHEET_NAME = "New"


def load_json_map(path: str) -> Dict[str, str]:
    """Read the json file located at a given path as a dictionary"""
    with open(path) as json_file:
        try:
            process_dict = json.load(json_file, parse_int=True)
        except TypeError:
            with open(path) as json_file:
                process_dict = json.load(json_file)
    return process_dict


def load_sheet(path: str, sheet_name: str) -> pd.DataFrame:
    """Read a named sheet from an excel file as a dataframe, skipping the second row"""
    sheet = pd.read_excel(path, sheet_name=sheet_name, skiprows=[1])
    return sheet


def fetch_db_name(meid: int, session: orm.Session) -> pd.DataFrame:
    """Pull a given modelable_entity_id's name from the epi DB"""
    me_name = query_tools.query_2_df(
        epic_queries.ME_NAME,
        session=session,
        parameters={"meid": meid}
    )
    me_name = me_name['modelable_entity_name'].item()
    return me_name


def read_files(json_path: str, excel_path: str) -> List[pd.DataFrame]:
    """Reads the main json map, as well as deprecations and updates, and returns all three"""
    current_map = load_json_map(json_path)
    deprecations = load_sheet(excel_path, _DEPRECATE_SHEET_NAME)
    updates = load_sheet(excel_path, _UPDATE_SHEET_NAME)
    return current_map, deprecations, updates


def parse_arguments() -> str:
    """Parses the single argument for update scripts, path to updates/deprecations excel"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--excel_path', type=str)
    args = parser.parse_args()
    return args.excel_path
