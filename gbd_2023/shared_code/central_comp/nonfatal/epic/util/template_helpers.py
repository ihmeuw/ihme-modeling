import argparse
import json
from typing import Dict

import pandas as pd
from sqlalchemy import orm

from db_tools import query_tools

import epic.lib.util.queries as epic_queries
from epic.lib.util import common

DEPRECATE_SHEET_NAME = "Deprecate"
UPDATE_SHEET_NAME = "New"


def load_json_map(path: str) -> Dict[str, common.ConfigDict]:
    """Read the json file located at a given path as a dictionary"""
    with open(path) as json_file:
        try:
            process_dict = json.load(json_file, parse_int=int)
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
        epic_queries.ME_NAME, session=session, parameters={"meid": meid}
    )
    me_name = me_name["modelable_entity_name"].item()
    return me_name


def parse_arguments() -> str:
    """Parses the single argument for update scripts, path to updates/deprecations excel"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--excel_path", type=str)
    args = parser.parse_args()
    return args.excel_path
