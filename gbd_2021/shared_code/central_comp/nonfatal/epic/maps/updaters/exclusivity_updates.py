import os
import json
import argparse

import pandas as pd
from typing import Dict

from db_tools import ezfuncs
import gbd.constants as gbd_constants

from epic.util import template_helpers

_EXCLUSIVITY_MAP_FP = 'exclusivity.json'


def fetch_map_name(map_dict: dict, residual: int) -> str:
    """Given a residual ID, pull the associated name from the mapping dictionary"""
    for key in map_dict.keys():
        bulk = map_dict[key]
        if str(residual) in bulk["out"].keys():
            map_residual = int(bulk["kwargs"]["me_map"]["resid"])
            if residual == map_residual:
                return key
    raise RuntimeError(f"No name found for residual {residual}")


def run_deprecations(deprecations: pd.DataFrame, current_map: dict) -> dict:
    """Remove entries from mapping dictionary given the contents of deprecation dataframe"""
    if len(deprecations) > 0:
        print("Deprecating mappings")
        for index, row in deprecations.iterrows():
            residual = row.residual_modelable_entity_id
            name = row.exclusivity_name
            if not pd.isnull(name):
                try:
                    current_map.pop(name)
                except KeyError:
                    raise KeyError(f"No exclusivity adjustment found for name {name}")
            elif not pd.isnull(residual):
                residual = int(residual)
                name = fetch_map_name(current_map, residual)
                current_map.pop(name)
            else:
                raise RuntimeError(
                    f"Expected at least one of name or residual ME, got name: {name}, "
                    f"residual: {residual}"
                )
    return current_map


def run_updates(updates: pd.DataFrame, current_map: dict) -> dict:
    """Add entires to mapping dictionary given the contents of update dataframe"""
    if len(updates) > 0:
        print("Adding new mappings")
        for index, row in updates.iterrows():
            envelope = int(row.envelope_modelable_entity_id)
            residual = int(row.residual_modelable_entity_id)
            sub_sequelae = row.sub_modelable_entity_id
            sub_sequelae = json.loads(sub_sequelae)
            sub_sequelae_types = row.sub_type
            name = row.exclusivity_name
            if pd.isnull(name):
                with ezfuncs.session_scope(gbd_conn_defs.EPI) as scoped_session:
                    residual_name = template_helpers.fetch_db_name(residual, scoped_session)
                name = residual_name.lower().replace(' ', '_')
            ins = {str(me): "__main__.ModelableEntity" for me in sub_sequelae + [envelope]}
            args = []
            if pd.isnull(sub_sequelae_types):
                sub_sequelae_dict = {str(me): {} for me in sub_sequelae}
                out = {str(residual): "__main__.ModelableEntity"}
            else:
                sub_sequelae_types = json.loads(sub_sequelae_types)
                sub_sequelae_dict = {
                    str(me): {
                        sub_sequelae_types[sub_sequelae_me]: sub_sequelae_me
                    } for me, sub_sequelae_me in zip(sub_sequelae, sub_sequelae_types)
                }
                out = {str(me): "__main__.ModelableEntity" for me in list(
                    sub_sequelae_types.keys()
                ) + [residual]}
            me_map = {"env": str(envelope), "resid": str(residual), "sub": sub_sequelae_dict}
            current_map[name] = {
                "class": "__main__.Exclusivity",
                "in": ins,
                "out": out,
                "args": args,
                "kwargs": {
                    "me_map": me_map
                }
            }
    return current_map


def run_all_exclusivity(excel_path: str) -> None:
    """Run deprecations and updates for excl adjustments and resave modified dict as json"""
    this_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(
        this_dir, '..', 'json', _EXCLUSIVITY_MAP_FP
    )
    current_map, deprecations, updates = template_helpers.read_files(json_path, excel_path)
    current_map = run_deprecations(deprecations, current_map)
    current_map = run_updates(updates, current_map)
    with open(json_path, 'w') as outfile:
        json.dump(current_map, outfile, indent=4)


if __name__ == '__main__':
    excel_path = template_helpers.parse_arguments()
    run_all_exclusivity(excel_path)
