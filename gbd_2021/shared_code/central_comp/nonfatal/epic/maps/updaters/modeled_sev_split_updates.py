import os
import json

import pandas as pd
from typing import Dict

import gbd.constants as gbd_constants

from epic.util import template_helpers

_MODELED_SEV_MAP_FP = 'modeled_proportion_severity_splits.json'
_MODELED_SEV_NAMING_PATTERN = 'modeled_proportion_{meid}'


def run_deprecations(deprecations: pd.DataFrame, current_map: dict) -> dict:
    """Remove entries from mapping dictionary given the contents of deprecation dataframe"""
    if len(deprecations) > 0:
        print("Deprecating mappings")
        for index, row in deprecations.iterrows():
            parent_me = int(row.parent_modelable_entity_id)
            name = _MODELED_SEV_NAMING_PATTERN.format(meid=parent_me)
            try:
                current_map.pop(name)
            except KeyError:
                raise KeyError(
                    f"No modeled severity split for parent: {parent_me}, name: {name}"
                )
    return current_map


def run_updates(updates: pd.DataFrame, current_map: dict) -> dict:
    """Add entires to mapping dictionary given the contents of update dataframe"""
    if len(updates) > 0:
        print("Adding new mappings")
        for parent_me in updates['parent_modelable_entity_id'].unique():
            parent_df = updates.loc[updates['parent_modelable_entity_id'] == parent_me]
            name = _MODELED_SEV_NAMING_PATTERN.format(meid=parent_me)
            ins = {str(parent_me): "__main__.ModelableEntity"}
            outs = {}
            kwargs = {"parent": int(parent_me)}
            for index, row in parent_df.iterrows():
                child_me = int(row.child_modelable_entity_id)
                proportion_me = int(row.proportion_modelable_entity_id)
                severity = str(row.severity)
                ins[str(proportion_me)] = "__main__.ModelableEntity"
                outs[str(child_me)] = "__main__.ModelableEntity"
                kwargs[severity] = {"child": child_me, "proportion": proportion_me}
            current_map[name] = {
                "class": "__main__.ModeledProportion",
                "in": ins,
                "out": outs,
                "kwargs": kwargs
            }
    return current_map


def run_all_modeled_sev(excel_path: str) -> None:
    """Run deprecations and updates for modeled sev splits and resave modified dict as json"""
    this_dir = os.path.dirname(os.path.realpath(__file__))
    json_path = os.path.join(
        this_dir, '..', 'json', _MODELED_SEV_MAP_FP
    )
    modeled_sev_map, deprecations, updates = template_helpers.read_files(
        json_path, excel_path
    )
    modeled_sev_map = run_deprecations(deprecations, modeled_sev_map)
    modeled_sev_map = run_updates(updates, modeled_sev_map)
    with open(json_path, 'w') as outfile:
        json.dump(modeled_sev_map, outfile, indent=4)


if __name__ == '__main__':
    excel_path = template_helpers.parse_arguments()
    run_all_modeled_sev(excel_path)
