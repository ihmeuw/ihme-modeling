import os
import json

import pandas as pd
import numpy as np

from db_tools import ezfuncs
from epic.util.constants import Params
from gbd.constants import GBD_ROUND_ID
from gbd.decomp_step import decomp_step_id_from_decomp_step


def generate_como_config(
        decomp_step_id: int,
        gbd_round_id: int=GBD_ROUND_ID
) -> None:
    here = os.path.dirname(os.path.realpath(__file__))

    # healthstate_id 639 is 
    # 'Post-COMO calculation for residuals (YLL/YLD ratio, other methods)'
    # and should not be used in initial COMO calculations.

    q = """
    SELECT
        modelable_entity_id
    FROM
        epic.sequela_hierarchy_history
    WHERE
        sequela_set_version_id = (
            SELECT
                sequela_set_version_id
            FROM
                sequela_set_version_active
            WHERE gbd_round_id = :gbd_round_id
                and decomp_step_id = :decomp_step_id)
        AND most_detailed = 1
        AND healthstate_id != 639
    """
    # get data
    me_ids = ezfuncs.query(
        q,
        parameters={
            Params.GBD_ROUND_ID: gbd_round_id,
            Params.DECOMP_STEP_ID: decomp_step_id
        },
        conn_def="epic"
    )["modelable_entity_id"].tolist()

    # build map
    process_map = {}
    process_map["como"] = {}
    process_map["como"]["class"] = "__main__.COMO"
    process_map["como"]["in"] = {}
    process_map["como"]["out"] = {}

    # add inputs
    for me_id in me_ids:
        process_map["como"]["in"][str(int(me_id))] = (
            "__main__.ModelableEntity")
    process_map["como"]["out"]["como_version"] = "__main__.ComoVersion"

    # write to disk
    with open(os.path.join(here, "..", "json", "como.json"), "w") as outfile:
        json.dump(process_map, outfile, sort_keys=True, indent=2)


def generate_severity_split_config(gbd_round_id=GBD_ROUND_ID) -> None:
    here = os.path.dirname(os.path.realpath(__file__))
    q = """
    select
        distinct parent_meid, child_meid, split_id
    from
        severity_splits.split_version
    join
        severity_splits.split_proportion using (split_version_id)
    where
        is_best = 1
        and gbd_round_id=:gbd_round_id
    """
    df = ezfuncs.query(
        q,
        parameters={"gbd_round_id": gbd_round_id},
        conn_def="epi"
    )

    dct = {}
    for key in df["parent_meid"].unique():

        dct["split_" + str(key)] = {}
        dct["split_" + str(key)]["class"] = "__main__.SevSplits"
        dct["split_" + str(key)]["in"] = {}
        dct["split_" + str(key)]["in"][str(key)] = "__main__.ModelableEntity"
        dct["split_" + str(key)]["out"] = {}
        for out in df.ix[df["parent_meid"] == key, "child_meid"].unique():
            dct["split_" + str(key)]["out"][str(out)
                                            ] = "__main__.ModelableEntity"

        dct["split_" + str(key)]["args"] = []
        split_id = (
            df.loc[
                df["parent_meid"] == key, "split_id"].unique().item())
        dct["split_" + str(key)]["kwargs"] = {
            "split_id": str(split_id)}

    this_file = os.path.realpath(__file__)
    this_dir = os.path.dirname(this_file)
    with open(os.path.join(
        this_dir, '..','json','severity_splits.json'), "w") as outfile:
        json.dump(dct, outfile, indent=2)


def generate_super_squeeze_config() -> None:
    code_dir = os.path.dirname(os.path.realpath(__file__))

    pre_pos_path = os.path.join(code_dir, "..", "..", "lib", "data",
                              "map_pre_pos_mes.csv")
    pre_pos_df = pd.read_csv(pre_pos_path)
    pre_pos_inputs = pre_pos_df.modelable_entity_id_source.unique().tolist()

    source_target_path = os.path.join(code_dir, "..", "..", "lib", "data",
                                    "source_target_maps.csv")
    source_target_df = pd.read_csv(source_target_path)
    source_target_inputs = source_target_df.me_id.unique().tolist()
    source_target_outputs = source_target_df.resid_target_me.unique().tolist()


    inputs = list(
      set(source_target_df[source_target_df.me_id.notnull()
                           ].me_id.unique().tolist() +
          pre_pos_df[pre_pos_df.modelable_entity_id_source.notnull()
                     ].modelable_entity_id_source.unique().tolist()))
    outputs = list(
      set(pre_pos_df[pre_pos_df.modelable_entity_id_target.notnull()
                     ].modelable_entity_id_target.unique().tolist()))

    dct = {}
    dct["super_squeeze"] = {}
    dct["super_squeeze"]["class"] = "__main__.SuperSqueeze"
    dct["super_squeeze"]["in"] = {}
    # envelopes
    for key in inputs + [2403, 9805, 9423, 9424, 9425, 9426, 9427]:
      dct["super_squeeze"]["in"][str(int(key))] = "__main__.ModelableEntity"
    dct["super_squeeze"]["out"] = {}
    # remainder of the envelopes
    for key in outputs + [2000, 1999, 2001, 2002, 2003]:
      if key != np.NaN:
          dct["super_squeeze"]["out"][str(int(key))] = (
            "__main__.ModelableEntity"
          )
    dct["super_squeeze"]["args"] = []
    dct["super_squeeze"]["kwargs"] = {}

    with open(os.path.abspath(
    os.path.join(code_dir, '../json/super_squeeze.json')), "w") as outfile:
      json.dump(dct, outfile, indent=2)


def generate_maps(decomp_step: int, gbd_round_id: int) -> None:
    generate_como_config(
        decomp_step_id_from_decomp_step(decomp_step, gbd_round_id),
        gbd_round_id
    )
    generate_severity_split_config(gbd_round_id)
    generate_super_squeeze_config()