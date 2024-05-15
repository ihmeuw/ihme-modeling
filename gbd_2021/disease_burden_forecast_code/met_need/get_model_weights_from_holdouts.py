"""Collects submodel predictive validity statistics to compile sampling weights for genem.

The output is a file called `all_model_weights.csv` in the out version
"""

import glob
import os
import re
from typing import List, Tuple

import numpy as np
import pandas as pd
from fhs_lib_file_interface.lib.pandas_wrapper import read_csv, write_csv
from fhs_lib_file_interface.lib.version_metadata import (
    FHSDirSpec,
    FHSFileSpec,
    VersionMetadata,
)

from fhs_lib_genem.lib.constants import FileSystemConstants, ModelConstants, SEVConstants
from fhs_lib_genem.lib.model_restrictions import ModelRestrictions


def pv_file_name_breakup(file_path: str) -> Tuple[str, str]:
    r"""Parse full path to predictive-validity (PV) file, extracting entity name and suffix.

    Ex: if file_path is /ihme/forecasting/data/6/future/paf/huh/dud_pv.csv,
        then entity = "dud", suffix = "_pv.csv".
        If \*/dud_intrinsic_pv.csv, then entity = "dud",
        suffix = "_intrinsic_pv.csv".

    Args:
        file_path (str): full pv-file path, expected to end with "_pv.csv".

    Returns:
        Tuple[str, str]: entity and suffix
    """
    filename = os.path.basename(file_path)
    match = re.match(r"(.*?)((_intrinsic)?_pv.csv)", filename)
    if not match:
        raise ValueError(
            "PV file path should be of the form 'foo_pv.csv' or 'foo_intrinsic_pv.csv'"
        )
    entity, suffix, _ = match.groups()
    if not entity or not suffix:
        raise ValueError(
            "PV file path should be of the form 'foo_pv.csv' or 'foo_intrinsic_pv.csv'"
        )
    return entity, suffix


def collect_model_rmses(
    out_version: VersionMetadata,
    gbd_round_id: int,
    submodel_names: List[str],
    subfolder: str,
) -> pd.DataFrame:
    """Collect submodel omega rmse values into a dataframe.

    Loops over pv versions, parses all entity-specific _pv.csv files,
    including subfolders.

    Args:
        out_version (VersionMetadata): the output version for the whole model. Where this
            function looks for submodels.
        gbd_round_id (int): gbd_round_id used in the model; used for looking for submodels if
            not provided with out_version
        submodel_names (List[str]): names of all the sub-models to collect.
        subfolder (str): subfolder name where intrinsics are stored.

    Returns:
        (pd.DataFrame): Dataframe that contains all columns needed to compute
            ensemble weights.
    """
    combined_pv_df = pd.DataFrame([])

    # loop over versions and entities
    for submodel_name in submodel_names:
        input_dir_spec = FHSDirSpec(
            version_metadata=out_version,
            sub_path=(
                FileSystemConstants.PV_FOLDER,
                submodel_name,
            ),
        )
        subfolder_dir_spec = FHSDirSpec(
            version_metadata=out_version,
            sub_path=(
                FileSystemConstants.PV_FOLDER,
                submodel_name,
                subfolder,
            ),
        )

        if not input_dir_spec.data_path().exists():
            raise FileNotFoundError(f"No such directory {input_dir_spec.data_path()}")
        files = glob.glob(str(input_dir_spec.data_path() / "*_pv.csv"))
        entities = dict([pv_file_name_breakup(file_path) for file_path in files])

        sub_entities = {}
        if (subfolder_dir_spec.data_path()).exists():  # check out the subfolder
            files = glob.glob(str(subfolder_dir_spec.data_path() / "*_pv.csv"))
            sub_entities = dict([pv_file_name_breakup(file_path) for file_path in files])
        entities.update(sub_entities)

        for ent, suffix in entities.items():
            sub_dir = subfolder if ent in sub_entities else ""
            suffix = entities[ent]

            input_file_spec = FHSFileSpec(
                version_metadata=input_dir_spec.version_metadata,
                sub_path=tuple(list(input_dir_spec.sub_path) + [sub_dir]),
                filename=ent + suffix,
            )
            pv_df = read_csv(input_file_spec, keep_default_na=False)

            pv_df["model_name"] = submodel_name
            pv_df["subfolder"] = sub_dir
            pv_df["intrinsic"] = (
                True if SEVConstants.INTRINSIC_SEV_FILENAME_SUFFIX in suffix else False
            )
            combined_pv_df = combined_pv_df.append(pv_df)

    # just to move the "entity" column to the front
    if "entity" in combined_pv_df.columns:
        combined_pv_df = combined_pv_df[
            ["entity"] + [col for col in combined_pv_df.columns if col != "entity"]
        ]

    return combined_pv_df


def make_omega_weights(
    submodel_names: List[str],
    subfolder: str,
    out_version: VersionMetadata,
    gbd_round_id: int,
    draws: int,
    model_restrictions: ModelRestrictions,
) -> None:
    """Collect submodel omega rmse values into a dataframe.

    Loops over pv versions, parses all entity-specific _pv.csv files,
    including subfolders.

    Args:
        submodel_names (List[str]): names of all the sub-models to collect.
        gbd_round_id (int): gbd round id.
        subfolder (str): subfolder name where intrinsics are stored.
        out_version (VersionMetadata): the output version for the whole model. Where this
            function looks for submodels.
        gbd_round_id (int): gbd_round_id used in the model; used for looking for submodels if
            not provided with out_version
        draws (int): number of total draws for the ensemble.
        model_restrictions (ModelRestrictions): any arc-only, mrbrt-only restrictions.
    """
    df = collect_model_rmses(
        out_version=out_version,
        gbd_round_id=gbd_round_id,
        submodel_names=submodel_names,
        subfolder=subfolder,
    )

    out = pd.DataFrame([])

    for entity in df["entity"].unique():
        for location_id in df["location_id"].unique():
            ent_loc_df = df.query(f"entity == '{entity}' & location_id == {location_id}")

            model_type = model_restrictions.model_type(entity, location_id)

            if model_type == "arc":
                # we effectively pull 0 draws from those where rmse == np.inf
                ent_loc_df.loc[ent_loc_df["model_name"] != "arc", "rmse"] = np.inf

            if model_type == "mrbrt":
                # we effectively pull 0 draws from those where rmse == np.inf
                ent_loc_df.loc[ent_loc_df["model_name"] == "arc", "rmse"] = np.inf

            # we use rmse values to determine draws sampled from submodels
            ent_loc_df = ent_loc_df.sort_values(by="rmse", ascending=True)

            # use 1/rmse to determine weight/draws
            rmse = ent_loc_df["rmse"] + ModelConstants.MIN_RMSE  # padding in case of 0
            rmse_recip = 1 / rmse
            model_wts = rmse_recip / rmse_recip.sum()

            # lowest rmse contributes the most draws
            sub_draws = (np.round(model_wts, 3) * draws).astype(int)

            # in the event that sum(sub_draws) != draws, we make up the diff
            # by adding the diff to the first element
            if sub_draws.sum() != draws:
                sub_draws.iloc[0] += draws - sub_draws.sum()

            # now assign sub-model weight and draws to df
            ent_loc_df["model_weight"] = model_wts
            ent_loc_df["draws"] = sub_draws

            out = out.append(ent_loc_df)

    write_csv(
        df=out,
        file_spec=FHSFileSpec(
            version_metadata=out_version, filename=ModelConstants.MODEL_WEIGHTS_FILE
        ),
        sep=",",
        na_rep=".",
        index=False,
    )
