"""Legacy code for saving files used by the model/viz tools."""
import os
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from stgpr_helpers.legacy import old
from stgpr_helpers.lib import transform_utils
from stgpr_helpers.lib.constants import columns, enums, parameters
from stgpr_helpers.lib.validation import data as data_validation


def save_to_files(
    data_df: pd.DataFrame,
    prepped_data_df: pd.DataFrame,
    covariates_df: Optional[pd.DataFrame],
    custom_stage_1_df: Optional[pd.DataFrame],
    square_df: pd.DataFrame,
    location_hierarchy_df: pd.DataFrame,
    holdout_df: Optional[pd.DataFrame],
    params: Dict[str, Any],
    output_path: str,
    population_df: pd.DataFrame,
) -> None:
    """Saves model inputs to files.

    This is a temporary step that translates new prep data structures into the old file format
    that the model and viz tools expect. As code and data is updated to use the new data
    format, code from this function should be deleted.
    """
    _save_parameters_to_file(params, output_path)
    _save_demographics_to_files(square_df, location_hierarchy_df, population_df, output_path)
    _save_data_to_file(
        data_df,
        square_df,
        prepped_data_df,
        covariates_df,
        custom_stage_1_df,
        location_hierarchy_df,
        holdout_df,
        output_path,
        params[parameters.DATA_TRANSFORM],
    )
    _save_custom_stage_1_to_file(
        output_path, custom_stage_1_df, params[parameters.DATA_TRANSFORM]
    )


def _save_parameters_to_file(params: Dict[str, Any], output_path: str,) -> None:
    config = params.copy()

    # Set legacy parameters needed for model/viz tools.
    config["custom_stage1"] = params[parameters.CUSTOM_STAGE_1]
    config["n_params"] = old.determine_n_parameter_sets(params)
    config["draws"] = params[parameters.GPR_DRAWS]

    # Update types and convert to DataFrame.
    for k, v in config.items():
        if isinstance(v, list):
            config[k] = ",".join([str(x) for x in v])
    config_df = pd.DataFrame(config, index=[0]).assign(
        bundle_id=lambda df: df.bundle_id.astype(float),
        crosswalk_version_id=lambda df: df.crosswalk_version_id.astype(float),
        covariate_id=lambda df: df.covariate_id.astype(float),
        gpr_amp_cutoff=lambda df: df.gpr_amp_cutoff.astype(float),
        holdouts=lambda df: df.holdouts.astype(float),
        model_index_id=lambda df: df.model_index_id.astype(float),
        modelable_entity_id=lambda df: df.modelable_entity_id.astype(float),
    )

    # Remove 0 from density cutoffs. It makes sense to include 0 in density cutoffs during
    # registration and prep, but the model expects density cutoffs not to start at 0.
    cutoffs = config_df["density_cutoffs"].iat[0]
    cutoffs = ",".join(cutoffs.split(",")[1:])
    config_df["density_cutoffs"] = cutoffs

    # Write to files, both HDF5 and CSV.
    config_df.to_hdf(f"{output_path}/parameters.h5", "parameters", index=False)
    config_df.to_csv(f"{output_path}/parameters.csv", index=False)

    # Save parameters for cross validation.
    grid = old.create_hyperparameter_grid(
        params, params[parameters.DENSITY_CUTOFFS], params[parameters.MODEL_TYPE]
    )
    old.set_up_hyperparam_system(output_path, grid, params[parameters.MODEL_TYPE])


def _save_demographics_to_files(
    square_df: pd.DataFrame,
    location_hierarchy_df: pd.DataFrame,
    population_df: pd.DataFrame,
    output_path: str,
) -> None:
    location_hierarchy_df = location_hierarchy_df.copy()
    for col in location_hierarchy_df:
        if col in [
            "level",
            "level_0",
            "level_1",
            "level_2",
            "level_3",
            "level_4",
            "level_5",
            "level_6",
        ]:
            location_hierarchy_df[col] = location_hierarchy_df[col].astype(object)
    path = f"{output_path}/square.h5"
    store = pd.HDFStore(path)
    store.put("square", square_df, format="fixed", data_columns=True)
    store.put("populations", population_df, format="fixed", data_columns=True)
    store.put("location_hierarchy", location_hierarchy_df, format="fixed", data_columns=True)
    store.close()
    os.chmod(path, 0o775)


def _save_data_to_file(
    data_df: pd.DataFrame,
    square_df: pd.DataFrame,
    prepped_df: pd.DataFrame,
    covariates_df: Optional[pd.DataFrame],
    custom_stage_1_df: Optional[pd.DataFrame],
    location_hierarchy_df: pd.DataFrame,
    holdout_df: Optional[pd.DataFrame],
    output_path: str,
    data_transform: str,
) -> None:
    h5_path = f"{output_path}/data.h5"

    # Save just the data.
    data_to_save_df = data_df.copy()
    data_to_save_df["data"] = data_to_save_df["val"]
    data_to_save_df.drop(columns="val", inplace=True)
    data_to_save_df.to_hdf(h5_path, "data", index=False)

    # Merge it all together.
    prepped_data_to_save_df = (
        square_df.merge(
            data_df.rename(columns={"val": "original_data", "variance": "original_variance"}),
            on=columns.DEMOGRAPHICS,
            how="left",
        )
        .merge(
            prepped_df,
            on=[
                *columns.DEMOGRAPHICS,
                columns.NID,
                columns.SAMPLE_SIZE,
                columns.IS_OUTLIER,
                columns.MEASURE_ID,
                columns.SEQ,
            ],
            how="left",
        )
        .merge(location_hierarchy_df, on=columns.LOCATION_ID, how="left")
        .assign(ko_0=enums.Holdout.USED_IN_MODEL.value)
    )
    if holdout_df is not None:
        prepped_data_to_save_df = prepped_data_to_save_df.merge(
            holdout_df, on=columns.DEMOGRAPHICS, how="left"
        )
    if covariates_df is not None:
        prepped_data_to_save_df = prepped_data_to_save_df.merge(
            covariates_df, on=columns.DEMOGRAPHICS, how="left"
        )
    if custom_stage_1_df is not None:
        data_validation.validate_data_bounds_for_transformation(
            custom_stage_1_df, columns.VAL, data_transform
        )
        prepped_data_to_save_df = prepped_data_to_save_df.merge(
            custom_stage_1_df.assign(
                cv_custom_stage_1=lambda df: transform_utils.transform_data(
                    custom_stage_1_df[columns.VAL], data_transform
                )
            ).drop(columns=columns.VAL),
            on=columns.DEMOGRAPHICS,
            how="left",
        )
    prepped_data_to_save_df.drop(
        columns=["region_id", "super_region_id", "location_set_id", "parent_id"],
        inplace=True,
    )
    for col in prepped_data_to_save_df:
        if col.startswith(columns.HOLDOUT_PREFIX):
            prepped_data_to_save_df[col] = prepped_data_to_save_df[col].astype(int)
            holdout_number = col.split("_")[1]
            prepped_data_to_save_df.rename(columns={col: f"ko_{holdout_number}"}, inplace=True)

        # I don't even want to think about why GPR Viz depends on this...
        if col in ["location_id", "year_id", "sex_id", "age_group_id"] + columns.DEMOGRAPHICS:
            prepped_data_to_save_df[col] = prepped_data_to_save_df[col].astype(int)
        elif col in [
            "level",
            "level_0",
            "level_1",
            "level_2",
            "level_3",
            "level_4",
            "level_5",
            "level_6",
        ]:
            prepped_data_to_save_df[col] = prepped_data_to_save_df[col].astype(object)
        elif col == "standard_location":
            prepped_data_to_save_df[col] = prepped_data_to_save_df[col].astype(bool)

    # Mark outliers
    prepped_data_to_save_df["data"] = prepped_data_to_save_df["val"]
    prepped_data_to_save_df["outlier_value"] = prepped_data_to_save_df["original_data"]
    prepped_data_to_save_df.loc[
        prepped_data_to_save_df[columns.IS_OUTLIER] == enums.Outlier.IS_NOT_OUTLIER.value,
        "outlier_value",
    ] = np.nan
    prepped_data_to_save_df.loc[
        prepped_data_to_save_df[columns.IS_OUTLIER] == enums.Outlier.IS_OUTLIER.value,
        "original_data",
    ] = np.nan
    prepped_data_to_save_df.loc[
        prepped_data_to_save_df[columns.IS_OUTLIER] == enums.Outlier.IS_OUTLIER.value, "data"
    ] = np.nan
    prepped_data_to_save_df.loc[
        prepped_data_to_save_df[columns.IS_OUTLIER] == enums.Outlier.IS_OUTLIER.value,
        "variance",
    ] = np.nan

    # Save "prepped" data as both HDF5 and CSV
    prepped_data_to_save_df.drop(columns="val", inplace=True)
    h5_path = f"{output_path}/data.h5"
    prepped_data_to_save_df.to_hdf(h5_path, "prepped", index=False)
    csv_path = f"{output_path}/prepped.csv"
    prepped_data_to_save_df.to_csv(csv_path, index=False)


def _save_custom_stage_1_to_file(
    output_path: str, custom_stage_1_df: Optional[pd.DataFrame], transform: str,
) -> None:
    if custom_stage_1_df is None:
        return
    custom_stage_1_df = custom_stage_1_df.copy()
    data_validation.validate_data_bounds_for_transformation(
        custom_stage_1_df, "val", transform
    )
    custom_stage_1_df["cv_custom_stage_1"] = transform_utils.transform_data(
        custom_stage_1_df["val"], transform
    )
    custom_stage_1_df.to_csv(f"{output_path}/custom_stage1_df.csv", index=False)
