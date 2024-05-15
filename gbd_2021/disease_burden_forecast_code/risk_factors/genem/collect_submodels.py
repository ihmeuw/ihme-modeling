"""Script to collect and collapse components into genem for future stage.
"""

from typing import Callable, List

import numpy as np
import pandas as pd
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_file_interface.lib.pandas_wrapper import read_csv
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_lib_genem.lib.constants import (
    FileSystemConstants,
    ModelConstants,
    ScenarioConstants,
    SEVConstants,
    TransformConstants,
)

logger = fhs_logging.get_logger()


def entity_specific_collection(
    entity: str,
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    transform: str,
    intercept_shift_from_reference: bool,
    uncross_scenarios: bool,
) -> None:
    """Collect, sample, collapse, and export a given risk.

    Args:
        entity (str): risk to collect across omegas.  If intrinsic SEV,
            then the rei will look like acause-rei.
        stage (str): stage of run (sev, mmr, etc.)
        versions (Versions): input and output versions
        gbd_round_id (int): gbd round id.
        years (YearRange): past_start:forecast_start:forecast_end.
        transform (str): name of transform to use for processing (logit, log, no-transform).
        intercept_shift_from_reference (bool): If True, and we are in multi-scenario mode, then
            the intercept-shifting during the above `transform` is calculated from the
            reference scenario but applied to all scenarios; if False then each scenario will
            get its own shift amount.
        uncross_scenarios (bool): whether to fix crossed scenarios. This is currently only used
            for sevs and should be deprecated soon.

    """
    input_model_weights_version_metadata = versions.get(past_or_future="future", stage=stage)
    input_model_weights_file_spec = FHSFileSpec(
        version_metadata=input_model_weights_version_metadata,
        filename=ModelConstants.MODEL_WEIGHTS_FILE,
    )

    omega_df = read_csv(file_spec=input_model_weights_file_spec, keep_default_na=False)

    locations: List[int] = omega_df["location_id"].unique().tolist()

    future_da = get_location_draw_omegas(
        versions=versions,
        gbd_round_id=gbd_round_id,
        stage=stage,
        entity=entity,
        omega_df=omega_df,
        locations=locations,
    )

    # Every entity has many rows, and the "intrinsic" and "subfolder" values
    # should be the same over all rows.  So we only need first row here.
    first_row = omega_df.query(f"entity == '{entity}'").iloc[0]

    intrinsic, subfolder = bool(first_row["intrinsic"]), str(first_row["subfolder"])

    if intrinsic:
        file_name = f"{entity}_{SEVConstants.INTRINSIC_SEV_FILENAME_SUFFIX}.nc"
    else:
        file_name = f"{entity}.nc"

    if intrinsic:
        # Set all intrinsic scenarios to reference
        non_ref_scenarios = [
            s
            for s in future_da["scenario"].values
            if s != ScenarioConstants.REFERENCE_SCENARIO_COORD
        ]
        for scenario in non_ref_scenarios:
            future_da.loc[{"scenario": scenario}] = future_da.sel(
                scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD
            )

    logger.info(f"Entering intercept-shift of {entity} submodel")
    future_da = intercept_shift_processing(
        stage=stage,
        versions=versions,
        gbd_round_id=gbd_round_id,
        years=years,
        transform=transform,
        subfolder=subfolder,
        future_da=future_da,
        file_name=file_name,
        shift_from_reference=intercept_shift_from_reference,
    )

    if uncross_scenarios:
        future_da = fix_scenario_crossing(years=years, future_da=future_da)

    # NOTE the following removes uncertainty from the scenarios
    # for scenario in non_ref_scenarios:
    #   da.loc[{"scenario": scenario}] = da.sel(scenario=scenario).mean("draw")

    output_version_metadata = versions.get(past_or_future="future", stage=stage)

    output_file_spec = FHSFileSpec(
        version_metadata=output_version_metadata,
        sub_path=(subfolder,),
        filename=file_name,
    )

    save_xr_scenario(
        xr_obj=future_da,
        file_spec=output_file_spec,
        metric="rate",
        space="identity",
        years=str(years),
        past_version=str(versions.get_version_metadata(past_or_future="past", stage=stage)),
        out_version=str(versions.get_version_metadata(past_or_future="future", stage=stage)),
        gbd_round_id=gbd_round_id,
    )


def read_location_draws(
    file_spec: FHSFileSpec, location_id: int, draw_start: int, n_draws: int
) -> xr.DataArray:
    """Read location-draws from file.

    Notably, this function will expand or contract the number of draws present to fit inside
    the closed range [`draw_start`, `draw_start` + `n_draws`], *reassigning coordinates* from
    whatever they are read in as.
    """
    da = open_xr_scenario(file_spec).sel(location_id=location_id).load()
    if "draw" in da.dims:  # some sub-models may be draw-less
        da = resample(da, n_draws)
        da = da.assign_coords(draw=range(draw_start, draw_start + n_draws))
    else:
        da = expand_dimensions(da, draw=range(draw_start, draw_start + n_draws))
    return da


def fix_scenario_crossing(years: YearRange, future_da: xr.DataArray) -> xr.DataArray:
    """Scenario cross the future data and fill missing results within [0, 1]."""
    # NOTE we're NOT fixing scenario-crossing in logit space here
    # NOTE Code assumes worse > reference > better

    # Ensure same years.past_end values across scenarios after transformations
    future_da_ref = future_da.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD)
    future_da_worse = future_da.sel(scenario=ScenarioConstants.WORSE_SCENARIO_COORD)
    future_da_better = future_da.sel(scenario=ScenarioConstants.BETTER_SCENARIO_COORD)

    future_worse_diff = future_da_worse.sel(year_id=years.past_end) - future_da_ref.sel(
        year_id=years.past_end
    )
    future_better_diff = future_da_better.sel(year_id=years.past_end) - future_da_ref.sel(
        year_id=years.past_end
    )

    future_new_worse = future_da_worse - future_worse_diff
    future_new_better = future_da_better - future_better_diff

    future_da = xr.concat([future_new_worse, future_da_ref, future_new_better], dim="scenario")

    dam = future_da.mean("draw")

    # For SEV's, worse >= ref >= better
    worse = dam.sel(scenario=ScenarioConstants.WORSE_SCENARIO_COORD)
    better = dam.sel(scenario=ScenarioConstants.BETTER_SCENARIO_COORD)
    ref = dam.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD)

    worse_diff = ref - worse  # should be <= 0 for SEV, so we keep the > 0's
    worse_diff = worse_diff.where(worse_diff < 0).fillna(0)  # keep > 0's

    better_diff = ref - better  # should be >= 0 for SEV, so we keep the < 0's
    better_diff = better_diff.where(better_diff > 0).fillna(0)  # keep < 0's

    # the worse draws that are below ref will have > 0 values added to them
    future_da.loc[dict(scenario=ScenarioConstants.WORSE_SCENARIO_COORD)] = (
        future_da.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD) - worse_diff
    )
    # the better draws that are above ref will have < 0 values added to them
    future_da.loc[dict(scenario=ScenarioConstants.BETTER_SCENARIO_COORD)] = (
        future_da.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD) - better_diff
    )

    # non-ref scenarios do not have uncertainty
    dim_order = ["draw"] + [x for x in future_da.dims if x != "draw"]
    future_da = future_da.transpose(*dim_order)  # draw-dim to 1st to broadcast

    # does not save computed past SEVs
    needed_years = np.concatenate(([years.past_end], years.forecast_years))
    future_da = future_da.sel(year_id=needed_years)

    future_da = future_da.where(future_da <= 1).fillna(1)
    future_da = future_da.where(future_da >= 0).fillna(0)

    return future_da


def intercept_shift_processing(
    stage: str,
    versions: Versions,
    gbd_round_id: int,
    years: YearRange,
    transform: str,
    subfolder: str,
    future_da: xr.DataArray,
    file_name: str,
    shift_from_reference: bool,
) -> xr.DataArray:
    """Perform ordered draw intercept shifting of past and future data."""
    # Here we do ordered-draw intercept-shift to ensure uncertainty fan-out
    past_version_metadata = versions.get(past_or_future="past", stage=stage)

    past_file_spec = FHSFileSpec(
        version_metadata=past_version_metadata,
        sub_path=(subfolder,),
        filename=file_name,
    )

    past_da = open_xr_scenario(past_file_spec).sel(
        sex_id=future_da["sex_id"],
        age_group_id=future_da["age_group_id"],
        location_id=future_da["location_id"],
    )

    if "draw" in past_da.dims and "draw" in future_da.dims:
        past_da = resample(past_da, len(future_da.draw.values))

    if "acause" in future_da.coords:
        future_da = future_da.drop_vars("acause")

    if "acause" in past_da.coords:
        past_da = past_da.drop_vars("acause")

    if transform != "no-transform":
        # NOTE logit transform requires all inputs > 0, but some PAFs can be < 0
        past_da = past_da.where(past_da >= ModelConstants.LOGIT_OFFSET).fillna(
            ModelConstants.LOGIT_OFFSET
        )

    processor_class = TransformConstants.TRANSFORMS[transform]
    future_da = processor_class.intercept_shift(
        modeled_data=future_da,
        past_data=past_da,
        years=years,
        offset=ModelConstants.LOGIT_OFFSET,
        intercept_shift="unordered_draw",
        shift_from_reference=shift_from_reference,
    )

    return future_da


def get_location_draw_omegas(
    entity: str,
    versions: Versions,
    gbd_round_id: int,
    stage: str,
    omega_df: pd.DataFrame,
    locations: List[int],
    read_location_draws_fn: Callable = read_location_draws,
) -> xr.DataArray:
    """Loop over locations and read location-draw omega files."""
    loc_das = []

    for location_id in locations:
        rows = omega_df.query(f"entity == '{entity}' & location_id == {location_id}")

        if len(rows) == 0:
            raise ValueError(f"{entity} for loc {location_id} has no weight info")

        omega_das = []  # to collect the omegas
        draw_start = 0

        for _, row in rows.iterrows():  # each row is an omega-model
            omega, model_name, n_draws, intrinsic, subfolder = (
                float(row["omega"]),
                str(row["model_name"]),
                int(row["draws"]),
                bool(row["intrinsic"]),
                str(row["subfolder"]),
            )

            if n_draws < 1:  # this could happen if inverse_rmse_order == True
                continue

            if intrinsic:
                file_name = f"{entity}_{SEVConstants.INTRINSIC_SEV_FILENAME_SUFFIX}_{omega}.nc"
            else:
                file_name = f"{entity}_{omega}.nc"

            version_metadata = versions.get(past_or_future="future", stage=stage)

            file_spec = FHSFileSpec(
                version_metadata=version_metadata,
                sub_path=(FileSystemConstants.SUBMODEL_FOLDER, model_name, subfolder),
                filename=file_name,
            )

            omega_das.append(
                read_location_draws_fn(file_spec, location_id, draw_start, n_draws)
            )

            draw_start = draw_start + n_draws

        loc_das.append(xr.concat(omega_das, dim="draw", coords="minimal"))

    future_da = xr.concat(loc_das, dim="location_id", coords="minimal")

    return future_da
