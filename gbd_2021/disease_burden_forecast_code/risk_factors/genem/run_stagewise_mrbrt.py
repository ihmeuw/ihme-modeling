"""A script that forecasts entities using MRBRT."""

from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import xarray as xr
from fhs_lib_data_transformation.lib import filter, processing
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.validate import assert_shared_coords_same
from fhs_lib_database_interface.lib.constants import DimensionConstants, StageConstants
from fhs_lib_database_interface.lib.query import location
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.version_metadata import FHSDirSpec, FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_model.lib.constants import ModelConstants
from fhs_lib_model.lib.stagewise_mrbrt import StagewiseMRBRT
from fhs_lib_year_range_manager.lib import YearRange
from mrtool import LinearCovModel
from stagemodel import OverallModel, StudyModel
from tiny_structured_logger.lib import fhs_logging

from fhs_lib_genem.lib import predictive_validity as pv
from fhs_lib_genem.lib.constants import (
    EntityConstants,
    FileSystemConstants,
    LocationConstants,
    ScenarioConstants,
    TransformConstants,
)

logger = fhs_logging.get_logger()

PROCESSOR_OFFSET = 1e-8
SCENARIO_QUANTILES = {
    -1: dict(year_id=ScenarioConstants.DEFAULT_WORSE_QUANTILE),
    0: None,
    1: dict(year_id=ScenarioConstants.DEFAULT_BETTER_QUANTILE),
    2: None,
}
STUDY_ID_COLS = "location_id"


def stagewise_mrbrt_all_omegas(
    entity: str,
    stage: str,
    versions: Versions,
    model_name: str,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    mrbrt_cov_stage1: str,
    mrbrt_cov_stage2: str,
    omega_min: float,
    omega_max: float,
    step: float,
    transform: str,
    predictive_validity: bool,
    intrinsic: bool,
    subfolder: str,
    national_only: bool,
    use_scenario_quantiles: bool,
    age_standardize: bool,
    remove_zero_slices: bool,
) -> None:
    r"""Fit and predict a given entity.

    Models accepted should be just MRBRT and two-stage MRBRT.

    Args:
        entity (str): entity.
        stage (str): Name of stage to be forecasted, e.g. ``incidence``
        versions (Versions): All relevant versions. e.g. FILEPATH
        model_name (str): Name to save the model under.
        years (YearRange): Forecasting time series.
        gbd_round_id (int): The ID of GBD round associated with the past data
        draws (int): number of draws.
        mrbrt_cov_stage1 (str): The covariate name to be used in the MRBRT first stage.
        mrbrt_cov_stage2 (str): The covariate name to be used in the MRBRT second stage.
        omega_min (float): Minimum omega weight value to run MRBRT.
        omega_max (float): Maximum omega weight value to run MRBRT.
        step (float): Step value to be used for the range of omega values.
        transform (str): transformation to perform on data before modeling.
        predictive_validity (bool): whether to run predictive-validity. If true, save csv
            files with rmse values.
        intrinsic (bool): whether to use intrinsic or distal SEVs. If true, use intrinsic
            SEVs. Default is distal SEVs.
        subfolder (str): whether to use subfolder name. If provided, use subfolder name.
        national_only (bool): Filter for only national locations.
        use_scenario_quantiles (bool): Use scenario quantiles specification in model
        age_standardize(bool) : whether to age standardize data before modeling.
        remove_zero_slices (bool): If True, remove zero-slices along certain dimensions, when
            pre-processing inputs, and add them back in to outputs.
    """
    logger.info("in stagewise_mrbrt_all_omegas()")

    covariates, node_models = _get_covariates_and_node_models(
        years=years,
        gbd_round_id=gbd_round_id,
        mrbrt_cov_stage1=mrbrt_cov_stage1,
        mrbrt_cov_stage2=mrbrt_cov_stage2,
    )

    # Open input data
    for p_or_f in ["past", "future"]:
        versions_to_check = {stage} | covariates.keys() if covariates else {stage}
        check_versions(versions, p_or_f, versions_to_check)

    file_name = entity
    if intrinsic:
        file_name = entity + "_intrinsic"

    dep_version_metadata = versions.get(past_or_future="past", stage=stage)

    dep_file_spec = FHSFileSpec(
        version_metadata=dep_version_metadata,
        sub_path=(subfolder,) if subfolder else (),
        filename=f"{file_name}.nc",
    )

    # Clean input data
    dep_data = open_xr_scenario(dep_file_spec)
    dep_data.name = stage

    # since we eventually fit on the mean, there's no point to sub-sample here
    cleaned_data = processing.subset_to_reference(
        data=dep_data,
        draws=len(dep_data["draw"]) if "draw" in dep_data.dims else None,
        year_ids=years.past_years,
    )

    cleaned_data_sub = _subset_ages_and_sexes(cleaned_data, gbd_round_id, age_standardize)

    cleaned_data_sub = _subset_locations(
        cleaned_data_sub,
        entity,
        gbd_round_id,
        national_only,
        mrbrt_cov_stage1,
    )

    cleaned_data_sub = cleaned_data_sub.where(cleaned_data_sub >= 0).fillna(0)

    if intrinsic:
        cleaned_data_sub = cleaned_data_sub.drop_vars("acause")

    processor = TransformConstants.TRANSFORMS[transform](
        years=years,
        offset=PROCESSOR_OFFSET,
        gbd_round_id=gbd_round_id,
        age_standardize=age_standardize,
        remove_zero_slices=remove_zero_slices,
        intercept_shift="mean",
    )

    prepped_input_data = processor.pre_process(cleaned_data_sub)

    cov_data_list = _get_covariate_data(
        dep_var_da=prepped_input_data,
        covariates=covariates,
        versions=versions,
        years=years,
        gbd_round_id=gbd_round_id,
        draws=draws,
        predictive_validity=predictive_validity,
    )
    intersection_locations = set(prepped_input_data.location_id.values)
    for cov in cov_data_list:
        intersection_locations = intersection_locations.intersection(
            set(cov.location_id.values)
        )

    prepped_input_data = prepped_input_data.sel(location_id=list(intersection_locations))

    stripped_input_data = processing.strip_single_coord_dims(prepped_input_data)

    if predictive_validity:
        all_omega_pv_results: List[xr.DataArray] = []

    # Loop over omega values

    for omega in pv.get_omega_weights(omega_min, omega_max, step):
        # Separate fits by sex if needed
        if (
            entity in EntityConstants.NO_SEX_SPLIT_ENTITY
            or "sex_id" not in stripped_input_data.dims
        ):
            forecast_data = _fit_and_predict_model(
                past_data=stripped_input_data,
                years=years,
                draws=draws,
                cov_data_list=cov_data_list,
                node_models=node_models,
                scenario_quantiles=SCENARIO_QUANTILES if use_scenario_quantiles else None,
                gbd_round_id=gbd_round_id,
                stage=stage,
                entity=entity,
                versions=versions,
                sex_id=None,
                omega=omega,
                predictive_validity=predictive_validity,
            )
        else:
            stripped_input_male = stripped_input_data.sel(sex_id=[1])
            stripped_input_female = stripped_input_data.sel(sex_id=[2])
            forecast_male = _fit_and_predict_model(
                past_data=stripped_input_male,
                years=years,
                draws=draws,
                cov_data_list=cov_data_list,
                node_models=node_models,
                scenario_quantiles=SCENARIO_QUANTILES if use_scenario_quantiles else None,
                gbd_round_id=gbd_round_id,
                stage=stage,
                entity=entity,
                versions=versions,
                sex_id=1,
                omega=omega,
                predictive_validity=predictive_validity,
            )
            forecast_female = _fit_and_predict_model(
                past_data=stripped_input_female,
                years=years,
                draws=draws,
                cov_data_list=cov_data_list,
                node_models=node_models,
                scenario_quantiles=SCENARIO_QUANTILES if use_scenario_quantiles else None,
                gbd_round_id=gbd_round_id,
                stage=stage,
                entity=entity,
                versions=versions,
                sex_id=2,
                omega=omega,
                predictive_validity=predictive_validity,
            )
            forecast_data = xr.concat([forecast_male, forecast_female], dim="sex_id")

        # Expand forecast data to include point coords and single coord dims
        # that were stripped off before forecasting.
        expanded_output_data = processing.expand_single_coord_dims(
            forecast_data, prepped_input_data
        )

        prepped_output_data = processor.post_process(
            expanded_output_data,
            cleaned_data_sub.sel(location_id=list(intersection_locations)),
        )

        # add to model weights dataframe (if predictive_validity = True),
        # or save the forecasts (if predictive_validity = False)
        if predictive_validity:
            all_omega_pv_results.append(
                pv.calculate_predictive_validity(
                    forecast=prepped_output_data, holdouts=dep_data, omega=omega
                )
            )

        else:
            # post-processing ("making sure the scenarios do not have uncertainty")

            prepped_output_data = prepped_output_data.transpose(
                *["draw", "sex_id", "location_id", "year_id", "scenario", "age_group_id"]
            )

            # Expand back to all national ids with zeros if subset to malaria locs
            if entity in EntityConstants.MALARIA_ENTITIES:
                location_set = location.get_location_set(
                    gbd_round_id=gbd_round_id,
                    include_aggregates=False,
                    national_only=True,
                )
                location_ids = location_set[DimensionConstants.LOCATION_ID].tolist()
                prepped_output_data = expand_dimensions(
                    prepped_output_data, location_id=location_ids, fill_value=0
                )

            for scenario in [-1, 1]:
                prepped_output_data.loc[{"scenario": scenario}] = prepped_output_data.sel(
                    scenario=scenario
                ).mean("draw")

            output_version_metadata = versions.get(past_or_future="future", stage=stage)

            output_file_spec = FHSFileSpec(
                version_metadata=output_version_metadata,
                sub_path=(
                    FileSystemConstants.SUBMODEL_FOLDER,
                    model_name,
                    subfolder,
                ),
                filename=f"{file_name}_{omega}.nc",
            )

            save_xr_scenario(
                xr_obj=prepped_output_data,
                file_spec=output_file_spec,
                metric="rate",
                space="identity",
            )

    if predictive_validity:
        pv_df = pv.finalize_pv_data(pv_list=all_omega_pv_results, entity=entity)

        pv.save_predictive_validity(
            file_name=file_name,
            gbd_round_id=gbd_round_id,
            model_name=model_name,
            pv_df=pv_df,
            stage=stage,
            subfolder=subfolder,
            versions=versions,
        )


def _calculate_weighted_se(
    df: pd.DataFrame, past_data: pd.DataFrame, omega: float
) -> pd.DataFrame:
    """Calculate weighted standard error; used in fitting and predicting MRBRT model."""
    renormalized_years = df["year_id"] - df["year_id"].min() + 1
    df[str(past_data.name) + ModelConstants.STANDARD_ERROR_SUFFIX] = df[
        str(past_data.name) + ModelConstants.STANDARD_ERROR_SUFFIX
    ] / np.sqrt(renormalized_years**omega)
    return df


def _fit_and_predict_model(
    past_data: xr.DataArray,
    years: YearRange,
    draws: int,
    cov_data_list: Optional[List[xr.DataArray]],
    node_models: List,
    scenario_quantiles: Optional[Dict],
    gbd_round_id: int,
    stage: str,
    entity: str,
    versions: Versions,
    sex_id: Optional[int],
    omega: float,
    predictive_validity: bool,
) -> xr.DataArray:
    """Instantiate, fit, save coefficients, and predict for model."""

    def df_func(df: pd.DataFrame) -> pd.DataFrame:
        return _calculate_weighted_se(df, past_data, omega)

    model_instance = StagewiseMRBRT(
        past_data=past_data,
        years=years,
        draws=draws,
        covariate_data=cov_data_list,
        node_models=node_models,
        study_id_cols=STUDY_ID_COLS,
        scenario_quantiles=scenario_quantiles,
        df_func=df_func,
        gbd_round_id=gbd_round_id,
    )

    model_instance.fit()

    save_entity = (
        f"{entity}_sex_id_{sex_id}_omega_{omega}" if sex_id else f"{entity}_omega_{omega}"
    )

    model_instance.save_coefficients(
        output_dir=FHSDirSpec(versions.get("future", stage)), entity=save_entity
    )

    if predictive_validity:
        forecast_data = model_instance.predict()
    else:
        forecast_data = model_instance.predict()
        forecast_data = limit_scenario_quantiles(forecast_data)

    return forecast_data


def _get_covariate_data(
    dep_var_da: xr.DataArray,
    covariates: Dict[str, processing.BaseProcessor],
    versions: Versions,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    predictive_validity: bool,
) -> List[xr.DataArray]:
    """Return a list of prepped dataarray for all of the covariates."""
    cov_data_list = []
    for cov_stage, cov_processor in covariates.items():
        cov_file = cov_stage
        filename = f"{cov_file}.nc"

        cov_past_version_metadata = versions.get(past_or_future="past", stage=cov_stage)

        cov_past_file_spec = FHSFileSpec(
            version_metadata=cov_past_version_metadata, filename=filename
        )

        if predictive_validity:
            cov_future_file_spec = cov_past_file_spec
        else:
            cov_future_version_metadata = versions.get(
                past_or_future="future", stage=cov_stage
            )

            cov_future_file_spec = FHSFileSpec(
                version_metadata=cov_future_version_metadata, filename=filename
            )

        cov_past_data = open_xr_scenario(cov_past_file_spec).sel(year_id=years.past_years)
        cov_future_data = (
            open_xr_scenario(cov_future_file_spec)
            .sel(year_id=years.forecast_years)
            .rename(cov_stage)
        )

        intersection_locations = set(dep_var_da.location_id.values).intersection(
            set(cov_past_data.location_id.values)
        )
        intersection_locations = intersection_locations.intersection(
            set(cov_future_data.location_id.values)
        )
        intersection_locations = list(intersection_locations)

        cov_past_data = cov_past_data.sel(location_id=intersection_locations)

        cov_future_data = cov_future_data.sel(location_id=intersection_locations)

        dep_var_da = dep_var_da.sel(location_id=intersection_locations)

        if DimensionConstants.STATISTIC in cov_past_data.dims:
            cov_past_data = cov_past_data.sel(statistic=DimensionConstants.MEAN, drop=True)
        if DimensionConstants.STATISTIC in cov_future_data.dims:
            cov_future_data = cov_future_data.sel(statistic=DimensionConstants.MEAN, drop=True)

        prepped_cov_data = processing.clean_covariate_data(
            past=cov_past_data,
            forecast=cov_future_data,
            dep_var=dep_var_da,
            years=years,
            draws=draws,
            gbd_round_id=gbd_round_id,
        )
        if DimensionConstants.SCENARIO not in prepped_cov_data.dims:
            prepped_cov_data = prepped_cov_data.expand_dims(
                scenario=[ScenarioConstants.REFERENCE_SCENARIO_COORD]
            )

        transformed_cov_data = cov_processor.pre_process(prepped_cov_data)

        try:
            assert_shared_coords_same(
                transformed_cov_data, dep_var_da.sel(year_id=years.past_end, drop=True)
            )
        except IndexError as ce:
            raise IndexError(f"After pre-processing {cov_stage}, " + str(ce))

        cov_data_list.append(transformed_cov_data)

    return cov_data_list


def _get_covariates_and_node_models(
    years: YearRange,
    gbd_round_id: int,
    mrbrt_cov_stage1: str,
    mrbrt_cov_stage2: str,
) -> tuple[dict[str, processing.BaseProcessor], list]:
    """Set up covariate list and node_models based on the required covariates."""
    if mrbrt_cov_stage1 == EntityConstants.ACT_ITN_COVARIATE:
        covariates = {
            "malaria_act": processing.NoTransformProcessor(
                years=years, gbd_round_id=gbd_round_id
            ),
            "malaria_itn": processing.NoTransformProcessor(
                years=years, gbd_round_id=gbd_round_id
            ),
        }
        stage_1_cov_models = [
            LinearCovModel("intercept", use_re=True),
            LinearCovModel("malaria_itn"),
            LinearCovModel("malaria_act"),
        ]
    else:
        covariates = {
            mrbrt_cov_stage1: processing.NoTransformProcessor(
                years=years, gbd_round_id=gbd_round_id, no_mean=True
            )
        }
        stage_1_cov_models = [
            LinearCovModel("intercept"),
            LinearCovModel(
                mrbrt_cov_stage1,
                use_spline=True,
                spline_knots=np.linspace(0.0, 1.0, 5),
                spline_l_linear=True,
                spline_r_linear=True,
            ),
        ]

    node_models = [
        OverallModel(cov_models=stage_1_cov_models),
        StudyModel(
            cov_models=[
                LinearCovModel(alt_cov="intercept"),
                LinearCovModel(alt_cov=mrbrt_cov_stage2),
            ]
        ),
    ]
    return covariates, node_models


def _subset_ages_and_sexes(
    da: xr.DataArray, gbd_round_id: int, age_standardize: bool
) -> xr.DataArray:
    """Subset the da to the correct ages and sexes for the given stage."""
    logger.info("in _subset_ages_and_sexes()")

    if "sex_id" not in da.dims or list(da.sex_id.values) != [3]:
        da = filter.make_most_detailed_sex(da)
    if age_standardize:
        da = filter.make_most_detailed_age(data=da, gbd_round_id=gbd_round_id)

    return da


def _subset_locations(
    da: xr.DataArray,
    entity: str,
    gbd_round_id: int,
    national_only: bool,
    mrbrt_cov_stage1: str,
) -> xr.DataArray:
    """Subset locations to appropriate location_ids."""
    da = filter.make_most_detailed_location(
        data=da,
        gbd_round_id=gbd_round_id,
        national_only=national_only,
    )
    if entity == EntityConstants.MALARIA:
        # Use locations with malaria act and itn data
        if mrbrt_cov_stage1 == EntityConstants.ACT_ITN_COVARIATE:
            da = da.sel(location_id=LocationConstants.MALARIA_ACT_ITN_LOCS)
        elif mrbrt_cov_stage1 == StageConstants.SDI:
            da = da.sel(location_id=LocationConstants.NON_MALARIA_ACT_ITN_LOCS)
        else:
            raise IndexError("Malaria is not forecasted with these covariates!")

    return da


def limit_scenario_quantiles(da: xr.DataArray) -> xr.DataArray:
    """Restrict scenarios so reference does not go outside of better/worse."""
    worse = da.sel(scenario=ScenarioConstants.WORSE_SCENARIO_COORD, drop=True)
    better = da.sel(scenario=ScenarioConstants.BETTER_SCENARIO_COORD, drop=True)
    ref = da.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD, drop=True)

    limited_worse = expand_dimensions(
        worse.where(worse > ref).fillna(ref),
        scenario=[ScenarioConstants.WORSE_SCENARIO_COORD],
    )
    limited_better = expand_dimensions(
        better.where(better < ref).fillna(ref),
        scenario=[ScenarioConstants.BETTER_SCENARIO_COORD],
    )

    limited_da = xr.concat(
        [
            limited_worse,
            expand_dimensions(ref, scenario=[ScenarioConstants.REFERENCE_SCENARIO_COORD]),
            limited_better,
        ],
        dim="scenario",
    )

    return limited_da
