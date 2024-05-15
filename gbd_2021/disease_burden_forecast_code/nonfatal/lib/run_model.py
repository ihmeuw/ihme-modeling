"""A script that forecasts nonfatal measures of health."""
from typing import Any, Dict, Iterable, List, Optional, Tuple

import xarray as xr
from fhs_lib_data_transformation.lib import processing
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_data_transformation.lib.validate import assert_shared_coords_same
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions, validate_versions_scenarios
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_model.lib import validate
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_nonfatal.lib import model_parameters, model_strategy_queries

logger = fhs_logging.get_logger()
OPIOID_EXCEPTION_LOCS = [101, 102]  # US and Canada


def one_cause_main(
    acause: str,
    stage: str,
    versions: Versions,
    years: YearRange,
    draws: int,
    gbd_round_id: int,
    output_scenario: Optional[int],
    national_only: bool,
    expand_scenarios: Optional[List[int]],
    seed: Optional[int],
) -> None:
    r"""Forecasts given stage for given cause.

    Args:
        acause (str): The cause to forecast.
        stage (str): Stage being forecasted, e.g. "yld_yll".
        versions (Versions): All relevant versions.
        years (YearRange): Forecasting time series.
        draws (int): The number of draws to compute with and output for betas and predictions.
        gbd_round_id (int): The numeric ID of GBD round associated with the past data
        output_scenario (Optional[int]): Optional output scenario ID
        national_only (bool): Whether to include subnational locations, or to include only
            nations.
        expand_scenarios (Optional[List[int]]): When present, throw away all but the reference
            scenario and expand the scenario dimension to these.
        seed (Optional[int]): Seed for random number generator (presently just in LimeTr).
    """
    # validate versions
    validate_versions_scenarios(
        versions=versions,
        output_scenario=output_scenario,
        output_epoch_stages=[("future", stage)],
    )

    # If there aren't _any_ model parameters associated with the
    # cause-stage then the script will exit with return code 0.
    (
        Model,
        processor,
        covariates,
        fixed_effects,
        fixed_intercept,
        random_effects,
        indicators,
        spline,
        predict_past_only,
        node_models,
        study_id_cols,
        scenario_quantiles,
        omega_selection_strategy,
    ) = _get_model_parameters(acause, stage, years, gbd_round_id)

    versions_to_check = {stage} | covariates.keys() if covariates else {stage}

    # BMI is our covariate name, but these are saved as SEVs
    if "bmi" in versions_to_check:
        check_versions(versions, "past", ["sev"])
        check_versions(versions, "future", ["sev"])

        versions_to_check.remove("bmi")
        check_versions(versions, "past", versions_to_check)
        check_versions(versions, "future", versions_to_check)
    else:
        check_versions(versions, "past", versions_to_check)
        check_versions(versions, "future", versions_to_check)

    past_file = FHSFileSpec(versions.get("past", stage), f"{acause}.nc")
    past_data = open_xr_scenario(past_file)
    cleaned_past_data, _ = processing.clean_cause_data(
        past_data,
        stage,
        acause,
        None,  # No draw-resampling should occur.
        gbd_round_id,
        year_ids=years.past_years,
        national_only=national_only,
    )

    location_dict = _get_location_subsets(
        acause=acause, all_locations_ids=cleaned_past_data.location_id.values
    )

    future_version_metadata = versions.get("future", stage)

    location_arrays = {}
    for loc_group, location_ids in location_dict.items():
        prepped_input_data = processor.pre_process(
            cleaned_past_data.sel(location_id=location_ids)
        )

        if covariates:
            cov_data_list = _get_covariate_data(
                prepped_input_data,
                covariates,
                versions,
                years,
                draws,
                gbd_round_id,
                national_only,
            )
        else:
            cov_data_list = None

        stripped_input_data = processing.strip_single_coord_dims(prepped_input_data)

        single_scenario_mode = True if future_version_metadata.scenario is not None else False

        model_instance = Model(
            stripped_input_data,
            years=years,
            draws=draws,
            covariate_data=cov_data_list,
            fixed_effects=fixed_effects,
            fixed_intercept=fixed_intercept,
            random_effects=random_effects,
            indicators=indicators,
            gbd_round_id=gbd_round_id,
            spline=spline,
            predict_past_only=predict_past_only,
            node_models=node_models,
            study_id_cols=study_id_cols,
            scenario_quantiles=scenario_quantiles,
            omega_selection_strategy=omega_selection_strategy,
            single_scenario_mode=single_scenario_mode,
            seed=seed,
        )

        model_instance.fit()

        forecast_path = versions.data_dir(gbd_round_id, "future", stage)
        model_instance.save_coefficients(forecast_path, acause + loc_group)

        forecast_data = model_instance.predict()
        location_arrays[loc_group] = forecast_data

    forecast_data = xr.concat(location_arrays.values(), "location_id")

    # Expand forecast data to include point coords and single coord dims that
    # were stripped off before forecasting.
    expanded_output_data = processing.expand_single_coord_dims(
        forecast_data, prepped_input_data
    )

    cleaned_past_data_resampled = resample(cleaned_past_data, draws)

    prepped_output_data = processor.post_process(
        expanded_output_data, cleaned_past_data_resampled
    )

    # Special exception for malaria PI model, all will be held constant as we don't actually
    # expect malaria prevalence/incidence ratio (a.k.a the duration of illness) to continue the
    # decrease that we see in the past. Without changing, we have unrealistic rapidly dropping
    # prevalence even if incidence increases.
    if (acause == "malaria") and (stage == "pi_ratio"):
        prepped_output_data = _malaria_pi_exception(years, cleaned_past_data_resampled)

    # For malaria incidence, we hold all of Venezuela constant and clip all locations' age
    # group 95+ to past maximum value.
    # We do this for Venezuela because it has very unusual past data and any ARC forecasts
    # create extreme and unrealistic growth in incidence so we hold it constant.
    # Age group 95+ also has extreme growth in the ARC model for many locations so we set an
    # upper limit to prevent exponential growth in our results.
    elif (acause == "malaria") and (stage == "incidence"):
        prepped_output_data = _malaria_incidence_exception(
            years, cleaned_past_data_resampled, prepped_output_data
        )

    # ArcMethod generates a fixed three scenarios, and we correct for that here, when
    # expand_scenarios is set, by throwing away non-reference scenarios and expanding to the
    # given list.
    if expand_scenarios and (
        "scenario" not in prepped_output_data.dims
        or expand_scenarios != list(prepped_output_data["scenario"].values)
    ):
        if "scenario" in prepped_output_data.dims:
            prepped_output_data = prepped_output_data.sel(scenario=0, drop=True)
        prepped_output_data = prepped_output_data.expand_dims(scenario=expand_scenarios)

    forecast_file = FHSFileSpec(future_version_metadata, f"{acause}.nc")
    save_xr_scenario(prepped_output_data, forecast_file, metric="rate", space="identity")


def _malaria_incidence_exception(
    years: YearRange,
    cleaned_past_data_resampled: xr.DataArray,
    prepped_output_data: xr.DataArray,
) -> xr.DataArray:
    """Special malaria in Venezuela exception.

    Takes the last past year and holds it constant for all future years.

    Additionally clips age group 95+ to past max. Due to extreme draw
    issues we consistently forecast unrealistic exponential growth for this
    age group, so set a limit here.
    """
    exception_loc_id = 133  # Venezuela
    past_exception_data = cleaned_past_data_resampled.sel(
        location_id=exception_loc_id, year_id=years.past_end
    ).drop("year_id")
    # create "future" data that's last past year held constant
    fill_data_future = expand_dimensions(
        past_exception_data, year_id=years.forecast_years, fill_value=past_exception_data
    )

    if "scenario" in prepped_output_data.dims:
        scenarios = prepped_output_data.coords.get("scenario")
        fill_data_future = fill_data_future.expand_dims(dim={"scenario": scenarios})

    prepped_output_data = prepped_output_data.drop_sel(location_id=exception_loc_id)
    # combine exception location back with the rest of the modeled data
    prepped_output_data = xr.concat([prepped_output_data, fill_data_future], "location_id")

    # Need to clip age group 95+ to highest previous maximum value. For each location/age/sex
    # we find the highest historical draw and limit all future draws to that value. This can
    # still allow for high values but clips the exponential growth we see otherwise.
    exception_age_group = 235

    age_group_max = cleaned_past_data_resampled.sel(age_group_id=exception_age_group).max(
        ["draw", "year_id"]
    )

    age_group_clipped = (
        prepped_output_data.sel(age_group_id=exception_age_group)
        .sortby(age_group_max.location_id)
        .clip(max=age_group_max)
    )

    prepped_output_data = xr.concat(
        [prepped_output_data.drop_sel(age_group_id=exception_age_group), age_group_clipped],
        "age_group_id",
    )

    return prepped_output_data


def _malaria_pi_exception(
    years: YearRange, cleaned_past_data_resampled: xr.DataArray
) -> xr.DataArray:
    """Special malaria PI model exception.

    Takes the last past year and holds it constant for all future years.

    """
    last_past_year = cleaned_past_data_resampled.sel(year_id=years.past_end).drop("year_id")

    # create "future" data that's last past year held constant
    prepped_output_data = expand_dimensions(
        last_past_year, year_id=years.forecast_years, fill_value=last_past_year
    )
    prepped_output_data = xr.concat(
        [cleaned_past_data_resampled, prepped_output_data], "year_id"
    )

    return prepped_output_data


def _get_location_subsets(
    acause: str, all_locations_ids: Iterable[int]
) -> Dict[str, List[int]]:
    """Split locations into groups depending on the cause.

    Ideally this will get offloaded to the strategy database.
    """
    if acause == "mental_drug_opioids":
        # special case for opioids, we're going to model US and Canada separately then model
        # all other locations because they have very different patterns
        location_dict = {
            "_exception": OPIOID_EXCEPTION_LOCS,
            "_other_locs": list(set(all_locations_ids) - set(OPIOID_EXCEPTION_LOCS)),
        }

    else:
        location_dict = {"": list(all_locations_ids)}

    return location_dict


def _get_covariate_data(
    dep_var_da: xr.DataArray,
    covariates: Dict[str, Any],
    versions: Versions,
    years: YearRange,
    draws: int,
    gbd_round_id: int,
    national_only: bool,
) -> List[xr.DataArray]:
    """Returns a list of prepped dataarray for all of the covariates."""
    cov_data_list = []
    for cov_stage, cov_processor in covariates.items():
        # Special circumstance if using BMI as a covariate--BMI is saved separately in adult
        # and child files, the past also has an age standardized group which is inaccurate
        # when we combine adult and child together, this section serves to open the correct
        # files, remove the age standardized group, and combine the adult/child files as one
        # BMI file.
        cov_past_data, cov_forecast_data = _load_cov_data(versions, cov_stage)

        cov_data = processing.clean_covariate_data(
            cov_past_data,
            cov_forecast_data,
            dep_var_da,
            years,
            draws,
            gbd_round_id,
            national_only=national_only,
        )

        prepped_cov_data = cov_processor.pre_process(cov_data)

        try:
            assert_shared_coords_same(
                prepped_cov_data, dep_var_da.sel(year_id=years.past_end, drop=True)
            )
        except IndexError as ce:
            raise IndexError(f"After pre-processing {cov_stage}, " + str(ce))

        cov_data_list.append(prepped_cov_data)

    validate.assert_covariates_scenarios(cov_data_list)
    return cov_data_list


def _load_cov_data(
    versions: Versions,
    cov_stage: str,
) -> Tuple[xr.DataArray, xr.DataArray]:
    if cov_stage == "bmi":
        cov_past_file_adult = FHSFileSpec(versions.get("past", "sev"), "metab_bmi_adult.nc")
        cov_past_data_adult = open_xr_scenario(cov_past_file_adult).drop_sel(age_group_id=27)

        cov_past_file_child = FHSFileSpec(versions.get("past", "sev"), "metab_bmi_child.nc")
        cov_past_data_child = open_xr_scenario(cov_past_file_child).drop_sel(age_group_id=27)

        cov_past_data = xr.concat([cov_past_data_child, cov_past_data_adult], "age_group_id")
        cov_past_data = processing.get_dataarray_from_dataset(cov_past_data).rename(cov_stage)

        cov_forecast_file_adult = FHSFileSpec(
            versions.get("future", "sev"), "metab_bmi_adult.nc"
        )
        cov_forecast_data_adult = open_xr_scenario(cov_forecast_file_adult)

        cov_forecast_file_child = FHSFileSpec(
            versions.get("future", "sev"), "metab_bmi_child.nc"
        )
        cov_forecast_data_child = open_xr_scenario(cov_forecast_file_child)

        cov_forecast_data = xr.concat(
            [cov_forecast_data_child, cov_forecast_data_adult], "age_group_id"
        )
        cov_forecast_data = processing.get_dataarray_from_dataset(cov_forecast_data).rename(
            cov_stage
        )
    else:
        cov_past_file = FHSFileSpec(versions.get("past", cov_stage), f"{cov_stage}.nc")
        cov_past_data = open_xr_scenario(cov_past_file)
        cov_past_data = processing.get_dataarray_from_dataset(cov_past_data).rename(cov_stage)

        cov_forecast_file = FHSFileSpec(versions.get("future", cov_stage), f"{cov_stage}.nc")
        cov_forecast_data = open_xr_scenario(cov_forecast_file)
        cov_forecast_data = processing.get_dataarray_from_dataset(cov_forecast_data).rename(
            cov_stage
        )

    return cov_past_data, cov_forecast_data


def _get_model_parameters(
    acause: str, stage: str, years: YearRange, gbd_round_id: int
) -> model_parameters.ModelParameters:
    """Gets modeling parameters associated with the given cause-stage.

    If there aren't model parameters associated with the cause-stage then the
    script will exit with return code 0.

    Args:
        acause (str): the cause to get params for
        stage (str): the stage to get params for
        years (YearRange): the years to get data for
        gbd_round_id (int): the gbd round of data

    Returns:
        model_parameters.ModelParameters
    """
    model_parameters = model_strategy_queries.get_cause_model(
        acause, stage, years, gbd_round_id
    )
    if not model_parameters:
        logger.info(f"{acause}-{stage} is not forecasted in this pipeline. DONE")
        exit(0)
        raise ValueError()

    return model_parameters
