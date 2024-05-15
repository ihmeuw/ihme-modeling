"""Run Simple Vaccines.

creation Date: 11/9/2020
Purpose: This script serves to produce forecasts for MCV1 and DTP3
Forecasts for scenarios are produced in log space, with the reference produced
in logit - the scenarios are then combined to produce output.
To clarify, better and worse scenarios are produced in log space
using the ARC method, while the reference scenario is produced
in logit space using Limetr. This decision was based off of how
scenarios look in logit and log space. The ARC method produced
wider bounds of uncertainty than Limetr, which produced overly
confined bounds of scenarios, and log is used because it makes the
scenarios diverge from reference more quickly in situations where
the coverage is close to 1 or 0.

Example call:

python run_simple_vaccines.py --vaccine dtp3 \
--versions FILEPATH \
-v FILEPATH \
--gbd_round_id 6 \
--years 1980:2020:2050 \
--draws 500 \
one-vaccine

"""
from typing import Dict, Optional

import click
import xarray as xr
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.processing import (
    clean_covariate_data,
    get_dataarray_from_dataset,
    strip_single_coord_dims,
)
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_data_transformation.lib.validate import assert_shared_coords_same
from fhs_lib_database_interface.lib.constants import DimensionConstants, ScenarioConstants
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_database_interface.lib.query.model_strategy import ModelStrategyNames
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_model.lib.validate import assert_covariates_scenarios
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_pipeline_vaccine.lib import model_strategy, model_strategy_queries

logger = get_logger()


def load_past_vaccine_data(
    versions: Versions,
    vaccine: str,
    gbd_round_id: int,
    draws: int,
    years: YearRange,
) -> xr.DataArray:
    r"""The past is loaded and resampled using draws argument.

    Args:
        versions (Versions):All relevant versions. e.g.
            past/vaccine/123, past/sdi/456, future/sdi/789
        vaccine (str): The vaccine to forecast.
        gbd_round_id (int): The numeric ID of GBD round associated with the
            past data
        draws (int): The number of draws to compute with and output for
            betas and predictions.
        years (YearRange): Forecasting time series.

    Returns:
        (xr.DataArray):
    """
    vacc_past_path = versions.data_dir(gbd_round_id, "past", "vaccine") / f"vacc_{vaccine}.nc"
    vacc_past_data = open_xr(vacc_past_path)
    vacc_past_data = get_dataarray_from_dataset(vacc_past_data).rename(vaccine)

    loc_6_full = get_location_set(gbd_round_id=gbd_round_id)
    countries_and_subnats = loc_6_full.location_id.values

    past = resample(vacc_past_data, draws)
    past = past.sel(year_id=years.past_years, location_id=countries_and_subnats)

    if "scenario" in past.dims:
        past = past.sel(scenario=0, drop=True)

    return past


def load_covariates(
    dep_var_da: xr.DataArray,
    covariates: Dict,
    versions: Versions,
    gbd_round_id: int,
    draws: int,
    years: YearRange,
) -> xr.DataArray:
    r"""To read in the covariate data and process according to covariate.

    Args:
        dep_var_da (xr.DataArray): The vaccine dataarray in question to
            set dimensions on
        covariates (Dict): Which covariate to read in (e.g. sdi, ldi,
            education)
        versions (Versions):All relevant versions. e.g.::
            past/vaccine/123
            past/sdi/456
            future/sdi/789
        gbd_round_id (int): The numeric ID of GBD round associated with
            the past data
        draws (int): the number of draws to compute with and output for
            betas and predictions.
        years (YearRange): Forecasting time series.

    Raises:
        IndexError: If past and forecast data don't line up across
            all dimensions except ``year_id`` and ``scenario``, e.g. if
            coordinates for of age_group_id are missing from forecast
            data, but not past data.
            If the covariate data is missing coordinates from a dim it
            shares with the dependent variable -- both **BEFORE** and
            AFTER pre-processing.
            If the covariates do not have consistent scenario coords.

    Returns:
        xr.DataArray
    """
    cov_data_list = []
    for cov_stage, cov_processor in covariates.items():
        cov_past_path = versions.data_dir(gbd_round_id, "past", cov_stage) / f"{cov_stage}.nc"
        cov_past_data = open_xr(cov_past_path)
        cov_past_data = get_dataarray_from_dataset(cov_past_data).rename(cov_stage)

        cov_forecast_path = (
            versions.data_dir(gbd_round_id, "future", cov_stage) / f"{cov_stage}.nc"
        )
        cov_forecast_data = open_xr(cov_forecast_path)
        cov_forecast_data = get_dataarray_from_dataset(cov_forecast_data).rename(cov_stage)
        cov_data = clean_covariate_data(
            cov_past_data,
            cov_forecast_data,
            dep_var_da,
            years,
            draws,
            gbd_round_id,
        )
        if DimensionConstants.SCENARIO not in cov_data.dims:
            cov_data = cov_data.expand_dims(
                scenario=[ScenarioConstants.REFERENCE_SCENARIO_COORD]
            )

        prepped_cov_data = cov_processor.pre_process(cov_data, draws)

        try:
            assert_shared_coords_same(
                prepped_cov_data, dep_var_da.sel(year_id=years.past_end, drop=True)
            )
        except IndexError as ce:
            err_msg = f"After pre-processing {cov_stage}," + str(ce)
            logger.error(err_msg)
            raise IndexError(err_msg)

        cov_data_list.append(prepped_cov_data)

    assert_covariates_scenarios(cov_data_list)
    return cov_data_list


def one_vaccine_main(
    vaccine: str,
    versions: Versions,
    years: YearRange,
    draws: int,
    gbd_round_id: int,
) -> xr.DataArray:
    r"""Forecasts given stage for given cause.

    Args:
        vaccine (str): The vaccine to forecast.
        versions (Versions): All relevant versions. e.g.::
            past/vaccine/123
            past/sdi/456
            future/sdi/789
        years (YearRange): Forecasting time series.
        draws (int): The number of draws to compute with and output for
            betas and predictions.
        gbd_round_id (int): The numeric ID of GBD round associated with the
            past data

    Returns:
        xr.DataArray
    """
    # Run linear model for reference scenario
    model_parameters = _get_model_parameters(
        vaccine, ModelStrategyNames.LIMETREE.value, years, gbd_round_id
    )
    if model_parameters:
        (
            model,
            processor,
            covariates,
            fixed_effects,
            fixed_intercept,
            random_effects,
            indicators,
            spline,
            predict_past_only,
        ) = model_parameters

    versions_to_check = {"vaccine"} | covariates.keys() if covariates else {"vaccine"}
    check_versions(versions, "past", versions_to_check)
    versions_to_check.remove("vaccine")
    check_versions(versions, "future", versions_to_check)

    vacc_da = load_past_vaccine_data(versions, vaccine, gbd_round_id, draws, years)
    # convert past vaccine data into logit space
    prep_vacc_da = processor.pre_process(vacc_da, draws)

    if covariates:
        cov_data_list = load_covariates(
            prep_vacc_da,
            covariates,
            versions,
            gbd_round_id,
            draws,
            years,
        )
    else:
        cov_data_list = None
    # remove single-variable dimensions (age_group_id, sex_id)
    prep_vacc_da = strip_single_coord_dims(prep_vacc_da)
    limetr_model = model(
        past_data=prep_vacc_da,
        years=years,
        draws=draws,
        covariate_data=cov_data_list,
        random_effects=random_effects,
        indicators=indicators,
        fixed_effects=fixed_effects,
        fixed_intercept=fixed_intercept,
        gbd_round_id=gbd_round_id,
    )
    limetr_model.fit()
    forecast_path = versions.data_dir(gbd_round_id, "future", "vaccine")
    limetr_model.save_coefficients(forecast_path, f"{vaccine}_limetr")
    limetr_forecasts = limetr_model.predict()
    vacc_da = vacc_da.drop_vars("age_group_id").squeeze("age_group_id")
    vacc_da = vacc_da.drop_vars("sex_id").squeeze("sex_id")
    # convert data back into rate space
    limetr_forecasts = processor.post_process(limetr_forecasts, vacc_da)
    # Run arc for scenarios - model should not use covariate data, just past data
    model_parameters = _get_model_parameters(
        vaccine, ModelStrategyNames.ARC.value, years, gbd_round_id
    )
    if model_parameters:
        (
            model,
            processor,
            covariates,
            fixed_effects,
            fixed_intercept,
            random_effects,
            indicators,
            spline,
            predict_past_only,
        ) = model_parameters
    # pre-process into log space
    prep_vacc_da = processor.pre_process(vacc_da, draws)
    arc_model = model(
        past_data=prep_vacc_da,
        years=years,
        draws=draws,
        select_omega=False,
        omega=1.0,
        reference_scenario_statistic="mean",
        mean_level_arc=False,
        truncate=False,
        gbd_round_id=gbd_round_id,
        scenario_roc="national",
    )
    arc_model.fit()
    forecast_path = versions.data_dir(gbd_round_id, "future", "vaccine")
    arc_model.save_coefficients(forecast_path, f"{vaccine}_arc")
    arc_forecasts = arc_model.predict()
    full_arc = xr.concat([prep_vacc_da, arc_forecasts], "year_id")
    full_arc.values[full_arc.values > 0] = 0
    # convert arc scenarios back into rate space in preparation for combination
    full_arc = processor.post_process(full_arc, vacc_da)
    # combine the scenarios with the reference - as arc is forecasted in log space and
    # limetree in logit, both must be transformed back into rate space before this step

    forecast_da = combine_scenarios(limetr_da=limetr_forecasts, arc_da=full_arc, years=years)
    forecast_da = expand_dimensions(forecast_da, age_group_id=[22])
    forecast_da = expand_dimensions(forecast_da, sex_id=[3])
    save_xr(forecast_da, forecast_path / f"vacc_{vaccine}.nc", metric="rate", space="identity")
    return forecast_da


def _get_model_parameters(
    vaccine: str, model_type: str, years: YearRange, gbd_round_id: int
) -> Optional[model_strategy.ModelParameters]:
    """Gets modeling parameters associated with vaccine and model type.

    Args:
        vaccine (str): which vaccine to include
        model_type (str): which model type to use
        years (YearRange): the years to combine across
        gbd_round_id (int): the gbd_round_id to use

    Returns:
        Optional[model_strategy.ModelParameters]

    If there aren't model parameters associated with the vaccine-model then the
    script will exit with return code 0.
    """
    model_parameters = model_strategy_queries.get_vaccine_model(
        vaccine, model_type, years, gbd_round_id
    )
    if not model_parameters:
        logger.info(f"{vaccine}-{model_type} is not forecasted in this pipeline. DONE")
        exit(0)
    else:
        return model_parameters


def combine_scenarios(
    limetr_da: xr.DataArray,
    arc_da: xr.DataArray,
    years: YearRange,
) -> xr.DataArray:
    """Function to combine scenarios into a single dataarray.

    Args:
        limetr_da (xr.DataArray):
            the output of the limetr model used to produce the reference scenario.
        arc_da (xr.DataArray):
            the ouptut of the arc model used to produce the better and worse
            scenarios.
        years (YearRange): Year range.

    Returns:
        combine_da (xr.DataArray): the combined dataarray

    This function takes the reference scenario from the limetr_da and uses
    it to find an offset between the limetr_da and the arc_da, effectively
    replacing the reference scenario of the arc_da with the reference
    scenario of the limetr_da
    This function also forces the better and worse scenario to be outside of the
    reference scenario by setting the better scenario to the highest value
    and the worse scenario to the lowest value
    Note: This function expects both data arrays to be in rate space
    """
    limetr_reference = limetr_da.sel(
        scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD, drop=True
    )
    arc_reference = arc_da.sel(scenario=ScenarioConstants.REFERENCE_SCENARIO_COORD, drop=True)

    # get difference between arc reference and limetr reference in first forecast year
    difference = arc_reference.sel(year_id=years.past_end, drop=True) - limetr_reference.sel(
        year_id=years.past_end, drop=True
    )

    # add difference between arc and limetr to limetr reference
    new_reference = limetr_reference + difference
    # replace arc reference with limetr reference
    new_forecast = xr.concat(
        [
            arc_da.sel(
                scenario=[
                    ScenarioConstants.WORSE_SCENARIO_COORD,
                    ScenarioConstants.BETTER_SCENARIO_COORD,
                ],
                year_id=years.forecast_years,
            ),
            new_reference.expand_dims({"scenario": [0]}),
        ],
        "scenario",
    )
    # Force the better and worse scenarios equal to reference if reference is
    # higher than better scenario or lower than worse scenario
    low_values = new_forecast.min("scenario")
    high_values = new_forecast.max("scenario")
    new_forecast.loc[{"scenario": ScenarioConstants.BETTER_SCENARIO_COORD}] = high_values
    new_forecast.loc[{"scenario": ScenarioConstants.WORSE_SCENARIO_COORD}] = low_values

    return new_forecast


@click.group()
@click.option(
    "--vaccine",
    type=str,
    required=True,
    help=("Vaccine being modeled e.g. `vacc_dtp3`"),
)
@click.option(
    "--versions",
    "-v",
    type=str,
    required=True,
    multiple=True,
    help=("Vaccine and SDI versions in the form FILEPATH"),
)
@click.option(
    "--years",
    type=str,
    required=True,
    help="Year range first_past_year:first_forecast_year:last_forecast_year",
)
@click.option(
    "--gbd_round_id",
    required=True,
    type=int,
    help="The gbd round id " "for all data",
)
@click.option(
    "--draws",
    required=True,
    type=int,
    help="Number of draws",
)
@click.pass_context
def cli(
    ctx: click.Context,
    versions: list,
    vaccine: str,
    gbd_round_id: int,
    years: str,
    draws: int,
) -> None:
    """Main cli function to parse args and pass them to the subcommands.

    Args:
        ctx (click.Context): ctx object.
        versions (list): which population and vaccine versions to pass to
            the script
        vaccine (str): Relevant vaccine
        gbd_round_id (int): Current gbd round id
        years (str): years for forecast
        draws (int): number of draws
    """
    versions = Versions(*versions)
    years = YearRange.parse_year_range(years)
    check_versions(versions, "future", ["sdi", "vaccine"])
    ctx.obj = {
        "versions": versions,
        "vaccine": vaccine,
        "gbd_round_id": gbd_round_id,
        "years": years,
        "draws": draws,
    }


@cli.command()
@click.pass_context
def one_vaccine(ctx: click.Context) -> None:
    """Call to main function.

    Args:
        ctx (click.Context): context object containing relevant params parsed
            from command line args.
    """
    FileSystemManager.set_file_system(OSFileSystem())

    one_vaccine_main(
        versions=ctx.obj["versions"],
        vaccine=ctx.obj["vaccine"],
        gbd_round_id=ctx.obj["gbd_round_id"],
        years=ctx.obj["years"],
        draws=ctx.obj["draws"],
    )


if __name__ == "__main__":
    cli()