"""A script that forecasts migration with limetr.

This run_model takes in covariate files that have already been cleaned and
produced by model_migration.py, and assumes that the data has
been prepped adequately for limetr. As such, it does no cleaning itself.

Example call for all causes for a given stage (using default model
specifications):

.. code:: bash

python -m pdb FILEPATH/run_model.py \
--gbd-round-id 6 \
--migration-version click_20210510_limetr_fixedint \
--stage migration \
--draws 1000 \
--mig-past past_mig_rate_single_years \
--years 1950:2020:2050 \
--version [*file paths for input versions]
"""
import argparse
import pandas as pd

from fbd_core.etl import assert_shared_coords_same, exc
from fhs_lib_data_transformation.lib.processing import get_dataarray_from_dataset
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
FileSystemManager.set_file_system(OSFileSystem())
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from migration import (
    model_strategy_queries,
    assert_covariates_scenarios,
    )

logger = fhs_logging.get_logger()


COV_MAP = {"population": "natural_pop_increase",
           "death": "shocks"
           }


def forecast_migration(
        migration_version, years, gbd_round_id, mig_past, stage, draws):
    r"""Forecasts given stage for given cause.

    Args:
        stage (str):
            Stage being forecasted (migration).
        Model (Model | None):
            Class, i.e. un-instantiated from
            ``FILEPATH.model.py``
        processor (Processor):
            The pre/post process strategy of the cause-stage, i.e. instance of
            a class defined in ``FILEPATH.processing.py``.
        covariates (dict[str, Processor]] | None):
            Maps each needed covariate, i.e. independent variable, to it's
            respective preprocess strategy, i.e. instance of a class defined in
            ``FILEPATH.processing.py``.
        fixed_effects (dict[str, list[float, float]] | None):
            Covariates to calculate fixed-effect coefficients for, mapped to
            their respective correlation restrictions (i.e. forcing
            coefficients to be positive, negative, or unrestricted). e.g.::

                    {"haq": [-float('inf'), float('inf')],
                     "edu": [0, 4.7]}

        fixed_intercept (str | None):
            To restrict the fixed intercept to be positive or negative, pass
            "positive" or "negative", respectively. "unrestricted" says to
            estimate a fixed effect intercept that is not restricted to
            positive or negative.
            If ``None`` then no fixed intercept is estimated.
            Currently all of the strings get converted to unrestricted.
        random_effects (dict[str, (list[str], float|None)] | None):
            A dictionary mapping covariates to the dimensions that their
            random slopes will be estimated for. Of the form
            ``dict[covariate, RandomEffect(list[dimension], std_prior)]``.
            e.g.::

                {"haq": RandomEffect(
                    ["location_id"], None),
                 "education": RandomEffect(
                    ["location_id", "age_group_id"], 3),
                 "location_age_intercept": RandomEffect(
                    ["location_id", "sex_id"], 1)}

            **NOTE** that random intercepts should be included here -- effects
            that aren't associated with covariates will be assumed to be random
            intercepts.
        indicators (dict[str, list[str]] | None):
                A dictionary mapping indicators to the dimensions that they are
                indicators on. e.g.::

                    {"ind_age_sex": ["age_group_id", "sex_id"],
                     "ind_loc": ["location_id"]}

        versions (Versions):
            All relevant versions. e.g.::
                FILEPATH/123
                FILEPATH/124
                FILEPATH/123

        years (fbd_core.YearRange):
            Forecasting time series.
        gbd_round_id (int):
            The numeric ID of GBD round associated with the past data
    Raises:
        CoordinatesError:
            If past and forecast data don't line up across all dimensions
            except ``year_id`` and ``scenario``, e.g. if coordinates for of
            ``age_group_id`` are missing from forecast data, but not past data.
        CoordinatesError:
            If the covariate data is missing coordinates from a dim it shares
            with the dependent variable -- both **BEFORE** and **AFTER**
            pre-processing.
        CoordinatesError:
            If the covariates do not have consistent scenario coords.
        DimensionError:
            If the past and forecasted data dims don't line up (after the past
            is broadcast across the scenario dim).
        RuntimeError:
            If there is missing version
        ValueError:
            If the given stage does NOT have any cause-strategy IDs
        ValueError:
            If the given mig_past/stage/gbd-round-id combo does not have a
            modeling strategy associated with it.
    """
    logger.info(f"Entering `forecast_migration` function")

    # This will read in 'covariates_single_years.csv' from the future folder
    # where it is currently saved when running model_migration.py.
    # Will be saved with predictions added, as final output of run_model.
    cov_single_path = FBDPath("FILEPATH/covariates_single_years.csv")
    covs_single = pd.read_csv(cov_single_path)

    # Reading past data for model
    past_path = (
        FBDPath(f"{gbd_round_id}/past/{stage}/{mig_past}/past_mig_rate_single_years.nc")
    )
    past_data = open_xr(past_path)

    # Retrieving the Model and processor for running the model.
    (Model, processor, covariates, fixed_effects, fixed_intercept,
     random_effects, indicators, spline, predict_past_only
     ) = _get_model_parameters(stage, years, gbd_round_id)

    logger.info(f"running limetr with the following covariates: {covariates}")
    if covariates:
        cov_data_list = _get_covariate_data(migration_version, past_data, covariates, years)
    else:
        cov_data_list = None

    # Running the model.
    model_instance = Model(
        past_data, years, draws=draws, covariate_data=cov_data_list,
        fixed_effects=fixed_effects, fixed_intercept=fixed_intercept,
        random_effects=random_effects, indicators=indicators,
        gbd_round_id=gbd_round_id)

    coefficients = model_instance.fit()

    forecast_path = FBDPath("FILEPATH")
    model_instance.save_coefficients(forecast_path, "migration")

    forecast_data = model_instance.predict()

    prepped_output_data = processor.post_process(
        forecast_data, past_data)
    # Saving model output. Not necessary, but could be useful.
    save_xr(
        prepped_output_data, forecast_path / f"{stage}_preds.nc",
        metric="rate", space="identity")

    # Prepping xarray file for csv conversion and merge with covariate data
    mig_preds = prepped_output_data
    mig_preds_mean = mig_preds.mean("draw")
    mig_preds_mean_pd = mig_preds_mean.sel(scenario=0, drop=True).to_pandas()
    mig_preds_mean_pd.reset_index(inplace=True)
    mig_preds_mean_pd = pd.melt(
                        mig_preds_mean_pd,
                        id_vars=["location_id"]).sort_values('location_id')
    # Merging covariate and migration predictions
    preds_df = pd.merge(covs_single,
                        mig_preds_mean_pd,
                        how='inner',
                        left_on=['location_id', 'year_id'],
                        right_on = ['location_id', 'year_id'])

    preds_df = (preds_df.sort_values(['location_id', "year_id"])
                        .rename(columns={"value": "predictions"}))
    # Saving final output of run_model.py. Next up: csv_to_xr.py
    model_6_single_years_path = FBDPath("FILEPATH/model_6_single_years.csv")
    preds_df.drop("scenario", axis=1)
    preds_df.to_csv(model_6_single_years_path, index=False)
    
    logger.info(f"Leaving `forecast_migration` function. DONE")


def _get_covariate_data(
        migration_version,
        dep_var_da,
        covariates,
        years,
    ):
    """Returns a list of prepped dataarray for all of the covariates"""
    cov_data_list = []
    for cov_stage, cov_processor in covariates.items():
        cov_past_path = FBDPath(f"FILEPATH/past_{COV_MAP[cov_stage]}_single_years.nc")

        cov_past_data = open_xr(cov_past_path)

        cov_past_data = get_dataarray_from_dataset(cov_past_data).rename(cov_stage)

        cov_forecast_path = FBDPath(f"FILEPATH/forecast_{COV_MAP[cov_stage]}_single_years.nc")
        
        cov_forecast_data = open_xr(cov_forecast_path)
        cov_forecast_data = get_dataarray_from_dataset(
            cov_forecast_data).rename(cov_stage)

        cov_data = cov_past_data.combine_first(cov_forecast_data)

        prepped_cov_data = cov_data

        try:
            assert_shared_coords_same(
                prepped_cov_data,
                dep_var_da.sel(year_id=years.past_end, drop=True)
                )
        except exc.CoordinatesError as ce:
            err_msg = f"Coordinates do not match for \
                        migration and {COV_MAP[cov_stage]}," + str(ce)
            logger.error(err_msg)
            raise exc.CoordinatesError(err_msg)

        cov_data_list.append(prepped_cov_data)

    assert_covariates_scenarios(cov_data_list)
    return cov_data_list


def _get_model_parameters(stage, years, gbd_round_id):
    """Gets modeling parameters associated with the given cause-stage.

    If there aren't model parameters associated with the cause-stage then the
    script will exit with return code 0.
    """
    model_parameters = model_strategy_queries.get_mig_model(
        stage, years, gbd_round_id
        )
    if not model_parameters:
        logger.info(
            f"{stage} is not forecasted in this pipeline. DONE")
        exit(0)
    else:
        return model_parameters


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--draws", type=int,
        help="Number of draws")
    parser.add_argument(
        "--migration-version", type=str, required=True,
        help="Migration Version")
    parser.add_argument(
        "--gbd-round-id", type=int, required=True,
        help="The numeric ID of GBD round associated with the past data")
    parser.add_argument(
        "--stage", type=str, required=False, default='migration',
        help="The gbd round id associated with the data.\n"
        "Default to migration.")
    parser.add_argument(
        "--mig-past", type=str, required=False,
        default="past_mig_rate_single_years",
        help="Migration past file name. Default to\n"
        "past_mig_rate_single_years")
    parser.add_argument("--years", type=str,
        help="past_start:forecast_start:forecast_end")
    args = parser.parse_args()

    args.years = YearRange.parse_year_range(args.years)


    forecast_migration(**args.__dict__)
