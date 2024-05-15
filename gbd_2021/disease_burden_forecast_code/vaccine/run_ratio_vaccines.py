"""Run Ratio Vaccines.

Simple vaccines have been introduced in every GBD country.
Ratio vaccines were first introduced more recently and have not yet
been added to the routine schedule in all countries. These newer
generation vaccines therefore require the additional step of forecasting
introduction dates. Due to the typical scheduling of rotavirus, pcv and hib
vaccine administration programs, coverage for these vaccines was assumed to
converge to dtp3 coverage over time (and thus cannot exceed dtp2 coverage).
(Foreman et al)

The ratio vaccines, and their corresponding simple vaccines, are:
- rotavirus (dtp3)
- pcv (dtp3)
- hib (dtp3)
- mcv2 (mcv1)

For countries with known introduction dates, the coverage forecast is
the ratio * simple vaccine coverage.

For countries without any observed or set introduction dates, use survival
analysis to simulate introduction dates. For each theoretically possible
introduction year (every year in the forecasts), generate a theoretical
scale-up curve (a forecast for what the coverage to simple vaccine ratio would
be, if the vaccine was introduced in that country, in that year, using a simple
linear mixed effects model).
Finally, match the theoretical scale-up curve to the simulated years of
introduction to get ratios for the locations without known vaccine rollout
years. This mirrors what is done by the GBD where DTP3 and measles coverage are
modeled directly but rota, pcv, and hib are modeled as ratios to DTP3
coverage.

Example Call
python run_ratio_vaccines.py \
--vaccine mcv2 \
--gbd-round-id 6 \
--years 1980:2020:2050 \
--draws 1000 \
--past-ratio-version 20210606 \
--future-ratio-version 20210606 \
--version FILEPATH \
-v FILEPATH \
--intro-version 20220615_vaccine_intro \
--gavi-version GAVI_eligible_countries_2018 \
main
"""

from typing import List, Optional, Tuple

import click
import numpy as np
import pandas as pd
import statsmodels.api as sm
import xarray as xr
from fhs_lib_data_transformation.lib.processing import (
    LogitProcessor,
    get_dataarray_from_dataset,
    logit_with_offset,
)
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.constants import ScenarioConstants
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.file_system_manager import FileSystemManager
from fhs_lib_file_interface.lib.os_file_system import OSFileSystem
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange
from lifelines import WeibullAFTFitter, WeibullFitter
from scipy.special import expit, logit
from scipy.stats import weibull_min
from tiny_structured_logger.lib.fhs_logging import get_logger

from fhs_pipeline_vaccine.lib.constants import ModelConstants
from fhs_pipeline_vaccine.lib.run_simple_vaccines import load_past_vaccine_data

logger = get_logger()

RATIO_COLUMN_MAP = dict(
    rotac="ratio_rotac_to_dtp3",
    pcv3="ratio_pcv3_to_dtp3",
    hib3="ratio_hib3_to_dtp3",
    mcv2="ratio_mcv2_to_mcv1",
)
RELEVANT_MEs = {
    "rotac": "vacc_rotac_dpt3_ratio",
    "pcv3": "vacc_pcv3_dpt3_ratio",
    "hib3": "vacc_hib3_dpt3_ratio",
    "mcv2": "vacc_mcv2_mcv1_ratio",
}
# MCV2 actually starts in 1963 but our data only goes back to 1980
START_YEARS = {"mcv2": 1980, "hib3": 1985, "pcv3": 1999, "rotac": 2005}
SCALE_DICT = {"rotac": 9.5, "pcv3": 14.5, "hib3": None, "mcv2": 35}
SMALL_VALS_THRESHOLD = 1e-4
DEMOG_COLS = ["location_id", "year_id"]


def generate_intro_times(
    shape: float,
    scale: float,
    vacc: str,
    scenario: int,
    num_draws: int,
    start_year: int,
    years: YearRange,
) -> List[float]:
    """Generate draws within the acceptable range for ratio vaccines.

    Args:
        shape (float): parameter for Weibull distribution
        scale (float): parameter for Weibull distribution
        vacc (str): name of the vaccine to run
        scenario (int): scenario ID to use
        num_draws (int): number of draws
        start_year (int): year vaccine was first introduced
        years (YearRange): past_start:forecast_start:forecast_end

    Returns:
        List[float]

    Raises:
        ValueError:
            If Weibull parameters shape and scale are invalid
            If the vaccine name inputted is invalid
            If the scenario ID inputted is not -1, 0, or 1
    """
    # Check that valid Weibull parameters were given

    if not (shape and scale):
        raise ValueError("Invalid Weibull parameters")

    # Check that vaccine is valid
    if vacc not in RATIO_COLUMN_MAP.keys():
        raise ValueError(
            f"Passed invalid vaccine shorthand ({vacc}), "
            f"must be one of {RATIO_COLUMN_MAP.keys()}"
        )

    # Check that the scenario ID is valid
    if scenario not in ScenarioConstants.SCENARIOS:
        raise ValueError(
            f"Passed invalid scenario argument, {scenario} - "
            f"must be one of {ScenarioConstants.SCENARIOS}"
        )

    # Calculate how long ago the vaccine was first released
    min_time = years.past_end - start_year

    # The scale must be above a set value or we don't get valid years
    # This does not occur with hib3
    if vacc != "hib3":
        min_scale = SCALE_DICT[vacc]
        if scale < min_scale:
            scale = min_scale

    # Use Weibull distribution to estimate the time until introduction
    times = weibull_min.rvs(shape, loc=0, scale=scale, size=num_draws)

    # Make sure that the resulting time is in the future
    ok_times = times[times >= min_time]

    # Repeat process until we have all of the draws needed
    while len(ok_times) < num_draws:
        new_draws = num_draws - len(ok_times)
        new_times = weibull_min.rvs(shape, loc=0, scale=scale, size=new_draws)
        ok_times = np.array(list(ok_times) + list(new_times))
        ok_times = ok_times[ok_times >= min_time]

    # Based on the scenario, select the appropriate percentile of the intro times
    scenario_pctiles = {0: 50, -1: 85, 1: 15}
    scenario_point_est = np.percentile(ok_times, scenario_pctiles[scenario])
    new_times = [scenario_point_est] * num_draws

    # Check again that we're not predicting dates in the past

    if np.min(new_times) < min_time:
        raise ValueError("Predicting introduction years in the past.")

    return new_times


def generate_theoretical_scaleups(
    past_ratios: pd.DataFrame,
    simulated_intro_locations: List[int],
    vaccine: str,
    years: YearRange,
    draws: int,
    year_intro_col: Optional[str] = None,
) -> pd.DataFrame:
    """Estimate the conditional coverage scaleup for all forecasted years.

    Args:
        past_ratios (pd.DataFrame): contains information with location and year specific ratios
        simulated_intro_locations (List[int]): a list of location_ids with
            locations to simulate introduction dates for
        vaccine (str): name of the vaccine to run
        years (YearRange): past_start:forecast_start:forecast_end
        draws (int): number of draws
        year_intro_col (str): name of the column in past_ratios with the introduction year

    Returns:
        pd.DataFrame
    """
    # Select for locations that are simulated and have past ratios
    sim_ratios = past_ratios.location_id.isin(simulated_intro_locations)

    # Generate theoretical curves for each year
    scale_ups = []

    # Estimate last known past year to intercept shift
    for year_id in range(years.past_end, years.forecast_end + 1):
        sample_ratios = past_ratios.copy()

        # If the location is simulated, use year_id as the theoretical
        # introduction date
        sample_ratios.loc[sim_ratios, year_intro_col] = year_id

        # Calculate the number of years since introduction using the
        # hypothetical introductory year

        sample_ratios["year_id"] = sample_ratios["year_id"] - sample_ratios[year_intro_col]

        # Redefine 'year_id' so that it's the number of years since introduction
        # This makes it easier to model later on

        # We only want post-introduction data
        sample_ratios = sample_ratios[sample_ratios["year_id"] >= 0]

        # Clip 0s and 1s
        sample_ratios[RATIO_COLUMN_MAP[vaccine]] = sample_ratios[
            RATIO_COLUMN_MAP[vaccine]
        ].clip(lower=SMALL_VALS_THRESHOLD, upper=1 - SMALL_VALS_THRESHOLD)

        # Prepare variables needed for regression
        sample_ratios["logit_ratio"] = logit(sample_ratios[RATIO_COLUMN_MAP[vaccine]])
        sample_ratios["log_income"] = np.log(sample_ratios["ldi"])
        not_nan_values = sample_ratios.dropna()

        # The formula is the same for most vaccines
        fixed_formula = "logit_ratio ~ simple_vacc_cov + log_income + year_id"
        re_formula = "~ education + 1"

        # Rotavirus equation is a bit different
        if vaccine == "rotac":
            fixed_formula = f"{fixed_formula} + education"
            re_formula = None
        elif sample_ratios.admin.max() != 0:
            # If admin is 0 for every row, don't use it as a covariate
            fixed_formula = f"{fixed_formula} + admin"

        # Fit model
        lme_model = sm.MixedLM.from_formula(
            fixed_formula,
            data=not_nan_values,
            groups=not_nan_values["location_id"],
            re_formula=re_formula,
        )

        # Make ratio predictions
        output = lme_model.fit(data=sample_ratios)
        sample_ratios["predictions"] = output.predict(exog=sample_ratios)
        sample_ratios["ratio"] = expit(sample_ratios["predictions"])

        intro_year_df = sample_ratios[["location_id", "year_id", "ratio", year_intro_col]]
        intro_year_df["year_id"] += intro_year_df[year_intro_col]
        intro_year_df = intro_year_df[
            intro_year_df.location_id.isin(simulated_intro_locations)
        ]
        intro_year_df["intro_year"] = year_id

        scale_ups.append(intro_year_df)

    all_curves = pd.concat(scale_ups)

    return all_curves


def parametrize_weibull(
    sdi_df: pd.DataFrame,
    gavi_version: str,
    simple_vacc_cov_scenarios: pd.DataFrame,
    vaccine: str,
    hib3_intro: pd.DataFrame,
    simulated_intro_locations: List[int],
    years: YearRange,
) -> Tuple[pd.DataFrame, int, dict]:
    """Calculate Weibull parameters for introduction year selection.

    Args:
        sdi_df (pd.DataFrame): dataframe with past and future SDI values
        gavi_version (str): name of the file with GAVI eligibility information
        simple_vacc_cov_scenarios (pd.DataFrame):
            contains simple vaccine coverage estimates with scenarios
        vaccine (str): name of the vaccine to run
        hib3_intro (pd.DataFrame): contains location specific introduction year data about hib3
        simulated_intro_locations (list):
            a list of location_ids with locations to simulate introduction dates for
        years (YearRange): past_start:forecast_start:forecast_end

    Returns:
        Tuple[pd.DataFrame, int, dict]
    """
    weibull_inputs = sdi_df.merge(
        simple_vacc_cov_scenarios, on=["location_id", "year_id", "scenario"]
    )

    # Make an indicator variable for GAVI eligibility
    weibull_inputs["gavi_eligible"] = 0
    gavi_df = pd.read_csv(
        "/FILEPATH/" f"{gavi_version}.csv", encoding="ISO-8859-1"
    )

    # Some files may include all locations, not just the GAVI eligible ones
    if "gavi_eligible" in gavi_df.columns:
        gavi_df = gavi_df[gavi_df.gavi_eligible == 1]

    gavi_locs = list(gavi_df.location_id.values)
    is_gavi = weibull_inputs.location_id.isin(gavi_locs)
    weibull_inputs.loc[is_gavi, "gavi_eligible"] = 1

    weibull_preds = weibull_inputs[
        ["location_id", "year_id", "scenario", "gavi_eligible", "sdi", "simple_vacc_cov"]
    ]

    weibull_preds["Intercept"] = 1

    hib3_intro = hib3_intro[hib3_intro["hib3_intro_yr_country"] != 9999]
    weibull_inputs = weibull_inputs.merge(hib3_intro, on="location_id")

    if vaccine != "hib3":
        weibull_inputs = weibull_inputs[
            weibull_inputs.location_id.isin(simulated_intro_locations)
        ]

    weibull_inputs["vacc_intro"] = 0

    vacc_intro = weibull_inputs.year_id == weibull_inputs.hib3_intro_yr_country

    weibull_inputs.loc[vacc_intro, "vacc_intro"] = 1

    post_intro = weibull_inputs.year_id > weibull_inputs.hib3_intro_yr_country

    weibull_inputs.loc[post_intro, "vacc_intro"] = 9999

    # Subset to post-Hib intro years
    weibull_inputs = weibull_inputs[
        weibull_inputs.year_id.isin(list(range(1986, years.past_end)))
    ]

    weibull_inputs["time_to_intro"] = weibull_inputs["year_id"] - 1985

    # Remove 9999
    weibull_inputs = weibull_inputs[weibull_inputs.vacc_intro != 9999]

    # Fit initial weibull model to get paramters without covariates
    wb = WeibullFitter()
    wb.fit(
        durations=weibull_inputs["time_to_intro"], event_observed=weibull_inputs["vacc_intro"]
    )

    # Extract shape parameter for reuse in later Weibull
    shape_param = wb.rho_

    cols_needed = ["time_to_intro", "vacc_intro", "gavi_eligible", "sdi", "simple_vacc_cov"]

    if vaccine == "hib3":
        cols_needed.remove("gavi_eligible")

    needed_data = weibull_inputs[cols_needed]

    # Fit weibull with covariates to extract coefficients
    wb_with_cov = WeibullAFTFitter(fit_intercept=True)
    wb_with_cov.fit(needed_data, duration_col="time_to_intro", event_col="vacc_intro")

    coeff_series = wb_with_cov.summary.coef.lambda_
    coeff_dict = dict(coeff_series.items())

    return (weibull_preds, shape_param, coeff_dict)


def draw_years_of_introduction(
    vaccine: str,
    weibull_preds: pd.DataFrame,
    simulated_intro_locations: List[int],
    coeff_dict: dict,
    shape_param: float,
    years: YearRange,
    draws: int,
    all_curves: pd.DataFrame,
) -> xr.DataArray:
    """Estimate years of introductions and their resulting scale up curves.

    Args:
        vaccine (str): name of the vaccine to run
        weibull_preds (pd.DataFrame): dataframe with covariate values
        simulated_intro_locations (list):
            a list of location_ids with locations to simulate introduction dates for
        coeff_dict (dict): dictionary with the slopes of the Weibull covariates
        shape_param (float): parameter given by the Weibull distribution
        years (YearRange): past_start:forecast_start:forecast_end
        draws (int): number of draws
        all_curves (pd.DataFrame): dataframe with the simulated
            scale up curves conditioned on the year of introduction

    Returns:
        xr.DataArray
    """
    start_year = START_YEARS[vaccine]

    needed_preds = weibull_preds[
        (weibull_preds.location_id.isin(simulated_intro_locations))
        & (weibull_preds.year_id == start_year)
    ]

    needed_preds["shape"] = shape_param

    if vaccine == "hib3":
        coeff_dict["gavi_eligible"] = 0

    # Go through each row and use the linear model
    # to find the logit scale value
    needed_preds["logit_scales"] = (
        coeff_dict["Intercept"]
        + (coeff_dict["gavi_eligible"] * needed_preds["gavi_eligible"])
        + (coeff_dict["simple_vacc_cov"] * needed_preds["simple_vacc_cov"])
        + (coeff_dict["sdi"] * needed_preds["sdi"])
    )

    needed_preds["scale"] = np.exp(needed_preds["logit_scales"])
    needed_preds["vaccine"] = vaccine

    draw_cols = []
    for ix in range(0, draws):
        draw_cols.append(f"draw_{ix}")

    # Initialize empty columns
    for col in draw_cols:
        needed_preds[col] = np.nan

    needed_preds[draw_cols] = needed_preds.apply(
        lambda x: pd.Series(
            {
                col: result
                for col, result in zip(
                    draw_cols,
                    generate_intro_times(
                        x["shape"],
                        x["scale"],
                        x["vaccine"],
                        x["scenario"],
                        draws,
                        start_year,
                        years,
                    ),
                )
            }
        ),
        axis=1,
    )

    needed_preds = needed_preds[["location_id", "scenario"] + draw_cols]
    needed_preds = needed_preds.set_index(["location_id", "scenario"]).stack().reset_index()
    needed_preds = needed_preds.rename(columns={0: "time_to_introduction", "level_2": "draw"})
    needed_preds.columns = ["location_id", "scenario", "draw", "time_to_introduction"]
    needed_preds["draw"] = needed_preds.draw.str.replace("draw_", "")
    needed_preds["draw"] = needed_preds.draw.astype("int64")

    # We don't deal with fractions of years so round the values
    needed_preds["time_to_introduction"] = np.round(needed_preds["time_to_introduction"])
    needed_preds["intro_year"] = needed_preds["time_to_introduction"] + start_year
    needed_preds_merged = needed_preds.merge(all_curves, on=["location_id", "intro_year"])
    needed_preds_merged = needed_preds_merged.drop(
        ["time_to_introduction", "intro_year"], axis=1
    )  # location, year, scenario, draw, ratio

    needed_preds_merged["age_group_id"] = 22
    needed_preds_merged["sex_id"] = 3
    final_cols = needed_preds_merged[
        ["location_id", "scenario", "draw", "year_id", "ratio", "age_group_id", "sex_id"]
    ]
    final_cols = final_cols.rename(columns={"ratio": "value"})
    final_cols["year_id"] = final_cols.year_id.astype("int64")
    indexed_df = final_cols.set_index(
        ["location_id", "scenario", "draw", "year_id", "age_group_id", "sex_id"]
    )

    da = indexed_df.to_xarray().to_array().fillna(0).sel(variable="value", drop=True)

    return da


def load_introduction(
    intro_version: str,
    gbd_round_id: int,
    okay_locations: List[int],
    vaccine: str,
    intro_column_name: Optional[str] = None,
) -> pd.DataFrame:
    """Estimate years of introductions and their resulting scale up curves.

    Args:
        intro_version (str): file name containing introduction dates
        gbd_round_id (int): the gbd round to draw data from
        okay_locations (List[int]): acceptable location IDs
        vaccine (str): name of the vaccine to run
        intro_column_name (str): name of the
            column in the introduction data with the years

    Returns:
        pd.DataFrame
    """
    og_intro = pd.read_csv(
        f"FILEPATH/{gbd_round_id}/past/"
        f"FILEPATH/{intro_version}.csv"
    )

    if not intro_column_name:
        intro_column_name = f"{vaccine}_intro_yr_country"

    rollout = og_intro[["ihme_loc_id", "me_name", "cv_intro", "location_id"]].drop_duplicates()
    relevant_info = rollout[rollout.me_name == RELEVANT_MEs[vaccine]]
    relevant_locs = relevant_info[relevant_info.location_id.isin(okay_locations)]
    cleaned_intro = relevant_locs.rename(columns={"cv_intro": intro_column_name}).drop(
        ["me_name", "ihme_loc_id"], axis=1
    )

    return cleaned_intro


def load_simple_vacc_cov(
    vaccine: str, version: Versions, gbd_round_id: int, draws: int
) -> pd.DataFrame:
    """Load future coverage for the ratio vaccine's corresponding simple vaccine.

    Args:
        vaccine (str): name of the vaccine to run
        version (Versions): a versions object containing covariates to forecast ratios
        gbd_round_id (int): the gbd round to draw data from
        draws (int): number of draws

    Returns:
        pd.DataFrame
    """
    if vaccine == "mcv2":
        simple_vacc = "mcv1"
    else:
        simple_vacc = "dtp3"

    simple_vacc_filepath = (
        version.data_dir(gbd_round_id, "future", "vaccine") / f"vacc_{simple_vacc}.nc"
    )

    simple_vacc_data = open_xr(simple_vacc_filepath)
    simple_vacc_data = resample(simple_vacc_data, draws)
    simple_vacc_data.name = "simple_vacc_cov"
    simple_vacc_cov_df = simple_vacc_data.mean("draw").to_dataframe().reset_index()
    simple_vacc_cov_scenarios = simple_vacc_cov_df[
        ["location_id", "year_id", "scenario", "simple_vacc_cov"]
    ]

    return simple_vacc_cov_scenarios


def load_single_covariate(
    version: Versions,
    stage: str,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    okay_locations: List[int],
) -> pd.DataFrame:
    """Load past and future covariate data.

    Args:
        version (Versions): a versions object containing covariates to forecast ratios
        stage (str): stage of the desired covariate
        gbd_round_id (int): the gbd round to draw data from
        years (YearRange): past_start:forecast_start:forecast_end
        draws (int): number of draws
        okay_locations (List[int]): acceptable location IDs

    Returns:
        pd.DataFrame
    """
    if stage == "education":
        entity = "maternal_education"
    else:
        entity = stage

    future_filepath = version.data_dir(gbd_round_id, "future", stage) / f"{entity}.nc"
    past_filepath = version.data_dir(gbd_round_id, "past", stage) / f"{entity}.nc"

    future_data = open_xr(future_filepath)
    past_data = open_xr(past_filepath)

    future_data = future_data.sel(year_id=years.forecast_years)
    future_data = resample(future_data, draws)

    past_data = past_data.sel(location_id=okay_locations, year_id=years.past_years)
    past_data = resample(past_data, draws)

    full = xr.concat([past_data, future_data], dim="year_id")

    needed = full.sel(year_id=range(years.past_start, years.forecast_end + 1)).mean("draw")
    needed.name = stage

    cleaned_df = needed.to_dataframe().reset_index()

    if "age_group_id" in cleaned_df.columns:
        cleaned_df = cleaned_df.drop(["age_group_id"], axis=1)

    if "sex_id" in cleaned_df.columns:
        cleaned_df = cleaned_df.drop(["sex_id"], axis=1)

    return cleaned_df


def create_past_ratios_df(
    past_ratio_version: str,
    gbd_round_id: int,
    vaccine: str,
    year_intro_col: str,
    okay_locations: List[int],
    location_metadata: pd.DataFrame,
    ldi: pd.DataFrame,
    education: pd.DataFrame,
    simple_vacc_cov: pd.DataFrame,
    introductions: pd.DataFrame,
    years: YearRange,
) -> pd.DataFrame:
    """Create a dataframe with all covariates needed to simulate the scale-up curves.

    Args:
        past_ratio_version (str): the version containing past vaccine ratios
        gbd_round_id (int): the gbd round to draw data from
        vaccine (str): name of the vaccine to run
        year_intro_col (str): name of the column in
            past_ratios with the introduction year
        okay_locations (List[int]): acceptable location IDs
        location_metadata (pd.DataFrame): id, name, and level of a location
        ldi (pd.DataFrame): past and future LDI data
        education (pd.DataFrame): past and future maternal education data
        simple_vacc_cov (pd.DataFrame): contains reference data
            for the corresponding simple vaccine
        introductions (pd.DataFrame): contains location specific introduction dates
        years (YearRange): past_start:forecast_start:forecast_end

    Returns:
        pd.DataFrame

    Raises:
        ValueError:
            If the dataframe produced is missing necessary columns
    """
    if vaccine == "mcv2":
        simple_vacc = "mcv1"
    else:
        simple_vacc = "dtp3"

    past_ratios = pd.read_csv(
        f"/FILEPATH"
        f"/FILEPATH/{vaccine}_{simple_vacc}_ratios.csv"
    )

    past_ratios = past_ratios.merge(simple_vacc_cov, on=DEMOG_COLS, how="outer")

    past_ratios = past_ratios.merge(
        ldi, on=["location_id", "year_id", "scenario"], how="outer"
    )

    past_ratios = past_ratios.merge(
        education, on=["location_id", "year_id", "scenario"], how="outer"
    )

    past_ratios = past_ratios.drop("ihme_loc_id", axis=1)

    location_metadata = location_metadata[["location_id", "ihme_loc_id"]]
    past_ratios = past_ratios.merge(location_metadata, on="location_id", how="outer")

    past_ratios = past_ratios.merge(introductions, on="location_id", how="outer")

    if not set(past_ratios.columns).issuperset(
        set(["simple_vacc_cov", "ldi", "education", f"{vaccine}_intro_yr_country"])
    ):
        raise ValueError("Past Ratios DataFrame is missing columns")

    past_ratios["age_group_id"] = 22
    past_ratios["sex_id"] = 3
    past_ratios[f"num_{vaccine}_intro_yr_country"] = np.nan

    not_9999 = past_ratios[year_intro_col] != 9999
    past_ratios.loc[not_9999, f"num_{vaccine}_intro_yr_country"] = (
        past_ratios["year_id"] - past_ratios[year_intro_col]
    )

    # For places with known intros, remove data before first intro year
    # Keep all data for places without a known intro year
    no_intros = past_ratios[year_intro_col] == 9999

    after_intro = past_ratios[f"num_{vaccine}_intro_yr_country"] >= 0
    past_ratios = past_ratios[no_intros | after_intro]

    # Ensure you are only using allowed locations in the modeling
    past_ratios = past_ratios[past_ratios.location_id.isin(okay_locations)]

    past_ratios = past_ratios[past_ratios.scenario == 0]

    # Add information needed to run the linear model R code
    past_ratios["admin"] = 0

    if set(past_ratios.location_id.unique()) != set(okay_locations):
        raise ValueError(
            "Past data is missing locations "
            f"{set(okay_locations) - set(past_ratios.location_id.unique())}"
        )

    past_ratios = past_ratios.loc[past_ratios.year_id.isin(years.years)]

    return past_ratios


def forecast_simulated_ratios(
    version: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    okay_locations: List[int],
    past_ratio_version: str,
    vaccine: str,
    location_metadata: pd.DataFrame,
    simple_vacc_cov: pd.DataFrame,
    introductions: pd.DataFrame,
    simulated_intro_locations: List[int],
    year_intro_col: str,
    gavi_version: str,
    simple_vacc_cov_scenarios: pd.DataFrame,
    hib3_intro: pd.DataFrame,
) -> None:
    """A function to forecast vaccine ratios that have not yet been introduced.

    Args:
        version (Versions): a versions object containing covariates to forecast ratios
        gbd_round_id (int): the gbd round to draw data from
        years (YearRange): past_start:forecast_start:forecast_end
        draws (int): number of draws
        okay_locations (List[int]): acceptable location IDs
        past_ratio_version (str): the version containing past vaccine ratios
        vaccine (str): name of the vaccine to run
        location_metadata (pd.DataFrame): id, name, and level of a location
        simple_vacc_cov (pd.DataFrame): contains reference data for the
            corresponding simple vaccine
        introductions (pd.DataFrame): contains location specific introduction dates
        simulated_intro_locations (List[int]): locations with no past introductions
        year_intro_col (str): name of the column in past_ratios with the introduction year
        gavi_version (str): name of the file with GAVI eligibility information
        simple_vacc_cov_scenarios (pd.DataFrame): contains scenario data for the
            corresponding simple vaccine
        hib3_intro (pd.DataFrame): contains introduction dates for hib3

    Returns:
        None
    """
    # Load in Covariates
    sdi_df = load_single_covariate(version, "sdi", gbd_round_id, years, draws, okay_locations)

    ldi_df = load_single_covariate(version, "ldi", gbd_round_id, years, draws, okay_locations)

    edu_df = load_single_covariate(
        version, "education", gbd_round_id, years, draws, okay_locations
    )

    past_ratios = create_past_ratios_df(
        past_ratio_version,
        gbd_round_id,
        vaccine,
        year_intro_col,
        okay_locations,
        location_metadata,
        ldi_df,
        edu_df,
        simple_vacc_cov,
        introductions,
        years,
    )

    # Estimate curves conditional on the introduction date
    all_curves = generate_theoretical_scaleups(
        past_ratios, simulated_intro_locations, vaccine, years, draws, year_intro_col
    )

    # Estimate coefficients and inputs for Weibull distribution
    # This will be used to select introduction years
    weibull_preds, shape_param, coeff_dict = parametrize_weibull(
        sdi_df,
        gavi_version,
        simple_vacc_cov_scenarios,
        vaccine,
        hib3_intro,
        simulated_intro_locations,
        years,
    )

    # Estimate introduction date
    # Combine with simulated curves conditional on the introduction date
    draw_data = draw_years_of_introduction(
        vaccine,
        weibull_preds,
        simulated_intro_locations,
        coeff_dict,
        shape_param,
        years,
        draws,
        all_curves,
    )

    return draw_data


def multiply_ratio_forecasts(
    vaccine: str,
    gbd_round_id: int,
    ratios: xr.DataArray,
    version: Versions,
    draws: int,
    years: YearRange,
) -> xr.DataArray:
    """Function to multiply ratio forecasts by simple vaccine forecasts.

    Args:
        vaccine (str): the ratio vaccine for which we will be loading ratios
        gbd_round_id (int): the gbd round to draw ratio from
        ratios (xr.DataArray): the version containing vaccine ratios
        version: the location to read the simple vaccine forecasts from
        draws (int): number of draws
        years (YearRange): past and forecast years

    Returns:
        xr.DataArray
    """
    if vaccine == "mcv2":
        simple_vacc = "mcv1"
    else:
        simple_vacc = "dtp3"

    vacc_forecast_path = (
        version.data_dir(gbd_round_id, "future", "vaccine") / f"vacc_{simple_vacc}.nc"
    )

    vacc_forecast_data = open_xr(vacc_forecast_path)
    vacc_forecast_data = get_dataarray_from_dataset(vacc_forecast_data).rename(simple_vacc)

    forecast = resample(vacc_forecast_data, draws)
    ratios = resample(ratios, draws)

    # Also need last past year estimates for intercept shifting
    years_needed = [years.past_end]
    years_needed.extend(list(years.forecast_years))

    forecast = forecast.sel(year_id=years_needed)
    multiplied_ratios = forecast * ratios

    return multiplied_ratios


def load_forecasted_ratios(
    future_ratio_version: str,
    gbd_round_id: int,
    vaccine: str,
    known_intro_dates: pd.DataFrame,
    years: YearRange,
    draws: int,
) -> xr.DataArray:
    """Open forecasted ratios given by the vaccine team.

    Args:
        future_ratio_version (str): the version containing future vaccine ratios
        gbd_round_id (int): the gbd round to draw data from
        vaccine (str): name of the vaccine to run
        known_intro_dates (pd.DataFrame): dataframe with location IDs with given ratios
        years (YearRange): past_start:forecast_start:forecast_end
        draws (int): number of draws

    Returns:
        xr.DataArray
    """
    if vaccine == "mcv2":
        simple_vacc = "mcv1"
    else:
        simple_vacc = "dtp3"

    ratios = pd.read_csv(
        f"FILEPATH/"
        f"FILEPATH/{vaccine}_{simple_vacc}_ratios.csv"
    )

    years_needed = list(years.forecast_years)
    years_needed.append(years.past_end)

    ratios_to_use = ratios[
        (ratios.location_id.isin(known_intro_dates.location_id))
        & (ratios.year_id.isin(years_needed))
    ]

    if "Unnamed: 0" in ratios_to_use.columns:
        ratios_to_use = ratios_to_use.drop("Unnamed: 0", axis=1)

    ind_cols = ["location_id", "age_group_id", "sex_id", "year_id"]

    if "scenario" in ratios_to_use.columns:
        ind_cols.append("scenario")
        draw_level = "level_5"
    else:
        draw_level = "level_4"

    ratios_to_use = ratios_to_use.set_index(ind_cols).stack().reset_index()

    cleaned_ratio_df = ratios_to_use.rename(columns={draw_level: "draw", 0: "value"})
    cleaned_ratio_df["draw"] = cleaned_ratio_df.draw.str.replace("draw_", "")
    cleaned_ratio_df["draw"] = cleaned_ratio_df.draw.astype("int64")

    ind_cols.append("draw")
    indexed_df = cleaned_ratio_df.set_index(ind_cols)

    ratio_da = indexed_df.to_xarray().to_array().fillna(0).sel(variable="value", drop=True)

    ratio_da = resample(ratio_da, draws)

    return ratio_da


def forecast_ratio_vaccines(
    intro_version: str,
    version: Versions,
    gbd_round_id: int,
    years: YearRange,
    draws: int,
    past_ratio_version: str,
    future_ratio_version: str,
    vaccine: str,
    year_intro_col: str,
    gavi_version: str,
    intro_column_name: str,
) -> None:
    """Forecast ratio vaccines like pcv3, hib3, rotac, and mcv2.

    Args:
        intro_version (str): file name containing introduction dates
        version (Versions): a versions object containing covariates to forecast ratios
        gbd_round_id (int): the gbd round to draw data from
        years (YearRange): past_start:forecast_start:forecast_end
        draws (int): number of draws
        past_ratio_version (str): the version containing past vaccine ratios
        future_ratio_version (str): the version containing future vaccine ratios
        vaccine (str): name of the vaccine to run
        year_intro_col (str): name of the column in past_ratios with the introduction year
        gavi_version (str): name of the file with GAVI eligibility information
        intro_column_name (str): name of the column in the introduction data with the years
    """
    # Past ratios will often have the same column name but not always
    if not year_intro_col:
        year_intro_col = f"{vaccine}_intro_yr_country"

    loc_6_full = get_location_set(gbd_round_id=gbd_round_id)
    location_metadata = loc_6_full[(loc_6_full["level"] >= 3)]
    okay_locations = location_metadata.location_id.values

    introductions = load_introduction(
        intro_version, gbd_round_id, okay_locations, vaccine, intro_column_name
    )

    # hib3 information gets used for other vaccines as well
    hib3_intro = load_introduction(
        intro_version, gbd_round_id, okay_locations, "hib3", intro_column_name
    )

    # Load in simple vaccine coverage to use as a covariate
    # Simple vaccine will be mcv1 for mcv2 and dtp3 for all other vaccines
    simple_vacc_cov_scenarios = load_simple_vacc_cov(vaccine, version, gbd_round_id, draws)
    simple_vacc_cov = simple_vacc_cov_scenarios[simple_vacc_cov_scenarios.scenario == 0]

    # Find what locations need to be simulated and which are given by the vaccines team
    known_intro_dates = introductions[introductions[year_intro_col] != 9999]
    simple_ratio_locations = np.unique(known_intro_dates.location_id.values)
    needed_locs = np.unique(simple_vacc_cov.location_id.values)
    simulated_intro_locations = list(set(needed_locs) - set(simple_ratio_locations))

    simulated_ratios = forecast_simulated_ratios(
        version,
        gbd_round_id,
        years,
        draws,
        okay_locations,
        past_ratio_version,
        vaccine,
        location_metadata,
        simple_vacc_cov,
        introductions,
        simulated_intro_locations,
        year_intro_col,
        gavi_version,
        simple_vacc_cov_scenarios,
        hib3_intro,
    )

    given_ratios = load_forecasted_ratios(
        future_ratio_version, gbd_round_id, vaccine, known_intro_dates, years, draws
    )

    all_ratios = xr.concat([given_ratios, simulated_ratios], dim="location_id")

    # Go from ratio_vaccine:simple_vaccine to just ratio_vaccine coverage
    final_unshifted = multiply_ratio_forecasts(
        vaccine, gbd_round_id, all_ratios, version, draws, years
    )

    final_unshifted = final_unshifted.fillna(0)

    past_vaccine = load_past_vaccine_data(version, vaccine, gbd_round_id, draws, years)
    past_vaccine_subset = past_vaccine.sel(
        year_id=[years.past_end], location_id=final_unshifted.location_id.values
    )

    processor = LogitProcessor(
        years=years,
        offset=ModelConstants.DEFAULT_OFFSET,
        no_mean=True,
        intercept_shift="unordered_draw",
        age_standardize=False,
        gbd_round_id=gbd_round_id,
    )

    final_shifted = processor.post_process(
        logit_with_offset(final_unshifted, 1e-8), past_vaccine_subset
    )

    if not np.isfinite(final_shifted).all():
        raise ValueError("Final values are invalid")

    forecast_path = version.data_dir(gbd_round_id, "future", "vaccine") / f"vacc_{vaccine}.nc"
    save_xr(
        final_shifted,
        forecast_path,
        metric="rate",
        space="identity",
    )


@click.group()
@click.option(
    "--vaccine",
    type=str,
    required=True,
    help=("Vaccine being modeled e.g. `hib3` or 'rotac'"),
)
@click.option(
    "--version",
    "-v",
    type=str,
    required=True,
    multiple=True,
    help=("Vaccine and SDI versions in the form /FILEPATH/version_name"),
)
@click.option(
    "--years",
    type=str,
    required=True,
    help="Year range first_past_year:first_forecast_year:last_forecast_year",
)
@click.option(
    "--gbd-round-id",
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
@click.option(
    "--intro-version",
    type=str,
    required=True,
    help="Version name of the file with vaccine introduction dates",
)
@click.option(
    "--past-ratio-version",
    type=str,
    required=True,
    help="Version name of the file with past ratios",
)
@click.option(
    "--future-ratio-version",
    type=str,
    required=True,
    help="Version name of the file with future ratios",
)
@click.option(
    "--gavi-version",
    type=str,
    required=True,
    help="Version name of the file indicating which locations are GAVI eligible",
)
@click.option(
    "--year-intro-col",
    type=str,
    required=False,
    help="Name of the column in the ratio files with the introduction year",
)
@click.option(
    "--intro-column-name",
    type=str,
    required=False,
    help="Name of the column in the introduction file with the introduction year",
)
@click.pass_context
def cli(
    ctx: click.Context,
    version: list,
    vaccine: str,
    gbd_round_id: int,
    years: str,
    draws: int,
    intro_version: str,
    past_ratio_version: str,
    future_ratio_version: str,
    gavi_version: str,
    year_intro_col: str or None,
    intro_column_name: str or None,
) -> None:
    """Main cli function to parse args and pass them to the subcommands.

    Args:
        ctx (click.Context): ctx object.
        version (list): which population and vaccine versions to pass to
            the script
        vaccine (str): Relevant vaccine
        gbd_round_id (int): Current gbd round id
        years (str): years for forecast
        draws (int): number of draws
        intro_version (str): file name containing introduction dates
        past_ratio_version (str): the version containing past vaccine ratios
        future_ratio_version (str): the version containing future vaccine ratios
        gavi_version (str): name of the file with GAVI eligibility information
        year_intro_col (str): name of the column in past_ratios with the introduction year
        intro_column_name (str): name of the column in the introduction data with the years
    """
    version = Versions(*version)
    years = YearRange.parse_year_range(years)
    check_versions(version, "future", ["sdi", "vaccine"])
    ctx.obj = {
        "version": version,
        "vaccine": vaccine,
        "gbd_round_id": gbd_round_id,
        "years": years,
        "draws": draws,
        "intro_version": intro_version,
        "past_ratio_version": past_ratio_version,
        "future_ratio_version": future_ratio_version,
        "gavi_version": gavi_version,
        "year_intro_col": year_intro_col,
        "intro_column_name": intro_column_name,
    }


@cli.command()
@click.pass_context
def main(ctx: click.Context) -> None:
    """Call to main function.

    Args:
        ctx (click.Context): context object containing relevant params parsed
            from command line args.
    """
    FileSystemManager.set_file_system(OSFileSystem())

    forecast_ratio_vaccines(
        version=ctx.obj["version"],
        vaccine=ctx.obj["vaccine"],
        gbd_round_id=ctx.obj["gbd_round_id"],
        years=ctx.obj["years"],
        draws=ctx.obj["draws"],
        intro_version=ctx.obj["intro_version"],
        past_ratio_version=ctx.obj["past_ratio_version"],
        future_ratio_version=ctx.obj["future_ratio_version"],
        gavi_version=ctx.obj["gavi_version"],
        year_intro_col=ctx.obj["year_intro_col"],
        intro_column_name=ctx.obj["intro_column_name"],
    )


if __name__ == "__main__":
    cli()