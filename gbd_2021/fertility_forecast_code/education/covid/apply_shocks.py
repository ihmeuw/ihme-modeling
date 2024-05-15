"""
A script that applies education shocks due to COVID school closures to educational attainment. All
calculations are done on the reference scenario at mean level. The means are used to shift education
draws for all scenarios at the end. The output data will end 5 years earlier than the input
education data due to age group interpolation. (E.g. if the `years` arg is "1990:2020:2150", the
output data will have years 2020-2145.) Age group interpolation is not actually necessary to produce
shocked education because the period data will look the same without it, but it is useful for
making sensible cohort plots for vetting.

Steps for applying COVID shocks to education:
    1. Age-split education from 5-year ages to single-year ages. (Interpolate ages < 25.)
    2. Convert single-year education to cohort space.
    3. Scale shocks such that education lost cannot be greater than the increase in educational
       attainment that year.
    4. Apply broadband terms to shocks as a proxy for online education.
    5. Shift shock age/years so that all shocks occurring before age 15 are introduced at age 15.
    6. Convert COVID shocks to cohort space.
    7. Sum shocks occurring before age 15 and shocks at age 15, since that is the youngest age in
       the education data.
    8. Expand dimensions of shocks to match education data.
    9. Compute cumulative shock values.
    10. Subtract cumulative shocks from education cohorts.
    11. Convert education data back to 5-year age groups in period space.
    12. Shift period education draws by the difference between shocked and unshocked education means
        in period space.

See comments in `apply_education_disruptions` to match above steps with specific functions and code
blocks.

..  code:: bash

    python apply_shocks.py \
        --gbd-round-id 11 \
        --years 1990:2020:2150 \
        --draws 500 \
        --edu-version unshocked_edu_vers \
        --shock-version shock_vers \
        --broadband-version broadband_vers \
        --output-version-tag some_tag_append_to_output_version_name (optional)

..  TODO:: 
        - Add type hints to all functions.
        - Make constants.py to store global constants.
"""


import click
import logging
import numpy as np
import pandas as pd
import warnings
import xarray as xr

from fhs_lib_database_interface.lib.query.age import get_ages
from fhs_lib_database_interface.lib.query.location import get_location_set
from fhs_lib_data_transformation.lib.dimension_transformation import expand_dimensions
from fhs_lib_data_transformation.lib.processing import log_with_offset, invlog_with_offset
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_file_interface.lib.file_interface import FBDPath
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr, save_xr
from fhs_lib_year_range_manager.lib.year_range import YearRange

logging.getLogger("console").setLevel(logging.INFO)
LOGGER = logging.getLogger(__name__)


# TODO: These will go to a constants.py since ETL code depends on some of the same variables.
MODELED_AGES = list(range(8, 21)) + [30, 31, 32] + [235] ## standard GBD 5-year age groups
UNDER_25_AGES = [8, 9] ## ages to interpolate
COHORT_AGE_START = 5
COHORT_AGE_START_ID = 53 ## age group ID for age 5
YOUNGEST_AGE_IN_DATA = 15
AGE_15_ID = 63
ALL_SINGLE_YEAR_AGE_IDS = list(range(53, 143)) ## used to generate cohort metadata
SINGLE_YEAR_15_TO_19 = np.array(range(63, 68)) ## used to make single-year age IDs for age-splitting
TERMINAL_AGE_ID = 235
TERMINAL_AGE_START = 95
SHOCK_YEARS = [2020, 2021, 2022]
AGE_GROUP_WIDTH = 5 # years
PERIOD_DF_COLUMNS = [
    "location_id",
    "year_id",
    "sex_id",
    "age_group_id",
    "value",
    "shocked_val",
    "shocked_val_broadband_corrected"
]
PLOT_DF_ID_COLUMNS = [
    "location_id",
    "ihme_loc_id",
    "location_ascii_name",
    "location_level",
    "year_id",
    "sex_id",
    "age_group_id",
    "age_group_name"
]
PLOT_DF_RENAME_DICT = {
    "value": "unshocked",
    "shocked_val": "shocked",
    "shocked_val_broadband_corrected": "shocked_broadband_corr"
}
PLOT_DF_VALUE_COLUMNS = list(PLOT_DF_RENAME_DICT.values())


def get_cohort_info_from_age_year(age_group_ids, years, age_metadata):
    """Calculates the cohort year for each age_group - year pair.
    In the context of education, a cohort year is the year the cohort 
    turned 5 years old and not the birth year.

    For a given age_group - year pair, its cohort year is calculated
    in the following manner:
                    
    cohort_year = (year - age_group_lower_bound) + 5

    The 5 is added at the end to force the cohort to start at age 5
    and not 0. 

    For example: 
    Let the age group be 10 and the year be 1990. Then this group
    will belong to  the (1990 - 25) + 5 = **1970** cohort.

    Args:
        age_group_ids (list[int]):
            List of age group ids.
        years (YearRange):
            The past and forecasted years.
    Returns:
        cohort_age_df (pandas.DataFrame):
            Dataframe with (age & year) to cohort mapping.
    """

    age_metadata = age_metadata[['age_group_id', 'age_group_years_start']]

    all_age_year_df = pd.MultiIndex.from_product(
        [age_group_ids, years.forecast_years], names=['age_group_id', 'year_id']
    ).to_frame().reset_index(drop=True)

    cohort_age_df = all_age_year_df.merge(
        age_metadata, on='age_group_id'
    ).astype({'age_group_years_start': int})

    # Finding the cohort from year and lower bound of age_group as described
    # above.
    cohort_age_df['cohort_year'] = (
        cohort_age_df.year_id - cohort_age_df.age_group_years_start + COHORT_AGE_START
    )

    return cohort_age_df.drop(columns="age_group_years_start")


def read_in_data(gbd_round_id, draws, edu_version, shock_version, broadband_version):
    """Reads in all inputs from the filesystem, and returns education draws, mean reference-only
    education, COVID shock proportions, and broadband correction terms.

    Args:
        gbd_round_id (int):
            The GBD round ID.
        draws (int):
            The number of draws desired. This can affect the mean values since resampling occurs
            before taking the mean.
        edu_version (str):
            The education version to pull.
        shock_version (str):
            The COVID shock version to pull.
        broadband_version (str):
            The broadband version to pull.
    Returns:
        education_resampled (xr.DataArray):
            Education draws.
        reference_mean_edu (xr.DataArray):
            Reference-only education means.
        shocks (xr.DataArray):
            The location/year-specific proportion of school closures due to COVID.
        broadband  (pd.DataFrame):
            Location-specific broadband access scaled between 0 and 0.5, so that the maximum amount
            of educational attainment that can be "protected" from COVID shocks via online education
            is half of education lost.
    """

    input_edu_path = FBDPath(f"")
    shock_path = FBDPath(f"")
    broadband_path = FBDPath(f"")

    education = open_xr(input_edu_path / "education.nc").data.sel(
        age_group_id=MODELED_AGES
    )
    education_resampled = resample(education, draws)
    LOGGER.info("Taking mean of education draws.")
    reference_mean_edu = education_resampled.sel(scenario=0, drop=True).mean("draw")

    shocks = open_xr(shock_path / "props.nc").data.sel(vac_status="all", drop=True)
    broadband = pd.read_csv(broadband_path / "broadband.csv")

    return education_resampled, reference_mean_edu, shocks, broadband

def compute_average_annual_change(education, age_group_id, year_id):
    """Computes the average annual absolute change within a cohort between two adjacent 5-year age
    groups (i.e. the annual absolute change within a cohort between t and t+5). This provides a
    rough estimate of how much education is attained each year in the lower age group.

    Args:
        education (xr.DataArray):
            Education means.
        age_group_id (int):
            The age group ID we want to calculate absolute change for.
        year_id (list[int]):
            The years for which we want to calculate absolute change..
    Returns:
        annual_change (xr.DataArray):
            Average annual absolute change over the cohort in the specified age group/years.
    """

    # The age group and years are shifted, so this is how much education a cohort is projected
    # to have at `age_group_id + 1` and `year_id + 5`.
    # (E.g. What 15 to 19 year olds will look like in 5 years at age 20-24.)
    edu_cohort_shifted = education.shift(age_group_id=-1, year_id=-AGE_GROUP_WIDTH).sel(
        age_group_id=age_group_id, year_id=year_id, drop=True
    )
    # This allows us to compute the average annual absolute change over 5 years as the cohort ages.
    annual_change = (
        edu_cohort_shifted -
        education.sel(age_group_id=age_group_id, year_id=year_id, drop=True)
    ) / AGE_GROUP_WIDTH

    return annual_change


def age_split_education(education, years):
    """Age splits education from 5-year age groups to single-year age groups. Single-year ages below
    25 are interpolated at a constant value of annual absolute change, which is estimated via the
    absolute change in the cohort over 5 years between adjacent 5-year age groups.

    Note: The script output data will end 5 years earlier than the input education data due to age
    group interpolation. (E.g. if the `years` arg is "1990:2020:2150", the output data will have
    years 2020-2145.) Age group interpolation is not actually necessary to produce shocked education
    because the period data will look the same without it, but it is useful for making sensible
    cohort plots for vetting.)

    Args:
        education (xr.DataArray):
            Education means with standard GBD age groups (5-year age groups).
        years (list[int]):
            The years of the data to be split.
    Returns:
        edu_age_split_df (pd.DataFrame):
            Education split into single-year age groups.
    """

    edu_age_split_list = []
    for i, age in enumerate(MODELED_AGES[:-1]):
        new_ages = SINGLE_YEAR_15_TO_19 + (i * AGE_GROUP_WIDTH)
        
        age_da = education.sel(age_group_id=age, year_id=years.forecast_years)
        age_split = expand_dimensions(age_da, age_group_id=new_ages)

        if age in UNDER_25_AGES:
            LOGGER.info(f"Interpolating single year ages for age-group-id {age}.")

            age_scalars = xr.DataArray(
                [-2, -1, 0, 1, 2], dims=["age_group_id"], coords=dict(age_group_id=new_ages)
            )

            annual_change = compute_average_annual_change(
                education, age_group_id=age, year_id=years.forecast_years
            )

            age_addends = (
                expand_dimensions(annual_change, age_group_id=new_ages) * age_scalars
            ).rename("age_addend")

            age_split = age_split + age_addends

        edu_age_split_list.append(age_split)

    edu_age_split = xr.concat(
        (edu_age_split_list +
        [education.sel(age_group_id=[TERMINAL_AGE_ID], year_id=years.forecast_years)]),
        dim="age_group_id"
    ).rename("value")

    edu_age_split_df = edu_age_split.to_dataframe().reset_index()

    return edu_age_split_df


def apply_broadband_terms(shocks, broadband):
    """UNESCO broadband access data is used as a proxy for the availability of online education. The
    proportion of broadband access in each location is scaled to the shock proportion, and then
    subtracted from the shock to create an "online education corrected shock". The broadband terms
    have already been scaled between 0 and 0.5 so that students cannot regain more than half the
    education they are predicted to lose from COVID school closures (per CJLM).

    Args:
        shocks (xr.DataArray):
            Proportion of days that school closed due to COVID in 2020, 2021, and 2022 out of 365
            days, pre-scaled
        broadband (pd.DataFrame):
            Proportion of broadband access per capita scaled between 0 and 0.5.
    Returns:
        shocks_df (pd.DataFrame):
            DataFrame with both uncorrected and broadband-corrected shocks.
    """

    LOGGER.info("Applying broadband correction.")

    shocks_df = shocks.rename("shock").to_dataframe().reset_index()
    shocks_df = shocks_df.merge(broadband)
    shocks_df["broadband_term"] = shocks_df.shock * shocks_df.broadband_term
    shocks_df["shock_minus_broadband"] = shocks_df.shock - shocks_df.broadband_term
    shocks_df.loc[shocks_df.shock_minus_broadband < 0, "shock_minus_broadband"] = 0
    shocks_df.drop(columns="broadband_term", inplace=True)

    return shocks_df


def shift_shock_age_years(shocks_df):
    """Education data is only forecasted for ages 15+, but many shocks occur before age 15. Thus,
    any shocks that occur before age 15 are age/year-shifted such that they are introduced in the
    future when the shocked cohort reaches age 15.

    Args:
        shocks_df (pd.DataFrame):
            DataFrame with both uncorrected and broadband-corrected shocks.
    Returns:
        shocks_df (pd.DataFrame):
            Shocks age/year-shifted such that they are introduced in the future when the shocked
            cohort reaches age 15.
    """

    shocks_df["age"] = _get_age_from_age_id(shocks_df.age_group_id)
    shocks_df["years_till_15"] = YOUNGEST_AGE_IN_DATA - shocks_df.age
    shocks_df.loc[shocks_df.years_till_15 < 0, "years_till_15"] = 0
    shocks_df["shock_year"] = shocks_df.year_id
    shocks_df["year_id"] = shocks_df.year_id + shocks_df.years_till_15
    shocks_df.loc[shocks_df.age < YOUNGEST_AGE_IN_DATA, "age_group_id"] = AGE_15_ID
    shocks_df.loc[shocks_df.age < YOUNGEST_AGE_IN_DATA, "age"] = YOUNGEST_AGE_IN_DATA
    shocks_df.drop(columns="years_till_15", inplace=True)

    return shocks_df


def expand_shock_cohorts(shock_cohorts, cohort_age_df):
    """For the application of the COVID shocks to educational attainment to make sense, the shocks
    must be cumulative and have the same dimensions as the education data. This function expands
    the dimensions of the shock data such that the data can be applied to educational attainment.

    Args:
        shocks_cohorts (pd.DataFrame):
            Shocks in cohort space.
        cohort_age_df (pandas.DataFrame):
            Dataframe with (age & year) to cohort mapping.
    Returns:
        shock_cohorts_expanded (pd.DataFrame):
            Shock cohorts with dimensions expanded to match education cohorts.
    """

    cohort_age_sex_location_df = expand_dimensions(
        cohort_age_df.set_index(["age_group_id", "year_id"]).to_xarray()["cohort_year"],
        location_id=shock_cohorts.location_id.unique(), sex_id=[1, 2]
    ).to_dataframe().reset_index()

    shock_cohorts_expanded = shock_cohorts.merge(
        cohort_age_sex_location_df,
        on=["location_id", "year_id", "age_group_id", "sex_id", "cohort_year"],
        how="outer"
    )

    shock_cohorts_expanded.loc[shock_cohorts_expanded.age.isnull(), "age"] = _get_age_from_age_id(
        shock_cohorts_expanded.age_group_id
    )
    shock_cohorts_expanded.loc[
        shock_cohorts_expanded.age_group_id == TERMINAL_AGE_ID, "age"
    ] = TERMINAL_AGE_START
    shock_cohorts_expanded.loc[shock_cohorts_expanded.shock.isnull(), "shock"] = 0
    shock_cohorts_expanded.loc[
        shock_cohorts_expanded.shock_minus_broadband.isnull(), "shock_minus_broadband"
    ] = 0

    return shock_cohorts_expanded


def make_cumulative_shocks(shock_cohorts):
    """Calculates cumulative shocks over the cohort to apply to the cohort education data.

    Args:
        shocks_cohorts (pd.DataFrame):
            Shock cohorts with dimensions expanded to match education cohorts.
    Returns:
        cumulative_shocks (pd.DataFrame):
            Shock cohorts with dimensions expanded to match education cohorts with cumulative
            shocks.
    """

    LOGGER.info("Making cumulative shocks.")

    cumulative_shocks = shock_cohorts.sort_values(["location_id", "year_id"])

    cumulative_shocks["cumulative_shock"] = cumulative_shocks.groupby(
        ["location_id", "sex_id", "cohort_year"]
    ).cumsum().shock

    cumulative_shocks["cumulative_shock_minus_broadband"] = cumulative_shocks.groupby(
        ["location_id", "sex_id", "cohort_year"]
    ).cumsum().shock_minus_broadband

    return cumulative_shocks


def shock_education(shock_cohorts, edu_cohorts):
    """Applies cumulative shocks to education cohort data.

    Args:
        shocks_cohorts (pd.DataFrame):
            Shock cohorts with dimensions expanded to match education cohorts with cumulative
            shocks.
    Returns:
        edu_shocked (pd.DataFrame):
            Cohort data with columns for education, shocked education, and shocks.
    """

    LOGGER.info("Shocking education.")

    edu_shocked = edu_cohorts.merge(shock_cohorts.query("age_group_id >= @AGE_15_ID"), how="outer")

    edu_shocked["shocked_val"] = edu_shocked.value - edu_shocked.cumulative_shock
    edu_shocked["shocked_val_broadband_corrected"] = (
        edu_shocked.value - edu_shocked.cumulative_shock_minus_broadband
    )

    return edu_shocked

def convert_to_period_space_gbd_ages(edu_cohorts_shocked):
    """Most uses of educational attainment require period educational attainment with 5-year GBD
    age groups. This function converts single-year age education cohorts to 5-year age group
    education in period space.

    Args:
        edu_cohorts_shocked (pd.DataFrame):
            Cohort data with columns for education, shocked education, and shocks.
    Returns:
        period_df (pd.DataFrame):
            Period-space education (shocked and unshocked) with GBD 5-year age groups.
    """

    LOGGER.info("Converting to period space.")

    age_bins = [SINGLE_YEAR_15_TO_19 + (i * 5) for i in range(0, int(len(MODELED_AGES) - 1))]
    five_year_to_single_year_age_dict = dict(zip(MODELED_AGES[:-1], age_bins))

    period_df_list = []
    for five_year_id, single_year_ids in five_year_to_single_year_age_dict.items():
    
        period_age_df = edu_cohorts_shocked.query(
            "age_group_id in @single_year_ids"
        )[PERIOD_DF_COLUMNS]
        
        period_age_df = period_age_df.groupby(
            ["location_id", "year_id", "sex_id"]
        ).mean().reset_index()
        
        period_age_df["age_group_id"] = five_year_id
        
        period_df_list.append(period_age_df)

    terminal_age_df = edu_cohorts_shocked.query(
        "age_group_id == @TERMINAL_AGE_ID"
    )[PERIOD_DF_COLUMNS]

    period_df = pd.concat(period_df_list + [terminal_age_df], ignore_index=True)

    return period_df


def shift_education_draws(reference_mean_edu, period_edu_shocked, education_draws):
    """Finds the log difference between reference scenario unshocked education means and shocked
    education means in period space. The difference is then subtracted from the draws in log space
    to make the shocked draws.

    Args:
        reference_mean_edu (xr.DataArray):
            Mean-level reference-only educational attainment in period space.
        period_edu_shocked (xr.DataArray):
            Shocked mean-level reference-only educational attainment in period space.
        education_draws (xr.DataArray):
            Draw-level educational attainment in period space with all scenarios.
    Returns:
        edu_shocked_draws (xr.DataArray):
            Shocked draw-level educational attainment in period space with all scenarios.

    """
    LOGGER.info("Making draws.")
    log_difference = (
        log_with_offset(reference_mean_edu, offset=0) -
        log_with_offset(period_edu_shocked, offset=0)
    )
    log_edu_shocked_draws = (
        log_with_offset(education_draws, offset=0) - log_difference
    )
    edu_shocked_draws = invlog_with_offset(log_edu_shocked_draws, offset=0, bias_adjust=False)

    return edu_shocked_draws


def make_plot_dfs(edu_cohorts_shocked, period_edu_shocked, age_metadata, location_metadata):
    """Makes DataFrames for later plotting and vetting.

    Args:
        edu_cohorts_shocked (pd.DataFrame):
            Single-year age education cohorts with shocks.
        period_edu_shocked (pd.DataFrame):
            5-year age group education in period space with shocks.
        age_metadata (pd.DataFrame):
            DataFrame with age group metadata.
        location_metadata (pd.DataFrame):
            DataFrame with location metadata.
    Returns:
        plot_dfs["edu_cohorts_shocked"] (pd.DataFrame):
            DataFrame for plotting shocked education cohorts with single-year ages.
        plot_dfs["period_edu_shocked"] (pd.DataFrame):
            DataFrame for plotting shocked education in period space with GBD 5-year age groups.
    """

    LOGGER.info("Making plot dfs.")

    age_metadata = age_metadata[["age_group_id", "age_group_name"]]
    location_metadata = location_metadata[
        ["location_id", "ihme_loc_id", "location_ascii_name", "level"]
    ].rename(columns={"level": "location_level"})

    plot_dfs = dict(edu_cohorts_shocked=edu_cohorts_shocked, period_edu_shocked=period_edu_shocked)
    for key, df in plot_dfs.items():

        plot_df = df.merge(location_metadata).merge(age_metadata).rename(
            columns=PLOT_DF_RENAME_DICT
        )

        try:
            plot_df_id_columns = PLOT_DF_ID_COLUMNS + ["cohort_year"]
            plot_df = plot_df[plot_df_id_columns + PLOT_DF_VALUE_COLUMNS]
        except:
            plot_df_id_columns = PLOT_DF_ID_COLUMNS
            plot_df = plot_df[plot_df_id_columns + PLOT_DF_VALUE_COLUMNS]

        plot_df = pd.melt(
            frame=plot_df,
            id_vars=plot_df_id_columns,
            value_vars=PLOT_DF_VALUE_COLUMNS,
            var_name="education_state",
            value_name="education_years"
        )

        plot_dfs[key] = plot_df

    return plot_dfs["edu_cohorts_shocked"], plot_dfs["period_edu_shocked"]


@click.command()
@click.option("--gbd-round-id", type=int)
@click.option("--years", type=str)
@click.option("--draws", type=int)
@click.option("--edu-version", type=str)
@click.option("--shock-version", type=str)
@click.option("--broadband-version", type=str)
@click.option("--output-version-tag", type=str, default=None)
def apply_education_disruptions(
    gbd_round_id: int,
    years: str,
    draws: int,
    edu_version: str,
    shock_version: str,
    broadband_version: str,
    output_version_tag: str
):
    # The output data will end 5 years earlier than the input education data due to age group
    # interpolation.
    years = YearRange.parse_year_range(years)
    years = YearRange(years.past_start, years.forecast_start, years.forecast_end - AGE_GROUP_WIDTH)

    output_version = edu_version + "_covid_shocks"
    if output_version_tag is not None:
        output_version += f"_{output_version_tag}"

    output_path = FBDPath(f"")

    age_metadata = get_ages()
    location_metadata = get_location_set(gbd_round_id)

    cohort_age_df = get_cohort_info_from_age_year(
        age_group_ids=ALL_SINGLE_YEAR_AGE_IDS + [TERMINAL_AGE_ID],
        years=years,
        age_metadata=age_metadata
    )

    education_draws, reference_mean_edu, shocks, broadband = read_in_data(
        gbd_round_id, draws, edu_version, shock_version, broadband_version
    )

    # Step 1
    single_year_edu = age_split_education(reference_mean_edu, years)

    # Step 2
    edu_cohorts = single_year_edu.merge(cohort_age_df, on=['year_id', 'age_group_id'])

    # Step 3
    # This is used to scale the shocks to how much education would have been gained if not for the
    # pandemic. Ideally this would be age-specific, but for now we only have ages >= 15.
    annual_change_in_edu_15to19_during_pandemic = compute_average_annual_change(
        reference_mean_edu, MODELED_AGES[0], SHOCK_YEARS
    )
    shocks_scaled = shocks * annual_change_in_edu_15to19_during_pandemic

    # Step 4
    shocks_broadband_corrected = apply_broadband_terms(shocks_scaled, broadband)

    # Step 5
    shocks_age_year_shifted = shift_shock_age_years(shocks_broadband_corrected)

    # Step 6
    shock_cohorts = shocks_age_year_shifted.merge(cohort_age_df, on=['year_id', 'age_group_id'])

    # Step 7
    # Sum shocks occurring before age 15 and shocks at age 15 since that is the youngest age in the
    # education data.
    shock_cohorts = shock_cohorts.groupby(
        ["location_id", "year_id", "age_group_id", "sex_id", "age", "cohort_year"]
    ).sum()[["shock", "shock_minus_broadband"]].reset_index()

    # Step 8
    shock_cohorts = expand_shock_cohorts(shock_cohorts, cohort_age_df)

    # Step 9
    shock_cohorts = make_cumulative_shocks(shock_cohorts)

    # Step 10
    edu_cohorts_shocked = shock_education(shock_cohorts, edu_cohorts)

    # Step 11
    period_edu_shocked = convert_to_period_space_gbd_ages(edu_cohorts_shocked)
    # Convert to DataArray for use in other pipelines.
    period_edu_shocked_da = period_edu_shocked.set_index(
        ["location_id", "year_id", "sex_id", "age_group_id"]
    ).to_xarray()["shocked_val_broadband_corrected"].rename("value")

    # Step 12
    edu_shocked_draws = shift_education_draws(
        reference_mean_edu, period_edu_shocked_da, education_draws
    )

    save_xr(
        edu_shocked_draws,
        output_path / "education.nc",
        metric="rate",
        space="identity",
        shock_version=shock_version,
        broadband_version=broadband_version
    )

    LOGGER.info(f"Shocked education draws saved to: {output_path}")

    cohort_plot_df, period_plot_df = make_plot_dfs(
        edu_cohorts_shocked, period_edu_shocked, age_metadata, location_metadata
    )
    cohort_plot_df.to_csv(output_path / "cohort_plot_df.csv", index=False)
    period_plot_df.to_csv(output_path / "period_plot_df.csv", index=False)

    LOGGER.info(f"Plot dfs saved to {output_path}.")


def _get_age_from_age_id(age_group_id_col):
    return age_group_id_col - COHORT_AGE_START_ID + COHORT_AGE_START


if __name__ == '__main__':
    apply_education_disruptions()