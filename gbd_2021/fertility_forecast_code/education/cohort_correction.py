"""This script performs cohort correction on education forecasts created by
the forecast_education.py. The correction is needed to avoid drops in years
of education within a cohort.

    Example:
        .. code:: bash

            python cohort_correction.py \
            --gbd-round-id 5 \
            --forecast-version 20190610_edu_sdg_scenario \
            --output-version 20190610_edu_sdg_scenario_cohort_corrected \
            --years 1950:2018:2140 \
            --draws 1000 \
            parallelize-by-draw \
            --draw-memory 6 \
            --merge-memory 40

"""

import glob
import logging
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import xarray as xr

from fbd_core import argparse, db
from fbd_core.file_interface import FBDPath, open_xr, save_xr

LOGGER = logging.getLogger(__name__)

AGE_25_GROUP_ID = 10

MODELED_SEX_IDS = (1, 2)
MODELED_AGE_GROUP_IDS = tuple(range(6, 21)) + (30, 31, 32, 235)
INDEX_COLS = ('location_id', 'year_id', 'age_group_id', 'sex_id', 'scenario')


def get_cohort_info_from_age_year(age_group_ids, years):
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
        age_group_ids (list(int)):
            List of age group ids.
        years (YearRange):
            The past and forecasted years.
    Returns:
        cohort_age_df (pandas.DataFrame):
            Dataframe with (age&year) to cohort mapping.
    """

    LOGGER.info("In get_cohort_info_from_age_year")
    age_meta_df = db.get_ages()[[
        'age_group_id', 'age_group_years_start']]

    all_age_year_df = pd.MultiIndex.from_product(
        [age_group_ids, years.years],
        names=['age_group_id', 'year_id']
    ).to_frame().reset_index(drop=True)

    cohort_age_df = all_age_year_df.merge(
        age_meta_df, on='age_group_id').astype({'age_group_years_start': int})

    # Finding the cohort from year and lower bound of age_group as described
    # above.
    cohort_age_df['cohort_year'] = (
            cohort_age_df['year_id'] - (
            cohort_age_df['age_group_years_start'] - 5))

    # All cohorts don't need correction.Only those that extend into the future.
    correction_cohorts_years = cohort_age_df[
        cohort_age_df['year_id'] > years.past_end]['cohort_year'].unique()
    cohort_age_df['need_correction'] = \
        cohort_age_df['cohort_year'].isin(correction_cohorts_years)

    cohort_age_df.drop('age_group_years_start', axis=1, inplace=True)

    return cohort_age_df


def apply_cohort_correction(cohort_df, years):
    """Perform correction on the cohort data. The purpose of the correction is
    to prevent forecasted education years from decreasing within a cohort and
    also from increasing after the age of 25.

    The correction id performed only on the forecasted years in a cohort.
    The past data is untouched. The correction involves the following checks
    and actions:

        1. Keep track of the running maximum value.

        2. If the current age is less than 25 and the value is less than the
        running max, then replace it with the running max.
        For example, consider the cohort of 2000. The cohort turned 15 in 2015
        and 20 in 2020. If the value of 2020 is less than the value at 15 then
        we replace the value at 2020. This is where tracking the running max
        proves useful.

        3. If the age is greater than 25 and the cohort turned 25 in the future,
        then replace the current value with the value at age 25.
        For example, consider the cohort of 2000. The cohort turned 25 in 2025
        and 30 in 2030. The value at 2030 is simply replaced with the value at 25.

        4. If the age is greater than 25 but the cohort turned 25 in the past,
        then replace the current value with the latest value of the past.
        For example, consider the cohort of 1980. The cohort turned 25 in the
        year 2005 and last year in the past for this cohort is 2015. The forecasts
        in this cohort will be replaced with the value at 2015.

    Args:
        cohort_df (pandas.Dataframe):
            A dataframe containing the data for a single cohort.
        years (YearRange):
            The past and forecasted years.

    Returns:
        cohort_df (pandas.DataFrame):
            Dataframe with corrected cohort data.
    """

    max_value = -1
    cohort_df['value'] = cohort_df['value']
    cohort_year = cohort_df['cohort_year'].values[0]

    age_25_year = cohort_df.query(
        'age_group_id == @AGE_25_GROUP_ID')['year_id'].values
    # If cohort started in the past, track the value at age 25.
    if cohort_year < years.forecast_start:
        constant_forecast_val = cohort_df.loc[
            cohort_df['year_id'] < years.forecast_start, 'value'].values[-1]

    for idx, row in cohort_df.iterrows():
        max_value = max(max_value, row['value'])

        # Only consider forecasts for adults for correction.
        if row['year_id'] < years.forecast_start or row["age_group_id"] <= 8:
            continue

        # Update value if younger than or equal to 25,
        if row['age_group_id'] <= AGE_25_GROUP_ID:
            # but only if val at previous age_group_id of cohort is
            # larger than current val
            prev_age = row["age_group_id"] - 1
            val_previous_age_is_max = (
                cohort_df[cohort_df.age_group_id==prev_age].value.iloc[0] ==
                max_value)
            if val_previous_age_is_max:
                cohort_df.loc[idx, 'value'] = max_value

        # If older and turned 25 in the future, then replace with the
        # value at age 25.
        elif age_25_year > years.past_end:
            age_25_val = cohort_df.loc[
                cohort_df['age_group_id'] == AGE_25_GROUP_ID, 'value'].values[0]
            cohort_df.loc[idx, 'value'] = age_25_val

        # If older but had turned 25 in the past, then replace value with
        # value from the last past year of the cohort.
        else:
            cohort_df.loc[idx, 'value'] = constant_forecast_val

    return cohort_df


def get_corrected_da(uncorrected_draw_da, cohort_age_df, years):
    """Accepts the uncorrected dataarray, converts it into cohort space,
    extracts cohorts that need correction and applies cohort correction.

    Args:
        uncorrected_draw_da (xr.DataArray):
            Dataarray with uncorrected education cohorts.
        cohort_age_df (pd.DataFrame):
            Dataframe that contains the ages and year_ids associated with
            each cohort years.
        years (YearRange):
            The past and forecasted years.

    Returns:
        corrected_da (xr.DataArray):
            Dataarray that contains the cohort corrected forecasts.
    """

    uncorrected_draw_df = uncorrected_draw_da.rename("value") \
        .to_dataframe() \
        .reset_index()

    uncorrected_draw_df.drop_duplicates(inplace=True)

    uncorrected_draw_df = uncorrected_draw_df.merge(
        cohort_age_df, on=['year_id', 'age_group_id'])

    uncorrected_draw_df = uncorrected_draw_df.sort_values(
        by=['location_id', 'sex_id', 'scenario', 'cohort_year',
            'year_id', 'age_group_id']).reset_index(drop=True)

    # Extracting cohorts that need correction.
    cohorts_to_correct_df = uncorrected_draw_df.query(
        "need_correction==True").copy(deep=True).reset_index(drop=True)

    # Applying correction
    corrected_cohorts = cohorts_to_correct_df.groupby([
        'location_id', 'sex_id', 'scenario', 'cohort_year']
    ).apply(apply_cohort_correction, years)

    # Re-combine with unmodified cohorts.
    unmodified_cohorts_df = uncorrected_draw_df.query(
        "need_correction==False").copy(deep=True).reset_index(drop=True)
    combined_df = pd.concat(
        [unmodified_cohorts_df, corrected_cohorts], ignore_index=True)

    # Convert back to dataarray
    combined_df = combined_df.sort_values(by=list(INDEX_COLS))
    corrected_da = combined_df.drop(
        ['cohort_year', 'need_correction'], axis=1
    ).set_index(list(INDEX_COLS))['value'].to_xarray()

    return corrected_da


def one_draw_main(gbd_round_id, years, draw, forecast_version, output_version):
    """Driver function that handles the education cohort correction for a
    single draw.

    Args:
        gbd_round_id (int):
            The gbd round id.
        years (YearRange):
            The past and forecasted years.
        draw (int):
            The draw number to perform the correction on.
        forecast_version (str):
            Forecast version of education.
        output_version (str):
            Cohort corrected version.
    """
    LOGGER.info("Applying cohort correction to draw: {}".format(draw))
    input_dir = FBDPath("".format())  # Path removed for security reasons
    uncorrected_da = open_xr(input_dir / "education.nc").data
    # subset to national and subnational location ids
    location_table = db.get_location_set(gbd_round_id)

    # modeling subnational estimates
    modeled_location_ids = list(location_table["location_id"].unique())
    avail_sex_ids = [
        sex for sex in uncorrected_da["sex_id"].values
        if sex in MODELED_SEX_IDS]

    # Age groups 2,3,4 and 5 gets filtered out here. Will add them back later.
    avail_age_group_ids = [
        age for age in uncorrected_da["age_group_id"].values
        if age in MODELED_AGE_GROUP_IDS]

    uncorrected_draw_da = uncorrected_da.sel(
        sex_id=avail_sex_ids,
        age_group_id=avail_age_group_ids,
        location_id=modeled_location_ids
    ).sel(draw=draw, drop=True)

    # Create cohort information from age groups and year ids.

    cohort_age_df = get_cohort_info_from_age_year(avail_age_group_ids, years)

    corrected_da = get_corrected_da(
        uncorrected_draw_da, cohort_age_df, years)

    # Combine with dropped age groups
    dropped_age_ids = [
        age for age in uncorrected_da["age_group_id"].values
        if age not in MODELED_AGE_GROUP_IDS]

    dropped_age_da = uncorrected_da.sel(
        sex_id=avail_sex_ids,
        age_group_id=dropped_age_ids,
        location_id=modeled_location_ids).sel(draw=draw, drop=True)

    combined_da = xr.concat([dropped_age_da, corrected_da], dim='age_group_id')
    combined_da['draw'] = draw
    op_dir = FBDPath("".format())

    save_xr(combined_da, op_dir / "corrected_edu_draw{}.nc".format(draw),
            metric="rate", space="identity")


def merge_main(output_version, gbd_round_id):
    """Combine all of the netcdf files generated by one_draw_main and save
    the combined file as `education.nc` in the same directory.

    Args:
        output_version (str):
            Cohort corrected version.
        gbd_round_id (int):
            The gbd round id.
    """
    input_dir = FBDPath("".format())  # Path removed for security reasons
    file_names = list(input_dir.glob('corrected_edu_draw*.nc'))
    edu_ds = xr.open_mfdataset(file_names, concat_dim="draw")
    edu_da = list(edu_ds.data_vars.values())[0]

    LOGGER.info("Saving corrected education.")
    edu_da.name = "value"
    edu_path = input_dir / "education.nc"
    edu_da.to_netcdf(str(edu_path))


def parallelize_main(
        draw_memory, merge_memory, years, draws, forecast_version,
        output_version,
        gbd_round_id):
    """Subprogram for submitting an array job to perform the cohort
    corrections by draw, and then a qsub for merging the draws together.

    Args:
        draw_memory (int):
            Cohort corrected version.
        merge_memory (int):
            The gbd round id.
        years (YearRange):
            The past and forecasted years.
        draws (int):
            Number of draws in the data.
        forecast_version (str):
            Forecast version of education.
        output_version (str):
            Cohort corrected version.
        gbd_round_id (int):
            The gbd round id.

    Raises:
        (RuntimeError):

            * If qsub failed while submitting the array job.
            * If qsub failed while submitting the merge job..
    """
    exec = sys.executable
    script = Path(__file__).absolute()

    qsub_template = (
        "qsub -N 'cohort-correct' "
        f"-l m_mem_free={draw_memory}G "
        "-l fthread=1 "
        "-l archive "
        "-l h_rt=03:00:00 "
        "-q all.q "
        "-P proj_forecasting "
        f"-t 1:{draws} "
        f"-tc 200 "
        f"-b y {exec} "
        f"{script} "
        f"--forecast-version {forecast_version} "
        f"--output-version {output_version} "
        f"--years {years} "
        f"--draws {draws} "
        f"--gbd-round-id {gbd_round_id} "
        "one-draw-correction"
    )

    LOGGER.info(qsub_template)
    qsub_proc = subprocess.Popen(
        qsub_template, shell=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    qsub_out, _ = qsub_proc.communicate()
    if qsub_proc.returncode:
        err_msg = "Cohort correction qsub failed."
        LOGGER.error(err_msg)
        raise RuntimeError(err_msg)

    try:
        hold_jid = int((qsub_out.split()[2]).split(b".")[0])
    except:
        err_msg = "Error getting the hold job id."
        LOGGER.error(err_msg)
        raise RuntimeError(err_msg)

    merge_qsub = (
        "qsub -N 'cohort-correct-merge' "
        f"-hold_jid {hold_jid} "
        f"-l m_mem_free={merge_memory}G "
        "-l fthread=1 "
        "-l archive "
        "-l h_rt=01:00:00 "
        "-q all.q "
        "-P proj_forecasting "
        f"-b y {exec} "
        f"{script} "
        f"--forecast-version {forecast_version} "
        f"--output-version {output_version} "
        f"--years {years} "
        f"--draws {draws} "
        f"--gbd-round-id {gbd_round_id} "
        "merge-draws"
    )

    LOGGER.info(merge_qsub)
    merge_qsub_proc = subprocess.Popen(
        merge_qsub, shell=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    merge_qsub_proc.wait()
    if merge_qsub_proc.returncode:
        merge_err_msg = "Merge qsub failed."
        LOGGER.error(merge_err_msg)
        raise RuntimeError(merge_err_msg)


if __name__ == "__main__":

    def get_draw(draw):
        sge_task_id = os.environ.get("SGE_TASK_ID")
        if draw:
            return draw
        elif sge_task_id:
            LOGGER.debug("SGE_TASK_ID: {}".format(sge_task_id))
            return int(sge_task_id) - 1
        else:
            err_msg = "rei and SGE_TASK_ID can not both be NoneType."
            LOGGER.error(err_msg)
            raise ValueError(err_msg)

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        "--gbd-round-id", type=int, required=True, help="GBD round of data")
    parser.add_argument(
        "--forecast-version", type=str, required=True,
        help="The version of education forecasts to pull.")
    parser.add_argument(
        "--output-version", type=str, required=True,
        help="The version of cohort corrected forecasts.")
    parser.add_arg_years(required=True)
    parser.add_arg_draws(required=True)

    subparsers = parser.add_subparsers()

    one_draw = subparsers.add_parser(
        "one-draw-correction", help=one_draw_main.__doc__)
    one_draw.add_argument(
        "--draw", type=int, help="Draw to apply the cohort correction.")
    one_draw = one_draw.set_defaults(func=one_draw_main)

    parallelize_by_draw = subparsers.add_parser(
        "parallelize-by-draw", help=parallelize_main.__doc__)
    parallelize_by_draw.add_argument(
        "--draw-memory", type=int, required=True, help="Memory for each job.")
    parallelize_by_draw.add_argument(
        "--merge-memory", type=int, required=True, help="Memory for merge job.")
    parallelize_by_draw.set_defaults(func=parallelize_main)

    merge_draws = subparsers.add_parser("merge-draws",
                                        help=merge_main.__doc__)
    merge_draws.set_defaults(func=merge_main)

    args = parser.parse_args()

    if args.func == parallelize_main:
        parallelize_main(
            args.draw_memory, args.merge_memory, args.years, args.draws,
            args.forecast_version, args.output_version, args.gbd_round_id)
    elif args.func == merge_main:
        merge_main(args.output_version, args.gbd_round_id)
    else:
        args.draw = get_draw(args.draw)
        one_draw_main(
            args.gbd_round_id, args.years, args.draw, args.forecast_version,
            args.output_version)
