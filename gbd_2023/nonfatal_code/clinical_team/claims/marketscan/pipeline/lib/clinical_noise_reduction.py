"""
Module to perform the steps of the Clinical noise reduction method.
"""
from typing import List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants
from crosscutting_functions.noise-reduction import clinical_nr
from loguru import logger

from marketscan.src.pipeline.lib import config


def run_noise_reduction(
    df_to_noise_reduce: pd.DataFrame,
    model_group: str,
    run_id: int,
    model_type: str,
    cols_to_nr: List[str],
    name: str,
):
    """Task runner for the Clinical Noise Reduction class"""
    noise_reduction = clinical_nr.ClinicalNoiseReduction(
        name=name,
        run_id=run_id,
        subnational=True,
        df_path=None,
        model_group=model_group,
        model_type=model_type,
        model_failure_sub="fill_average",
        cols_to_nr=cols_to_nr,
    )

    if len(cols_to_nr) > 1:
        raise AttributeError("This does not support noise reducing more than a single column")

    noise_reduction.df = df_to_noise_reduce

    logger.info(f"Running Noise Reduction on {len(cols_to_nr)} column(s).")
    funcs_args = (
        (_validate_input_args, []),
        (_match_expected_shape_and_check_square, []),
        (_aggregate_to_national_estimates, []),
        (_fit_model, []),
        (_apply_noise_reduction, []),
        (_rake_noise_reduced_results, []),
        (_apply_non_zero_floor, [cols_to_nr]),
    )

    for func, args in funcs_args:
        func(noise_reduction, *args)
        if len(noise_reduction.df) != len(df_to_noise_reduce):
            raise RuntimeError("The rowsize of the data has changed")

    # review the floor. It's unlikely but possible that some non-zero pre-noise-reduction rates
    # get replaced with zero.
    noise_reduction.floor_review = noise_reduction.df.query(
        f"{constants.COLUMN_TO_NOISE_REDUCE}_final == 0 "
        f"and {constants.COLUMN_TO_NOISE_REDUCE} > 0"
    )
    logger.info(
        f"{len(noise_reduction.floor_review)} non-zero pre noise reduction mean "
        "values have been replaced with zero"
    )
    return noise_reduction


def _validate_input_args(noise_reduction: clinical_nr.ClinicalNoiseReduction) -> None:
    settings = config.get_settings()
    noise_reduction._assign_release_id(settings.release_id_for_us_states)
    noise_reduction._eval_group()
    noise_reduction.check_required_columns


def _match_expected_shape_and_check_square(
    noise_reduction: clinical_nr.ClinicalNoiseReduction,
) -> None:
    """Noise reduction requires certain columns and the data must be square"""
    noise_reduction._create_col_to_fit_model()
    noise_reduction.check_square()
    noise_reduction.create_count_col()


def _aggregate_to_national_estimates(
    noise_reduction: clinical_nr.ClinicalNoiseReduction,
) -> None:
    """Subnational data is raked to the national level, so we must first aggregate the
    subnational data to create a national dataset to run noise reduction on and use when raking
    """
    noise_reduction.create_national_df()


def _fit_model(noise_reduction: clinical_nr.ClinicalNoiseReduction) -> None:
    """Fits the self.model_type type of model to the input data. Currently noise reduction only
    supports a Poisson model"""
    noise_reduction.df, noise_reduction.df_model_object = noise_reduction.fit_model(
        noise_reduction.df.copy()
    )
    (
        noise_reduction.national_df,
        noise_reduction.nat_df_model_object,
    ) = noise_reduction.fit_model(noise_reduction.national_df.copy())


def _apply_noise_reduction(noise_reduction: clinical_nr.ClinicalNoiseReduction) -> None:
    """Applies the noise_reduction algorithm"""
    noise_reduction.df = noise_reduction.noise_reduce(noise_reduction.df)
    noise_reduction.national_df = noise_reduction.noise_reduce(noise_reduction.national_df)


def _rake_noise_reduced_results(noise_reduction: clinical_nr.ClinicalNoiseReduction) -> None:
    """Combines the national and subnational results to perform raking using a raking class
    inherited"""
    # manually fill the national vs subnational identifier col for raking,
    noise_reduction.df["is_nat"] = 0
    noise_reduction.national_df["is_nat"] = 1
    noise_reduction.raked_df = pd.concat(
        [noise_reduction.df, noise_reduction.national_df], sort=False
    )
    # do raking
    noise_reduction.raking()
    # drop the input columns
    noise_reduction.raked_df.drop(noise_reduction.nr_cols, axis=1, inplace=True)

    # extract the subnational raked data
    noise_reduction.df = noise_reduction.raked_df.query("is_nat == 0").copy()
    logger.info("Noise reduced results have been raked to the national level")


def _apply_non_zero_floor(
    noise_reduction: clinical_nr.ClinicalNoiseReduction, cols_to_nr: List[str]
) -> None:
    """Noise reduction can produce impossibly small rates (1e-200) This applies a simple method
    of replacing any noise reduced results which fall below the smallest pre-noise-reduced rate
    with zero"""
    for col in cols_to_nr:
        pre_min = noise_reduction.df[col].min()
        noise_reduction.apply_floor()
        post_min = noise_reduction.df.loc[noise_reduction.df[col] > 0, f"{col}_final"].min()
        assert post_min >= pre_min, "The floor application seems to have failed"
    logger.info("The post noise reduction floor has been applied")