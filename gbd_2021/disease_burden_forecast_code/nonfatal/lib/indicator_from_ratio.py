r"""Computes target indicator from existing ratio and indicator data.

Parallelized by cause.

Note:
    The past version of the available indicator should line up with the
    forecast version, otherwise there will be unsolvable intercept shift issues.
    This mostly applies to mortality, where we need to use the past version of
    mortality that mortality uses, rather than the final GBD output that
    nonfatal uses to make MP and MI ratios.
"""
from typing import List, Optional, Tuple

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.processing import (
    LogitProcessor,
    LogProcessor,
    clean_cause_data,
    concat_past_and_future,
    mad_truncate,
    make_shared_dims_conform,
)
from fhs_lib_database_interface.lib.query.model_strategy import RATIO_INDICATORS
from fhs_lib_file_interface.lib.check_input import check_versions
from fhs_lib_file_interface.lib.query.io_helper import read_single_cause
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions, validate_versions_scenarios
from fhs_lib_file_interface.lib.xarray_wrapper import save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_nonfatal.lib.constants import (
    MADTruncateConstants,
    ModelConstants,
    StageConstants,
)
from fhs_pipeline_nonfatal.lib.ratio_from_indicators import ratio_transformation

logger = fhs_logging.get_logger()


def one_cause_main(
    acause: str,
    target_indicator_stage: str,
    ratio_stage: str,
    versions: Versions,
    gbd_round_id: int,
    draws: int,
    years: YearRange,
    output_scenario: Optional[int],
    national_only: bool,
) -> None:
    """Compute target indicator using existing ratio and indicator data.

    If target_is_numerator is true, use ratio multiply available_indicator,
    otherwise divide available_indicator by ratio

    For example: death = mi_ratio * incidence

    Args:
        acause (str): The cause for which a ratio of two indicators is being calculated
        target_indicator_stage (str): What stage to save target indicator
        ratio_stage (str): The ratio stage we are using to convert
        versions: (Versions) A Versions object that keeps track of all the versions and their
            respective data directories.
        gbd_round_id: (int) What gbd_round_id the indicators and ratio are saved under
        draws (int): How many draws to save for the ratio output
        years (YearRange): Forecasting time series year range
        output_scenario (Optional[int]): Optional output scenario ID
        national_only (bool): Whether to include subnational locations, or to include only
            nations.
    """
    # validate versions
    validate_versions_scenarios(
        versions=versions,
        output_scenario=output_scenario,
        output_epoch_stages=[("future", target_indicator_stage)],
    )

    available_indicator_stage, target_is_numerator = _get_available_indicator_stage(
        target_indicator_stage, ratio_stage
    )

    _check_versions(target_indicator_stage, ratio_stage, available_indicator_stage, versions)

    ratio_version_metadata = versions.get("future", ratio_stage).default_data_source(
        gbd_round_id
    )

    # Indicator is calculated using scenario specific ratios.
    # If `ratio` covariates (ie: SDI) vary by scenario, then ratios will vary by scenario.
    # If `ratio` covariates are equal across scenarios, then between scenario differences are
    # driven by mortality alone.
    ratio = read_single_cause(
        acause=acause,
        stage=ratio_stage,
        version_metadata=ratio_version_metadata,
    )

    # Set floor for rounding errors at ModelConstants.MIN_VALUE
    ratio = ratio.where(ratio > ModelConstants.MIN_VALUE, other=ModelConstants.MIN_VALUE)
    # For now, we won't worry about slicing the ratio data on the ``year_id``
    # dim, because we are for the available indicator, and so the arithmetic
    # (which is inner-join logic) will force consistency on that dim.
    clean_ratio, _ = clean_cause_data(
        ratio,
        ratio_stage,
        acause,
        draws,
        gbd_round_id,
        year_ids=None,
        national_only=national_only,
    )

    available_indicator = _past_and_future_data(
        acause, available_indicator_stage, versions, years, gbd_round_id, draws, national_only
    )
    # Set floor for rounding errors at ModelConstants.MIN_VALUE
    available_indicator = available_indicator.where(
        available_indicator > ModelConstants.MIN_VALUE, other=ModelConstants.MIN_VALUE
    )
    # A few death values can be > 1, which throws off the logit function
    if available_indicator_stage == "death":
        available_indicator = available_indicator.clip(max=1).fillna(
            1 - ModelConstants.DEFAULT_OFFSET
        )

    if target_is_numerator:
        modeled_target_indicator = available_indicator * clean_ratio
    else:
        modeled_target_indicator = ratio_transformation(
            available_indicator, clean_ratio, ratio_stage
        )

    # Fill non-finite target-indicator values with zeros; these have zeros in
    # the denominator.
    modeled_target_indicator = modeled_target_indicator.where(
        np.isfinite(modeled_target_indicator)
    ).fillna(0)

    # Get target indicator past to intercept shift
    target_indicator_past = read_single_cause(
        acause=acause,
        stage=target_indicator_stage,
        version_metadata=versions.get("past", target_indicator_stage).default_data_source(
            gbd_round_id
        ),
    )
    clean_target_indicator_past, _ = clean_cause_data(
        target_indicator_past,
        target_indicator_stage,
        acause,
        draws,
        gbd_round_id,
        year_ids=years.past_years,
        national_only=national_only,
    )

    target_indicator_past = make_shared_dims_conform(
        clean_target_indicator_past, modeled_target_indicator, ignore_dims=["year_id"]
    )
    # Special exception for malaria due to problematic data
    if (acause == "malaria") & (target_indicator_stage == "prevalence"):
        max_multiplier = 12
        median_dims = ["age_group_id", "location_id"]
    else:
        max_multiplier = MADTruncateConstants.MAX_MULTIPLIER
        median_dims = list(MADTruncateConstants.MEDIAN_DIMS)

    truncated_target_indicator = mad_truncate(
        modeled_target_indicator,
        median_dims=median_dims,
        pct_coverage=MADTruncateConstants.PCT_COVERAGE,
        max_multiplier=max_multiplier,
        multiplier_step=MADTruncateConstants.MULTIPLIER_STEP,
    )

    # Need to intercept shift in log/logit space to avoid negatives in output.
    if target_indicator_stage == "prevalence":
        processor = LogitProcessor(
            years=years,
            gbd_round_id=gbd_round_id,
            remove_zero_slices=False,
            no_mean=True,
            bias_adjust=False,
            intercept_shift="unordered_draw",
            age_standardize=False,
            shift_from_reference=False,
        )
        truncated_target_indicator = truncated_target_indicator.clip(
            max=StageConstants.PREVALENCE_MAX
        )
    else:
        processor = LogProcessor(
            years=years,
            gbd_round_id=gbd_round_id,
            remove_zero_slices=False,
            no_mean=True,
            bias_adjust=False,
            intercept_shift="unordered_draw",
            age_standardize=False,
            shift_from_reference=False,
        )

    processed_data = processor.pre_process(truncated_target_indicator)
    shifted_truncated_target_indicator = processor.post_process(
        processed_data, target_indicator_past
    )

    target_indicator_file = FHSFileSpec(
        versions.get("future", target_indicator_stage), f"{acause}.nc"
    )

    save_xr_scenario(
        shifted_truncated_target_indicator,
        target_indicator_file,
        metric="rate",
        space="identity",
    )


def _past_and_future_data(
    acause: str,
    stage: str,
    versions: Versions,
    years: YearRange,
    gbd_round_id: int,
    draws: int,
    national_only: bool,
) -> xr.DataArray:
    """Get past and future data for a cause/stage."""

    def get_data(past_or_future: str, year_ids: List[int]) -> xr.DataArray:
        """Internal method to pull and clean single cause data."""
        data = read_single_cause(
            acause=acause,
            stage=stage,
            version_metadata=versions.get(past_or_future, stage).default_data_source(
                gbd_round_id
            ),
        )
        clean_data, _ = clean_cause_data(
            data,
            stage,
            acause,
            draws,
            gbd_round_id,
            year_ids=year_ids,
            national_only=national_only,
        )
        return clean_data

    forecast_data = get_data("future", years.forecast_years)
    past_data = get_data("past", years.past_years)

    return concat_past_and_future(past_data, forecast_data)


def _get_available_indicator_stage(
    target_indicator_stage: str, ratio_stage: str
) -> Tuple[str, bool]:
    """Get available indicator, and check if target is numerator.

    Args:
        target_indicator_stage (str): The stage of the target indicator
        ratio_stage (str): The stage of ratio to check

    Raises:
        ValueError: if the `target_indicator_stage` doesn't match any ratio stage.

    Returns:
        Tuple[str, bool]: the available indicator stage and whether the target is the numerator
    """
    if RATIO_INDICATORS[ratio_stage].numerator == target_indicator_stage:
        available_indicator_stage = RATIO_INDICATORS[ratio_stage].denominator
        target_is_numerator = True
    elif RATIO_INDICATORS[ratio_stage].denominator == target_indicator_stage:
        available_indicator_stage = RATIO_INDICATORS[ratio_stage].numerator
        target_is_numerator = False
    else:
        raise ValueError(f"{target_indicator_stage} does not match ratio stage")

    return available_indicator_stage, target_is_numerator


def _check_versions(
    target_indicator_stage: str,
    ratio_stage: str,
    available_indicator_stage: str,
    versions: Versions,
) -> None:
    """Checks that all expected versions are given."""
    expected_future_stages = {target_indicator_stage, ratio_stage, available_indicator_stage}
    check_versions(versions, "future", expected_future_stages)

    expected_past_stages = {target_indicator_stage, available_indicator_stage}
    check_versions(versions, "past", expected_past_stages)
