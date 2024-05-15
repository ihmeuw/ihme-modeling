r"""Computes ratio of two indicators for past data.

This ratio would be forecasted. Parallelized by cause.
"""
from typing import Optional

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib import processing
from fhs_lib_database_interface.lib.query import cause
from fhs_lib_database_interface.lib.query.model_strategy import RATIO_INDICATORS
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec, VersionMetadata
from fhs_lib_file_interface.lib.versioning import Versions, validate_versions_scenarios
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_nonfatal.lib.constants import ModelConstants, StageConstants

logger = fhs_logging.get_logger()


def ratio_transformation(
    numerator_da: xr.DataArray, denominator_da: xr.DataArray, ratio_stage: str
) -> xr.DataArray:
    """Simple division if not MP ratio. logit(m) - logit(p) if MP ratio."""
    if ratio_stage == StageConstants.MP_RATIO:
        logit_numerator_da = processing.logit_with_offset(numerator_da)
        logit_denominator_da = processing.logit_with_offset(denominator_da)
        result = logit_numerator_da - logit_denominator_da
        result = processing.invlogit_with_offset(result, bias_adjust=False)
    else:
        result = numerator_da / denominator_da

    return result


def one_cause_main(
    acause: str,
    draws: int,
    gbd_round_id: int,
    ratio_stage: str,
    versions: Versions,
    years: YearRange,
    output_scenario: Optional[int] = None,
    national_only: bool = False,
) -> None:
    """Compute ratio of two indicators.

    For example: mi_ratio = death / incidence

    Args:
        acause (str): The cause for which a ratio of two indicators is being calculated.
        draws (int): How many draws to save for the ratio output
        gbd_round_id: (int) What gbd_round_id the indicators and ratio are saved under
        ratio_stage (str): What stage to save the ratio into
        versions: (Versions) A Versions object that keeps track of all the versions and their
            respective data directories.
        years (YearRange): Forecasting timeseries abstraction
        output_scenario (Optional[int]): Optional output scenario ID
        national_only (bool): Whether to include subnational locations, or to include only
            nations.
    """
    # validate versions
    validate_versions_scenarios(
        versions=versions,
        output_scenario=output_scenario,
        output_epoch_stages=[("past", ratio_stage)],
    )

    numerator_indicator_stage = RATIO_INDICATORS[ratio_stage].numerator
    # We want to infer the stage estimates of the given cause for the numerator
    # indicator and the denominator indicator since there is a chance we don't
    # actually have estimates for one or both of those stages.
    inferred_numerator_acause = cause.get_inferred_acause(acause, numerator_indicator_stage)

    numerator_indicator = open_xr_scenario(
        FHSFileSpec(
            version_metadata=versions.get("past", numerator_indicator_stage),
            filename=f"{inferred_numerator_acause}.nc",
        )
    )

    clean_numerator_indicator, numerator_warn_msg = processing.clean_cause_data(
        data=numerator_indicator,
        stage=numerator_indicator_stage,
        acause=acause,
        draws=draws,
        gbd_round_id=gbd_round_id,
        year_ids=years.past_years,
        national_only=national_only,
    )
    _assert_all_finite(clean_numerator_indicator, "past")

    denominator_indicator_stage = RATIO_INDICATORS[ratio_stage].denominator
    inferred_denominator_acause = cause.get_inferred_acause(
        acause, denominator_indicator_stage
    )

    denominator_indicator = open_xr_scenario(
        FHSFileSpec(
            version_metadata=versions.get("past", denominator_indicator_stage),
            filename=f"{inferred_denominator_acause}.nc",
        )
    )

    clean_denominator_indicator, denominator_warn_msg = processing.clean_cause_data(
        data=denominator_indicator,
        stage=denominator_indicator_stage,
        acause=acause,
        draws=draws,
        gbd_round_id=gbd_round_id,
        year_ids=years.past_years,
        national_only=national_only,
    )
    _assert_all_finite(clean_denominator_indicator, "past")

    # A few death values can be > 1, which throws off the logit function
    if numerator_indicator_stage == "death":
        clean_numerator_indicator = clean_numerator_indicator.clip(max=1).fillna(
            1 - ModelConstants.DEFAULT_OFFSET
        )

    ratio = ratio_transformation(
        clean_numerator_indicator, clean_denominator_indicator, ratio_stage
    )

    # Fill non-finite ratios with zeros; these have zeros in the denominator
    ratio = ratio.where(np.isfinite(ratio)).fillna(0)

    ratio_file_spec = FHSFileSpec(
        version_metadata=versions.get("past", ratio_stage),
        filename=f"{acause}.nc",
    )

    save_xr_scenario(
        xr_obj=ratio,
        file_spec=ratio_file_spec,
        metric="rate",
        space="identity",
    )

    warning_msg = ""
    if numerator_warn_msg:
        warning_msg += numerator_warn_msg
    if denominator_warn_msg:
        warning_msg += denominator_warn_msg
    _write_warning_msg(acause, warning_msg, ratio_file_spec.version_metadata)


def _assert_all_finite(data: xr.DataArray, past_or_future: str) -> None:
    """Validate that all values in `data` are finite."""
    if not np.isfinite(data).all():
        raise ValueError(f"{past_or_future} {data.name} data  has non-finite values!")


def _write_warning_msg(acause: str, warning_msg: str, out_path: VersionMetadata) -> None:
    """Write a warning msg related to creating the ratio for this cause."""
    if warning_msg:
        warning_dir = out_path.data_path() / "warnings"
        warning_dir.mkdir(parents=True, exist_ok=True)
        warning_file = warning_dir / f"{acause}.txt"

        with open(str(warning_file), "w") as file_obj:
            file_obj.write(warning_msg)
