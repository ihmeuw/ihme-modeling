r"""Computes and saves YLDs using prevalence forecasts and average disability weight.

.. code:: python

    yld = prevalence * disability_weight

Parallelized by cause.
"""
from typing import Optional

from fhs_lib_data_transformation.lib import processing
from fhs_lib_file_interface.lib.query.io_helper import read_single_cause
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions, validate_versions_scenarios
from fhs_lib_file_interface.lib.xarray_wrapper import save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange


def one_cause_main(
    acause: str,
    years: YearRange,
    versions: Versions,
    gbd_round_id: int,
    draws: int,
    output_scenario: Optional[int],
    national_only: bool,
) -> None:
    """Calculate yld from prevalence and disability weight.

    Args:
        acause (str): The cause for yld is being calculated.
        years (YearRange): Forecasting time series.
        versions (Versions): A Versions object that keeps track of all the versions and their
            respective data directories.
        gbd_round_id (int): What gbd_round_id that yld, prevalence and disability weight are
            saved under
        draws (int): How many draws to save for the yld output
        output_scenario (Optional[int]): Optional output scenario ID
        national_only (bool): Whether to include subnational locations, or to include only
            nations.
    """
    # validate versions
    validate_versions_scenarios(
        versions=versions,
        output_scenario=output_scenario,
        output_epoch_stages=[("future", "yld")],
    )

    prevalence = read_single_cause(
        acause=acause,
        stage="prevalence",
        version_metadata=versions.get("future", "prevalence").default_data_source(
            gbd_round_id
        ),
    )
    cleaned_prevalence, _ = processing.clean_cause_data(
        prevalence,
        "prevalence",
        acause,
        draws,
        gbd_round_id,
        years.forecast_years,
        national_only=national_only,
    )

    disability_weight = read_single_cause(
        acause=acause,
        stage="disability_weight",
        version_metadata=versions.get("past", "disability_weight").default_data_source(
            gbd_round_id
        ),
    )
    cleaned_disability_weight, _ = processing.clean_cause_data(
        disability_weight,
        "disability_weight",
        acause,
        draws,
        gbd_round_id,
        national_only=national_only,
    )

    yld = cleaned_prevalence * cleaned_disability_weight
    yld_file = FHSFileSpec(versions.get("future", "yld"), f"{acause}.nc")
    save_xr_scenario(yld, yld_file, metric="rate", space="identity")
