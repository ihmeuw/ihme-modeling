r"""Calculates YLLs or YLL rates.

Calculates YLLs or YLL rates from reference life expectancy, deaths, and
mean age of death (:math:`a_x`)

If death counts are given then this pipeline calculates YLLs. However, if
death rates (mortality rates--deaths divided by mid-year-population) are given,
then this function calculates YLL rates.

YLL calculation is

.. math::

    \mbox{YLL}_{las} = m_{las} * \bar{e}_{a}

where

#. :math:`{YLL}_{las}` is location, age-group, sex specific YLLs (or YLL rates)
#. :math:`m_{las}` is location, age-group, sex specific deaths (or rates)
#. :math:`\bar{e}_{a}` is reference life expectancy (at life starting year)

:math:`\bar{e}_{a}` is described in `fhs_pipeline_yll.lib.ylls.calculate_ylls`.

Example call:

.. code-block:: bash

    fhs_pipeline_yll_console \
        --gbd-round-id 5 \
        --version \
            -v FILEPATH \
        --draws 1000 \
        --years 1990:2018:2100 \
        --past include \
        parallelize_by_cause

"""
from typing import List

from fhs_lib_data_transformation.lib.dimension_transformation import drop_single_point
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.query import reference_lex as ref_lex_module
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_yll.lib import yll as yll_module

logger = fhs_logging.get_logger()


def one_cause_main(
    acause: str,
    draws: int,
    gbd_round_id: int,
    epoch: str,
    versions: Versions,
    years: YearRange,
) -> None:
    """Compute YLL (rate) at cause level.

    Args:
        acause (str): Acause to run over
        draws (int): How many draws to save for the daly output.
        gbd_round_id (int):  What gbd_round_id that yld, yll and daly are saved under.
        epoch (str): Which epoch to calculate YLLs for, either past or future
        versions (Versions): A Versions object that keeps track of all the versions and their
            respective data directories.
        years (YearRange): Forecasting year range
    """
    years_to_compute = determine_years_to_compute(years=years, epoch=epoch)

    death_file = FHSFileSpec(
        versions.get(past_or_future=epoch, stage="death"),
        f"{acause}.nc",
    )
    mx = resample(open_xr_scenario(death_file).sel(year_id=years_to_compute), draws)
    mx = drop_single_point(mx, "acause")

    lifetable_file = FHSFileSpec(
        versions.get(past_or_future=epoch, stage="life_expectancy"),
        "lifetable_ds.nc",
    )
    ax = resample(open_xr_scenario(lifetable_file).ax.sel(year_id=years_to_compute), draws)
    ax = drop_single_point(ax, "acause")

    reference_lex = ref_lex_module.get_reference_lex(gbd_round_id)  # calls db

    yll = yll_module.calculate_ylls(mx, ax, reference_lex, gbd_round_id)

    yll_slice_file = FHSFileSpec(
        versions.get(past_or_future=epoch, stage="yll"),
        filename=f"{acause}.nc",
    )

    save_xr_scenario(
        yll,
        yll_slice_file,
        metric="rate",
        space="identity",
        death=str(death_file.data_path()),
        ax=str(lifetable_file.data_path()),
    )

    logger.info(f"Leaving `one_cause_yll` function for {acause}. DONE")


def determine_years_to_compute(years: YearRange, epoch: str) -> List[int]:
    """Return the list of years to compute, based on the ``epoch`` specification."""
    if epoch == "future":
        years_in_slice = years.forecast_years
    elif epoch == "past":
        years_in_slice = years.past_years
    else:
        raise RuntimeError("epoch must be `past` or `future`")

    return list(years_in_slice)