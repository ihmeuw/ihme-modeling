r"""Compute HALE (Health-Adjusted Life Expectancy) from YLD rate and Life Table.

**NOTE:** The input is all causes YLD rate and Life Table. HALE will be
saved in the hale stage directory, and the filename will be `hale.nc`.

.. math::
    \text{adjusted_nLx} = \text{nLx} * \text{100000} * (1-YLD_{\text{rate}})

.. math::
    \text{adjusted_Tx} = \sum\limits_{a=x}^{\text{max-age}} \text{adjusted_nLa}

.. math::
    \text{hale} = \text{adjusted_Tx} / \text{lx}

Example calculate-hale call:

.. code:: bash

    fhs_pipeline_hale_console calculate-hale \
    --versions FILEPATH1 \
    -v FILEPATH2 \
    -v FILEPATH3 \
    --draws 500 \
    --gbd-round-id 89 \
    --years 1776:1812:1944

Example parallelize call:

.. code:: bash

    fhs_pipeline_hale_console parallelize \
    --versions FILEPATH1 \
    -v FILEPATH2 \
    -v FILEPATH3 \
    -v FILEPATH4 \
    --draws 500 \
    --gbd-round-id 89 \
    --years 1776:1812:1944

"""  # noqa: E501
from typing import List, Tuple

import numpy as np
import xarray as xr
from fhs_lib_data_transformation.lib.resample import resample
from fhs_lib_database_interface.lib.constants import DimensionConstants
from fhs_lib_database_interface.lib.query.age import get_ages
from fhs_lib_file_interface.lib.version_metadata import FHSFileSpec
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_file_interface.lib.xarray_wrapper import open_xr_scenario, save_xr_scenario
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()

LIFE_TABLE_NLX_FACTOR = 1e5


def calculate_hale_main(
    draws: int,
    gbd_round_id: int,
    past_or_future: str,
    run_on_means: bool,
    versions: Versions,
    year_set: List[int],
    include_nLx: bool = False,
) -> None:
    """Compute hale from yld and life table.

    Args:
        draws (int): the number of draws to forecast on
        gbd_round_id (int): What gbd_round_id that yld, life table and hale are saved under.
        past_or_future (str): whether we'll be reading data from the past or future.
        run_on_means (bool): whether to run data on means instead of draws
        versions (Versions): A Versions object that keeps track of all the versions and their
            respective data directories.
        year_set (List[int]): the years we care about in this task.
        include_nLx (bool): Defaults to ``False``. Flag to calculate and save adjusted nLx.
            If ``True``, will save ``adjusted_nLx`` data.
    """
    yld, nLx, lx = _load_data(
        draws, gbd_round_id, past_or_future, run_on_means, versions, year_set
    )

    # Compute adjusted_nLx
    adjusted_nLx = (nLx * (1 - yld)) * LIFE_TABLE_NLX_FACTOR
    adjusted_nLx.name = "adjusted_nLx"

    # Compute adjusted_Tx
    adjusted_Tx = _compute_adjusted_Tx(adjusted_nLx, gbd_round_id)

    # Compute hale
    hale = adjusted_Tx / lx
    hale.name = "hale"

    # Save out hale
    hale_version_metadata = versions.get(past_or_future, "hale").default_data_source(
        gbd_round_id
    )
    save_xr_scenario(
        xr_obj=hale,
        file_spec=FHSFileSpec(version_metadata=hale_version_metadata, filename="hale.nc"),
        metric="number",
        space="identity",
    )
    if include_nLx:
        save_xr_scenario(
            xr_obj=adjusted_nLx,
            file_spec=FHSFileSpec(
                version_metadata=hale_version_metadata,
                filename="adjusted_nLx.nc",
            ),
            metric="rate",
            space="identity",
        )


def _load_data(
    draws: int,
    gbd_round_id: int,
    past_or_future: str,
    run_on_means: bool,
    versions: Versions,
    year_set: List[int],
) -> Tuple[xr.DataArray, xr.DataArray, xr.DataArray]:
    """Load ``yld``, ``nLx``, and ``lx`` data; resampling or aggregating as required."""
    # Setup input data dirs
    life_expectancy_version_metadata = versions.get(
        past_or_future, "life_expectancy"
    ).default_data_source(gbd_round_id)
    yld_version_metadata = versions.get(past_or_future, "yld").default_data_source(
        gbd_round_id
    )

    # Read life table data
    if (life_expectancy_version_metadata.data_path("lifetable_ds_agg.nc")).exists():
        life_table_file = "lifetable_ds_agg.nc"
    else:
        life_table_file = "lifetable_ds.nc"

    life_table = open_xr_scenario(
        file_spec=FHSFileSpec(
            version_metadata=life_expectancy_version_metadata, filename=life_table_file
        )
    )

    nLx: xr.DataArray = life_table["nLx"]
    lx: xr.DataArray = life_table["lx"]

    # Read in YLD data: either from a summary file or from a file containing draws
    if run_on_means:
        logger.info("Reading data without draws")
        yld = open_xr_scenario(
            file_spec=FHSFileSpec(
                version_metadata=yld_version_metadata,
                sub_path=("summary_agg"),
                filename="summary.nc",
            )
        )
        yld = yld.sel(acause="_all", statistic="mean", drop=True)

        # Take mean over draws if they're present
        if DimensionConstants.DRAW in life_table.dims:
            nLx = nLx.mean(DimensionConstants.DRAW)
            lx = lx.mean(DimensionConstants.DRAW)

    else:
        logger.info("Reading data with draws")
        yld = open_xr_scenario(
            file_spec=FHSFileSpec(version_metadata=yld_version_metadata, filename="_all.nc")
        )

        if isinstance(yld, xr.Dataset):
            yld = yld["value"]
        yld = resample(data=yld, num_of_draws=draws)

        if "acause" in yld.dims:
            yld = yld.sel(acause="_all", drop=True)

        nLx = resample(data=nLx, num_of_draws=draws)
        lx = resample(data=lx, num_of_draws=draws)

    # Subset to just the years we're running on
    logger.info("Subsetting yld, nLx, and lx onto relevant years")
    year_set_dict = {DimensionConstants.YEAR_ID: year_set}
    yld = yld.sel(**year_set_dict)
    nLx = nLx.sel(**year_set_dict)
    lx = lx.sel(**year_set_dict)

    return yld, nLx, lx


def _compute_adjusted_Tx(adjusted_nLx: xr.DataArray, gbd_round_id: int) -> xr.DataArray:
    """Compute the adjusted_Tx by age group."""
    # Create age_group_id, age_group_years_start mapping
    age_df = get_ages(gbd_round_id=gbd_round_id)[["age_group_id", "age_group_years_start"]]
    age_dict = age_df.set_index("age_group_id")["age_group_years_start"].to_dict()

    # Compute adjusted_Tx
    adjusted_Tx = adjusted_nLx.copy(deep=True)
    for age_group_id in adjusted_Tx.age_group_id.data:
        logger.debug(f"Computing adjusted_Tx for age group {age_group_id}")
        age_group_years_start = age_dict[age_group_id]  # noqa: F841
        older_age_group_ids = np.intersect1d(
            age_df.query("age_group_years_start >= @age_group_years_start")[
                "age_group_id"
            ].unique(),
            adjusted_Tx.age_group_id.data,
        )
        adjusted_Tx.loc[dict(age_group_id=age_group_id)] = adjusted_nLx.sel(
            age_group_id=older_age_group_ids
        ).sum("age_group_id")

    return adjusted_Tx


def determine_year_set(years: YearRange, past: str) -> List[int]:
    """Determine which years we care about based on the value of ``past``."""
    if past == "include":
        year_set = list(years.years)
    elif past == "only":
        year_set = list(years.past_years)
    else:
        year_set = list(years.forecast_years)

    return year_set


def determine_past_or_future(years: YearRange, year_set: List[int]) -> str:
    """Determine whether the current ``year_set`` represents past or future data."""
    if set(year_set).issubset(years.past_years):
        return "past"
    return "future"