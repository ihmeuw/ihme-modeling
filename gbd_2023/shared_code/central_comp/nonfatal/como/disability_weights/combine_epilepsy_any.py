from typing import List

import pandas as pd
from loguru import logger

from core_maths.interpolate import pchip_interpolate
from core_maths.scale_split import scale
from gbd.constants import measures
from gbd.estimation_years import estimation_years_from_release_id
from get_draws.api import internal_get_draws

from como.lib import constants
from como.lib.comorbidity_calculation import comorbidity_row_combine
from como.lib.fileshare_inputs import get_standard_disability_weights
from como.lib.utils import ordered_draw_columns
from como.lib.version import ComoVersion

PROPORTION_GROUP_COLS = ["location_id", "year_id", "age_group_id", "sex_id"]
PROPORTION_SCALAR = 1.0


def get_proportion_models(
    cv: ComoVersion,
    year_id: List[int],
    location_id: int,
    ages: List[int],
    release_id: int,
    num_workers: int,
) -> pd.DataFrame:
    """Pulls epilepsy proportion models, which are then used to estimate disability
    weights by location, year, age & sex.
    """
    logger.info(f"getting location: {location_id}, year: {year_id}")
    cache_dir = "FILEPATH"

    prev_dfs = internal_get_draws(
        gbd_id_type="modelable_entity_id",
        gbd_id=list(constants.EPILEPSY_PREVALENCE_MODELABLE_ENTITIES.values()),
        source="epi",
        measure_id=measures.PREVALENCE,
        location_id=location_id,
        year_id=year_id,
        age_group_id=ages,
        release_id=release_id,
        num_workers=num_workers,
        diff_cache_dir=cache_dir,
    )

    # Extract proportions
    props = scale(
        df=prev_dfs,
        value_cols=ordered_draw_columns(prev_dfs),
        group_cols=PROPORTION_GROUP_COLS,
        scalar=PROPORTION_SCALAR,
    )
    props = props[
        PROPORTION_GROUP_COLS + ["modelable_entity_id"] + ordered_draw_columns(props)
    ]
    return props


def epilepsy_any(cv: ComoVersion, location_id: int, n_processes: int) -> None:
    """Combines diability weights and prevalence for 3 modelable entities into a single
    epilepsy disability weight.

    Parameters
    ----------
    cv: ComoVersion
        a file-system backed tree of quantities drawn from different databases
    location_id: int
        the location_id for this query
    n_processes: int
        the number of threads to use when collecting the prevalence draws

    Returns
    -------
    None

    Notes
    -----
    Outputs a DataFrame to disk at
    FILEPATH
    """
    # this file has been constant for many GBDs
    standard_dws = get_standard_disability_weights().copy(deep=True)
    standard_dws.rename(
        columns={d: d.replace("draw", "dw") for d in ordered_draw_columns(standard_dws)},
        inplace=True,
    )

    healthstates = cv.sequela_list[["modelable_entity_id", "healthstate_id"]]

    # Get a,s,y,l specific prevalences for back-calculation
    years = estimation_years_from_release_id(cv.release_id)
    ages = cv.simulation_index["age_group_id"]
    prop_dfs = get_proportion_models(
        cv,
        location_id=location_id,
        year_id=years,
        ages=ages,
        release_id=cv.release_id,
        num_workers=n_processes,
    )
    prop_dfs = prop_dfs.merge(healthstates)
    draw_columns = ordered_draw_columns(prop_dfs)
    prop_dfs.rename(
        columns={d: d.replace("draw", "prop") for d in draw_columns}, inplace=True
    )

    # Combine DWs
    dws_to_weight = prop_dfs.merge(standard_dws, on="healthstate_id", how="left")
    dws_to_weight = dws_to_weight.join(
        pd.DataFrame(
            data=(
                dws_to_weight.filter(like="dw_").values
                * dws_to_weight.filter(like="prop_").values
            ),
            index=dws_to_weight.index,
            columns=draw_columns,
        )
    )

    index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
    combined_dws = (
        dws_to_weight.groupby(index_cols).apply(comorbidity_row_combine).reset_index()
    )
    combined_dws["healthstate_id"] = constants.HEALTHSTATES["epilepsy_any"]
    combined_dws = combined_dws[
        index_cols + ["healthstate_id"] + ordered_draw_columns(combined_dws)
    ]

    # fill in missing years
    year_id = list(range(min(years), max(years) + 1))
    interp = pchip_interpolate(
        df=combined_dws,
        id_cols=["location_id", "age_group_id", "sex_id", "healthstate_id"],
        value_cols=ordered_draw_columns(combined_dws),
        time_col="year_id",
        time_vals=year_id,
    )
    combined_dws = pd.concat([combined_dws, interp])
    combined_dws = combined_dws[combined_dws.year_id.isin(year_id)]

    # export data
    combined_dws.to_hdf(
        "FILEPATH",
        "draws",
        mode="w",
        format="table",
        data_columns=["location_id", "year_id", "age_group_id", "sex_id"],
    )
