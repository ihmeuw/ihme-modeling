import concurrent.futures
from loguru import logger
import pandas as pd

from core_maths.scale_split import scale
from gbd.constants import measures
from gbd.decomp_step import decomp_step_from_decomp_step_id
from gbd.estimation_years import estimation_years_from_gbd_round_id
from get_draws.sources.epi import Epi
from core_maths.interpolate import pchip_interpolate


DRAWCOLS = ["draw_%s" % d for d in range(1000)]


def get_props(args):
    """
    This pulls epilepsy proportion models, which are then used to estimate
    disability weights by location, year, age & sex

    Note: get_epi_prev() pulls in a prevalence that is different from the
    prevalence used to calculate YLDs
    """
    location, year, ages, gbd_round_id, decomp_step_id = args
    logger.info(f"getting location: {location}, year: {year}")

    def get_epi_prev(modelable_entity_id, gbd_round_id, decomp_step_id, filters):
        df = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=modelable_entity_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(decomp_step_id),
        )
        return df.content(filters=filters)

    filters = {
        "location_id": [location],
        "year_id": [year],
        "age_group_id": ages,
        "measure_id": [measures.PREVALENCE],
    }

    prev_dfs = [
        get_epi_prev(
            modelable_entity_id=me,
            gbd_round_id=gbd_round_id,
            decomp_step_id=decomp_step_id,
            filters=filters,
        )
        for me in [1951, 1952, 1953]
    ]
    prev_dfs = pd.concat(prev_dfs)

    index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
    props = scale(df=prev_dfs, value_cols=DRAWCOLS, group_cols=index_cols, scalar=1)
    props = props[index_cols + ["modelable_entity_id"] + DRAWCOLS]
    return props


def epilepsy_any(cv, location_id, n_processes):
    standard_dws = pd.read_csv("FILEPATH")
    standard_dws.rename(columns={d: d.replace("draw", "dw") for d in DRAWCOLS}, inplace=True)

    healthstates = cv.sequela_list[["modelable_entity_id", "healthstate_id"]]

    years = estimation_years_from_gbd_round_id(cv.gbd_round_id)
    ages = cv.simulation_index["age_group_id"]
    args = [(location_id, y, ages, cv.gbd_round_id, cv.decomp_step_id) for y in years]
    with concurrent.futures.ProcessPoolExecutor(n_processes) as executor:
        prop_dfs = executor.map(get_props, args)
    prop_dfs = pd.concat(prop_dfs)
    prop_dfs = prop_dfs.merge(healthstates)
    prop_dfs.rename(columns={d: d.replace("draw", "prop") for d in DRAWCOLS}, inplace=True)

    dws_to_weight = prop_dfs.merge(standard_dws, on="healthstate_id", how="left")
    dws_to_weight = dws_to_weight.join(
        pd.DataFrame(
            data=(
                dws_to_weight.filter(like="dw_").values
                * dws_to_weight.filter(like="prop_").values
            ),
            index=dws_to_weight.index,
            columns=DRAWCOLS,
        )
    )

    def combine_dws(df):
        draws_to_combine = df.filter(like="draw_")
        combined_draws = 1 - (1 - draws_to_combine).prod()
        return combined_draws

    epilepsy_id = 772
    index_cols = ["location_id", "year_id", "age_group_id", "sex_id"]
    combined_dws = dws_to_weight.groupby(index_cols).apply(combine_dws).reset_index()
    combined_dws["healthstate_id"] = epilepsy_id
    combined_dws = combined_dws[index_cols + ["healthstate_id"] + DRAWCOLS]

    year_id = list(range(min(years), max(years) + 1))
    interp = pchip_interpolate(
        df=combined_dws,
        id_cols=["location_id", "age_group_id", "sex_id", "healthstate_id"],
        value_cols=DRAWCOLS,
        time_col="year_id",
        time_vals=year_id,
    )
    combined_dws = combined_dws.append(interp)
    combined_dws = combined_dws[combined_dws.year_id.isin(year_id)]

    combined_dws.to_hdf(
        f"FILEPATH",
        "draws",
        mode="w",
        format="table",
        data_columns=["location_id", "year_id", "age_group_id", "sex_id"],
    )
