from copy import deepcopy
from multiprocessing import Pool
import pandas as pd

from core_maths.scale_split import scale
from gbd.constants import measures
from gbd.decomp_step import decomp_step_from_decomp_step_id
from gbd.estimation_years import estimation_years_from_gbd_round_id
from get_draws.sources.epi import Epi
from ihme_dimensions.gbdize import GBDizeDataFrame
from core_maths.interpolate import pchip_interpolate


DRAWCOLS = ['draw_%s' % d for d in range(1000)]


def get_props(args):
    location, year, ages, gbd_round_id, decomp_step_id = args
    print(location, year)

    def get_epi_prev(modelable_entity_id, gbd_round_id, decomp_step_id,
                     filters):
        df = Epi.create_modelable_entity_draw_source(
            n_workers=1,
            modelable_entity_id=modelable_entity_id,
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(decomp_step_id))
        return df.content(filters=filters)

    filters = {
        "location_id": [location],
        "year_id": [year],
        "age_group_id": ages,
        "measure_id": [measures.PREVALENCE]}
    # modelable_entity_id 1951 - Idiopathic, seizure-free, treated epilepsy
    # modelable_entity_id 1952 - Idiopathic, less severe epilepsy
    # modelable_entity_id 1953 - Idiopathic, severe epilepsy
    prev_dfs = [
        get_epi_prev(modelable_entity_id=me,
                     gbd_round_id=gbd_round_id,
                     decomp_step_id=decomp_step_id,
                     filters=filters) for me in [1951, 1952, 1953]]
    prev_dfs = pd.concat(prev_dfs)

    # Extract proportions
    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    props = scale(df=prev_dfs, value_cols=DRAWCOLS, group_cols=index_cols,
                  scalar=1)
    props = props.groupby(['location_id', 'year_id', 'modelable_entity_id'])
    props = props.mean().reset_index()
    props = props[['location_id', 'year_id', 'modelable_entity_id'] + DRAWCOLS]
    return props


def epilepsy_any(cv):
    standard_dws = pd.read_csv(
        "FILEPATH/dw.csv")
    standard_dws.rename(
            columns={d: d.replace("draw", "dw") for d in DRAWCOLS},
            inplace=True)

    healthstates = cv.sequela_list[["modelable_entity_id", "healthstate_id"]]

    # Get country-year specific prevalences for back-calculation
    locations = cv.simulation_index['location_id']
    years = estimation_years_from_gbd_round_id(cv.gbd_round_id)
    ages = cv.simulation_index['age_group_id']
    args = [(l, y, ages, cv.gbd_round_id, cv.decomp_step_id)
            for l in locations for y in years]
    pool = Pool(20)
    prop_dfs = pool.map(get_props, args)
    pool.close()
    pool.join()
    prop_dfs = pd.concat(prop_dfs)
    prop_dfs = prop_dfs.merge(healthstates)
    prop_dfs.rename(
            columns={d: d.replace("draw", "prop") for d in DRAWCOLS},
            inplace=True)

    # Combine DWs
    dws_to_weight = prop_dfs.merge(
        standard_dws, on='healthstate_id', how='left')
    dws_to_weight = dws_to_weight.join(pd.DataFrame(
        data=(
            dws_to_weight.filter(like='dw_').values *
            dws_to_weight.filter(like='prop_').values),
        index=dws_to_weight.index,
        columns=DRAWCOLS))

    def combine_dws(df):
        draws_to_combine = df.filter(like='draw_')
        combined_draws = 1 - (1 - draws_to_combine).prod()
        return combined_draws

    # healthstate_id 772 - Epilepsy
    epilepsy_id = 772
    combined_dws = dws_to_weight.groupby(['location_id', 'year_id']).apply(
        combine_dws).reset_index()
    combined_dws['healthstate_id'] = epilepsy_id
    combined_dws = combined_dws[
        ['location_id', 'year_id', 'healthstate_id'] + DRAWCOLS]

    combined_dws.to_hdf(
        f"{cv.como_dir}/info/epilepsy_any_dws.h5",
        'draws',
        mode='w',
        format='table',
        data_columns=['location_id', 'year_id'])

    # fill in missing years
    year_id = list(range(1990,2020))
    interp = pchip_interpolate(
        df=combined_dws,
        id_cols=['location_id', 'healthstate_id'],
        value_cols=DRAWCOLS,
        time_col="year_id",
        time_vals=year_id)
    combined_dws = combined_dws.append(interp)
    combined_dws = combined_dws[combined_dws.year_id.isin(year_id)]

    combined_dws.to_hdf(
        f"{cv.como_dir}/info/epilepsy_any_dws.h5",
        'draws',
        mode='w',
        format='table',
        data_columns=['location_id', 'year_id'])
