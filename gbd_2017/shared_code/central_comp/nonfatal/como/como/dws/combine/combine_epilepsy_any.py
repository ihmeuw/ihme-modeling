from copy import deepcopy

import pandas as pd
from hierarchies import dbtrees
from core_maths.scale_split import scale
from get_draws.sources.epi import Epi
from multiprocessing import Pool
from ihme_dimensions.gbdize import GBDizeDataFrame


drawcols = ['draw_%s' % d for d in range(1000)]

me_1951 = Epi.create_modelable_entity_draw_source(
    n_workers=1,
    modelable_entity_id=1951, gbd_round_id=5)
me_1952 = Epi.create_modelable_entity_draw_source(
    n_workers=1,
    modelable_entity_id=1952, gbd_round_id=5)
me_1953 = Epi.create_modelable_entity_draw_source(
    n_workers=1,
    modelable_entity_id=1953, gbd_round_id=5)


def get_props(args):
    location, year = args
    print(location, year)
    filters = {
        "location_id": [location],
        "year_id": [year],
        "age_group_id": [
            2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 30, 31, 32, 235],
        "measure_id": [5]}
    me_1951_df = me_1951.content(filters=filters)
    me_1952_df = me_1952.content(filters=filters)
    me_1953_df = me_1953.content(filters=filters)
    prev_dfs = pd.concat([me_1951_df, me_1952_df, me_1953_df])

    # Extract proportions
    index_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
    props = scale(
        prev_dfs,
        drawcols,
        index_cols,
        scalar=1)
    props = props.groupby(
        ['location_id', 'year_id', 'modelable_entity_id'])
    props = props.mean().reset_index()
    props = props[
        ['location_id', 'year_id', 'modelable_entity_id'] + drawcols]
    return props


def epilepsy_any(cv):
    standard_dws = pd.read_csv(
        "FILEPATH/dw.csv")
    healthstates = cv.sequela_list[["modelable_entity_id", "healthstate_id"]]

    # Get country-year specific prevalences for back-calculation
    lt = dbtrees.loctree(location_set_id=35, gbd_round_id=5)
    locations = [l.id for l in lt.leaves()]
    years = [1990, 1995, 2000, 2005, 2010, 2017]
    prop_dfs = []
    args = [(l, y) for l in locations for y in years]

    pool = Pool(20)
    prop_dfs = pool.map(get_props, args)
    pool.close()
    pool.join()
    prop_dfs = pd.concat(prop_dfs)
    prop_dfs = prop_dfs.merge(healthstates)
    renames = {'draw_%s' % i: 'dw_prop_%s' % i for i in range(1000)}
    prop_dfs.rename(columns=renames, inplace=True)

    # Combine DWs
    dws_to_weight = prop_dfs.merge(
        standard_dws, on='healthstate_id', how='left')
    dws_to_weight = dws_to_weight.join(pd.DataFrame(
        data=(
            dws_to_weight.filter(like='draw').values *
            dws_to_weight.filter(like='dw_prop_').values),
        index=dws_to_weight.index,
        columns=drawcols))

    def combine_dws(df):
        draws_to_combine = df.filter(like='draw')
        combined_draws = 1 - (1 - draws_to_combine).prod()
        return combined_draws

    combined_dws = dws_to_weight.groupby(['location_id', 'year_id']).apply(
        combine_dws).reset_index()
    combined_dws['healthstate_id'] = 772

    col_order = ['location_id', 'year_id', 'healthstate_id'] + drawcols
    combined_dws = combined_dws[col_order]

    # fill in missing years
    dims = deepcopy(cv.nonfatal_dimensions.get_simulation_dimensions(3))
    dims.index_dim.add_level('healthstate_id', 772)
    dims.index_dim.replace_level("year_id", list(range(1990, 2018)))
    for index_dim in dims.index_groups:
        if index_dim not in ['location_id', 'year_id', 'healthstate_id']:
            dims.index_dim.drop_level(index_dim)
    gbdizer = GBDizeDataFrame(dims)
    combined_dws = gbdizer.fill_year_by_interpolating(
        df=combined_dws,
        rank_df=combined_dws[combined_dws.year_id == 2005].reset_index())

    combined_dws.to_hdf(
        "{}/info/epilepsy_any_dws.h5".format(cv.como_dir),
        'draws',
        mode='w',
        format='table',
        data_columns=['location_id', 'year_id'])
