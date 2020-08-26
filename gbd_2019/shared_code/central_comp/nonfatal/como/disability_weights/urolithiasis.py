import pandas as pd

from core_maths.interpolate import pchip_interpolate
from gbd.gbd_round import gbd_round_from_gbd_round_id
from hierarchies import dbtrees


def to_como(como_dir, location_set_id, gbd_round_id):
    df = pd.read_csv(
        "FILEPATH/urolith_symp_dws.csv")

    # fill for new locs
    lt = dbtrees.loctree(location_set_id=location_set_id,
                         gbd_round_id=gbd_round_id)
    locmap = lt.flatten()
    reg_avgs = df.merge(
        locmap[['leaf_node', 'level_2']],
        left_on='location_id', right_on='leaf_node')
    reg_avgs = reg_avgs[
        ['level_2', 'year_id', 'healthstate_id'] +
        list(df.filter(like='draw').columns)]
    reg_avgs = reg_avgs.groupby(['level_2', 'year_id'])
    reg_avgs = reg_avgs.mean().reset_index()
    reg_avgs.rename(columns={'level_2': 'location_id'}, inplace=True)
    df = df.append(reg_avgs)

    filllen = 0
    for ln in list(locmap.leaf_node.unique()):
        if ln not in list(df.location_id):
            for i in reversed(range(6)):
                fill_loc = locmap.loc[
                    locmap.leaf_node == ln, 'level_%s' % i].squeeze()
                filldf = df[df.location_id == fill_loc]
                if len(filldf) > 0:
                    filldf['location_id'] = ln
                    df = df.append(filldf)
                    filllen = filllen + 1
                    break
    df = df[df.location_id.isin([l.id for l in lt.leaves()])]

    # fill in missing years
    extra = df.query("year_id == 2013")
    extra['year_id'] = 2019
    df = df.append(extra)
    df = df.filter(regex='(.*_id|draw_)')
    interp = pchip_interpolate(
        df=df,
        id_cols=['location_id', 'healthstate_id'],
        value_cols=['draw_%s' % d for d in range(1000)],
        time_col="year_id",
        time_vals=list(range(1990, 2020)))
    df = df.append(interp)
    df = df[df.year_id.isin(list(range(1990, 2020)))]

    # save for como run
    df.to_hdf(
        f"{como_dir}/info/urolith_dws.h5",
        'draws',
        mode='w',
        format='table',
        data_columns=['location_id', 'year_id'])
