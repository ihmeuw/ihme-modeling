import pandas as pd
from hierarchies import dbtrees
from db_tools import ezfuncs


def to_como(como_dir):
    df = pd.read_csv(
        "filepath/03_custom/"
        "urolith_symp_dws.csv")

    iso3map = ezfuncs.query("""
        SELECT location_id, ihme_loc_id as iso3
        FROM shared.location_hierarchy_history
        WHERE location_set_version_id = 4""", conn_def="shared")
    df = df.rename(columns={'year': 'year_id'})

    df = df.merge(iso3map, on='iso3')
    lt = dbtrees.loctree(None, 35)
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
                fill_loc = locmap.ix[
                    locmap.leaf_node == ln, 'level_%s' % i].squeeze()
                filldf = df[df.location_id == fill_loc]
                if len(filldf) > 0:
                    filldf['location_id'] = ln
                    df = df.append(filldf)
                    filllen = filllen + 1
                    break
    df = df[df.location_id.isin([l.id for l in lt.leaves()])]
    df['year_id'] = df.year_id.replace({2013: 2016})
    df.rename(columns={
        'draw%s' % d: 'draw_%s' % d for d in range(1000)}, inplace=True)
    df = df.filter(regex='(.*_id|draw_)')
    df.to_hdf(
        "{}/info/urolith_dws.h5".format(como_dir),
        'draws',
        mode='w',
        format='table',
        data_columns=['location_id', 'year_id'])
