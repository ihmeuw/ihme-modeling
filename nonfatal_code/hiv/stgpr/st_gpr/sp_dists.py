import pandas as pd
import numpy as np
from hierarchies import dbtrees


def calc_spatial_distances(location_set_version_id, o_locs):
    lt = dbtrees.loctree(location_set_version_id)
    o_locs = np.atleast_1d(o_locs).ravel()
    nlvls = lt.max_depth()

    leaf_lvls = []
    for lvl in reversed(range(nlvls+1)):
        if len(set(lt.leaves()) & set(lt.level_n_descendants(lvl))) > 0:
            leaf_lvls.append(lvl)

    lflat = lt.flatten()
    o_locdf = pd.DataFrame({'leaf_node': o_locs})
    o_locdf = o_locdf.merge(lflat, on='leaf_node')
    d_df = []
    for lvl in leaf_lvls:
        leaf_df = lflat[lflat['level_%s' % lvl].notnull()]
        lflat = lflat[lflat['level_%s' % lvl].isnull()]
        d0_locs = (
            np.atleast_2d(leaf_df['level_%s' % lvl].values).T ==
            np.atleast_2d(o_locdf['level_%s' % lvl].values)).astype(int)
        d1_locs = (
            np.atleast_2d(leaf_df['level_%s' % (lvl-1)].values).T ==
            np.atleast_2d(o_locdf['level_%s' % (lvl-1)].values)).astype(int)
        d2_locs = (
            np.atleast_2d(leaf_df['level_%s' % (lvl-2)].values).T ==
            np.atleast_2d(o_locdf['level_%s' % (lvl-2)].values)).astype(int)

        d_locs = d0_locs+d1_locs+d2_locs
        d_df.append(pd.DataFrame(
                d_locs.T, columns=leaf_df['level_%s' % lvl].values,
                index=o_locs))
    d_df = pd.concat(d_df, axis=1)
    d_df = d_df.fillna(0)
    return d_df
