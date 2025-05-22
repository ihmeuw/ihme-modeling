import pandas as pd

from core_maths.interpolate import pchip_interpolate
from gbd.estimation_years import estimation_years_from_release_id
from hierarchies import dbtrees

from como.lib.fileshare_inputs import get_urolithiasis_disability_weights
from como.lib.utils import ordered_draw_columns


def to_como(como_dir: str, location_set_id: int, release_id: int) -> None:
    """Writes to disk a draw file with demographics:
    ("location_id", "healthstate_id" (all are 822), "year_id")
    """
    df = get_urolithiasis_disability_weights().copy(deep=True)

    # fill for new locs
    lt = dbtrees.loctree(location_set_id=location_set_id, release_id=release_id)
    locmap = lt.flatten()
    num_levels = len(locmap.filter(like="level").columns)
    reg_avgs = df.merge(
        locmap[["leaf_node", "level_2"]], left_on="location_id", right_on="leaf_node"
    )
    reg_avgs = reg_avgs[
        ["level_2", "year_id", "healthstate_id"] + ordered_draw_columns(reg_avgs)
    ]
    reg_avgs = reg_avgs.groupby(["level_2", "year_id"])
    reg_avgs = reg_avgs.mean().reset_index()
    reg_avgs.rename(columns={"level_2": "location_id"}, inplace=True)
    df = pd.concat([df, reg_avgs])

    filllen = 0
    for ln in list(locmap.leaf_node.unique()):
        if ln not in list(df.location_id):
            for i in reversed(range(num_levels)):
                fill_loc = locmap.loc[locmap.leaf_node == ln, "level_%s" % i].squeeze()
                filldf = df[df.location_id == fill_loc]
                if len(filldf) > 0:
                    filldf["location_id"] = ln
                    df = pd.concat([df, filldf])
                    filllen = filllen + 1
                    break
    df = df[df.location_id.isin([leaf.id for leaf in lt.leaves()])]

    # fill new years
    est_years = estimation_years_from_release_id(release_id)
    annual_years = list(range(min(est_years), max(est_years) + 1))
    extra = df.query("year_id == 2013")
    extra["year_id"] = max(annual_years)
    df = pd.concat([df, extra])
    df = df.filter(regex="(.*_id|draw_)")

    # fill in missing years
    interp = pchip_interpolate(
        df=df,
        id_cols=["location_id", "healthstate_id"],
        value_cols=["draw_%s" % d for d in range(1000)],
        time_col="year_id",
        time_vals=annual_years,
    )
    df = pd.concat([df, interp])
    df = df[df.year_id.isin(annual_years)]

    # save for como run
    df.to_hdf(
        "FILEPATH",
        "draws",
        mode="w",
        format="table",
        data_columns=["location_id", "year_id"],
    )
