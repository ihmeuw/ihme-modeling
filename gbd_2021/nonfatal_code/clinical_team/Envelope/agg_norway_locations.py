"""
Aggregate STGPR draws for GBD2019
"""
import pandas as pd
import glob
import os
import db_queries


def get_norway_locs():
    """Get the data from the ST GPR model and assign the new location id"""

    # raw env data
    files = glob.glob("FILEPATH")

    norway_dict = {
        60133: [4918, 4919],
        60135: [4912, 4913],
        60137: [4928, 4927],
        60134: [4917, 4916],
        60132: [4921, 4922],
        60136: [4911, 4915, 4914],
    }

    nor_agg_locs = [item for sublist in norway_dict.values() for item in sublist]
    nor_paths = [
        f for f in files if os.path.basename(f)[:-4] in [str(n) for n in nor_agg_locs]
    ]

    if len(nor_agg_locs) != len(nor_paths):
        raise ValueError("something went wrong")

    df = pd.concat([pd.read_csv(f) for f in nor_paths], sort=False, ignore_index=True)
    df["new_location_id"] = -1

    for new_loc, old_loc_list in norway_dict.items():
        for old_loc in old_loc_list:
            df.loc[df["location_id"] == old_loc, "new_location_id"] = new_loc
    if (df["new_location_id"] < 0).any():
        raise ValueError("There should not be new locations less than 0")
    return df


def agg_norway_locs():
    """Multiply env rate by population, sum draws and pop over new locations
    and then go back to rate space"""

    df = get_norway_locs()
    est_cols = df.filter(regex="draw_").columns.tolist()

    groups = df.drop(est_cols + ["location_id"], axis=1).columns.tolist()
    pop = db_queries.get_population(
        gbd_round_id=6,
        decomp_step="iterative",
        age_group_id=df["age_group_id"].unique().tolist(),
        sex_id=[1, 2],
        year_id=df["year_id"].unique().tolist(),
        location_id=df["location_id"].unique().tolist(),
    )
    pop.drop("run_id", axis=1, inplace=True)
    df = df.merge(
        pop,
        how="left",
        on=pop.drop("population", axis=1).columns.tolist(),
        validate="1:1",
    )
    # get to count space
    for col in est_cols:
        df[col] = df[col] * df["population"]
    # sum admission draw counts and population
    sum_dict = dict(zip(est_cols + ["population"], ["sum"] * (len(est_cols) + 1)))
    df = df.groupby(groups).agg(sum_dict).reset_index()
    # back to rates with aggregated admit counts
    for col in est_cols:
        df[col] = df[col] / df["population"]
    df.drop("population", axis=1, inplace=True)

    # rename new loc to just loc id
    df.rename(columns={"new_location_id": "location_id"}, inplace=True)

    return df


def write_by_loc(df, write_dir):
    """write CSVs by location id to match inputs"""
    for loc in df.location_id.unique():
        tmp = df[df.location_id == loc]
        write_path = f"{write_dir}/{loc}.csv"
        tmp.to_csv(write_path, index=False)

    return


if __name__ == "__main__":
    df = agg_norway_locs()
    write_by_loc(df, "FILEPATH")
