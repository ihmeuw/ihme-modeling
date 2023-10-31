import concurrent.futures
from loguru import logger
import os
import pandas as pd

DRAWCOLS = ["draw_%s" % i for i in range(1000)]
THIS_PATH = os.path.dirname(__file__)


def combine_dws(row):
    location_id = int(row["location_id"])
    year_id = int(row["year_id"])
    age_group_id = int(row["age_group_id"])
    sex_id = int(row["sex_id"])
    logger.info(
        f"getting location: {location_id}, year: {year_id}, age_group: {age_group_id}, sex: {sex_id} "
    )

    all_combined = []
    for healthstate_id, rows in combos.groupby("healthstate_id"):
        healthstates_to_combine = rows["healthstates_to_combine"]

        draws_to_combine = standard_dws[
            standard_dws.healthstate.isin(healthstates_to_combine)
        ]

        draws_to_combine = draws_to_combine.append(pd.DataFrame([row]), sort=True)

        combined = 1 - (1 - draws_to_combine.filter(like="draw").values).prod(axis=0)
        combined = pd.DataFrame(data=[combined], columns=DRAWCOLS)

        combined = combined.join(
            pd.DataFrame(
                data=[
                    {
                        "location_id": location_id,
                        "year_id": year_id,
                        "age_group_id": age_group_id,
                        "sex_id": sex_id,
                        "healthstate_id": healthstate_id,
                    }
                ]
            )
        )
        all_combined.append(combined)
    all_combined = pd.concat(all_combined)
    return all_combined


def epilepsy_combos(como_dir, location_id, n_processes):
    global standard_dws, epilepsy_dws, combos
    standard_dws = pd.read_csv("FILEPATH")

    epilepsy_dws = pd.read_hdf(
        f"FILEPATH"
    )

    combos = pd.read_excel("FILEPATH" % THIS_PATH)
    combos = combos[combos.healthstates_to_combine != "epilepsy_any"]

    rowlist = [row for i, row in epilepsy_dws.iterrows()]

    with concurrent.futures.ProcessPoolExecutor(n_processes) as executor:
        all_combined = executor.map(combine_dws, rowlist)
    all_combined = pd.concat(all_combined)

    col_order = [
        "location_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "healthstate_id",
    ] + DRAWCOLS
    all_combined = all_combined[col_order]
    all_combined.to_hdf(
        f"FILEPATH",
        "draws",
        mode="w",
        format="table",
        data_columns=["location_id", "year_id", "age_group_id", "sex_id"],
    )
