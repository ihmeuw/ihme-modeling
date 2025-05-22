import os
import pathlib
from typing import Dict, Final, List, Optional

import pandas as pd

from chronos.lib import expansion
from db_queries.api import internal as db_queries_internal
from gbd import constants as gbd_constants
from get_draws.api import internal_get_draws

from epic.legacy.util import constants
from epic.lib.util import common

_THIS_DIR = pathlib.Path(__file__).resolve().parent

_SHIFT_PROPORTION: Final[float] = 0.9
_DEMO_INDEX = [
    gbd_constants.columns.AGE_GROUP_ID,
    gbd_constants.columns.LOCATION_ID,
    gbd_constants.columns.SEX_ID,
    gbd_constants.columns.YEAR_ID,
]
_ANEMIA_CAUSE_INDEX = ["anemia_cause"] + _DEMO_INDEX


def pull_input_prevalence(
    anemia_map: pd.DataFrame,
    output_dir: str,
    location_id: int,
    release_id: int,
    demographics: Dict[str, List[int]],
    demographic_cols: List[str],
    n_draws: int,
) -> pd.DataFrame:
    """Pull input prevalence by anemia_cause"""
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    anemia_cause_metadata = anemia_map[
        ["anemia_cause", "input_modelable_entity_id", "input_measure_id", "iron_responsive"]
    ]
    prevalence_dfs: List[pd.DataFrame] = []
    for row in anemia_cause_metadata.itertuples():
        anemia_cause_data = internal_get_draws(
            gbd_id_type="modelable_entity_id",
            gbd_id=int(row.input_modelable_entity_id),
            source="epi",
            location_id=location_id,
            sex_id=demographics["sex_id"],
            age_group_id=demographics["age_group_id"],
            measure_id=row.input_measure_id,
            release_id=release_id,
            n_draws=n_draws,
            downsample=True,
            diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
        )
        for demo in [gbd_constants.columns.SEX_ID, gbd_constants.columns.AGE_GROUP_ID]:
            demographics_to_append = list(
                set(demographics[demo]) - set(anemia_cause_data[demo])
            )
            if demographics_to_append:
                anemia_cause_data = db_queries_internal.append_demographics(
                    df=anemia_cause_data,
                    demographic=demo,
                    demographic_values=demographics_to_append,
                    demographic_columns=demographic_cols,
                )
        anemia_cause_data["anemia_cause"] = row.anemia_cause
        anemia_cause_data["iron_responsive"] = row.iron_responsive
        anemia_cause_data = anemia_cause_data[
            _ANEMIA_CAUSE_INDEX + ["iron_responsive"] + draw_cols
        ]
        prevalence_dfs.append(anemia_cause_data)
    prevalence = pd.concat(prevalence_dfs)
    prevalence = common.make_long(
        df=prevalence, new_col="prevalence", id_cols=_ANEMIA_CAUSE_INDEX + ["iron_responsive"]
    )
    prevalence["prevalence"] = prevalence["prevalence"].fillna(0)
    return prevalence


def pull_hemo_shifts(
    anemia_map: pd.DataFrame,
    output_dir: str,
    location_id: int,
    release_id: int,
    demographics: Dict[str, List[int]],
    demographic_cols: List[str],
    n_draws: int,
) -> pd.DataFrame:
    """Reads and returns hemoglobin shifts by anemia_cause."""
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    hemo_shifts: List[pd.DataFrame] = []
    for row in anemia_map.itertuples():
        if row.population_group_id != gbd_constants.population_group.ALL_POPULATION:
            continue
        hemo_shift = internal_get_draws(
            gbd_id_type="modelable_entity_id",
            gbd_id=row.shift_modelable_entity_id,
            source="epi",
            location_id=location_id,
            sex_id=demographics[gbd_constants.columns.SEX_ID],
            age_group_id=demographics[gbd_constants.columns.AGE_GROUP_ID],
            release_id=release_id,
            n_draws=n_draws,
            downsample=True,
            diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
        )
        hemo_shift["anemia_cause"] = row.anemia_cause
        hemo_shift = hemo_shift[["anemia_cause"] + demographic_cols + draw_cols]
        hemo_shifts.append(hemo_shift)
    hemo_shifts: pd.DataFrame = pd.concat(hemo_shifts)
    hemo_shifts = common.make_long(
        df=hemo_shifts, new_col="hemo_shift", id_cols=["anemia_cause"] + demographic_cols
    )
    # Hemo shifts are negative for decreases, but we want them positive for counterfactual
    hemo_shifts["hemo_shift"] = -1 * hemo_shifts["hemo_shift"]
    # Clip any protective shifts to 0 for now
    hemo_shifts["hemo_shift"] = hemo_shifts["hemo_shift"].clip(lower=0)
    return hemo_shifts


def main(
    location_id: int, release_id: int, output_dir: str, n_draws: int, in_mem: bool = False
) -> Optional[pd.DataFrame]:
    """Calculates and saves TMREL by location."""
    draw_cols = [f"draw_{i}" for i in range(n_draws)]
    demographics = common.get_demographics(path=output_dir)

    # Pulling exposure, but used to calculate TMREL
    tmrel = internal_get_draws(
        gbd_id_type="modelable_entity_id",
        gbd_id=constants.MEIDS.MEAN_HEMO,
        source="epi",
        measure_id=gbd_constants.measures.CONTINUOUS,
        location_id=location_id,
        release_id=release_id,
        n_draws=n_draws,
        downsample=True,
        diff_cache_dir=os.path.join(output_dir, constants.FilePaths.GET_DRAWS_DIFF_CACHE),
    )
    tmrel = tmrel[_DEMO_INDEX + draw_cols]
    tmrel = common.make_long(df=tmrel, new_col="exposure", id_cols=_DEMO_INDEX)
    tmrel["normal"] = tmrel.groupby("draw")["exposure"].transform(lambda x: x.quantile(0.95))

    flatfile_dir = "FILEPATH"

    anemia_map = pd.read_csv(flatfile_dir / constants.FilePaths.Anemia.CA_MAP)
    anemia_map = anemia_map[~anemia_map["input_modelable_entity_id"].isna()]

    prevalence = pull_input_prevalence(
        anemia_map=anemia_map,
        output_dir=output_dir,
        location_id=location_id,
        release_id=release_id,
        demographics=demographics,
        demographic_cols=_DEMO_INDEX,
        n_draws=n_draws,
    )

    # Pull hemo shift draws by anemia cause
    hemo_shifts = pull_hemo_shifts(
        anemia_map=anemia_map,
        output_dir=output_dir,
        location_id=location_id,
        release_id=release_id,
        demographics=demographics,
        demographic_cols=_DEMO_INDEX,
        n_draws=n_draws,
    )
    prevalence = prevalence.merge(hemo_shifts, how="left", on=_ANEMIA_CAUSE_INDEX + ["draw"])
    prevalence["prev_weighted_hemo_shift"] = (
        prevalence["prevalence"] * prevalence["hemo_shift"]
    )
    shift_index = _DEMO_INDEX + ["iron_responsive", "draw"]
    prevalence = prevalence.set_index(shift_index)
    prevalence["prev_weighted_hemo_shift"] = (
        prevalence[["prev_weighted_hemo_shift"]].groupby(shift_index).sum()
    )
    prevalence = prevalence.reset_index()
    prevalence = prevalence[
        _DEMO_INDEX + ["iron_responsive", "draw", "prev_weighted_hemo_shift"]
    ].drop_duplicates()

    tmrel = tmrel.merge(
        prevalence[prevalence["iron_responsive"] == 0], how="left", on=_DEMO_INDEX + ["draw"]
    )
    tmrel["tmrel"] = tmrel["normal"] - tmrel["prev_weighted_hemo_shift"]
    tmrel = tmrel.drop(["iron_responsive", "prev_weighted_hemo_shift"], axis=1)

    tmrel = tmrel.merge(
        prevalence[prevalence["iron_responsive"] == 1], how="left", on=_DEMO_INDEX + ["draw"]
    )
    tmrel.loc[tmrel["exposure"] > tmrel["tmrel"], "tmrel"] = tmrel["exposure"] + (
        tmrel["prev_weighted_hemo_shift"] / _SHIFT_PROPORTION
    )
    tmrel = tmrel.drop(
        ["normal", "exposure", "iron_responsive", "prev_weighted_hemo_shift"], axis=1
    )

    tmrel = common.make_wide(tmrel, value_col="tmrel", id_cols=_DEMO_INDEX)

    # Get expected annual years for the passed release
    expected_annual_years = common.get_annualized_year_set_by_release_id(
        release_id=release_id
    )

    # Metric ID isn't used, but the interpolate function expects a column.
    tmrel[gbd_constants.columns.METRIC_ID] = 0

    tmrel = expansion.interpolate(
        input_data=tmrel,
        gbd_id=constants.MEIDS.MEAN_HEMO,
        gbd_id_type="modelable_entity_id",
        year_start_id=min(expected_annual_years),
        year_end_id=max(expected_annual_years),
    )

    # Drop garbage metric ID
    tmrel = tmrel.drop(gbd_constants.columns.METRIC_ID, axis=1)

    if in_mem:
        return tmrel
    else:
        tmrel.to_csv(
            pathlib.Path(output_dir) / f"{constants.MEIDS.IRON_DEF_TMREL}/{location_id}.csv",
            index=False,
        )
