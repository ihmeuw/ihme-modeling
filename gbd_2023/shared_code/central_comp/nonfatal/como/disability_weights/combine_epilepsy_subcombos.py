import pandas as pd
from loguru import logger

from gbd.constants import columns as gbd_columns

from como.lib import constants
from como.lib.comorbidity_calculation import comorbidity_value_combine
from como.lib.fileshare_inputs import get_standard_disability_weights
from como.lib.utils import ordered_draw_columns


def epilepsy_combos(como_dir: str, location_id: int, n_processes: int) -> None:
    """Combines healthstate combinations into aggregate healthstates for epilepsy."""
    standard_dws = get_standard_disability_weights().copy(deep=True)

    # Read epilepsy dw file generated in combine_epilepsy_any
    # nrows = (1 health state - 772) x (2 sex) x (# age groups) x (# years)
    # ncols = 1000 draws +
    # ['healthstate_id', 'sex_id', 'age_group_id', 'location_id', 'year_id']
    epilepsy_dws = pd.read_hdf(
        "FILEPATH"
    )

    all_combined = []
    for (
        sink_healthstate,
        source_healthstates,
    ) in constants.EPILEPSY_HEALTHSTATE_COMBOS.items():
        logger.info(f"Combining {source_healthstates} into {sink_healthstate}.")
        source_healthstates = [i for i in source_healthstates if i != "epilepsy_any"]
        sink_healthstate_id = constants.HEALTHSTATES[sink_healthstate]

        # List of single-row disability weights for the source healthstates
        values_to_combine = [
            standard_dws[standard_dws.healthstate.values == i][
                ordered_draw_columns(standard_dws)
            ].values
            for i in source_healthstates
        ]
        # combine with each row of the epilepsy_any results for this sink healthstate
        values_to_combine.append(epilepsy_dws[ordered_draw_columns(epilepsy_dws)].values)
        combined_values = comorbidity_value_combine(values_to_combine)
        all_combined.append(epilepsy_dws.copy())
        all_combined[-1][ordered_draw_columns(all_combined[-1])] = combined_values
        all_combined[-1]["healthstate_id"] = sink_healthstate_id

    all_combined = pd.concat(all_combined)

    # Output to file
    col_order = [
        gbd_columns.LOCATION_ID,
        gbd_columns.YEAR_ID,
        gbd_columns.AGE_GROUP_ID,
        gbd_columns.SEX_ID,
        gbd_columns.HEALTHSTATE_ID,
    ] + ordered_draw_columns(all_combined)
    all_combined = all_combined[col_order]
    all_combined.to_hdf(
        "FILEPATH",
        "draws",
        mode="w",
        format="table",
        data_columns=["location_id", "year_id", "age_group_id", "sex_id"],
    )
