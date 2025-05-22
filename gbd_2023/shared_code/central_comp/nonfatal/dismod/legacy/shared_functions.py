from typing import List

import pandas as pd

from db_queries import (  # noqa: F401
    get_age_metadata,
    get_age_spans,
    get_age_weights,
    get_covariate_estimates,
    get_demographics,
    get_envelope,
    get_location_metadata,
    get_population,
)
from db_queries.legacy import get_outputs_helpers
from gbd import constants as gbd_constants
from save_results.legacy.model_types import DismodSaveResults  # noqa: F401


def get_outputs(
    process_version_id: int,
    cause_id: int,
    location_ids: List[int],
    age_group_ids: List[int],
    year_ids: List[int],
    sex_ids: List[int],
) -> pd.DataFrame:
    """Query gbd database for CSMR.

    We can't use standard get_outputs because codcorrect 192 is step2 2021 which
    has no release_id, and get_outputs requires process versions to have a release id.

    So we should move away from this after get_outptus supports step2 2021 queries.

    Because this function doesn't have full functionality of get_outputs, we
    do not support reading from archival servers. Since no models use GBD 2019 CSMR
    any more, I think that's safe for now.

    Some issues I can think of possibly happening that would break this:
        internal updates to get_outputs_helpers
        archival of gbd 2021 results
    """
    conn_def = "gbd"
    filters = {
        "location_id": location_ids,
        "age_group_id": age_group_ids,
        "year_id": year_ids,
        "sex_id": sex_ids,
        "metric_id": gbd_constants.metrics.RATE,
        "measure_id": gbd_constants.measures.DEATH,
        "cause_id": cause_id,
    }
    PV = get_outputs_helpers.versioning.ProcessVersion([process_version_id], conn_def)
    tables = [v for (k, v) in PV.tables.items() if "single" in k]
    df = get_outputs_helpers.tables.run_queries(tables, filters)
    return df
