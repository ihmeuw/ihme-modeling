import sys

from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema
from stgpr_helpers import columns, parameters

from stgpr.lib import expansion, location_aggregation


def _upload_spacetime_estimates(
    stgpr_version_id: int,
    model_iteration_id: int,
    parameter_set_number: int,
    session: orm.Session,
) -> None:
    """Uploads spacetime estimates to the database.

    Also aggregates estimates from country level up to global.

    Depends on the assumption that a model iteration is paired correctly with its corresponding
    parameter set number.
    """
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)
    params = file_utility.read_parameters()
    spacetime_df = (
        file_utility.read_spacetime_estimates(parameter_set_number)
        .rename(columns={"st": columns.VAL})
        .drop_duplicates()
    )

    # Only run location aggregation if metric id is non-null
    if params[parameters.METRIC_ID]:
        spacetime_df = location_aggregation.aggregate_locations(
            stgpr_version_id, spacetime_df, data_in_model_space=True
        )

    spacetime_df = expansion.expand_results(data=spacetime_df, params=params)

    stgpr_helpers.load_spacetime_estimates(
        stgpr_version_id, spacetime_df, model_iteration_id, session
    )


if __name__ == "__main__":
    stgpr_version_id = int(sys.argv[1])
    model_iteration_id = int(sys.argv[2])
    parameter_set_number = int(sys.argv[3])
    last_upload_job = bool(sys.argv[4])
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        # Extend length to provide sufficient time for location aggregation and expansion
        scoped_session.execute("SET wait_timeout=600")
        _upload_spacetime_estimates(
            stgpr_version_id, model_iteration_id, parameter_set_number, scoped_session
        )

        # Set model status to next step if this was the last upload job
        if last_upload_job:
            db_stgpr.update_model_status(
                stgpr_version_id, stgpr_schema.ModelStatus.descanso, scoped_session
            )
