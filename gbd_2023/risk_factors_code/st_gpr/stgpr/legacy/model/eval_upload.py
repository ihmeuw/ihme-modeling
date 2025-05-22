import sys

from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema

from stgpr.lib import utils


def _upload_fit_statistics(
    stgpr_version_id: int,
    model_iteration_id: int,
    parameter_set_number: int,
    session: orm.Session,
) -> None:
    """Uploads fit statistics to the database.

    Depends on the assumption that a model iteration is paired correctly with its corresponding
    parameter set number.
    """
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)

    fit_stats_df = file_utility.read_fit_statistics(parameter_set_number)
    stgpr_helpers.load_fit_statistics(
        stgpr_version_id, fit_stats_df, model_iteration_id, session
    )

    # For selection models, update database if this set is the best parameter set
    if db_stgpr.is_selection_model(
        stgpr_version_id, session
    ) and parameter_set_number == utils.get_best_parameter_set(stgpr_version_id):
        db_stgpr.update_best_model_iteration(model_iteration_id, session)


if __name__ == "__main__":
    stgpr_version_id = int(sys.argv[1])
    model_iteration_id = int(sys.argv[2])
    parameter_set_number = int(sys.argv[3])
    last_upload_job = bool(sys.argv[4])
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        _upload_fit_statistics(
            stgpr_version_id, model_iteration_id, parameter_set_number, scoped_session
        )

        # Set model status to next step if this was the last upload job
        if last_upload_job:
            db_stgpr.update_model_status(
                stgpr_version_id, stgpr_schema.ModelStatus.rake, scoped_session
            )
