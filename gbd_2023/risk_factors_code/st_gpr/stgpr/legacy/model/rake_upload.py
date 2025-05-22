import sys

from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema
from stgpr_helpers import columns

from stgpr.lib import utils


# TODO: delete parameter_set_number from function parameters and don't pass into job
def _upload_final_estimates(
    stgpr_version_id: int,
    model_iteration_id: int,
    parameter_set_number: int,
    session: orm.Session,
) -> None:
    """Uploads final estimates for best parameter set to the database.

    Depends on the assumption that a model iteration is paired correctly with its corresponding
    parameter set number.
    """
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)
    best_parameter_set = utils.get_best_parameter_set(stgpr_version_id)

    raked_df = (
        file_utility.read_final_estimates(best_parameter_set)
        .rename(
            columns={
                "gpr_mean": columns.VAL,
                "gpr_lower": columns.LOWER,
                "gpr_upper": columns.UPPER,
            }
        )
        .drop_duplicates()
    )
    stgpr_helpers.load_final_estimates(
        stgpr_version_id, raked_df, model_iteration_id, session
    )


if __name__ == "__main__":
    stgpr_version_id = int(sys.argv[1])
    model_iteration_id = int(sys.argv[2])
    parameter_set_number = int(sys.argv[3])
    last_upload_job = bool(sys.argv[4])
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        _upload_final_estimates(
            stgpr_version_id, model_iteration_id, parameter_set_number, scoped_session
        )

        # Set model status to next step if this was the last upload job
        if last_upload_job:
            db_stgpr.update_model_status(
                stgpr_version_id, stgpr_schema.ModelStatus.clean, scoped_session
            )
