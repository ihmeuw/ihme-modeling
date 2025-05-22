import sys

from sqlalchemy import orm

import db_stgpr
import db_tools_core
import stgpr_helpers
import stgpr_schema
from stgpr_helpers import columns, parameters

from stgpr.lib import constants


def _upload_amplitude_nsv(
    stgpr_version_id: int,
    model_iteration_id: int,
    parameter_set_number: int,
    session: orm.Session,
) -> None:
    """Uploads amplitude/NSV to the database.

    Depends on the assumption that a model iteration is paired correctly with its
    corresponding parameter set number.
    """
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)

    amplitude_df = file_utility.read_amplitude(parameter_set_number)

    # Convert unscaled MAD (amplitude) into scaled MAD for storage purposes
    amplitude_df[columns.AMPLITUDE] = (
        amplitude_df[columns.AMPLITUDE] * constants.amplitude.MAD_SCALAR
    )

    add_nsv = file_utility.read_parameters()[parameters.ADD_NSV]
    nsv_df = file_utility.read_nsv(parameter_set_number) if add_nsv else None
    stgpr_helpers.load_amplitude_nsv(
        stgpr_version_id, amplitude_df, nsv_df, model_iteration_id, session
    )


if __name__ == "__main__":
    stgpr_version_id = int(sys.argv[1])
    model_iteration_id = int(sys.argv[2])
    parameter_set_number = int(sys.argv[3])
    last_upload_job = bool(sys.argv[4])
    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        _upload_amplitude_nsv(
            stgpr_version_id, model_iteration_id, parameter_set_number, scoped_session
        )

        # Set model status to next step if this was the last upload job
        if last_upload_job:
            db_stgpr.update_model_status(
                stgpr_version_id, stgpr_schema.ModelStatus.gpr, scoped_session
            )
