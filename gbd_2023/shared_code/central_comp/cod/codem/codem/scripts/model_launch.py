"""This script is called by mainly just CodViz to launch one CODEm model
through a Jobmon workflow.
"""

import shutil
import sys

from gbd import conn_defs
from jobmon.core.constants import WorkflowStatus

from codem.data.parameters import get_model_parameters
from codem.joblaunch.CODEmTask import CODEmTask
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow

CODVIZ_DB_HOST_TO_CONN_DEF = {
    "modeling-cod-db": conn_defs.CODEM,
    "cod-dev-db": conn_defs.CODEM_TEST,
}


def main() -> None:
    """Main entry point script for running a CODEm model."""
    model_version_id, db_host = sys.argv[1], sys.argv[2]
    if db_host not in CODVIZ_DB_HOST_TO_CONN_DEF:
        raise ValueError(
            f"Invalid database host {db_host} provided by CoDViz, please file a ticket "
            "with Central Comp."
        )
    conn_def = CODVIZ_DB_HOST_TO_CONN_DEF[db_host]
    model_parameters = get_model_parameters(
        model_version_id=model_version_id, conn_def=conn_def, update=False
    )
    codem_wf = CODEmWorkflow(
        name=f"codem_{model_version_id}",
        description=f"codem_codviz_model_{model_version_id}",
        project=model_parameters["cluster_project"],
        model_version_id=model_version_id,
        cause_id=model_parameters["cause_id"],
        acause=model_parameters["acause"],
        user=model_parameters["inserted_by"],
    )
    codem_task = CODEmTask(
        model_version_id=model_version_id, conn_def=conn_def, max_attempts=3
    ).get_task()
    wf = codem_wf.get_workflow()
    wf.add_task(codem_task)
    exit_status = wf.run(resume=True, seconds_until_timeout=1036800)  # 12 days of runtime
    if exit_status != WorkflowStatus.DONE:
        # TODO: send failed model email
        pass
    else:
        shutil.rmtree(codem_wf.stderr, ignore_errors=True)


if __name__ == "__main__":
    main()
