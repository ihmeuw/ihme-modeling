from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from codem.joblaunch.CODEmTask import CODEmTask

import pytest


def test_codem_workflow(model_version_id, db_connection):
    name = 'jobmon workflow codem pytest'
    wf = CODEmWorkflow(name, description='CODEm test', resume=True,
                       reset_running_jobs=True)
    wf.add_task(CODEmTask(model_version_id, db_connection))
    assert wf.project == 'proj_codem'
    assert wf.stderr == 'FILEPATH' + name
    assert len(wf.task_dag.tasks) == 1
