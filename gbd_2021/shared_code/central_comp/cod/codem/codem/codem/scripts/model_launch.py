"""This script is called by mainly just CodViz to launch one CODEm model
through a Jobmon workflow.
"""
import shutil
import sys

from codem.joblaunch.CODEmTask import CODEmTask
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow


def main():
    model_version_id, db_connection = sys.argv[1], sys.argv[2]
    ct = CODEmTask(model_version_id=model_version_id,
                   db_connection=db_connection,
                   max_attempts=3)
    wf = CODEmWorkflow(name=f"codem_{model_version_id}",
                       description=f"codem_codviz_model_{model_version_id}",
                       reset_running_jobs=True,
                       resume=True)
    wf.add_task(ct)
    exit_status = wf.run()
    if exit_status:
        pass
        # TODO: send failed model email
    else:
        shutil.rmtree(wf.stderr)


if __name__ == '__main__':
    main()
