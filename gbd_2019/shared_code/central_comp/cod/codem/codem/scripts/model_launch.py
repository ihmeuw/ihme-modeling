"""This script is called by mainly just CodViz to launch one CODEm model
through a Jobmon workflow.
"""

import sys

from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from codem.joblaunch.CODEmTask import CODEmTask


def main():
    model_version_id, db_connection = sys.argv[1], sys.argv[2]
    ct = CODEmTask(model_version_id=model_version_id,
                   db_connection=db_connection,
                   max_attempts=3)
    wf = CODEmWorkflow(name="codem_{}".format(model_version_id),
                       description="codem_codviz_model_{}".format(model_version_id),
                       reset_running_jobs=True,
                       resume=True)
    wf.add_task(ct)
    wf.run()


if __name__ == '__main__':
    main()
