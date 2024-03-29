from getpass import getuser

from jobmon.client.swarm.workflow.workflow import Workflow


class CODEmWorkflow(Workflow):
    """
    This class can run a workflow for CODEm and takes a list of model version IDs
    and ONE database argument.
    """
    def __init__(self, name, description, resume, reset_running_jobs, project='proj_codem'):
        super().__init__(name=name,
                         workflow_args=name,
                         description=description,
                         project=project,
                         stderr=f"FILEPATH/codem/workflow_logs/{name}",
                         stdout=f"FILEPATH/codem/workflow_logs/{name}",
                         working_dir=f'/homes/{getuser()}',
                         seconds_until_timeout=60*60*24*16,
                         resume=resume,
                         reset_running_jobs=reset_running_jobs)
