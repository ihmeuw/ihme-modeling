# This script launches the read worker script for each file in the repo containing
# Russia MOH data.

from pathlib import Path
from getpass import getuser
from datetime import datetime
import sys
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.executors.base import ExecutorParameters

# UPDATE GLOB REGEX HERE WITH NEW DATA
fp_list = Path('FILEPATH').glob('*.docx')

# workflow params
event_name = 'READ_RUSSIA'
working_dir = f'FILEPATH'
name = f'{event_name}_{datetime.now()}'

# create workflow
workflow = Workflow(workflow_args=name,
                    project='proj_hospital',
                    stderr=f'FILEPATH/error',
                    stdout=f'FILEPATH/output',
                    working_dir=working_dir,
                    seconds_until_timeout=86400)

# loop over all the filepaths and add a task
for fp in fp_list:
    script = f'FILEPATH/read_worker.py'
    name = fp.name

    params = ExecutorParameters(m_mem_free='100G',
                                num_cores=2,
                                queue='all.q',
                                max_runtime_seconds=86400)
                                    
    t = PythonTask(script=script,
                   args=[fp.as_posix(), '-V', 'FILEPATH'],
                   name=name,
                   max_attempts=3,
                   executor_parameters=params)

    workflow.add_task(t)

# run the workflow
exit_code = workflow.run()

if exit_code == 0:
    print('Successfully read all files!')
else:
    print("Workflow failure! Please qstat or check Jobmon DB.")