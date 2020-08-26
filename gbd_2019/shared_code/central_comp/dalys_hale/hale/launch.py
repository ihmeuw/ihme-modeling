import getpass
from typing import Dict, List, Optional

from jobmon.client.swarm import workflow as wf

from hale import metadata
from hale.common.constants import paths, resources, task_names


def create_hale_workflow(
        hale_version: int,
        project: str,
        error_dir: Optional[str],
        output_dir: Optional[str]
) -> wf.workflow.Workflow:
    """
    Builds the HALE jobmon workflow, which consists of tasks to calculate HALE,
    a task to upload results, and a task to delete temporary files.
    """
    # Initialize workflow.
    user = getpass.getuser()
    error_dir = error_dir or paths.SGE_OUTPUT_ERRORS.format(user=user)
    output_dir = output_dir or paths.SGE_OUTPUT_OUTPUT.format(user=user)
    workflow = wf.workflow.Workflow(
        workflow_args=f'hale_version_{hale_version}',
        project=project,
        stderr=error_dir,
        stdout=output_dir,
        resume=True
    )

    # Load metadata and make dictionary to track workflow tasks.
    hale_meta = metadata.load_metadata(hale_version)
    tasks: Dict[str, wf.python_task.PythonTask] = {}

    # Add tasks to workflow.
    _add_hale_calculation_tasks(
        workflow, tasks, hale_version, hale_meta.location_ids
    )
    _add_upload_task(
        workflow,
        tasks,
        hale_version,
        hale_meta.location_ids,
        hale_meta.conn_def
    )
    _add_clean_task(workflow, tasks, hale_version)

    return workflow


def _add_hale_calculation_tasks(
        workflow: wf.workflow.Workflow,
        tasks: Dict[str, wf.python_task.PythonTask],
        hale_version: int,
        location_ids: List[int]
) -> None:
    for location_id in location_ids:
        calculation_task_name = task_names.HALE_CALCULATION_FORMAT.format(
            version=hale_version, location=location_id
        )
        calculation_task = wf.python_task.PythonTask(
            name=calculation_task_name,
            script=paths.RUN_HALE_CALCULATION,
            args=[
                '--hale_version', hale_version,
                '--location_id', location_id
            ],
            num_cores=resources.CALCULATE_HALE_CORES,
            m_mem_free=resources.CALCULATE_HALE_MEMORY,
            max_runtime_seconds=resources.CALCULATE_HALE_RUNTIME,
            queue=resources.QUEUE,
            max_attempts=2
        )
        tasks[calculation_task_name] = calculation_task
        workflow.add_task(calculation_task)


def _add_upload_task(
        workflow: wf.workflow.Workflow,
        tasks: Dict[str, wf.python_task.PythonTask],
        hale_version: int,
        location_ids: List[int],
        conn_def: str
) -> None:
    upload_task_name = task_names.UPLOAD_FORMAT.format(version=hale_version)
    upload_task = wf.python_task.PythonTask(
        name=upload_task_name,
        script=paths.RUN_UPLOAD,
        args=['--hale_version', hale_version, '--conn_def', conn_def],
        num_cores=resources.UPLOAD_CORES,
        m_mem_free=resources.UPLOAD_MEMORY,
        max_runtime_seconds=resources.UPLOAD_RUNTIME,
        queue=resources.QUEUE,
        max_attempts=1
    )
    for location_id in location_ids:
        calculation_task_name = task_names.HALE_CALCULATION_FORMAT.format(
            version=hale_version, location=location_id
        )
        calculation_task = tasks[calculation_task_name]
        upload_task.add_upstream(calculation_task)
    tasks[upload_task_name] = upload_task
    workflow.add_task(upload_task)


def _add_clean_task(
        workflow: wf.workflow.Workflow,
        tasks: Dict[str, wf.python_task.PythonTask],
        hale_version: int
) -> None:
    clean_task_name = task_names.CLEAN_FORMAT.format(version=hale_version)
    clean_task = wf.python_task.PythonTask(
        name=clean_task_name,
        script=paths.RUN_CLEAN,
        args=['--hale_version', hale_version],
        num_cores=resources.CLEAN_CORES,
        m_mem_free=resources.CLEAN_MEMORY,
        max_runtime_seconds=resources.CLEAN_RUNTIME,
        queue=resources.QUEUE,
        max_attempts=1
    )
    upload_task_name = task_names.UPLOAD_FORMAT.format(version=hale_version)
    clean_task.add_upstream(tasks[upload_task_name])
    tasks[clean_task_name] = clean_task
    workflow.add_task(clean_task)
