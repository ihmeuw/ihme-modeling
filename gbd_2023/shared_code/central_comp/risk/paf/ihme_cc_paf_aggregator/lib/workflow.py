import getpass
import pathlib
import sys

from ihme_cc_cache import FileBackedCacheReader
from ihme_cc_risk_utils.lib import jobmon_utils
from jobmon.client.tool import Tool, Workflow

from ihme_cc_paf_aggregator.lib import constants, types

_CLUSTER_TYPE = "slurm"


def create_workflow(manifest_path: types.PathOrStr) -> Workflow:
    """Create a jobmon workflow that runs PAF aggregation."""
    cache_reader = FileBackedCacheReader(manifest_path)
    settings = constants.PafAggregatorSettings(
        **cache_reader.get(constants.CacheContents.SETTINGS)
    )

    user = getpass.getuser()
    script_path = 

    workflow_name = f"ihme_cc_paf_aggregator_{settings.version_id}"
    # Passing the default "", jobmon generates a hash for the args
    workflow_args = ""
    if settings.resume:
        # retrieve the args. With identical name and args, jobmon will resume and not restart
        workflow_args = jobmon_utils.get_failed_workflow_args(
            workflow_name=workflow_name, return_status=False
        )

    tool = Tool(name="ihme_cc_paf_aggregator")
    workflow = tool.create_workflow(name=workflow_name, workflow_args=workflow_args)

    task_template = tool.get_task_template(
        default_compute_resources={
            "queue": "all.q" if _CLUSTER_TYPE == "slurm" else "null.q",  # for testing
            "cores": 1,
            "memory": f"{4*len(settings.year_id)}G",
            "runtime": f"{1500*len(settings.year_id)}s",
            "stdout": ,
            "stderr": ,
            "project": "proj_centralcomp",
        },
        template_name="paf_aggregation",
        default_cluster_name=_CLUSTER_TYPE,
        command_template=(
            "{python} {script_path} "
            "--location_id {location_id} --manifest_path {manifest_path}"
        ),
        node_args=["location_id"],
        task_args=["manifest_path"],
        op_args=["python", "script_path"],
    )
    tasks = [
        task_template.create_task(
            name=f"location_{location_id}",
            max_attempts=3,
            python=sys.executable,
            script_path=script_path,
            manifest_path=manifest_path,
            location_id=location_id,
        )
        for location_id in settings.location_id
    ]
    workflow.add_tasks(tasks)
    return workflow
