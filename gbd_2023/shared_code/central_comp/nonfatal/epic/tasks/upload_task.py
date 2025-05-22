import argparse
import os
import sys

from jobmon.client.api import Tool
from jobmon.client.task import Task
from save_results.legacy import _db as sr_db

from epic.legacy.util.constants import DAG, FilePaths, Params
from epic.lib.util.common import pull_mvid

_THIS_FILE = os.path.realpath(__file__)


def parse_arguments() -> argparse.Namespace:
    """Parse upload task arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_env", required=True, type=str)
    parser.add_argument("--output_dir", required=True, type=str)
    parser.add_argument("--modelable_entity_id", required=True, type=int)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--best", required=True, type=int)
    parser.add_argument("--test", required=True, type=int)
    args = parser.parse_args()
    return args


class UploadFactory(object):
    """Upload task creation class."""

    def __init__(self, tool: Tool) -> None:
        self.tool = tool
        self.python = sys.executable

        self.task_template = self.tool.get_task_template(
            template_name=DAG.Tasks.UPLOAD,
            command_template=(
                "{python} {script} --db_env {db_env} --output_dir {output_dir} "
                "--modelable_entity_id {modelable_entity_id} --version {version} "
                "--best {best} --test {test}"
            ),
            node_args=[
                Params.DB_ENV,
                Params.OUTPUT_DIR,
                Params.MODELABLE_ENTITY_ID,
                Params.VERSION,
                Params.BEST,
                Params.TEST,
            ],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(modelable_entity_id: int) -> str:
        """Return task name given ME ID."""
        return f"{DAG.Tasks.UPLOAD}_{modelable_entity_id}"

    def get_task(
        self,
        db_env: str,
        output_dir: str,
        modelable_entity_id: int,
        version: int,
        best: bool,
        test: bool,
    ) -> Task:
        """Returns an EPIC upload_task given task args."""
        node_args = {
            Params.DB_ENV: db_env,
            Params.OUTPUT_DIR: output_dir,
            Params.MODELABLE_ENTITY_ID: modelable_entity_id,
            Params.VERSION: version,
            Params.BEST: int(best),
            Params.TEST: int(test),
        }
        compute_resources = {
            DAG.ArgNames.MEMORY: "5Gb",
            DAG.ArgNames.NUM_CORES: 2,
            DAG.ArgNames.RUNTIME: f"{60 * 60 * 1}s",  # 1 hour
        }
        task = self.task_template.create_task(
            script=_THIS_FILE,
            compute_resources=compute_resources,
            fallback_queues=DAG.Workflow.FALLBACK_QUEUES,
            max_attempts=DAG.Workflow.MAX_ATTEMPTS,
            name=self.get_task_name(modelable_entity_id),
            python=self.python,
            **node_args,
        )
        return task


if __name__ == "__main__":
    # parse command line args
    args = parse_arguments()

    for param in [Params.BEST, Params.TEST]:
        if vars(args)[param] not in [0, 1]:
            raise ValueError(
                f"{param} must be one of (0, 1), got {vars(args)[param]} instead."
            )

    # create an EpiModel instance for uploading draws, taken from SR Epi
    data_dir = FilePaths.TEST_DATA_DIR if vars(args)[Params.TEST] else FilePaths.DATA_DIR
    mvid = pull_mvid(
        parent_dir=os.path.join(data_dir, str(vars(args)[Params.VERSION])),
        modelable_entity_id=vars(args)[Params.MODELABLE_ENTITY_ID],
    )
    db = sr_db.EpiModel(env=vars(args)[Params.DB_ENV], model_version_id=mvid)
    db.upload_summaries("FILEPATH")

    # mark the model best if specified
    if vars(args)[Params.BEST]:
        db.mark_best()
