import os
import json
import argparse
from typing import List, Union

from epic.util.constants import DAG
from save_results._db import EpiModel
from jobmon.client.swarm.workflow.python_task import PythonTask

from epic.util.constants import FilePaths, Params
from epic.util.common import pull_mvid

this_file = os.path.realpath(__file__)


class UploadFactory(object):

    @staticmethod
    def get_task_name(modelable_entity_id) -> str:
        return f"{DAG.Tasks.UPLOAD}_{modelable_entity_id}"

    def get_task(
        self,
        db_env: str,
        output_dir: str,
        modelable_entity_id: int,
        version: int
    ) -> PythonTask:

        name = self.get_task_name(modelable_entity_id)
        task = PythonTask(
            script=this_file,
            args=[
                "--db_env", db_env,
                "--output_dir", output_dir,
                "--modelable_entity_id", modelable_entity_id,
                "--version", version
            ],
            name=name,
            num_cores=2,
            m_mem_free="5.0G",
            max_attempts=DAG.Tasks.MAX_ATTEMPTS,
            tag=DAG.Tasks.UPLOAD,
            queue=DAG.Tasks.QUEUE
        )

        return task


if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_env", required=True, type=str)
    parser.add_argument("--output_dir", required=True, type=str)
    parser.add_argument("--modelable_entity_id", required=True, type=int)
    parser.add_argument("--version", required=True, type=int)

    args = parser.parse_args()

    # create an EpiModel instance for uploading draws, taken from SR Epi
    mvid = pull_mvid(
        parent_dir=os.path.join(FilePaths.DATA_DIR, str(args.version)),
        modelable_entity_id=args.modelable_entity_id
    )
    db = EpiModel(
        env=args.db_env,
        model_version_id=mvid
    )
    db.upload_summaries(
        os.path.join(
            "FILEPATH", 'model_estimate_final.csv'
        )
    )
    db.mark_best()
