import json
import os
import argparse

from epic.util.constants import MEIDS, DAG, FilePaths, Params
import gbd.constants as gbd
from save_results import save_results_epi
from jobmon.client.swarm.workflow.python_task import PythonTask

this_file = os.path.realpath(__file__)

class SaveFactory(object):

    @staticmethod
    def get_task_name(modelable_entity_id):
        return f"{DAG.Tasks.SAVE}_{modelable_entity_id}"

    def get_task(self, parent_dir, input_dir, input_file_pattern,
            modelable_entity_id, description, measure_id, year_id, decomp_step,
            n_draws
    ) -> PythonTask:
        # make task
        name = self.get_task_name(modelable_entity_id)
        task = PythonTask(
            script=this_file,
            args=[
                "--parent_dir", parent_dir,
                "--input_dir", input_dir,
                "--input_file_pattern", input_file_pattern,
                "--modelable_entity_id", str(modelable_entity_id),
                "--description", description,
                "--measure_id", " ".join([str(x) for x in measure_id]),
                "--year_id", " ".join([str(x) for x in year_id]),
                "--decomp_step", decomp_step,
                "--n_draws", str(n_draws)
            ],
            name=name,
            num_cores=17,
            m_mem_free="80.0G",
            max_attempts=DAG.Tasks.MAX_ATTEMPTS,
            tag=DAG.Tasks.SAVE,
            queue=DAG.Tasks.QUEUE)

        return task

    @staticmethod
    def save_model_metadata(
        parent_dir: str,
        modelable_entity_id: int,
        model_version_id: int,
        decomp_step: str
    ) -> None:
        metadata_dict = {
            Params.MODELABLE_ENTITY_ID: modelable_entity_id,
            Params.MODEL_VERSION_ID: model_version_id,
            Params.DECOMP_STEP: decomp_step
        }
        # write to disk
        with open(
            os.path.join(
                parent_dir,
                FilePaths.INPUT_FILES_DIR,
                f"{modelable_entity_id}.json"
            ),"w"
        ) as outfile:
            json.dump(metadata_dict, outfile, sort_keys=True, indent=2)



if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent_dir", required=True, type=str)
    parser.add_argument("--input_dir", required=True, type=str)
    parser.add_argument("--input_file_pattern", required=True, type=str)
    parser.add_argument("--modelable_entity_id", required=True, type=int)
    parser.add_argument("--description", required=True, type=str)
    parser.add_argument("--measure_id", required=True, type=int, nargs="*")
    parser.add_argument("--year_id", required=True, type=int, nargs="*")
    parser.add_argument("--decomp_step", required=True, type=str)
    parser.add_argument("--n_draws", required=True, type=int)

    args = parser.parse_args()

    if args.modelable_entity_id in MEIDS.FEMALE_ONLY:
        sex_id = [gbd.sex.FEMALE]
    else:
        sex_id = [gbd.sex.MALE, gbd.sex.FEMALE]

    if args.modelable_entity_id in MEIDS.PREVALENCE_ONLY:
        measure_id = [gbd.measures.PREVALENCE]
    else:
        measure_id = args.measure_id

    if args.modelable_entity_id in MEIDS.INCLUDE_BIRTH_PREV:
        birth_prev = True
    else:
        birth_prev = False

    # call save results
    mvid_df = save_results_epi(
        input_dir=args.input_dir,
        input_file_pattern=args.input_file_pattern,
        modelable_entity_id=args.modelable_entity_id,
        description=args.description,
        measure_id=measure_id,
        year_id=args.year_id,
        sex_id=sex_id,
        metric_id=[gbd.metrics.RATE],
        mark_best=True,
        birth_prevalence=birth_prev,
        decomp_step=args.decomp_step,
        n_draws=args.n_draws
    )
    SaveFactory.save_model_metadata(
        args.parent_dir,
        args.modelable_entity_id,
        int(mvid_df[Params.MODEL_VERSION_ID].iat[0]),
        args.decomp_step
    )
