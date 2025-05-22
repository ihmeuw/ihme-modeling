import argparse
import os
import sys
from typing import List, Union

import gbd.constants as gbd_constants
from jobmon.client.api import Tool
from jobmon.client.task import Task
from save_results import save_results_epi, save_results_risk

from epic.legacy.util.constants import DAG, MEIDS, Params
from epic.lib.util import common
from epic.lib.util.metadata import save_model_metadata

_THIS_FILE = os.path.realpath(__file__)


def parse_arguments() -> argparse.Namespace:
    """Parse save task arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent_dir", required=True, type=str)
    parser.add_argument("--input_dir", required=True, type=str)
    parser.add_argument("--input_file_pattern", required=True, type=str)
    parser.add_argument("--modelable_entity_id", required=True, type=int)
    parser.add_argument("--description", required=True, type=str)
    parser.add_argument("--measure_id", required=True, type=int, nargs="*")
    parser.add_argument("--year_id", required=True, type=int, nargs="*")
    parser.add_argument("--release_id", required=True, type=int)
    parser.add_argument("--n_draws", required=True, type=int)
    parser.add_argument("--best", required=True, type=int)
    parser.add_argument("--population_path", required=True, type=str)
    parser.add_argument("--annual", required=True, type=int)
    parser.add_argument("--sr_type", required=True, type=str)
    args = parser.parse_args()
    return args


class SaveFactory(object):
    """Save task creation class."""

    def __init__(self, tool: Tool) -> None:
        self.tool = tool
        self.python = sys.executable

        self.task_template = self.tool.get_task_template(
            template_name=DAG.Tasks.SAVE,
            command_template=(
                "{python} {script} --parent_dir {parent_dir} --input_dir {input_dir} "
                "--input_file_pattern {input_file_pattern} "
                "--modelable_entity_id {modelable_entity_id} --description {description} "
                "--measure_id {measure_id} --year_id {year_id} --release_id {release_id} "
                "--n_draws {n_draws} --best {best} --population_path {population_path} "
                "--annual {annual} --sr_type {sr_type}"
            ),
            node_args=[
                Params.PARENT_DIR,
                Params.INPUT_DIR,
                Params.INPUT_FILE_PATTERN,
                Params.MODELABLE_ENTITY_ID,
                Params.DESCRIPTION,
                Params.MEASURE_ID,
                Params.YEAR_ID,
                Params.RELEASE_ID,
                Params.N_DRAWS,
                Params.BEST,
                Params.POPULATION_PATH,
                Params.ANNUAL,
                Params.SR_TYPE,
            ],
            op_args=["python", "script"],
        )

    @staticmethod
    def get_task_name(modelable_entity_id: int) -> str:
        """Return task name given ME ID."""
        return common.get_save_task_name(modelable_entity_id=modelable_entity_id)

    def get_task(
        self,
        parent_dir: str,
        input_dir: str,
        input_file_pattern: str,
        modelable_entity_id: int,
        description: str,
        measure_id: Union[int, List[int]],
        year_id: Union[int, List[int]],
        release_id: int,
        n_draws: int,
        best: bool,
        population_path: str,
        annual: bool = False,
        sr_type: str = "epi",
    ) -> Task:
        """Returns an EPIC save_task given task args."""
        node_args = {
            Params.PARENT_DIR: parent_dir,
            Params.INPUT_DIR: input_dir,
            Params.INPUT_FILE_PATTERN: input_file_pattern,
            Params.MODELABLE_ENTITY_ID: str(modelable_entity_id),
            Params.DESCRIPTION: description,
            Params.MEASURE_ID: " ".join([str(x) for x in measure_id]),
            Params.YEAR_ID: " ".join([str(x) for x in year_id]),
            Params.RELEASE_ID: release_id,
            Params.N_DRAWS: n_draws,
            Params.BEST: int(best),
            Params.POPULATION_PATH: population_path,
            Params.ANNUAL: int(annual),
            Params.SR_TYPE: sr_type,
        }
        compute_resources = {
            DAG.ArgNames.MEMORY: "80Gb",
            DAG.ArgNames.NUM_CORES: 17,
            DAG.ArgNames.RUNTIME: f"{60 * 60 * 3}s",  # 3 hours
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


if __name__ == "__main__":  # noqa: C901
    # parse arguments
    args = parse_arguments()

    # special exception for sex specific
    if vars(args)[Params.MODELABLE_ENTITY_ID] in MEIDS.FEMALE_ONLY:
        sex_id = [gbd_constants.sex.FEMALE]
    else:
        sex_id = [gbd_constants.sex.MALE, gbd_constants.sex.FEMALE]

    if vars(args)[Params.BEST] not in [0, 1]:
        raise ValueError(
            f"{Params.BEST} must be one of (0, 1), got {vars(args)[Params.BEST]} instead."
        )

    if vars(args)[Params.ANNUAL]:
        year_set = common.get_annualized_year_set_by_release_id(
            release_id=vars(args)[Params.RELEASE_ID]
        )
    else:
        year_set = vars(args)[Params.YEAR_ID]

    if vars(args)[Params.SR_TYPE] == "epi":
        if vars(args)[Params.MODELABLE_ENTITY_ID] in MEIDS.PREVALENCE_ONLY:
            measure_id = [gbd_constants.measures.PREVALENCE]
        elif vars(args)[Params.MODELABLE_ENTITY_ID] in MEIDS.INCIDENCE_TO_PREVALENCE:
            measure_id = [gbd_constants.measures.PREVALENCE]
        else:
            measure_id = vars(args)[Params.MEASURE_ID]
        if vars(args)[Params.MODELABLE_ENTITY_ID] in MEIDS.INCLUDE_BIRTH_PREV:
            birth_prev = True
        else:
            birth_prev = False
        try:
            mvid_df = save_results_epi(
                input_dir=vars(args)[Params.INPUT_DIR],
                input_file_pattern=vars(args)[Params.INPUT_FILE_PATTERN],
                modelable_entity_id=vars(args)[Params.MODELABLE_ENTITY_ID],
                description=vars(args)[Params.DESCRIPTION],
                measure_id=measure_id,
                year_id=year_set,  # Try to save with annualized draws if requested first
                sex_id=sex_id,
                metric_id=[gbd_constants.metrics.RATE],
                mark_best=bool(vars(args)[Params.BEST]),
                birth_prevalence=birth_prev,
                release_id=vars(args)[Params.RELEASE_ID],
                n_draws=vars(args)[Params.N_DRAWS],
                upload_summaries=False,
                pop_df=vars(args)[Params.POPULATION_PATH],
            )
        except RuntimeError as e:
            # Only retry with fallback years if specifically missing values from the year_id
            # column.Note that this logic specifically relies on exception text thrown in the
            # quality checks for save_results_epi.
            if e.args and "year_id is missing value(s)" in e.args[0]:
                mvid_df = save_results_epi(
                    input_dir=vars(args)[Params.INPUT_DIR],
                    input_file_pattern=vars(args)[Params.INPUT_FILE_PATTERN],
                    modelable_entity_id=vars(args)[Params.MODELABLE_ENTITY_ID],
                    description=vars(args)[Params.DESCRIPTION],
                    measure_id=measure_id,
                    year_id=vars(args)[Params.YEAR_ID],
                    sex_id=sex_id,
                    metric_id=[gbd_constants.metrics.RATE],
                    mark_best=bool(vars(args)[Params.BEST]),
                    birth_prevalence=birth_prev,
                    release_id=vars(args)[Params.RELEASE_ID],
                    n_draws=vars(args)[Params.N_DRAWS],
                    upload_summaries=False,
                    pop_df=vars(args)[Params.POPULATION_PATH],
                )
            else:
                raise e
    elif vars(args)[Params.SR_TYPE] == "risk":
        mvid_df = save_results_risk(
            input_dir=vars(args)[Params.INPUT_DIR],
            input_file_pattern=vars(args)[Params.INPUT_FILE_PATTERN],
            modelable_entity_id=vars(args)[Params.MODELABLE_ENTITY_ID],
            description=vars(args)[Params.DESCRIPTION],
            risk_type="tmrel",
            measure_id=vars(args)[Params.MEASURE_ID],
            year_id=year_set,  # Iron deficiency TMREL is always annual
            sex_id=sex_id,
            mark_best=bool(vars(args)[Params.BEST]),
            release_id=vars(args)[Params.RELEASE_ID],
            n_draws=vars(args)[Params.N_DRAWS],
        )

    save_model_metadata(
        vars(args)[Params.PARENT_DIR],
        vars(args)[Params.MODELABLE_ENTITY_ID],
        int(mvid_df[Params.MODEL_VERSION_ID].iat[0]),
        vars(args)[Params.RELEASE_ID],
    )
