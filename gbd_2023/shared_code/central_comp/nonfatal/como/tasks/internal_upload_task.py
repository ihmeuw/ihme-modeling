import logging
import sys
from pathlib import Path
from typing import List, Tuple, Union

import click

import db_tools_core
from db_tools.loaders import Infiles
from gbd import conn_defs
from gbd import constants as gbd_constants
from gbd_outputs_versions import GBDProcessVersion
from jobmon.client.api import Tool
from jobmon.client.task import Task

from como.legacy.common import name_task
from como.lib import constants as como_constants
from como.lib.version import ComoVersion

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_THIS_FILE = str(Path(__file__).resolve())
_INTERNAL_UPLOAD_COMPUTE_RESOURCES = {"cores": 1, "memory": "6G", "runtime": "3h"}
REVERSE_MEASURES = {v: k.lower() for k, v in gbd_constants.measures.items()}

ALLOWED_MEASURES = [
    gbd_constants.measures["YLD"],
    gbd_constants.measures["PREVALENCE"],
    gbd_constants.measures["INCIDENCE"],
]

TABLE_NAME_MAP = {
    como_constants.Component.CAUSE.value: "epi",
    como_constants.Component.IMPAIRMENT.value: "impairment",
    como_constants.Component.INJURY.value: "injury",
    como_constants.Component.SEQUELA.value: "sequela",
}


def consolidate_year_summaries(
    component: str,
    location_id: int,
    measure_id: int,
    como_dir: str,
    output_file: Union[str, Path],
) -> None:
    """Consolidate multiple single-year files into a single file.

    Notes:
        - In testing, this was friendlier to the mariadb undo transaction log and
        the growth of the ibdata1 file.
    """
    como_version = ComoVersion(como_dir)
    como_version.load_cache()
    year_ids = sorted(como_version.year_id)

    dir_path = ("FILEPATH")
    files = [dir_path / f"{year_id}.csv" for year_id in year_ids]
    missing = [file for file in files if not file.exists()]
    if len(missing) > 0:
        raise RuntimeError(f"Expected these missing files {missing}")

    with open(output_file, "w") as out_fp:
        for file in files:
            with open(file, "r") as in_fp:
                if file != files[0]:
                    # advance past the header, except for the first one
                    in_fp.readline()
                out_fp.write(in_fp.read())

    logger.info(f"Consolidated single year files to {output_file}.")


def get_table_info(
    component: str, measure_id: int, year_type: str, process_version_id: int
) -> Tuple[str, str]:
    """Provides the destination table name and host."""
    args = locals()
    component_to_context_id = {"cause": 1, "sequela": 6, "impairment": 3, "injuries": 5}

    compare_context_id = component_to_context_id.get(component)
    if compare_context_id is None:
        raise ValueError(f"Unknown component: {component}")

    pv = GBDProcessVersion(process_version_id)
    process_table = pv.table_info

    table_row = process_table[
        (process_table["measure_id"] == measure_id)
        & (process_table["compare_context_id"] == compare_context_id)
        & (process_table["output_table_name"].str.contains(year_type))
    ]
    if table_row.empty:
        raise ValueError(f"No matching row found in the process table for arguments {args}.")
    if len(table_row) != 1:
        raise ValueError(
            f"More than one row returned in the process table for arguments {args}."
        )

    table_name = table_row.iloc[0]["output_table_name"]
    table_host = table_row.iloc[0]["output_data_host_name"]
    return table_name, table_host


class InternalUploadTaskFactory:
    """Internal upload task factory, creates and returns tasks and associated task
    names.
    """

    def __init__(
        self,
        como_version: ComoVersion,
        component: str,
        measure_id: int,
        year_type: str,
        tool: Tool,
    ):
        self.como_version = como_version
        self.component = component
        self.measure_id = measure_id
        self.year_type = year_type
        command_template = (
            "{python} {script} --como_dir {como_dir} --location_id {location_id} "
            "--component {component} --year_type {year_type} --measure_id {measure_id}"
        )
        template_name = (
            f"como_internal_upload_{self.component}_{self.measure_id}_{self.year_type}"
        )
        self.task_template = tool.get_task_template(
            template_name=template_name,
            command_template=command_template,
            node_args=["location_id"],
            task_args=["como_dir", "component", "measure_id", "year_type"],
            op_args=["python", "script"],
        )

    def get_task_name(self, location_id: int) -> str:
        """Return a task name given a location ID."""
        return name_task(
            base_name=self.task_template.template_name,
            unique_params={"location_id": location_id},
        )

    def get_task(self, location_id: int, upstream_tasks: List[Task]) -> Task:
        """Create and return a task given a location ID and optional upstream location
        ID.
        """
        return self.task_template.create_task(
            python=sys.executable,
            script=_THIS_FILE,
            como_dir=self.como_version.como_dir,
            location_id=location_id,
            component=self.component,
            measure_id=self.measure_id,
            year_type=self.year_type,
            name=self.get_task_name(location_id),
            compute_resources=_INTERNAL_UPLOAD_COMPUTE_RESOURCES,
            upstream_tasks=upstream_tasks,
        )


@click.command
@click.option(
    "como_dir", "--como_dir", type=str, required=True, help="Directory of prior COMO run"
)
@click.option(
    "location_id", "--location_id", type=int, required=True, help="location_id for this task"
)
@click.option(
    "component",
    "--component",
    type=click.Choice([i.value for i in como_constants.Component]),  # type: ignore
    required=True,
    help="component for this task",
)
@click.option(
    "year_type",
    "--year_type",
    type=click.Choice([i.value for i in como_constants.YearType]),  # type: ignore
    required=True,
    help="year_type for this task",
)
@click.option(
    "measure_id", "--measure_id", type=int, required=True, help="measure_id for this task"
)
def run_internal_upload_task(
    como_dir: str, location_id: int, component: str, year_type: str, measure_id: int
) -> None:
    """Run internal upload task."""
    if measure_id not in ALLOWED_MEASURES:
        raise ValueError(f"measure_id {measure_id} not one of {ALLOWED_MEASURES}")

    como_version = ComoVersion(como_dir)
    como_version.load_cache()

    table, host = get_table_info(
        component=component,
        measure_id=measure_id,
        year_type=year_type,
        process_version_id=como_version.gbd_process_version_id,
    )

    conn_def = conn_defs.get_conn_def_from_host_name_and_user_name(
        host_name=host, user_name="USERNAME"
    )

    # multi-year files are already consoldiated into a single file
    delete_after_upload = False
    file_path = ("FILEPATH")
    if year_type == como_constants.YearType.SINGLE:
        consolidate_year_summaries(
            component=component,
            location_id=location_id,
            measure_id=measure_id,
            como_dir=como_version.como_dir,
            output_file=file_path,
        )
        delete_after_upload = True

    with db_tools_core.session_scope(conn_def=conn_def) as scoped_session:
        infiler = Infiles(table, "gbd", scoped_session)
        logger.info(f"Conn def = {conn_def}, table = {table}, file_path = {file_path}")
        infiler.infile(
            path=str(file_path), commit=False, with_replace=False, rename_cols={"mean": "val"}
        )

    if delete_after_upload:
        file_path.unlink()
        logger.info(f"Deleted {file_path}")


if __name__ == "__main__":
    run_internal_upload_task()
