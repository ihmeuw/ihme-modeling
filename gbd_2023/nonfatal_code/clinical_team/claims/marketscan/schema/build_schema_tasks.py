"""Create a jobmon dag to process data by year, is_otp, in_std_sample, is_duplicate, which
are 4 of the 5 partition columns.
"""

import itertools
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from crosscutting_functions.clinical_constants.pipeline import marketscan as constants
from jobmon.client.task_template import TaskTemplate
from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow

from marketscan.schema import config


@dataclass(eq=True, frozen=True)
class ParamStore:
    """Small class to hold task parameters."""

    year: int
    is_otp: int
    in_std_sample: int
    is_duplicate: int
    write_denominator: int
    mem: str


def task_builder(
    workflow: Workflow, template: TaskTemplate, script_path: str, years: List[int]
) -> Workflow:
    """Appends tasks to the workflow."""

    total_denominator_writes = 0
    for task_params in build_parameters(years):
        total_denominator_writes += task_params.write_denominator

        task = template.create_task(
            name=(
                f"MS_{task_params.year}_{task_params.is_otp}_{task_params.in_std_sample}_"
                f"{task_params.is_duplicate}_{task_params.write_denominator}"
            ),
            script_path=script_path,
            python=sys.executable,
            year=task_params.year,
            is_otp=task_params.is_otp,
            in_std_sample=task_params.in_std_sample,
            is_duplicate=task_params.is_duplicate,
            write_denominator=task_params.write_denominator,
        )
        task.update_compute_resources(memory=task_params.mem)

        workflow.add_task(task)

    if total_denominator_writes != len(years):
        raise RuntimeError(
            "There should be exactly as many write denominator args as years, eg 1 per year"
        )
    return workflow


def get_mem(is_otp: int, year: int) -> str:
    """Assign mem based on inpatient vs outpatient and year."""
    settings = config.get_settings()

    if settings.e2e_test:
        return "2G"

    if year == 2000:
        return "50G"

    if is_otp:
        return "400G" if year >= 2015 else "600G"
    else:
        return "75G" if year >= 2015 else "150G"


def build_parameters(years: List[int]) -> List[ParamStore]:
    """Logic to build the jobmon workflow."""

    all_combos = list(
        itertools.product(
            years, constants.IS_OTPS, constants.IN_STD_SAMPLES, constants.IS_DUPLICATES
        )
    )

    # there is no duplicated inpatient data
    filtered = [
        (year, is_otp, in_std_sample, is_duplicate)
        for (year, is_otp, in_std_sample, is_duplicate) in all_combos
        if not (is_duplicate and not is_otp)
    ]
    # add a write_denominator-1 parameter for one inpatient task per year
    with_write_arg = [
        (
            (year, is_otp, in_std_sample, is_duplicate, 1)
            if not is_otp and not in_std_sample
            else (year, is_otp, in_std_sample, is_duplicate, 0)
        )
        for (year, is_otp, in_std_sample, is_duplicate) in filtered
    ]

    # use the param store to save task args rather than a 5 element tuple
    with_mem = [
        (
            ParamStore(
                year=year,
                is_otp=is_otp,
                in_std_sample=in_std_sample,
                is_duplicate=is_duplicate,
                write_denominator=write_denominator,
                mem=get_mem(is_otp, year),
            )
        )
        for (year, is_otp, in_std_sample, is_duplicate, write_denominator) in with_write_arg
    ]

    if len(with_mem) != len(set(with_mem)):
        raise RuntimeError("There are duplicate tasks in the DAG")
    if len(with_mem) != len(years) * 6:
        raise RuntimeError("We expect exactly 6 tasks per year")
    return with_mem


def create_ms_schema_years(years: List[int]) -> Optional[str]:
    """Main function for creating MS yearly schema."""
    settings = config.get_settings()

    script_path = str(Path(__file__).with_name("create_yearly_ms_schema.py"))

    tool = Tool(name="marketscan_schema")

    # create workflow
    workflow = tool.create_workflow(
        name="create_marketscan_schema", description="icd_mart_marketscan_schema_creation"
    )

    err_dir = "FILEPATH"
    out_dir = "FILEPATH"

    os.makedirs(err_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)

    template = tool.get_task_template(
        default_cluster_name="slurm",
        default_compute_resources={
            "queue": "all.q",
            "cores": 2,
            "memory": "20G",
            "runtime": "48h",
            "stderr": err_dir,
            "stdout": out_dir,
            "project": "proj_hospital",
        },
        template_name="marketscan_schema",
        node_args=["year", "is_otp", "in_std_sample", "is_duplicate", "write_denominator"],
        command_template=(
            "{python} {script_path} --year {year} --is_otp {is_otp} "
            "--in_std_sample {in_std_sample} --is_duplicate {is_duplicate} "
            "--write_denominator {write_denominator}"
        ),
        op_args=["python", "script_path"],
    )

    workflow = task_builder(
        workflow=workflow, template=template, script_path=script_path, years=years
    )

    workflow_run = workflow.run()
    print(f"Workflow finished with status: {workflow_run}")

    return workflow_run