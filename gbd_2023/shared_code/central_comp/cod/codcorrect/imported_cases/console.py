from typing import Optional

import click

from imported_cases.lib import generate as lib_generate
from imported_cases.lib import workflow as lib_workflow


@click.command()
@click.argument("release_id", type=click.INT)
@click.option("--test", is_flag=True)
@click.option("--resume", is_flag=True)
@click.option("--version_id", type=click.INT)
def workflow(release_id: int, test: bool, resume: bool, version_id: Optional[int]) -> None:
    """Generate imported cases for spacetime restricted causes.

    Args:
        release_id: ID of the GBD release for the imported cases generation
        test: whether run is a test or not. Doesn't mark best if so
        resume: resume run or not
        version_id: version id to resume if resuming
    """
    lib_workflow.run_workflow(
        release_id=release_id, test=test, resume=resume, version_id=version_id
    )


@click.command()
@click.argument("cause_id", type=click.INT)
@click.argument("version_id", type=click.INT)
@click.argument("release_id", type=click.INT)
@click.argument("output_dir", type=click.STRING)
@click.argument("test", type=click.INT)
@click.option("--debug", is_flag=True)
def run_cause(
    cause_id: int, version_id: int, release_id: int, output_dir: str, test: int, debug: bool
) -> None:
    """Generate imported cases for the given cause.

    Generates 1000 draws of imported cases and uploads to the database
    via save_results_cod, given that there is data for the cause in the
    relevant (restricted) locations for that cause.

    Args:
        cause_id: ID of the cause to generate imported cases
        version_id: imported cases version id
        release_id: ID of the GBD release for the imported cases generation
        output_dir: the directory of the imported cases run
        test: whether run is a test (1) or not (0). Doesn't mark best if so
        debug: whether to drop into a debugging session
    """
    if debug:
        import pdb

        pdb.set_trace()

    lib_generate.run_imported_cases(
        cause_id=cause_id,
        output_dir=output_dir,
        release_id=release_id,
        version_id=version_id,
        test=test,
    )
