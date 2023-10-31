import click

from imported_cases.lib import generate as lib_generate
from imported_cases.lib import workflow as lib_workflow


@click.command()
@click.argument("gbd_round_id", type=click.INT)
@click.argument("decomp_step", type=click.STRING)
def workflow(gbd_round_id: int, decomp_step: str) -> None:
    """Generate imported cases for spacetime restricted causes.

    Args:
        gbd_round_id: ID of the GBD round for the imported cases generation
        decomp_step: decomp step for the imported cases generation
    """
    lib_workflow.run_workflow(gbd_round_id=gbd_round_id, decomp_step=decomp_step)


@click.command()
@click.argument("cause_id", type=click.INT)
@click.argument("version_id", type=click.INT)
@click.argument("gbd_round_id", type=click.INT)
@click.argument("decomp_step", type=click.STRING)
@click.argument("output_dir", type=click.STRING)
@click.option("--debug", is_flag=True)
def run_cause(
    cause_id: int,
    version_id: int,
    gbd_round_id: int,
    decomp_step: str,
    output_dir: str,
    debug: bool,
) -> None:
    """Generate imported cases for the given cause.

    Generates 1000 draws of imported cases and uploads to the database
    via save_results_cod, given that there is data for the cause in the
    relevant (restricted) locations for that cause.

    Args:
        cause_id: ID of the cause to generate imported cases
        version_id: imported cases version id
        gbd_round_id: ID of the GBD round for the imported cases generation
        decomp_step: decomp step for the imported cases generation
        output_dir: the directory of the imported cases run
        debug: whether to drop into a debugging session
    """
    if debug:
        import pdb

        pdb.set_trace()

    lib_generate.run_imported_cases(
        cause_id=cause_id,
        output_dir=output_dir,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        version_id=version_id,
    )
