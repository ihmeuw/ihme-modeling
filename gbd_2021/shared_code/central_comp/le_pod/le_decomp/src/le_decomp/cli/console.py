from typing import List, Union

import click

from gbd import constants as gbd_constants

from le_decomp.legacy import le_master

StringList = str


@click.command()
@click.argument("cause_set_id", type=click.INT, default=2)
@click.argument("cause_level", type=(click.INT, click.STRING), default="most_detailed")
@click.argument("location_set_id", type=click.INT, default=35)
@click.argument("compare_version_id", type=click.INT)
@click.argument("gbd_round_id", type=click.INT, default=gbd_constants.GBD_ROUND_ID)
@click.argument("decomp_step", type=click.STRING, default="iterative")
@click.argument("sex_ids", type=click.STRING, default="1 2 3")
@click.argument("start_years", type=click.STRING, default="")
@click.argument("end_years", type=click.STRING, default="")
@click.argument("year_ids", type=click.STRING, default="")
@click.argument("environment", type=click.STRING, default="dev")
@click.option("--verbose", is_flag=True, help="Keep track of file writing progress")
@click.argument("n_processes", type=click.INT, default=8)
def le_decomp(
    cause_set_id: int,
    cause_level: Union[int, str],
    location_set_id: int,
    compare_version_id: int,
    gbd_round_id: int,
    decomp_step: str,
    sex_ids: StringList,
    start_years: StringList,
    end_years: StringList,
    year_ids: StringList,
    environment: str,
    verbose: bool,
    n_processes: int,
) -> None:
    """Run life expectancy decomp.

    Args:
        cause_set_id: id of the cause set. Defaults to 2
        cause_level: the cause level to run for. Can be a string ('most_detailed') or
            an int (1, 2)
        location_set_id: id of the location set. Defaults to 35
        compare_version_id: compare version to pull mortality rates from
        gbd_round_id: ID of the GBD round for the imported cases generation.
            Defaults to current GBD round
        decomp_step: decomp step for the imported cases generation (GBD decomp step).
            Defaults to iterative
        sex_ids: sex ids to run as a space-separated string. Defaults to sexs 1, 2, and 3
        start_years: start years for LE comparision as a space-separated string.
            Defaults to []
        end_years: end years for LE comparision as a space-separated string. Defaults to []
        year_ids: years to compare. All possible combinations of year pairs will be run.
            Defaults to []
        environment: prod or dev environment. Defaults to 'dev'
        verbose: Whether to print file writing process or not. Defaults to off
        n_processes: number of processes to run at a time. Defaults to 8.
    """
    try:
        cause_level = int(cause_level)
    except ValueError:
        pass

    le_decomp_runner = le_master.LEMaster(
        cause_set_id=cause_set_id,
        cause_level=cause_level,
        location_set_id=location_set_id,
        compare_version_id=compare_version_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        sex_ids=_process_string_list(sex_ids),
        start_years=_process_string_list(start_years),
        end_years=_process_string_list(end_years),
        year_ids=_process_string_list(year_ids),
        env=environment,
        verbose=verbose,
    )
    le_decomp_runner.run_all(n_processes)


def _process_string_list(param: StringList) -> List[int]:
    """Convert a space-separated string parameter to a list of ints.

    Empty strings become empty lists.

    Ex: "1990 1995 2000" -> [1990, 1995, 2000]
    """
    if not param:
        return []

    try:
        result = [int(value) for value in param.split(" ")]
    except ValueError:
        raise ValueError(f"Unable to convert parameter '{param}' to list of ints")

    return result
