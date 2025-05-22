import pathlib

import click

from ihme_cc_paf_calculator.lib import io_utils


@click.command
@click.option(
    "output_dir",
    "--output_dir",
    type=str,
    required=True,
    help="root directory of a specific paf calculator run",
)
@click.option(
    "location_id",
    "--location_id",
    type=int,
    required=True,
    help="location to delete cached exposure and tmrel for",
)
def main(output_dir: str, location_id: int) -> None:
    """Deletes cached exposure, exposure_sd, and tmrel draws for a location."""
    _run_task(output_dir, location_id)


def _run_task(root_dir: str, location_id: int) -> None:
    """Deletes cached exposure, exposure_sd, and tmrel draws for a location."""
    root_dir = pathlib.Path(root_dir)
    io_utils.delete_exposure_tmrel_cache(root_dir, location_id)


if __name__ == "__main__":
    main()
