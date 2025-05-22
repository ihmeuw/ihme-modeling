from dataclasses import field, make_dataclass
from typing import Union

import click
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import (
    InpatientWrappers,
)

from inpatient.Clinical_Runs.utils.constants import RunDBSettings
from inpatient.Envelope.bundle_estimates import (
    create_five_year_bundle_estimates as five_year,
)
from inpatient.Envelope.bundle_estimates import (
    create_single_year_bundle_estimates as single_year,
)


@click.command()
@click.option("--age_group", required=True, type=click.INT, help="GBD age group")
@click.option("--sex", required=True, type=click.INT, help="GBD sex id")
@click.option("--year_start", required=True, type=click.INT, help="Year start id")
@click.option("--run_id", required=True, type=click.INT, help="Clinical run directory")
@click.option("--draws", required=True, type=click.INT, help="Number of draws")
@click.option(
    "--bin_years", required=True, type=click.BOOL, help="Aggergation to five year bins"
)
def main(age_group, sex, year_start, run_id, draws, bin_years):
    iw = InpatientWrappers(run_id, RunDBSettings.iw_profile)
    map_version = iw.pull_run_metadata()["map_version"].item()
    config_args = make_dataclass(
        "Config_Args",
        [
            ("age_group", int, field(default=age_group)),
            ("sex", int, field(default=sex)),
            ("year_start", int, field(default=year_start)),
            ("run_id", int, field(default=run_id)),
            ("draws", int, field(default=draws)),
            ("map_version", int, field(default=map_version)),
            ("bin_years", bool, field(default=bin_years)),
        ],
        frozen=True,
    )
    if bin_years:
        five_year.main(config_args)
    else:
        single_year.main(config_args)


if __name__ == "__main__":
    main()
