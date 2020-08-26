import argparse
import dataclasses
from typing import List, Optional

import gbd

from hale import initialize
from hale.common import logging_utils
from hale.common.constants import connection_definitions


@dataclasses.dataclass
class InitializationArgs:
    conn_def: str
    population_run_id: Optional[int]
    life_table_run_id: Optional[int]
    como_version: Optional[int]
    gbd_round_id: int
    decomp_step: str
    location_set_ids: List[int]
    year_ids: Optional[List[int]]
    draws: int


def parse_args() -> InitializationArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conn_def',
        type=str,
        choices=[connection_definitions.GBD, connection_definitions.GBD_TEST],
        default=connection_definitions.GBD,
        help='Connection definition for uploading to the GBD database'
    )
    parser.add_argument(
        '--population_run_id',
        type=int,
        required=False,
        help=(
            'Mortality run ID with which to pull populations. '
            'Defaults to current best run ID'
        )
    )
    parser.add_argument(
        '--life_table_run_id',
        type=int,
        required=False,
        help=(
            'Mortality run ID with which to pull life tables. '
            'Defaults to current best run ID'
        )
    )
    parser.add_argument(
        '--como_version',
        type=int,
        required=False,
        help=(
            'Version with which to pull YLDs. '
            'Defaults to current best COMO version'
        )
    )
    parser.add_argument(
        '--gbd_round_id',
        type=int,
        default=gbd.constants.GBD_ROUND_ID,
        help='GBD round ID to run HALE for'
    )
    parser.add_argument(
        '--decomp_step',
        type=str,
        required=True,
        choices=list(gbd.constants.decomp_step.values()),
        help='Decomp step to run HALE for'
    )
    parser.add_argument(
        '--location_set_ids',
        type=int,
        required=True,
        nargs='+',
        help='One or more location set IDs to run HALE for'
    )
    parser.add_argument(
        '--year_ids',
        type=int,
        required=False,
        nargs='+',
        help='Years to run HALE for'
    )
    parser.add_argument(
        '--draws',
        type=int,
        default=1000,
        help='Number of draws to run HALE for'
    )
    args = vars(parser.parse_args())
    return InitializationArgs(**args)


def main() -> None:
    logging_utils.configure_logging()
    args = parse_args()
    initialize.initialize(
        args.conn_def,
        args.population_run_id,
        args.life_table_run_id,
        args.como_version,
        args.gbd_round_id,
        args.decomp_step,
        args.location_set_ids,
        args.year_ids,
        args.draws
    )


if __name__ == '__main__':
    main()
