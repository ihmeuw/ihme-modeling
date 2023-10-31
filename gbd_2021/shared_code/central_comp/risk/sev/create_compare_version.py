"""Script called at end of run to create a compare version with all involved GBD processes."""
from argparse import ArgumentParser, Namespace
import logging

import pandas as pd
from sqlalchemy import orm

import db_tools_core
from gbd.constants import gbd_metadata_type, gbd_process, gbd_process_version_status
from gbd_outputs_versions import CompareVersion

import upload


SEV_OUTPUT_DIR = "FILEPATH"


def parse_args() -> Namespace:
    """Parse arguments."""
    parser = ArgumentParser()
    parser.add_argument("--version_id", type=str, help="Version of the SEV calculator run")
    parser.add_argument("--gbd_round_id", type=int, help="GBD round id")
    parser.add_argument("--decomp_step", type=str, help="Decomp step")
    parser.add_argument(
        "--sevs_were_generated",
        type=int,
        help="Boolean representing if SEVs were generated (1) or not (0) for this run"
    )

    return parser.parse_args()


def main() -> None:
    """Create the compare version with inputs to and outputs from this SEV calculator run."""
    args = parse_args()
    
    # Set SEV and RR max versions to same value
    sev_version = rr_max_version = args.version_id

    sev_metadata = pd.read_csv(f"{SEV_OUTPUT_DIR}/{args.version_id}/version.csv")
    como_version = sev_metadata["como_version"].iat[0]
    codcorrect_version = sev_metadata["codcorrect_version"].iat[0]

    # Convert internal versions into GBD process versions for compare version
    with db_tools_core.session_scope("gbd") as session:
        process_version_ids = [
            upload.get_gbd_process_version_id(
                gbd_metadata_type.RR_MAX,
                rr_max_version,
                [gbd_process_version_status.BEST, gbd_process_version_status.ACTIVE],
                session,
            )
        ]

        # If SEVs were generated, add SEV, CodCorrect, and COMO versions to list of
        # process version ids. Otherwise, only RR max was made with no other machinery inputs
        if bool(args.sevs_were_generated):
            process_version_ids += [
                upload.get_gbd_process_version_id(
                    metadata_type_id,
                    internal_version,
                    [gbd_process_version_status.BEST, gbd_process_version_status.ACTIVE],
                    session,
                )
                for metadata_type_id, internal_version in [
                    (gbd_metadata_type.SEV, sev_version),
                    (gbd_metadata_type.COMO, como_version),
                    (gbd_metadata_type.CODCORRECT, codcorrect_version),
                ]
            ]
            description = (
                f"SEV v{sev_version}, RR max v{rr_max_version}, "
                f"CoDCorrect v{codcorrect_version}, COMO v{como_version}"
            )
        else:
            description = (
                f"RR max v{rr_max_version}"
            )

    # make GBD compare version and mark active
    cv = CompareVersion.add_new_version(
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        compare_version_description=description,
    )
    cv.add_process_version(process_version_ids)
    cv._update_status(gbd_process_version_status.ACTIVE)

    logging.info(f"Compare version {cv.compare_version_id} created: '{description}'")


if __name__ == "__main__":
    main()
