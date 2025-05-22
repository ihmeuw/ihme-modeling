import pathlib

import click
import pandas as pd

from gbd.constants import measures

from ihme_cc_paf_calculator.lib import constants, io_utils
from ihme_cc_paf_calculator.lib.custom_pafs import injuries, occupational_noise


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
    help="location to convert PAFs for",
)
def main(output_dir: str, location_id: int) -> None:
    """Converts non-cause PAFs to standard cause PAFs for a location."""
    _run_task(output_dir, location_id)


def _run_task(root_dir: str, location_id: int) -> None:
    """Converts non-cause PAFs to standard cause PAFs for a location.

    For two risks, low bone mineral density and occupational noise, the RRs are not
    modeled for standard GBD causes. In both cases, we perform a post-processing
    step on the PAFs after we calculate them in order to convert them to cause PAFs.

    First, the original pre-converted PAFs are filtered to a single measure and saved
    for the SEV Calculator to read directly so as to match causes in RRs.

    Next the PAFs are converted. For BMD, the conversion replaces hip/non-hip
    fractures with GBD injury causes. For occupational noise, the conversion
    aggregates sequela PAFs to the hearing loss cause. After conversion, the new PAFs
    are saved and PAFs for the original causes are deleted.

    We do this separately after the PAF calculation because PAFs for all causes are
    needed for the conversion and PAF calculation is run by cause.
    """
    root_dir = pathlib.Path(root_dir)
    settings = io_utils.read_settings(root_dir)

    # Set up some parameters for the risk being run
    if settings.rei_id == constants.BMD_REI_ID:
        input_paf_causes = constants.FRACTURE_CAUSE_IDS
        measure_id = measures.YLL
    elif settings.rei_id == constants.OCCUPATIONAL_NOISE_REI_ID:
        # We've validated that the "causes" in the RRs are the set of hearing sequelae
        rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA)
        input_paf_causes = rr_metadata["cause_id"].tolist()
        measure_id = measures.YLD
    else:
        raise RuntimeError(
            "Internal error: PAF cause conversion can only be run on REI IDs "
            f"{constants.BMD_REI_ID} and {constants.OCCUPATIONAL_NOISE_REI_ID}, "
            f"not {settings.rei_id}."
        )

    # Read in PAFs for non GBD causes (fractures or sequela)
    pafs = []
    for cause_id in input_paf_causes:
        pafs.append(io_utils.read_paf(root_dir, location_id, cause_id))

    pafs = pd.concat(pafs, ignore_index=True)

    # Save original pre-converted PAFs for the SEV Calculator, subsetting to
    # a single measure
    io_utils.write_paf_for_sev_calculator(
        df=pafs.query(f"measure_id == {measure_id}"),
        rei_id=settings.rei_id,
        location_id=location_id,
    )

    # Perform the appropriate conversion
    if settings.rei_id == constants.BMD_REI_ID:
        converted_pafs = injuries.calculate_injury_pafs(
            pafs, settings.rei_id, settings.release_id
        )
    else:
        converted_pafs = occupational_noise.aggregate_hearing_sequela_to_cause(
            sequela_pafs=pafs,
            location_id=location_id,
            year_id=settings.year_id,
            release_id=settings.release_id,
            n_draws=settings.n_draws,
            como_version_id=settings.como_version_id,
        )

    # Save converted cause PAFs for the PAF Calculator
    io_utils.write_paf(converted_pafs, root_dir, location_id)

    # Delete original draw files (different causes, so they aren't overwritten)
    for cause_id in input_paf_causes:
        file = 
        pathlib.Path(file).unlink()


if __name__ == "__main__":
    main()
