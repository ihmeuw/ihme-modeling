import pathlib

import click
import pandas as pd

import ihme_cc_risk_utils

from ihme_cc_paf_calculator.lib import constants, io_utils

# Set min exposure percentile to 0.01 (1st percentile); max = 1 - min = 0.99 (99th percentile)
EXPOSURE_PERCENTILE: float = 0.01


@click.command
@click.option(
    "output_dir",
    "--output_dir",
    type=str,
    required=True,
    help="root directory of a specific paf calculator run",
)
def main(output_dir: str) -> None:
    """Cache exposure min and max for continuous risks."""
    _run_task(output_dir)


def _run_task(root_dir: str) -> None:
    """Cache exposure min and max for continuous risks. For risks with 2-stage mediation,
    we also find the min and max for all mediator risks. The cached dataframe will contain
    data for all risks, identified by the rei_id column.
    """
    root_dir = pathlib.Path(root_dir)

    settings = io_utils.read_settings(root_dir)
    all_rei_metadata = io_utils.get(root_dir, constants.CacheContents.REI_METADATA)
    rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA)
    mediators = rr_metadata[rr_metadata["source"] == "delta"]["med_id"].unique().tolist()
    if mediators:
        mediator_rei_metadata = io_utils.get(
            root_dir, constants.CacheContents.MEDIATOR_REI_METADATA
        )
        all_rei_metadata = pd.concat([all_rei_metadata, mediator_rei_metadata])
    me_ids = io_utils.get(root_dir, constants.CacheContents.MODEL_VERSIONS)

    # get the exposure min and max for the distal and any mediator risks
    min_max_dfs = []
    for rei_id in [settings.rei_id] + mediators:
        exposure = ihme_cc_risk_utils.get_exposure(
            rei_id=rei_id,
            release_id=settings.release_id,
            me_ids=me_ids,
            rei_metadata=all_rei_metadata[all_rei_metadata["rei_id"] == rei_id],
        )
        exposure_min_max = ihme_cc_risk_utils.get_exposure_min_max(
            rei_id=rei_id,
            exposure=exposure,
            rei_metadata=all_rei_metadata[all_rei_metadata["rei_id"] == rei_id],
            percentile=EXPOSURE_PERCENTILE,
            return_min_or_max=False,
        )
        min_max_dfs.append(exposure_min_max)
    all_exposure_min_max = pd.concat(min_max_dfs)

    io_utils.write_exposure_min_and_max_cache(root_dir, all_exposure_min_max)


if __name__ == "__main__":
    main()
