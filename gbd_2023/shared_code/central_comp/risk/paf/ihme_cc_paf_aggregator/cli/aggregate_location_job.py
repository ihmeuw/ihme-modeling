from pathlib import Path

import click
import pandas as pd

from ihme_cc_cache import FileBackedCacheReader

from ihme_cc_paf_aggregator.cli import utils as cli_utils
from ihme_cc_paf_aggregator.lib import (
    aggregation,
    attribution,
    constants,
    custom_pafs,
    hierarchy,
    io,
    logging_utils,
    restrictions,
)

logger = logging_utils.module_logger(__name__)


@click.command
@click.option(
    "manifest_path",
    "--manifest_path",
    type=str,
    required=True,
    callback=cli_utils.read_access_path,
    help="Path to a cache manifest.",
)
@click.option(
    "location_id",
    "--location_id",
    type=int,
    required=True,
    help="Location ID for this aggregation",
)
def _aggregate(manifest_path: Path, location_id: int) -> None:
    """Performs PAF aggregation up a risk hierarchy."""
    aggregate(**locals())


def aggregate(manifest_path: Path, location_id: int) -> None:
    """Performs PAF aggregation up a risk hierarchy.

    Arguments:
        manifest_path: Path to a cache manifest json.
        location_id: Location ID for this aggregation
    """
    # Log the arguments
    for arg, value in locals().items():
        logger.info(f"{Path(__file__).name} called with {arg}: {value}")

    # Read in the driving DataFrames
    cache_reader = FileBackedCacheReader(manifest_path)
    input_models = cache_reader.get(constants.CacheContents.INPUT_MODELS)
    risk_hierarchy = cache_reader.get(constants.CacheContents.RISK_HIERARCHY)
    mediation_matrix = cache_reader.get(constants.CacheContents.MEDIATION_MATRIX)
    age_metadata = cache_reader.get(constants.CacheContents.AGE_METADATA)
    settings = constants.PafAggregatorSettings(
        **cache_reader.get(constants.CacheContents.SETTINGS)
    )

    # read two types of input pafs
    total_pafs = io.read_total_pafs(
        input_models_df=input_models,
        location_id=location_id,
        year_ids=settings.year_id,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
    )
    unmediated_pafs = io.read_unmediated_pafs(
        input_models_df=input_models,
        location_id=location_id,
        year_ids=settings.year_id,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
    )
    if unmediated_pafs.empty:
        # empty dataframe with correct columns, for simpler null processing
        unmediated_pafs = pd.DataFrame(columns=total_pafs.columns)

    # Calculate and append on custom PAFs prior to aggregation
    total_pafs = custom_pafs.calculate_and_append_custom_pafs(total_pafs, settings.release_id)

    # 100% attributable risk/causes need to be inserted prior to aggregation
    total_pafs = attribution.append_pafs_of_one(
        location_id=location_id,
        year_ids=settings.year_id,
        paf_df=total_pafs,
        release_id=settings.release_id,
    )
    unmediated_pafs = attribution.append_pafs_of_one(
        location_id=location_id,
        year_ids=settings.year_id,
        paf_df=unmediated_pafs,
        release_id=settings.release_id,
    )

    # apply any cause level restrictions
    total_pafs = restrictions.apply_restrictions_from_db(total_pafs, settings.release_id)
    unmediated_pafs = restrictions.apply_restrictions_from_db(
        unmediated_pafs, settings.release_id
    )

    # apply any special restrictions
    total_pafs = restrictions.apply_special_restrictions(total_pafs, age_metadata)
    unmediated_pafs = restrictions.apply_special_restrictions(unmediated_pafs, age_metadata)

    total_pafs = hierarchy.subset_risk_outcome_pairs(
        total_pafs, risk_hierarchy, settings.all_paf_causes
    )

    unmediated_pafs = hierarchy.subset_risk_outcome_pairs(
        unmediated_pafs, risk_hierarchy, settings.all_paf_causes
    )

    # do the aggregation
    if settings.skip_aggregation:
        aggregated_df = pd.DataFrame()
    else:
        aggregation_hierarchy = risk_hierarchy[
            [constants.REI_ID, constants.PARENT_ID]
        ].drop_duplicates()
        aggregated_df = aggregation.aggregate_pafs(
            total_pafs, aggregation_hierarchy, mediation_matrix, unmediated_pafs
        )

    # write the result
    inputs_and_aggregates = pd.concat([total_pafs, aggregated_df], ignore_index=True)

    io.write_output_for_location(
        inputs_and_aggregates, location_id, settings.year_id, Path(settings.output_dir)
    )
    logger.info("Finished")


if __name__ == "__main__":
    _aggregate()
