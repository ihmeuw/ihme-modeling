import click
import pandas as pd

from ihme_cc_sev_calculator.lib import constants, io, logging_utils, parameters, sev

logger = logging_utils.module_logger(__name__)


@click.command
@click.option("rei_id", "--rei_id", type=int, help="REI ID to calculate SEVs for.")
@click.option(
    "location_id", "--location_id", type=int, help="Location ID to calculate SEVs for."
)
@click.option("version_id", "--version_id", type=int, help="Internal version ID.")
def calculate_sev(rei_id: int, location_id: int, version_id: int) -> None:
    """Calculate SEVs for the given REI ID."""
    params = parameters.Parameters.read_from_cache(version_id)
    rei_metadata = params.read_rei_metadata().query(f"rei_id == {rei_id}")
    pafs_of_one = params.read_file("pafs_of_one").query(f"rei_id == {rei_id}")
    logger.info(
        f"Starting SEV calculation for '{rei_metadata['rei_name'].iat[0]}', "
        f"REI ID {rei_id}."
    )

    rr_max_df = io.read_rr_max_draws(rei_id, params.output_dir)

    # Pull PAFs
    if rei_id in constants.CONVERTED_OUTCOMES_REI_IDS:
        # The PAF Calculator converts outcomes for a few risks (BMD, occupational noise) to
        # match GBD causes. The SEV Calculator directly reads the pre-conversion versions so
        # as to match causes in RRmax.
        logger.info("Reading intermediate PAF draws prior to conversion to GBD causes.")
        paf_df = io.read_pre_conversion_paf_draws(
            rei_id=rei_id,
            location_id=location_id,
            year_ids=params.year_ids,
            n_draws=params.n_draws,
        )
    else:
        # Read PAF draws directly from PAF Aggregator outputs
        logger.info(f"Reading PAF draws from PAF Aggregator {params.paf_version_id}.")
        paf_df = io.read_paf_draws(
            rei_id=rei_id,
            location_id=location_id,
            year_ids=params.year_ids,
            n_draws=params.n_draws,
            paf_version_id=params.paf_version_id,
        )

        # A handful of risks like LBW/SG and CKD are estimated for parent causes but split
        # into child causes within the PAF Calculator. To match RR draws, we do the opposite
        # here: aggregate child PAFs to the parent cause, weighing by cause burden.
        # Unrelated PAFs are passed back unaffected.
        if constants.LBWSG_PARENT_CAUSE_ID in rr_max_df["cause_id"].unique():
            logger.info("Aggregating PAF draws to parent cause: LBW/SG.")
            paf_df = sev.aggregate_paf_to_parent_cause(
                paf_df=paf_df,
                rr_max_df=rr_max_df,
                parent_id=constants.LBWSG_PARENT_CAUSE_ID,
                location_id=location_id,
                year_id=constants.ARBITRARY_MACHINERY_YEAR_ID,
                release_id=params.release_id,
                n_draws=params.n_draws,
                draw_cols=params.draw_cols,
                codcorrect_version_id=params.codcorrect_version_id,
                como_version_id=None,
                population=None,
                cause_metadata=params.read_cause_metadata(constants.LBWSG_CAUSE_SET_ID),
            )

        for parent_cause_id in constants.OTHER_PARENT_CAUSE_IDS_TO_AGGREGATE_PAFS_FOR:
            if parent_cause_id in rr_max_df["cause_id"].unique():
                logger.info(f"Aggregating PAF draws to parent cause: {parent_cause_id}.")
                paf_df = sev.aggregate_paf_to_parent_cause(
                    paf_df=paf_df,
                    rr_max_df=rr_max_df,
                    parent_id=parent_cause_id,
                    location_id=location_id,
                    year_id=constants.ARBITRARY_MACHINERY_YEAR_ID,
                    release_id=params.release_id,
                    n_draws=params.n_draws,
                    draw_cols=params.draw_cols,
                    codcorrect_version_id=params.codcorrect_version_id,
                    como_version_id=params.como_version_id,
                    population=params.read_population(constants.DEFAULT_LOCATION_SET_ID),
                    cause_metadata=params.read_cause_metadata(
                        constants.COMPUTATION_CAUSE_SET_ID
                    ),
                )

    pafs_of_one_df = paf_df.query(f"cause_id.isin({pafs_of_one['cause_id'].tolist()})")
    paf_df = paf_df.query(f"~cause_id.isin({pafs_of_one['cause_id'].tolist()})")

    logger.info("Calculating SEVs.")
    indexes_must_match = sev.paf_and_rr_max_indexes_must_match(
        rei_id=rei_id,
        aggregate_rei_ids=params.aggregate_rei_ids,
        rei_metadata=params.read_rei_metadata(constants.AGGREGATION_REI_SET_ID),
    )
    sev_df = sev.calculate_sevs(
        paf_df=paf_df,
        rr_max_df=rr_max_df,
        draw_cols=params.draw_cols,
        indexes_must_match=indexes_must_match,
    )

    # Handling for SEVs for select PAFs of 1
    if rei_id in constants.SEVS_FOR_PAFS_OF_ONE_REI_IDS:
        logger.info("Adding SEVs for PAFs of 1.")
        sevs_for_pafs_of_one_df = sev.calculate_sevs_for_pafs_of_one(
            rei_id=rei_id,
            paf_of_one_df=pafs_of_one_df,
            pafs_of_one=pafs_of_one,
            location_id=location_id,
            year_ids=params.year_ids,
            estimation_year_ids=params.read_file("demographics")["year_id"],
            release_id=params.release_id,
            n_draws=params.n_draws,
            me_ids=params.read_rei_me_ids(rei_id),
            draw_cols=params.draw_cols,
            root_dir=params.output_dir,
        )
        sev_df = pd.concat([sev_df, sevs_for_pafs_of_one_df]).reset_index(drop=True)

    logger.info("Saving SEV draws.")

    # Save draws by cause if requested
    if params.by_cause:
        io.save_sev_draws(
            sev_df=sev_df,
            rei_id=rei_id,
            location_id=location_id,
            draw_cols=params.draw_cols,
            root_dir=params.output_dir,
            by_cause=True,
        )

    sev_df = sev.average_sevs_across_causes(sev_df)
    io.save_sev_draws(
        sev_df=sev_df,
        rei_id=rei_id,
        location_id=location_id,
        draw_cols=params.draw_cols,
        root_dir=params.output_dir,
        by_cause=False,
    )


if __name__ == "__main__":
    calculate_sev()
