import pathlib
from typing import Optional, Tuple

import click
import pandas as pd

import ihme_cc_averted_burden
import ihme_cc_risk_utils
from gbd.constants import release

from ihme_cc_paf_calculator.lib import (
    constants,
    input_utils,
    intervention_utils,
    io_utils,
    logging_utils,
    math,
)
from ihme_cc_paf_calculator.lib.custom_pafs import (
    air_pmhap,
    averted_burden,
    blood_lead,
    drug_dependence,
    hiv,
    intimate_partner_violence,
    lbwsg,
    subcause_split_utils,
)

logger = logging_utils.module_logger(__name__)

DROP_COLS = ["model_version_id", "metric_id", "modelable_entity_id", "location_id"]


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
    help="location to calculate PAF for",
)
@click.option(
    "cause_id", "--cause_id", type=int, required=True, help="cause to calculate PAF for"
)
def main(output_dir: str, location_id: int, cause_id: int) -> None:
    """Calculates categorical PAFs for a specific cause/location"""
    _run_task(output_dir, location_id, cause_id)


def _run_categorical_paf(
    root_dir: pathlib.Path,
    location_id: int,
    cause_id: int,
    settings: constants.PafCalculatorSettings,
    rei_metadata: pd.DataFrame,
    rr_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Reads input data from the cache, performs some setup, and calls the math functions
    to calculate categorical PAFs.
    """
    # read cached input models
    exposure = io_utils.get_location_specific(
        root_dir, constants.LocationCacheContents.EXPOSURE, location_id=location_id
    )

    # read relative risk draws for the given cause_id
    rr_is_year_specific = (rr_metadata["year_specific"] == 1).bool()
    rr = ihme_cc_risk_utils.get_rr_draws(
        rei_id=settings.rei_id,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
        cause_id=cause_id,
        year_id=settings.year_id if rr_is_year_specific else None,
        location_id=location_id,
    )

    # Confirm exposure and RR have the same categorical parameters present.
    input_utils.validate_categorical_parameters_match(
        exposure=exposure, rr=rr, root_dir=root_dir
    )

    rr = rr.drop(DROP_COLS + ["exposure"], axis=1)
    rr = ihme_cc_risk_utils.add_tmrel_indicator_to_categorical_data(rr)

    # Invert both exposure and relative risk for when exposure is protective
    # and the risk isn't an Averted Burden drug class for the averted counterfactual.
    flip_exposure_and_rr = (rei_metadata["inv_exp"] == 1).bool()
    if settings.rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
        if (
            settings.rei_id
            == ihme_cc_averted_burden.get_all_rei_ids_for_drug(settings.rei_id).averted_rei_id
        ):
            flip_exposure_and_rr = False

    if flip_exposure_and_rr:
        draw_cols = [f"draw_{i}" for i in range(settings.n_draws)]
        exposure[draw_cols] = 1 - exposure[draw_cols]
        rr[draw_cols] = 1 / rr[draw_cols]

    result = math.calculate_categorical_paf(rr, exposure)
    return result


def _run_continuous_paf(
    root_dir: pathlib.Path,
    location_id: int,
    cause_id: int,
    settings: constants.PafCalculatorSettings,
    rei_metadata: pd.DataFrame,
    mediator_rei_metadata: Optional[pd.DataFrame],
    rr_metadata: pd.DataFrame,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Reads input data from the cache, performs some setup, and calls the math functions
    to calculate continuous PAFs.

    When this risk-cause is partially mediated through another risk, the PAF calculation
    is done twice: once using the modeled RRs to generate total PAFs, and once using
    mediated RRs to generate the unmediated PAF, which is used downstream in risk
    aggregation.

    Additionally, when this risk-cause is calculated using 2-stage mediation with multiple
    mediators, we must calculate the total PAFs through each of the mediators and then
    aggregate the results together to produce the total risk-cause PAF.

    Arguments:
        root_dir: root directory for this PAF calculator run
        location_id: the location this job is running for
        cause_id: the cause this job is running for
        settings: global parameters for this PAF calculator run
        rei_metadata: information about the risk
        mediator_rei_metadata: information about the mediator risk(s) for 2-stage mediation
        rr_metadata: information about the relative risk model. Will contain multiple rows
            if we have multiple 2-stage mediators

    Returns a tuple containing the total PAF dataframe and the unmediated PAF dataframe,
    which will be None if mediation does not apply to this risk-cause.
    """
    cause_metadata = io_utils.get(root_dir, constants.CacheContents.CAUSE_METADATA)

    # Total mediation factors for this risk-cause. Will be empty if mediation doesn't apply
    # or if the risk-cause is 100% mediated, meaning the unmediated PAF is 0 by definition.
    # Mediation factors are not used for 2-stage mediation
    mediation_factors = io_utils.get(root_dir, constants.CacheContents.MEDIATION_FACTORS)
    mediation_factors = (
        mediation_factors
        if mediation_factors.empty
        else mediation_factors.query("cause_id == @cause_id")
    )

    has_multiple_2stage_mediators = len(rr_metadata) > 1
    if has_multiple_2stage_mediators:
        if cause_id not in constants.CAUSES_WITH_MULTIPLE_2_STAGE_MEDIATORS:
            raise RuntimeError(
                "Internal error: two-stage mediation with multiple mediators is not "
                f"supported for cause_id {cause_id}. This indicates a problem with the "
                "mediation matrix."
            )
        if not mediation_factors.empty:
            raise RuntimeError(
                "Internal error: mediation factors less than 100% are present for a "
                "two-stage mediated cause, indicating a problem with the mediation matrix."
            )

    # Read cached exposure percentiles for the distal risk and any mediators
    exposure_min_max = io_utils.get(root_dir, constants.CacheContents.EXPOSURE_MIN_MAX)

    # It's possible to have more than 1 mediator in 2-stage mediation. This means we have
    # multiple rr_metadata rows for the cause. We loop through these and calculate PAFs
    # for the risk-cause using each mediator. There will not be an unmediated PAF in this
    # case because 2-stage causes are 100% mediated by definition, and this is confirmed
    # by the validation above
    all_mediator_results = []
    unmediated_paf_result = None
    for _, row in rr_metadata.iterrows():
        risk_mediator_result, unmediated_paf_result = _run_paf_for_mediator_cause(
            root_dir=root_dir,
            location_id=location_id,
            cause_id=cause_id,
            settings=settings,
            exposure_min_max=exposure_min_max,
            rei_metadata=rei_metadata,
            mediator_rei_metadata=mediator_rei_metadata,
            rr_metadata=rr_metadata.query(f"med_id == {row['med_id']}"),
            cause_metadata=cause_metadata,
            mediation_factors=mediation_factors,
            rr_rei_id=row["med_id"],
            rr_is_year_specific=row["year_specific"] == 1,
            is_two_stage=row["source"] == "delta",
        )
        all_mediator_results.append(risk_mediator_result)

    total_paf_result = pd.concat(all_mediator_results)

    # If we had multiple mediators, we will have multiple sets of PAFs so we need to
    # aggregate them back together
    if has_multiple_2stage_mediators:
        total_paf_result = math.aggregate_pafs(
            paf_df=total_paf_result, collapse_cols=["med_id"]
        )

    return total_paf_result, unmediated_paf_result


def _run_paf_for_mediator_cause(
    root_dir: pathlib.Path,
    location_id: int,
    cause_id: int,
    settings: constants.PafCalculatorSettings,
    exposure_min_max: pd.DataFrame,
    rei_metadata: pd.DataFrame,
    mediator_rei_metadata: Optional[pd.DataFrame],
    rr_metadata: pd.DataFrame,
    cause_metadata: pd.DataFrame,
    mediation_factors: pd.DataFrame,
    rr_rei_id: int,
    rr_is_year_specific: bool,
    is_two_stage: bool,
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Use the math module to calculate total and possibly unmediated PAFs. When the
    risk-cause is partially mediated through another risk (mediation factor less than
    100%), the PAF calculation is done twice: once using the modeled RRs to generate
    total PAFs, and once using mediated RRs to generate the unmediated PAF, which is
    used downstream in risk aggregation.

    If the risk-cause is mediated through 2-stage mediation, this function deals with
    a single mediator and must be called separately for each 2-stage mediator.

    Arguments:
        root_dir: root directory for this PAF calculator run
        location_id: the location this job is running for
        cause_id: the cause this job is running for
        settings: global parameters for this PAF calculator run
        exposure_min_max: dataframe of min and max exposures by age/rei for the distal
            risk and any mediators with columns
            age_group_id/rei_id/exposure_min/exposure_max
        rei_metadata: information about the risk
        mediator_rei_metadata: optional dataframe with information about the mediator
            risk(s) for 2-stage mediation
        rr_metadata: information about the relative risk model, a single-row dataframe
        cause_metadata: the computation cause hierarchy from db_queries, used for
            subcause splitting
        mediation_factors: dataframe of total mediation factors for this risk-cause,
            used for calculating the unmediated PAF. Contains columns
            cause_id/draw_*
        rr_rei_id: the risk that is the source of the RR model. For 2-stage mediation,
            this will be the mediator rei_id. Otherwise it will be the distal rei_id
        rr_is_year_specific: whether the RR model varies by year
        is_two_stage: whether the risk-cause is calculated through 2-stage mediation

    Returns a tuple containing the total PAF dataframe and the unmediated PAF dataframe,
    which will be None if mediation does not apply to this risk-cause.
    """
    # Read relative risk draws for the given cause_id. If cause is 2-stage mediated, the
    # RR model will be for the mediator risk
    rr = ihme_cc_risk_utils.get_rr_draws(
        rei_id=rr_rei_id,
        release_id=settings.release_id,
        n_draws=settings.n_draws,
        cause_id=cause_id,
        year_id=settings.year_id if rr_is_year_specific else None,
        location_id=location_id,
    )

    # Read effect size between the intervention and risk factor.
    intervention_effect_size = None
    if settings.intervention_rei_id:
        intervention_effect_size = intervention_utils.get_intervention_effect_size_draws(
            intervention_rei_id=settings.intervention_rei_id,
            rei_id=settings.rei_id,
            n_draws=settings.n_draws,
        )

    # Put together all the necessary input dfs. If this is a 2-stage cause, this will
    # include the mediator inputs for the given mediator. If risk is the health outcome of an
    # intervention, also includes intervention coverage and effect size.
    distal_exposure_age_groups = exposure_min_max.query(f"rei_id == {settings.rei_id}")[
        "age_group_id"
    ]
    two_stage_mediator_rei_id = rr_rei_id if is_two_stage else None
    input_models = input_utils.ContinuousInputContainer(
        root_dir=root_dir,
        location_id=location_id,
        rr=rr,
        exposure_age_groups=distal_exposure_age_groups,
        two_stage_mediator_rei_id=two_stage_mediator_rei_id,
        intervention_rei_id=settings.intervention_rei_id,
        intervention_effect_size=intervention_effect_size,
    )

    # Calculate PAFs for this cause, and 2-stage mediator if there is one
    if is_two_stage:
        if mediator_rei_metadata is None:
            raise RuntimeError(
                "Internal error: mediator REI metadata not passed for two-stage mediation."
            )
        else:
            mediator_rei_metadata = mediator_rei_metadata.query(f"rei_id == {rr_rei_id}")
    total_paf_result = math.calculate_continuous_paf(
        settings=settings,
        location_id=location_id,
        rei_metadata=rei_metadata,
        mediator_rei_metadata=mediator_rei_metadata,
        rr_metadata=rr_metadata.query(f"med_id == {rr_rei_id}"),
        exposure_min_max=exposure_min_max,
        input_models=input_models,
        mediation_factors=None,
    )

    # Label these rows with the mediator so we can aggregate later if we had multiple
    # mediators. If not 2-stage mediation, this will just be the distal rei_id
    total_paf_result = total_paf_result.assign(med_id=rr_rei_id)

    # Post-processing: some causes are modeled at the parent cause level and must be
    # split to subcauses
    if cause_id in constants.PARENT_LEVEL_CAUSES:
        # Split if we can, warn if we can't. The USRE release has the required YLL/YLD results
        # available in flat files. For other releases, splitting requires COMO and CodCorrect.
        if settings.release_id == release.USRE or (
            settings.codcorrect_version_id is not None
            and settings.como_version_id is not None
        ):
            total_paf_result = subcause_split_utils.split_and_append_subcauses(
                settings=settings,
                paf_df=total_paf_result,
                mediator_rei_id=rr_rei_id,
                cause_metadata=cause_metadata,
            )
        else:
            logger.warning(
                f"Skipping subcause splitting for cause_id {cause_id}. Required COMO and/or "
                f"CodCorrect results are not available for release_id {settings.release_id}."
            )

    if mediation_factors.empty:
        unmediated_paf_result = None
    else:
        # We have mediation factors so calculate a second set of PAFs using the
        # mediation factors to adjust the relative risk
        unmediated_paf_result = math.calculate_continuous_paf(
            settings=settings,
            location_id=location_id,
            rei_metadata=rei_metadata,
            mediator_rei_metadata=mediator_rei_metadata,
            rr_metadata=rr_metadata,
            exposure_min_max=exposure_min_max,
            input_models=input_models,
            mediation_factors=mediation_factors,
        )

    return total_paf_result, unmediated_paf_result


def _run_custom_paf(
    root_dir: pathlib.Path,
    location_id: int,
    cause_id: int,
    settings: constants.PafCalculatorSettings,
) -> None:
    """Sets up inputs and calculates custom PAFs.

    These functions have separate logic and write their own results. cause_id may or may not
    be ignored.
    """
    if settings.rei_id == constants.BLOOD_LEAD_REI_ID:
        blood_lead.calculate_envir_lead_blood(root_dir, location_id, settings)
    elif settings.rei_id == constants.IPV_REI_ID:
        intimate_partner_violence.calculate_abuse_ipv_paf(root_dir, location_id, settings)
    elif settings.rei_id == constants.AIR_PMHAP_REI_ID:
        air_pmhap.calculate_air_pmhap_paf(root_dir, location_id, settings)
    elif settings.rei_id == constants.DRUG_DEPENDENCE_REI_ID:
        drug_dependence.calculate_drugs_illicit_suicide(
            root_dir, location_id, cause_id, settings
        )
    elif settings.rei_id == constants.UNSAFE_SEX_REI_ID:
        hiv.calculate_hiv_due_to_unsafe_sex_paf(root_dir, location_id, settings)
    elif settings.rei_id == constants.LBWSGA_REI_ID:
        lbwsg.calculate_nutrition_lbw_preterm_paf(root_dir, location_id, settings)
    else:
        raise RuntimeError(
            "Internal error: PAF Calculator cannot calculate custom PAFs for REI ID "
            f"{settings.rei_id}."
        )


def _run_task(root_dir: str, location_id: int, cause_id: int) -> None:
    """Calculates and saves PAF draws for a specific cause/location. For continuous
    risks, will also save unmediated PAFs if they were generated.
    """
    root_dir = pathlib.Path(root_dir)
    settings = io_utils.read_settings(root_dir)

    # metadata for the distal risk (a single row)
    rei_metadata = io_utils.get(root_dir, constants.CacheContents.REI_METADATA)
    paf_calc_type = rei_metadata["rei_calculation_type"].astype(int).iat[0]

    # TODO: remove when rei metadata is updated (by 2/1/2024)
    if settings.rei_id == constants.LBWSGA_REI_ID:
        paf_calc_type = constants.CalculationType.CUSTOM

    # relative risk metadata for this risk-mediator-cause. Can have multiple rows
    # if there are multiple 2-stage mediators for the given cause
    rr_metadata = io_utils.get(root_dir, constants.CacheContents.RR_METADATA).query(
        "cause_id == @cause_id"
    )
    # 2-stage mediation requires metadata for the mediator risk
    mediator_rei_metadata = (
        io_utils.get(root_dir, constants.CacheContents.MEDIATOR_REI_METADATA)
        if not rr_metadata.empty and rr_metadata["source"].iat[0] == "delta"
        else None
    )

    unmediated_result = None
    unavertable_result = None
    if paf_calc_type == constants.CalculationType.CONTINUOUS:
        result, unmediated_result = _run_continuous_paf(
            root_dir=root_dir,
            location_id=location_id,
            cause_id=cause_id,
            settings=settings,
            rei_metadata=rei_metadata,
            mediator_rei_metadata=mediator_rei_metadata,
            rr_metadata=rr_metadata,
        )
        if settings.rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
            unavertable_result = averted_burden.calculate_unavertable_paf(
                settings=settings, avertable_draws=result
            )
    elif paf_calc_type == constants.CalculationType.CATEGORICAL:
        result = _run_categorical_paf(
            root_dir=root_dir,
            location_id=location_id,
            cause_id=cause_id,
            settings=settings,
            rei_metadata=rei_metadata,
            rr_metadata=rr_metadata,
        )
        if settings.rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID:
            unavertable_result = averted_burden.calculate_unavertable_paf(
                settings=settings, avertable_draws=result
            )
    elif paf_calc_type in [
        constants.CalculationType.CUSTOM,
        constants.CalculationType.DIRECT,
    ]:
        # Custom and direct PAFs: these functions have separate logic and write
        # their own results
        _run_custom_paf(root_dir, location_id, cause_id, settings)
        return
    else:
        raise RuntimeError(
            f"Internal error: PAF calculation type {paf_calc_type} for REI ID "
            f"{settings.rei_id} not recognized."
        )

    # Save draws for total PAF
    io_utils.write_paf(result, root_dir, location_id)
    if unmediated_result is not None:
        # Save draws for unmediated PAF
        io_utils.write_paf(unmediated_result, root_dir, location_id, is_unmediated=True)
    if unavertable_result is not None:
        # Save draws for Averted Burden unavertable PAF
        io_utils.write_paf_for_unavertable(unavertable_result, root_dir, location_id)

    return


if __name__ == "__main__":
    main()
