import pathlib
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import db_queries
import db_tools_core
import gbd_outputs_versions
import ihme_cc_risk_utils
import ihme_cc_rules_client
from db_queries.lib.wormhole_utils import get_linked_wormhole_mvid
from gbd import conn_defs, wormhole
from gbd.constants import gbd_metadata_type, gbd_process, release

from ihme_cc_paf_calculator.lib import (
    constants,
    io_utils,
    logging_utils,
    machinery,
    model_utils,
)
from ihme_cc_paf_calculator.lib.custom_pafs import occupational_noise

logger = logging_utils.module_logger(__name__)

# Low birth weight/short gestation REI IDs that are not supported by the PAF Calculator
_NOT_SUPPORTED_LBWSG_REI_IDS: List[int] = [
    constants.SHORT_GESTATION_REI_ID,
    constants.LOW_BIRTH_WEIGHT_REI_ID,
]
# Unavertable REIs in the Averted Burden REI set are not supported by the PAF Calculator
_UNAVERTABLE_PARENT_REI_ID: int = 557


def validate_inputs(
    rei_id: int,
    year_id: List[int],
    n_draws: int,
    release_id: int,
    skip_save_results: bool,
    rei_set_id: int,
    rei_metadata: pd.DataFrame,
    demographics: Dict[str, List[int]],
    model_version_id: Optional[int],
    test: bool,
) -> None:
    """Validate inputs."""
    # Validate MVID first as if users actually want to resume, they aren't expected
    # to pass in other non-required args
    validate_model_version_id(model_version_id)

    validate_release_id(release_id, test)
    validate_rei_id(rei_id, rei_set_id, rei_metadata)
    validate_rei_metadata(rei_id, rei_metadata)
    validate_year_id(year_id, demographics, release_id)
    validate_n_draws(n_draws)
    validate_skip_save_results(rei_id, skip_save_results)


def validate_model_version_id(model_version_id: Optional[int]) -> None:
    """Validate that model_version_id is None. If given, resume should be passed as well."""
    if model_version_id is not None:
        raise ValueError(
            f"model_version_id {model_version_id} given but resume=False. If resuming a "
            "failed PAF model, also provide resume=True. Otherwise, do not pass a "
            "model_version_id."
        )


def _release_is_supported(release_id: int) -> bool:
    """Query the rules service to determine if PAFs can run for a given release_id."""
    try:
        return ihme_cc_rules_client.RulesManager(
            research_area=ihme_cc_rules_client.ResearchAreas.RISK,
            tool=ihme_cc_rules_client.Tools.PAF_CALCULATOR,
            release_id=release_id,
        ).get_rule_value(ihme_cc_rules_client.Rules.MODEL_CAN_RUN)
    except ihme_cc_rules_client.exceptions.RulesApiMissingDetailsError:
        # Missing rule is the same as MODEL_CAN_RUN=False
        return False


def validate_release_id(release_id: int, test: bool) -> None:
    """Validates release_id using rules. Not validated for test runs."""
    if test:
        return

    supported_releases = [
        release_id
        for release_id in sorted(release.values())
        if _release_is_supported(release_id)
    ]
    if release_id not in supported_releases:
        raise ValueError(
            f"The PAF Calculator currently only supports release_ids {supported_releases}. "
            f"Given release_id {release_id} is not supported."
        )


def validate_rei_id(rei_id: int, rei_set_id: int, rei_metadata: pd.DataFrame) -> None:
    """Validate given rei_id.

    Validates:
        * rei_id is in rei_metadata
        * Not nutrition_lbw or nutrition_preterm
        * Not rei_calculation_type == 0
        * Not an Averted Burden "Unavertable with drug class ____" REI

    Raises:
        ValueError: if any of the validations are failed
    """
    if rei_id not in rei_metadata["rei_id"].tolist():
        raise ValueError(f"REI ID {rei_id} is not in REI set ID {rei_set_id}.")
    if rei_id in _NOT_SUPPORTED_LBWSG_REI_IDS:
        raise ValueError(
            "Low birth weight/short gestation PAFs are calculated with the joint risk, "
            f"REI ID {constants.LBWSGA_REI_ID} (nutrition_lbw_preterm), not REI ID "
            f"{' or '.join([str(i) for i in _NOT_SUPPORTED_LBWSG_REI_IDS])}."
        )

    if rei_set_id == constants.AVERTED_BURDEN_REI_SET_ID and (
        rei_metadata.query(f"rei_id == {rei_id}")["parent_id"].astype(int).iat[0]
        == _UNAVERTABLE_PARENT_REI_ID
    ):
        raise ValueError(
            f"Drug class PAFs for unavertable burden REIs in REI set ID {rei_set_id} are "
            f"calculated with the avertable drug class REI ID, not REI ID {rei_id}."
        )

    if (
        rei_metadata.query(f"rei_id == {rei_id}")["rei_calculation_type"].astype(int).iat[0]
        == constants.CalculationType.AGGREGATE
    ):
        raise ValueError(
            f"REI ID {rei_id} is an aggregate risk and will be calculated centrally "
            "after most detailed risk PAFs are ready."
        )


def validate_rei_metadata(rei_id: int, rei_metadata: pd.DataFrame) -> None:
    """Check that risk metadata is consistent. Unrelated to user inputs, but good to check
    before starting a run.
    """
    rei_metadata = rei_metadata.query(f"rei_id == {rei_id}")

    # categorical risks with inverse (protective) exposure must be dichotomous, meaning they
    # use only 2 exposure categories (exposed or unexposed)
    exposure_type = rei_metadata["exposure_type"].iat[0]
    is_protective = (
        pd.notna(rei_metadata["inv_exp"].iat[0]) and int(rei_metadata["inv_exp"].iat[0]) == 1
    )
    if (
        int(rei_metadata["rei_calculation_type"].iat[0])
        == constants.CalculationType.CATEGORICAL
        and is_protective
        and not exposure_type == "dichotomous"
    ):
        raise RuntimeError(
            "Categorical risks with protective exposure are expected to be dichotomous, "
            f"but risk metadata indicates this risk is {exposure_type}. Please file a "
            "ticket."
        )


def validate_paf_model_type(rei_id: int, me_ids: pd.DataFrame) -> None:
    """Validate if the given REI ID is a custom PAF REI."""
    paf_model = me_ids.query(f"rei_id == {rei_id} & draw_type == 'paf'")
    if paf_model.empty:
        raise RuntimeError(
            f"No modelable entity found for REI ID {rei_id} PAFs. Please submit a "
            "ticket to create or activate a modelable entity."
        )
    if paf_model["model_type"].iat[0] != "paf calculator":
        raise ValueError(
            f"REI ID {rei_id} is a custom PAF calculated by the modeler. Not supported "
            "by the PAF Calculator."
        )


def validate_year_id(
    year_id: List[int], demographics: Dict[str, List[int]], release_id: int
) -> None:
    """Validate year_id.

    Verify all years are within min and max years in demographics.
    """
    all_year_ids = range(min(demographics["year_id"]), max(demographics["year_id"]) + 1)
    invalid_year_ids = list(set(year_id) - set(all_year_ids))

    if invalid_year_ids:
        raise ValueError(
            f"The following year ID(s) are invalid for release ID {release_id}: "
            f"{', '.join([str(i) for i in invalid_year_ids])}"
        )


def validate_n_draws(n_draws: int) -> None:
    """Validate number of draws is between 1 and 1000 (inclusive)."""
    if n_draws < 1 or n_draws > 1000:
        raise ValueError(f"n_draws must be between 1 - 1000 (inclusive). Got {n_draws}")


def validate_skip_save_results(rei_id: int, skip_save_results: bool) -> None:
    """Validate skip_save_results is True if running for air_pmhap."""
    if rei_id == constants.AIR_PMHAP_REI_ID and not skip_save_results:
        raise ValueError(
            f"skip_save_results must be True if calculating PAFs for REI ID {rei_id}, "
            "particulate matter pollution"
        )


def validate_input_models(
    rei_id: int,
    year_id: List[int],
    release_id: int,
    demographics: Dict[str, List[int]],
    me_ids: pd.DataFrame,
    rr_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """Validate input models for the PAF Calculator.

    In particular:
        * Validates best model versions exist for all required models
        * Expected demographics exist if ST-GPR models
            (only expected to be an option for exposure and exposure SD models)
        * Estimates exist for all requested years for TMREL, exposure, exposure SD, RR models
        * RR models that are not year-specific skip the year validation
        * Occupational Noise RRs contain the correct hearing sequelae
    """
    edited_me_ids = validate_required_input_models(rei_id, me_ids)

    types_to_validate = ["exposure", "exposure_sd", "tmrel", "rr"]
    for _, row in edited_me_ids.query(f"draw_type.isin({types_to_validate})").iterrows():
        if row["model_type"] == "stgpr":
            # ST-GPR models could lack GBD ages/sexes, so confirm they have at least one
            validate_stgpr_demographics(
                row["model_version_id"],
                demographics,
                row["draw_type"],
                release_id=release_id,
                modelable_entity_id=row["modelable_entity_id"],
            )

        # For RR models, pass through whether they are year-specific models or not
        year_specific = (
            rr_metadata.query(f"model_version_id == {row['model_version_id']}")[
                "year_specific"
            ].iat[0]
            if row["draw_type"] == "rr"
            else None
        )
        validate_input_model_years(
            modelable_entity_id=row["modelable_entity_id"],
            model_version_id=row["model_version_id"],
            model_type=row["model_type"],
            draw_type=row["draw_type"],
            year_specific=year_specific,
            year_id=year_id,
            release_id=release_id,
            demographics=demographics,
        )

        # Occupational noise uses sequela information. Validate that our RR inputs match
        # the sequelae in the hierarchy and save the sequelae metadata for caching
        if rei_id == constants.OCCUPATIONAL_NOISE_REI_ID:
            occupational_noise.validate_hearing_sequelae(release_id, rr_metadata)

    return edited_me_ids


def validate_required_input_models(rei_id: int, me_ids: pd.DataFrame) -> pd.DataFrame:
    """Validate input models required for PAF calculation.

    All MEs linked to an REI (other than PAFs/unmediated PAFs) are expected to have
    a best model version in order to calculate PAFs.
    """
    non_paf_models = me_ids.query("~draw_type.isin(['paf', 'paf_unmediated'])")
    missing_models = non_paf_models.query("model_version_id.isna()")
    if not missing_models.empty:
        raise RuntimeError(
            f"The following required PAF inputs for rei_id {rei_id} do not have best "
            f"model versions:\n{missing_models}"
        )
    return non_paf_models.assign(
        model_version_id=lambda df: df["model_version_id"].astype(int)
    )


def validate_input_model_years(
    modelable_entity_id: int,
    model_version_id: int,
    model_type: str,
    draw_type: str,
    year_specific: Optional[int],
    year_id: List[int],
    release_id: int,
    demographics: Optional[Dict[str, List[int]]],
) -> None:
    """Validate the model version has estimates for the requested years.

    Intended for TMREL, RR, PAF (without wormhole), exposure, and exposure SD models.

    TMREL RR, and PAF models do not save summaries in the db and thus aren't accessable via
    get_model_results like exposure and exposure SD model summaries are. This is because
    the epi schema is fundamentally incompatible with risk results, so save_results skips
    saving summaries. Instead, we check summaries written to the filesystem for existing
    years.

    This validation is skipped for RR models that are not year-specific, specified by
    `year_specific`.
    """
    if draw_type == "tmrel":
        model_year_ids = (
            pd.read_csv(
                constants.TMREL_SUMMARY_FILE.format(model_version_id=model_version_id)
            )["year_id"]
            .unique()
            .tolist()
        )
    elif draw_type == "rr":
        if year_specific is None:
            raise RuntimeError(
                "Internal error: year_specific cannot be None when validating RR model years."
            )
        elif year_specific:
            model_year_ids = (
                pd.read_csv(
                    constants.RR_SUMMARY_FILE.format(model_version_id=model_version_id)
                )["year_id"]
                .unique()
                .tolist()
            )
        else:
            # RR model is not year specific, no need to validate years
            return
    elif draw_type == "paf":
        model_year_ids = (
            pd.read_csv(constants.PAF_SUMMARY_FILE.format(model_version_id=model_version_id))[
                "year_id"
            ]
            .unique()
            .tolist()
        )
    elif draw_type in ["exposure", "exposure_sd"]:
        # Exposure models can be modeled in or out of ST-GPR
        if demographics is None:
            raise RuntimeError(f"Demographics cannot be None for draw_type {draw_type}.")
        gbd_team = "stgpr" if model_type == "stgpr" else "epi"
        model_year_ids = (
            db_queries.get_model_results(
                gbd_team=gbd_team,
                gbd_id=modelable_entity_id,
                location_id=demographics[constants.LOCATION_ID][0],
                year_id=year_id,
                release_id=release_id,
                model_version_id=model_version_id,
            )["year_id"]
            .unique()
            .tolist()
        )
    else:
        raise RuntimeError(
            f"Internal error: cannot validate years for draw type '{draw_type}'"
        )

    invalid_year_ids = list(set(year_id) - set(model_year_ids))
    if invalid_year_ids:
        raise RuntimeError(
            f"Not all provided year_ids are present in current best {draw_type} model, "
            f"model version {model_version_id}, for modelable entity {modelable_entity_id}. "
            f"Missing years: {', '.join([str(i) for i in invalid_year_ids])}"
        )


def validate_stgpr_demographics(
    stgpr_version_id: int,
    demographics: Dict[str, List[int]],
    draw_type: str,
    release_id: int,
    modelable_entity_id: int,
) -> None:
    """Validate an input ST-GPR model has acceptable demograhics.

    In particular, exposure and exposure_sd models can be ST-GPR models, which aren't
    guaranteed to have the age group and sex demographics required by save_results_epi.

    Here we valildate that an input ST-GPR model has at least one overlaping age group and
    one overlaping sex with GBD age groups and sexes. Previously we queried the ST-GPR
    service for this information but we use get_model_results now because it includes
    Wormhole-filled estimates for models that are out of rotation.
    """
    # Get age groups and sex ids the ST-GPR model has results for
    model_results = db_queries.get_model_results(
        gbd_team="stgpr",
        gbd_id=modelable_entity_id,
        location_id=demographics[constants.LOCATION_ID][0],
        release_id=release_id,
        model_version_id=stgpr_version_id,
    )[[constants.AGE_GROUP_ID, constants.SEX_ID]]
    age_group_ids = model_results[constants.AGE_GROUP_ID].unique().tolist()
    sex_ids = model_results[constants.SEX_ID].unique().tolist()

    valid_age_group_ids = set(age_group_ids).intersection(demographics["age_group_id"])
    valid_sex_ids = set(sex_ids).intersection(demographics["sex_id"])

    error_msg = (
        f"Input {draw_type} model version ID {stgpr_version_id} from ST-GPR "
        "contains no estimates for any of the most detailed GBD "
    )
    error_suffix = (
        "\n\nEstimates must have a least one age group and sex that overlaps with GBD "
        "age groups and sexes."
    )
    if not valid_age_group_ids:
        error_msg += (
            f"age group IDs.\nGBD age group IDs: {demographics['age_group_id']}\n"
            f"Model age group ID(s): {age_group_ids}"
        )
        raise RuntimeError(error_msg + error_suffix)

    if not valid_sex_ids:
        error_msg += (
            f"sex IDs.\nGBD sex IDs: {demographics['sex_id']}\n" f"Model sex ID(s): {sex_ids}"
        )
        raise RuntimeError(error_msg + error_suffix)


def validate_input_machinery_versions(
    rei_id: int,
    release_id: int,
    codcorrect_version_id: Optional[int],
    como_version_id: Optional[int],
    cause_ids: List[int],
    year_id: List[int],
    n_draws: int,
) -> Tuple[Optional[int], Optional[int]]:
    """Validate input machinery versions.

    Determine and validate COMO and CodCorrect versions (or None) to use for PAF calculation,
    taking into account whether the given rei_id requires these machinery, and whether or not
    these machinery are available in the given release_id.

    Returns:
        codcorrect_version_id, como_version_id as updated by the function.
    """
    codcorrect_version_id = validate_internal_version_id(
        gbd_process.COD,
        rei_id,
        release_id,
        codcorrect_version_id,
        cause_ids,
        year_id,
        n_draws,
    )
    como_version_id = validate_internal_version_id(
        gbd_process.EPI, rei_id, release_id, como_version_id, cause_ids, year_id, n_draws
    )
    return (codcorrect_version_id, como_version_id)


def validate_internal_version_id(
    gbd_process_id: int,
    rei_id: int,
    release_id: int,
    internal_version_id: Optional[int],
    cause_ids: List[int],
    year_id: List[int],
    n_draws: int,
) -> Optional[int]:
    """Validate internal version ID.

    If a risk does not use the machinery corresponding to gbd_process_id and an
    internal_version_id is given, warns and returns None.

    Returns:
        Validated internal_version_id.
    """
    # Determine machinery and version argument names.
    machinery_name = constants.PROCESS_ID_TO_MACHINERY_NAME[gbd_process_id]
    version_name = f"{str.lower(machinery_name)}_version_id"

    # Handle USRE special case.
    if release_id == release.USRE:
        if internal_version_id is not None:
            warnings.warn(
                f"USRE, release_id {release_id}, does not use {machinery_name} in its PAF "
                f"calculation. Overwriting {version_name}={internal_version_id} to None."
            )
        return None

    # Select and validate internal_version_id, taking machinery availability into account.
    if machinery.release_has_machinery(release_id, gbd_process_id):
        internal_version_id = _validate_available_machinery(
            gbd_process_id,
            rei_id,
            release_id,
            internal_version_id,
            cause_ids,
            year_id,
            n_draws,
        )
    else:
        internal_version_id = _validate_unavailable_machinery(
            gbd_process_id, rei_id, release_id, internal_version_id, cause_ids
        )

    return internal_version_id


def _validate_unavailable_machinery(
    gbd_process_id: int,
    rei_id: int,
    release_id: int,
    internal_version_id: Optional[int],
    cause_ids: List[int],
) -> None:
    """Logic to validate arguments when the machinery corresponding to gbd_process_id is
    unavailable for the given release_id. Raises if all cause outcomes of rei_id use this
    machinery, and returns None otherwise, warning if some outcomes use the machinery or
    if the internal_version_id is not already None.
    """
    # Determine machinery and version argument names.
    machinery_name = constants.PROCESS_ID_TO_MACHINERY_NAME[gbd_process_id]
    version_name = f"{str.lower(machinery_name)}_version_id"

    # Determine cause outcomes of rei_id that use the given machinery.
    outcomes_using_machinery = set(cause_ids).intersection(
        constants.CAUSE_IDS_THAT_USE_MACHINERY[gbd_process_id]
    )
    all_outcomes_use_machinery = (
        rei_id in constants.REI_IDS_THAT_USE_MACHINERY_FOR_ALL_YEARS[gbd_process_id]
        or rei_id in constants.REI_IDS_THAT_USE_MACHINERY_FOR_ALL_CAUSES[gbd_process_id]
        or (len(cause_ids) > 0 and set(cause_ids).issubset(outcomes_using_machinery))
    )

    # Raise exceptions or warnings if appropriate.
    if all_outcomes_use_machinery:
        raise ValueError(
            f"All cause outcomes for rei_id {rei_id} depend on {machinery_name} but no "
            f"{machinery_name} version exists yet for release_id {release_id}."
        )
    elif len(outcomes_using_machinery) > 0:
        if internal_version_id is not None:
            logger.info(f"Overwriting {version_name}={internal_version_id} to None.")
        warnings.warn(
            f"Some cause outcomes for rei_id {rei_id} depend on {machinery_name} but no "
            f"{machinery_name} version exists yet for release_id {release_id}. The following "
            "parent cause(s) will have PAFs computed, but their child causes will not: "
            f"{outcomes_using_machinery}."
        )
    else:
        # This machinery is not used by this REI.
        if internal_version_id is not None:
            warnings.warn(
                f"rei_id {rei_id} does not use {machinery_name} in its PAF calculation. "
                f"Overwriting {version_name}={internal_version_id} to None."
            )

    return None


def _validate_available_machinery(
    gbd_process_id: int,
    rei_id: int,
    release_id: int,
    internal_version_id: Optional[int],
    cause_ids: List[int],
    year_id: List[int],
    n_draws: int,
) -> Optional[int]:
    """Logic to validate arguments when the machinery corresponding to gbd_process_id is
    available for the given release_id. Handles default internal_version_id as well as draw
    count and year validations if the machinery is used.
    """
    # Determine machinery and version argument names.
    machinery_name = constants.PROCESS_ID_TO_MACHINERY_NAME[gbd_process_id]
    version_name = f"{str.lower(machinery_name)}_version_id"

    # Determine if any cause outcomes of rei_id use the given machinery.
    cause_ids_that_use_machinery = constants.CAUSE_IDS_THAT_USE_MACHINERY[gbd_process_id]
    any_outcomes_use_machinery = (
        rei_id in constants.REI_IDS_THAT_USE_MACHINERY_FOR_ALL_YEARS[gbd_process_id]
        or rei_id in constants.REI_IDS_THAT_USE_MACHINERY_FOR_ALL_CAUSES[gbd_process_id]
        or len(set(cause_ids).intersection(cause_ids_that_use_machinery)) > 0
    )

    # Logic to select the machinery version and perform the appropriate validation.
    if any_outcomes_use_machinery:
        internal_version_id = (
            internal_version_id
            if internal_version_id is not None
            else machinery.get_default_machinery_version_id(gbd_process_id, release_id)
        )
        validation_year_ids = (
            year_id
            if rei_id in constants.REI_IDS_THAT_USE_MACHINERY_FOR_ALL_YEARS[gbd_process_id]
            else [machinery.get_arbitrary_year_id_for_subcause_splitting(release_id)]
        )
        _validate_machinery_version(
            internal_version_id, gbd_process_id, validation_year_ids, n_draws
        )
    else:
        # This machinery is not used by this REI.
        if internal_version_id is not None:
            warnings.warn(
                f"rei_id {rei_id} does not use {machinery_name} in its PAF calculation. "
                f"Overwriting {version_name}={internal_version_id} to None."
            )
            internal_version_id = None

    return internal_version_id


def _validate_machinery_version(
    internal_version_id: int, gbd_process_id: int, year_id: List[int], n_draws: int
) -> None:
    """Validate given machinery version.

    For CodCorrect or COMO. Validates year_id and n_draws.
    """
    # Translate from GBD process ID to expected name
    machinery_name = constants.PROCESS_ID_TO_MACHINERY_NAME[gbd_process_id]

    # Get process version for given internal version. Fail if version doesn't exist
    # or we cannot instantiate a GBDProcessVersion
    try:
        pv_id = gbd_outputs_versions.internal_to_process_version(
            internal_version_id, gbd_process_id
        )
        pv = gbd_outputs_versions.GBDProcessVersion(pv_id)
    except (RuntimeError, ValueError) as e:
        raise ValueError(
            f"Could not find {machinery_name} version {internal_version_id}. Are you sure "
            "is exists and hasn't been deleted?"
        ) from e

    # Confirm all requested years and draws also exist for version
    machinery_year_ids = list(np.atleast_1d(pv.metadata[gbd_metadata_type.YEAR_IDS]))
    invalid_year_ids = list(set(year_id) - set(machinery_year_ids))
    if invalid_year_ids:
        raise ValueError(
            f"Not all provided year_ids are present in {machinery_name} "
            f"v{internal_version_id}. Missing year(s): "
            f"{', '.join([str(i) for i in invalid_year_ids])}"
        )

    machinery_n_draws = pv.metadata[gbd_metadata_type.N_DRAWS]
    if n_draws > machinery_n_draws:
        raise ValueError(
            f"{machinery_name} v{internal_version_id} was run with {machinery_n_draws} draws "
            f"but {n_draws} draws were requested. PAFs must be run for less than or equal "
            f"to the number of draws {machinery_name} was run with."
        )


def edit_rei_metadata(
    rei_id: int, rei_metadata: pd.DataFrame, me_ids: pd.DataFrame
) -> pd.DataFrame:
    """Edit REI metadata for the PAF Calculator run.

    Edits:
        * Subsets down to only one row, the row corresponding to rei_id
        * If a TMREL ME is in me_ids (not including any mediator MEs), set TMREL-related
            metadata to None (if not already None)
        * Converts numeric columns to their proper type in case the dataframe from
            db_queries has them as strings
    """
    # metadata is specific to the REI even if it's in the hierarchy more than once,
    # keep the first instance only.
    rei_metadata = rei_metadata.query(f"rei_id == {rei_id}").drop_duplicates(
        subset=["rei_id"], keep="first"
    )

    if "tmrel" in me_ids.query(f"rei_id == {rei_id}")["draw_type"].tolist():
        rei_metadata = rei_metadata.assign(
            tmred_dist=np.NaN, tmrel_lower=np.NaN, tmrel_upper=np.NaN
        )

    # convert columns to numeric where possible; strings will be ignored.
    # some columns can have nulls so set to 0
    rei_metadata.loc[rei_metadata["inv_exp"].isnull(), "inv_exp"] = 0
    rei_metadata = rei_metadata.apply(pd.to_numeric, errors="ignore")

    return rei_metadata


class ContinuousInputContainer:
    """Class for setting up and holding all the input model dataframes that can potentially
    go into a continuous PAF.

    On initialization, this will handle loading all appropriate dataframes
    from the cache for the given location. The RRs must be passed in because they are not
    cached. After data is loaded, we perform some minor formatting and apply age
    restrictions to all inputs, as well as subsetting to the sexes that occur in both the
    exposure and RR models. For causes that use 2-stage mediation, we store inputs from
    both distal and mediator risk. Otherwise, the mediator inputs will be None. Although
    2-stage mediation can involve multiple mediators, this class only stores inputs for one.

    This container provides the following dataframes:
        exposure: dataframe of exposure draws with columns
            location_id/year_id/age_group_id/sex_id/parameter/draw_*
        exposure_sd: dataframe of exposure standard deviation draws with
            columns location_id/year_id/age_group_id/sex_id/draw_*
        tmrel_df: dataframe of tmrel draws with columns
            location_id/year_id/age_group_id/sex_id/draw_*
        rr: dataframe of rr draws with columns
            rei_id/location_id/(optional)year_id/age_group_id/sex_id/cause_id
            /mortality/morbidity/exposure/draw_*
        mediator_exposure: (if 2-stage mediation applies) dataframe of exposure draws with
            columns location_id/year_id/age_group_id/sex_id/parameter/draw_*
        mediator_exposure_sd: (if 2-stage mediation applies) dataframe of exposure standard
            deviation draws with columns
            location_id/year_id/age_group_id/sex_id/draw_*
        mediator_tmrel_df: (if 2-stage mediation applies) dataframe of tmrel draws with
            columns location_id/year_id/age_group_id/sex_id/draw_*
        intervention_coverage: (if risk is health outcome of an intervention) dataframe of
            coverage draws with columns location_id/year_id/age_group_id/sex_id/draw_*
        intervention_effect_size: (if risk is health outcome of an intervention) dataframe of
            effect size draws between the intervention and risk factor with columns
            is_absolute/exposure_threshold/draw_*

    Arguments:
        root_dir: path to the root dir of the PAF calculator run
        location_id: the location to read cached inputs from
        rr: dataframe of RR draws. Not present in the cache so needs to be passed in
        exposure_age_groups: a list of age_group_ids relevant to the exposure models for
            this risk-cause. Input models don't necessarily respect restrictions so we need
            to apply age restrictions to all inputs before using them for PAFs
        two_stage_mediator_rei_id: the mediator rei_id providing the RR model for 2-stage
            mediation. Determines whether to read inputs for a mediator risk, and if
            multiple mediators, which mediator to use
        intervention_rei_id: the intervention rei_id providing the coverage and effect size.
            Determines whether to read those inputs.
        intervention_effect_size: dataframe of effect size draws between the intervention and
            risk factor. Not present in the cache so needs to be passed in.
    """

    exposure: pd.DataFrame
    exposure_sd: pd.DataFrame
    tmrel: pd.DataFrame
    mediator_exposure: Optional[pd.DataFrame] = None
    mediator_exposure_sd: Optional[pd.DataFrame] = None
    mediator_tmrel: Optional[pd.DataFrame] = None
    rr: pd.DataFrame
    two_stage_mediator_rei_id: Optional[int] = None
    intervention_rei_id: Optional[int] = None
    intervention_coverage: Optional[pd.DataFrame] = None
    intervention_effect_size: Optional[pd.DataFrame] = None

    def __init__(
        self,
        root_dir: pathlib.Path,
        location_id: int,
        rr: pd.DataFrame,
        exposure_age_groups: List[int],
        two_stage_mediator_rei_id: Optional[int] = None,
        intervention_rei_id: Optional[int] = None,
        intervention_effect_size: Optional[pd.DataFrame] = None,
    ) -> None:
        self._read_cached_inputs(
            root_dir=root_dir,
            location_id=location_id,
            two_stage_mediator_rei_id=two_stage_mediator_rei_id,
            intervention_rei_id=intervention_rei_id,
        )
        self.rr = rr
        self.intervention_effect_size = intervention_effect_size
        self._prepare_cached_inputs(location_id, exposure_age_groups)

    def _read_cached_inputs(
        self,
        root_dir: pathlib.Path,
        location_id: int,
        two_stage_mediator_rei_id: Optional[int],
        intervention_rei_id: Optional[int],
    ) -> None:
        """Populate input models from the cache. Two-stage and intervention model inputs will
        also be read from the cache if they apply.
        """
        self.exposure = io_utils.get_location_specific(
            root_dir=root_dir,
            key=constants.LocationCacheContents.EXPOSURE,
            location_id=location_id,
        )
        self.exposure_sd = io_utils.get_location_specific(
            root_dir=root_dir,
            key=constants.LocationCacheContents.EXPOSURE_SD,
            location_id=location_id,
        )
        self.tmrel = io_utils.get_location_specific(
            root_dir=root_dir,
            key=constants.LocationCacheContents.TMREL,
            location_id=location_id,
        )

        # if risk-cause is 2-stage, read mediator input models
        self.two_stage_mediator_rei_id = two_stage_mediator_rei_id
        if self.two_stage_mediator_rei_id:
            self.mediator_exposure = io_utils.get_location_specific(
                root_dir=root_dir,
                key=constants.LocationCacheContents.MEDIATOR_EXPOSURE,
                location_id=location_id,
            ).query(f"rei_id == {two_stage_mediator_rei_id}")
            self.mediator_exposure_sd = io_utils.get_location_specific(
                root_dir=root_dir,
                key=constants.LocationCacheContents.MEDIATOR_EXPOSURE_SD,
                location_id=location_id,
            ).query(f"rei_id == {two_stage_mediator_rei_id}")
            self.mediator_tmrel = io_utils.get_location_specific(
                root_dir=root_dir,
                key=constants.LocationCacheContents.MEDIATOR_TMREL,
                location_id=location_id,
            ).query(f"rei_id == {two_stage_mediator_rei_id}")

        # If risk is the health outcome of an intervention, read intervention input models.
        self.intervention_rei_id = intervention_rei_id
        if self.intervention_rei_id:
            self.intervention_coverage = io_utils.get_location_specific(
                root_dir=root_dir,
                key=constants.LocationCacheContents.INTERVENTION_COVERAGE,
                location_id=location_id,
            )

    def _prepare_cached_inputs(
        self, location_id: int, exposure_age_groups: List[int]
    ) -> None:
        """Prepare input models for PAF calculation: modify columns for RR and TMREL, apply
        age restrictions, subset inputs to modeled sexes.
        """
        # Subset RR to the columns we need for continuous PAFs. RR draws are typically
        # global, so set to this task's location. TMRELs are the same if the risk has
        # uploaded TMREL draws
        self.rr = self.rr.drop(
            ["model_version_id", "metric_id", "modelable_entity_id", "parameter"], axis=1
        ).assign(location_id=location_id)
        self.tmrel = self.tmrel.assign(location_id=location_id)

        # apply age restrictions to all dfs
        self._restrict_ages(exposure_age_groups)
        # subset to sexes occurring in both exposure and RR
        self._subset_sexes()

    def _restrict_ages(self, exposure_age_groups: List[int]) -> None:
        """Filter all input dataframes to the relevant age groups. Inputs don't necessarily
        reflect restrictions: some inputs may include restricted age groups, and others may
        use only a subset of the valid age groups. We want to keep the age groups that are
        in exposure, relative risk, and intervention (when available) draws.
        """
        modeled_age_groups = set.intersection(
            set(exposure_age_groups), set(self.rr["age_group_id"])
        )
        if self.intervention_rei_id:
            modeled_age_groups = set.intersection(
                set(modeled_age_groups), set(self.intervention_coverage["age_group_id"])
            )
        self.exposure = self.exposure[self.exposure["age_group_id"].isin(modeled_age_groups)]
        self.exposure_sd = self.exposure_sd[
            self.exposure_sd["age_group_id"].isin(modeled_age_groups)
        ]
        self.tmrel = self.tmrel[self.tmrel["age_group_id"].isin(modeled_age_groups)]
        self.rr = self.rr[self.rr["age_group_id"].isin(modeled_age_groups)]

        if self.two_stage_mediator_rei_id:
            self.mediator_exposure = self.mediator_exposure[
                self.mediator_exposure["age_group_id"].isin(modeled_age_groups)
            ]
            self.mediator_exposure_sd = self.mediator_exposure_sd[
                self.mediator_exposure_sd["age_group_id"].isin(modeled_age_groups)
            ]
            self.mediator_tmrel = self.mediator_tmrel[
                self.mediator_tmrel["age_group_id"].isin(modeled_age_groups)
            ]

        if self.intervention_rei_id:
            self.intervention_coverage = self.intervention_coverage[
                self.intervention_coverage["age_group_id"].isin(modeled_age_groups)
            ]

    def _subset_sexes(self) -> None:
        """Filter all input dataframes to the relevant sexes. Exposure, RR, and intervention
        coverage (when available) models may include different sexes, and we can only compute
        PAFs for the sexes that are present in these models.
        """
        modeled_sexes = set.intersection(set(self.exposure["sex_id"]), set(self.rr["sex_id"]))
        if self.intervention_rei_id:
            modeled_sexes = set.intersection(
                set(modeled_sexes), set(self.intervention_coverage["sex_id"])
            )
        self.exposure = self.exposure[self.exposure["sex_id"].isin(modeled_sexes)]
        self.exposure_sd = self.exposure_sd[self.exposure_sd["sex_id"].isin(modeled_sexes)]
        self.tmrel = self.tmrel[self.tmrel["sex_id"].isin(modeled_sexes)]
        self.rr = self.rr[self.rr["sex_id"].isin(modeled_sexes)]

        if self.two_stage_mediator_rei_id:
            self.mediator_exposure = self.mediator_exposure[
                self.mediator_exposure["sex_id"].isin(modeled_sexes)
            ]
            self.mediator_exposure_sd = self.mediator_exposure_sd[
                self.mediator_exposure_sd["sex_id"].isin(modeled_sexes)
            ]
            self.mediator_tmrel = self.mediator_tmrel[
                self.mediator_tmrel["sex_id"].isin(modeled_sexes)
            ]

        if self.intervention_rei_id:
            self.intervention_coverage = self.intervention_coverage[
                self.intervention_coverage["sex_id"].isin(modeled_sexes)
            ]

    def index_by_demographic(self) -> None:
        """Index the input dataframes by demographic cols. We'll refer to the data by
        demographic during PAF calculation. Relative risks also need to be indexed by
        morbidity/mortality, and the year column may not be present. This will modify
        the stored dataframes.
        """
        self.exposure.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)
        self.exposure_sd.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)
        self.tmrel.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)
        if self.two_stage_mediator_rei_id:
            self.mediator_exposure.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)
            self.mediator_exposure_sd.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)
            self.mediator_tmrel.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)
        if self.intervention_rei_id:
            self.intervention_coverage.set_index(constants.DEMOGRAPHIC_COLS, inplace=True)

        # relative risks may be year specific, or not. If they're not, they're
        # missing a year_id column. In that case we'll need to index into the RR
        # data differently. We could expand the RR data but that would balloon
        # memory
        rr_index_cols = constants.DEMOGRAPHIC_COLS + constants.MORB_MORT_COLS
        if constants.YEAR_ID not in self.rr:
            rr_index_cols = [col for col in rr_index_cols if col != constants.YEAR_ID]
        self.rr.set_index(rr_index_cols, inplace=True)

    def reset_all_indices(self) -> None:
        """The various PAF calculation functions reindex some of the dfs to do their work.
        This will reset the indices of all dfs to their initial state so those functions
        can safely be run multiple times.
        """
        self.exposure.reset_index(inplace=True)
        self.exposure_sd.reset_index(inplace=True)
        self.tmrel.reset_index(inplace=True)
        self.rr.reset_index(inplace=True)
        if self.two_stage_mediator_rei_id:
            self.mediator_exposure.reset_index(inplace=True)
            self.mediator_exposure_sd.reset_index(inplace=True)
            self.mediator_tmrel.reset_index(inplace=True)
        if self.intervention_rei_id:
            self.intervention_coverage.reset_index(inplace=True)


def get_cause_set_id(rei_id: int) -> int:
    """
    Choose correct default cause set based on the rei_id.

    Right now, only air_pmhap and LBW/SG have a special cause set id.
    """
    if rei_id in constants.DEFAULT_CAUSE_SET_PER_REI:
        logger.info(
            "Setting cause_set_id to special cause set id "
            f"{constants.DEFAULT_CAUSE_SET_PER_REI[rei_id]} for rei_id {rei_id}"
        )

    return constants.DEFAULT_CAUSE_SET_PER_REI[rei_id]


def validate_unmediated_paf_if_required_else_set_to_none(
    modelable_entities: model_utils.PafModelableEntities, mediation_factors: pd.DataFrame
) -> model_utils.PafModelableEntities:
    """Confirm that the modelable_entity list and the mediation matrix agree on
    whether we need to produce an unmediated PAF model version. Return updated PAF
    modelable entities, dropping the unmediated PAF modelable entity if unmediated
    PAFs will not be computed.

    If a mediation pathway is added or removed from the mediation matrix but the
    modelable_entity metadata for the input models is not kept up to date, we could
    have a "paf_unmediated" ME in our input model list that is not actually used,
    or we could silently fail to save an unmediated PAF that is required. This
    sanity check will alert us to a mismatch before the run starts.
    """
    if modelable_entities.paf_unmediated and mediation_factors.empty:
        logger.warning(
            f"Found unmediated PAF ME {modelable_entities.paf_unmediated} but no total "
            "mediation factors < 1. Unmediated PAFs will not be produced; setting unmediated "
            "PAF ME = None."
        )
        modelable_entities.paf_unmediated = None
    if not modelable_entities.paf_unmediated and not mediation_factors.empty:
        raise ValueError(
            "The mediation matrix calls for an unmediated PAF for this risk but "
            "no modelable entity was found for unmediated PAFs. Please submit a "
            "ticket to create or activate a modelable entity."
        )
    return modelable_entities


def validate_model_description(
    extra_description: Optional[str], rei_id: int, mediation_factors: pd.DataFrame
) -> None:
    """Validate that adding the user's extra description text won't make a description
    too long to store in the epi database.
    """
    unmediated = "" if mediation_factors.empty else " unmediated"
    full_description = constants.MODEL_DESCRIPTION.format(
        unmediated=unmediated, rei_id=rei_id, extra_description=extra_description
    )
    if len(full_description) > constants.MAX_DESCRIPTION_LENGTH:
        overflow = len(full_description) - constants.MAX_DESCRIPTION_LENGTH
        raise ValueError(
            "The provided model description is too long for save_results. The total "
            f"description length must be {constants.MAX_DESCRIPTION_LENGTH} characters "
            f"or less so you need to shorten your description by {overflow} characters."
        )


def drop_pafs_of_one(rr_metadata: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """Drop PAFs of one from rr_metadata prior to launching PAF calculation."""
    return (
        rr_metadata.merge(
            ihme_cc_risk_utils.get_pafs_of_one(
                release_id=release_id, include_all_causes=True
            ),
            on=["rei_id", "cause_id"],
            how="left",
            indicator=True,
        )
        .query('_merge=="left_only"')
        .drop(columns="_merge")
    )


def validate_categorical_parameters_match(
    exposure: pd.DataFrame, rr: pd.DataFrame, root_dir: pathlib.Path
) -> None:
    """Validate that categorical exposure and RR DataFrames have the same parameters present,
    raising a RuntimeError if they do not.
    """
    params_in_exp_not_rr = set(exposure["parameter"]) - set(rr["parameter"])
    params_in_rr_not_exp = set(rr["parameter"]) - set(exposure["parameter"])

    error_message = ""
    if params_in_exp_not_rr:
        me_ids = io_utils.get(root_dir, constants.CacheContents.MODEL_VERSIONS)
        missing_categories = pd.DataFrame(
            {"exp_categ": sorted(params_in_exp_not_rr, key=lambda x: (len(x), x))}
        )
        missing_categories = missing_categories.merge(me_ids, on="exp_categ", how="left")
        missing_categories["modelable_entity_name"] = missing_categories[
            "modelable_entity_name"
        ].fillna("Residual Category")
        formatted_strings = "\n".join(
            missing_categories.apply(
                lambda row: f"{row['exp_categ']} - {row['modelable_entity_name']}", axis=1
            )
        )
        error_message += (
            f"\nFound {len(missing_categories)} category(-ies) in exposure but not relative "
            f"risk:\n{formatted_strings}\n\nYour relative risk model must include ALL "
            "exposure categories, including the residual (unexposed) category.\n"
        )
    if params_in_rr_not_exp:
        formatted_strings = "\n".join(sorted(params_in_rr_not_exp, key=lambda x: (len(x), x)))
        error_message += (
            f"\nFound {len(params_in_rr_not_exp)} category(-ies) in relative risk but not "
            f"exposure:\n{formatted_strings}\n\nAll relative risk categories other than the "
            "residual must have corresponding exposure models saved.\n"
        )
    if params_in_exp_not_rr or params_in_rr_not_exp:
        raise RuntimeError(
            "Categorical parameters in exposure and relative risk must match."
            f"\n{error_message}\nSee for more information."
        )


def validate_mediator_paf_years(
    rei_id: int,
    rr_metadata: pd.DataFrame,
    release_id: int,
    year_id: List[int],
    codcorrect_version_id: Optional[int],
    como_version_id: Optional[int],
) -> None:
    """Validate that mediator PAFs exist and have the required years, if we will be performing
    mediated subcause splitting.
    """
    # Determine outcomes that will use mediated subcause splitting, with special logic for
    # AIR_PMHAP.
    mediated_parent_cause_rr_metadata = (
        rr_metadata.query("source == 'delta' and cause_id in @constants.PARENT_LEVEL_CAUSES")
        if rei_id != constants.AIR_PMHAP_REI_ID
        else rr_metadata
    )
    # Outside of special cases for USRE and AIR_PMHAP, we will not perform subcause splitting
    # unless COMO and CodCorrect are available.
    if not mediated_parent_cause_rr_metadata.empty and (
        release_id == release.USRE
        or rei_id == constants.AIR_PMHAP_REI_ID
        or (codcorrect_version_id is not None and como_version_id is not None)
    ):
        # We will perform mediated subcause splitting; validate mediator PAFs.
        mediator_paf_model_versions = db_queries.get_best_model_versions(
            entity="rei",
            ids=mediated_parent_cause_rr_metadata.med_id.unique().tolist(),
            release_id=release_id,
            source="paf",
        )
        mediators_missing_pafs = set(mediated_parent_cause_rr_metadata.med_id) - set(
            mediator_paf_model_versions.rei_id
        )
        if mediators_missing_pafs:
            raise RuntimeError(
                "The following mediator risk(s) do not have PAF models needed for mediated "
                f"subcause splitting: {mediators_missing_pafs}."
            )
        for _, row in mediator_paf_model_versions.iterrows():
            # Validate model years if the PAF is in-rotation or has no linked wormhole model.
            if (
                row.release_id == release_id
                or _get_wormhole_paf_mvid(row.model_version_id, release_id) is None
            ):
                validate_input_model_years(
                    modelable_entity_id=row.modelable_entity_id,
                    model_version_id=row.model_version_id,
                    model_type=row.model_type,
                    draw_type=row.draw_type,
                    year_specific=None,
                    year_id=year_id,
                    release_id=release_id,
                    demographics=None,
                )


def _get_wormhole_paf_mvid(base_model_version_id: int, release_id: int) -> Optional[int]:
    """Wrapper around db_queries.lib.wormhole_utils.get_linked_wormhole_mvid."""
    with db_tools_core.session_scope(conn_defs.WORMHOLE) as session:
        return get_linked_wormhole_mvid(
            base_mvid=base_model_version_id,
            base_model_type_id=wormhole.BASE_MODEL_TYPES.PAF,
            release_id=release_id,
            summaries=False,
            session=session,
        )
