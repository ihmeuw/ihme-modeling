import warnings
from typing import Any, Dict, Final, Iterator, List, Optional, Set, Type, Union, cast

import toposort
from sqlalchemy import orm

import db_queries
import db_stgpr
import ihme_cc_rules_client
import stgpr_schema
from db_stgpr.api.enums import BundleShape, MeModelType
from gbd import constants as gbd_constants
from gbd import estimation_years as gbd_estimation_years
from gbd import release as gbd_release
from hierarchies.legacy import dbtrees
from ihme_cc_rules_client import ResearchAreas, Rules, Tools
from stgpr_schema.models import ValidationError

from stgpr_helpers.lib import covariate_client_utils, model_type_utils
from stgpr_helpers.lib.constants import columns, demographics, draws
from stgpr_helpers.lib.constants import exceptions as exc
from stgpr_helpers.lib.constants import modelable_entities, parameters


class _ValidationError(ValidationError):
    """Error information for a failed parameter validation.

    Inherits from stgpr_schema.models.ValidationError.

    Attributes:
        exception: The class of the exception associated with the error.
    """

    exception: Type[Exception]


class _Validation:
    """Base parameter validation class.

    This class should get subclassed for individual validations.

    Attributes:
        parameters: Dictionary of all parameters.
        stgpr_session: Session with the ST-GPR database.
        covariates_session: Session with the covariates database.
        epi_session: Session with the epi database.
        dependencies: Validations that must run before this validation runs.
        required_parameters: Parameters that are required for this validation to run. This
            is purely a sanity check.
    """

    # Static attributes. These should be set in subclasses.
    dependencies: List[Type["_Validation"]]
    required_parameters: List[str]

    def __init__(
        self,
        parameters: Dict[str, Any],
        stgpr_session: orm.Session,
        epi_session: orm.Session,
        covariates_session: orm.Session,
        shared_vip_session: orm.Session,
    ):
        self.parameters: Dict[str, Any] = parameters
        self.stgpr_session: orm.Session = stgpr_session
        self.epi_session: orm.Session = epi_session
        self.covariates_session: orm.Session = covariates_session
        self.shared_vip_session: orm.Session = shared_vip_session

    def validate(self) -> Optional[_ValidationError]:
        """Runs the validation.

        Returns:
            Object containing information about the failed validation, or None if the
            validation succeeded.
        """
        raise NotImplementedError()


class _ReleaseIdValidation(_Validation):
    """Checks that release id is valid and provided if required."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.RELEASE_ID]

    def validate(self) -> Optional[_ValidationError]:
        release_id = self.parameters[parameters.RELEASE_ID]

        try:
            gbd_release.validate_release_id(release_id)
        except ValueError as e:
            return _ValidationError(
                exception=exc.ReleaseIdError,
                message=str(e),
                relevant_parameters=self.required_parameters,
            )


class _DescriptionValidation(_Validation):
    """Checks that the description is 140 characters or less."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.DESCRIPTION]

    def validate(self) -> Optional[_ValidationError]:
        description = self.parameters[parameters.DESCRIPTION]
        if len(description) > 140:
            return _ValidationError(
                exception=exc.DescriptionTooLong,
                message=f"Description must be 140 characters or less, got {len(description)}",
                relevant_parameters=self.required_parameters,
            )


class _HoldoutsValidation(_Validation):
    """Checks that holdouts are between 0 and 10."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.HOLDOUTS]

    def validate(self) -> Optional[_ValidationError]:
        holdouts = self.parameters[parameters.HOLDOUTS]
        if holdouts < 0 or holdouts > 10:
            return _ValidationError(
                exception=exc.InvalidHoldoutNumber,
                message=f"Holdouts must be within 0-10, got {holdouts}",
                relevant_parameters=self.required_parameters,
            )


class _DrawsValidation(_Validation):
    """Checks that there are 0, 100, or 1000 draws."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.GPR_DRAWS]

    def validate(self) -> Optional[_ValidationError]:
        gpr_draws = self.parameters[parameters.GPR_DRAWS]
        if gpr_draws not in draws.ALLOWED_GPR_DRAWS:
            return _ValidationError(
                exception=exc.InvalidDrawNumber,
                message=f"Draws must be 0, 100, or 1000; got {gpr_draws}",
                relevant_parameters=self.required_parameters,
            )


class _UniqueDensityCutoffsValidation(_Validation):
    """Checks that density cutoffs are unique."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.DENSITY_CUTOFFS]

    def validate(self) -> Optional[_ValidationError]:
        density_cutoffs = self.parameters[parameters.DENSITY_CUTOFFS]
        if len(density_cutoffs) != len(set(density_cutoffs)):
            return _ValidationError(
                exception=exc.NonUniqueDensityCutoffs,
                message=f"Density cutoffs must be unique; got {density_cutoffs}",
                relevant_parameters=self.required_parameters,
            )


class _MetricIdValidation(_Validation):
    """Checks that metric id is either number or rate."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.METRIC_ID]

    def validate(self) -> Optional[_ValidationError]:
        settings = stgpr_schema.get_settings()
        metric_id = self.parameters[parameters.METRIC_ID]

        # Null metric_id is allowed if `metric_id_required` feature flag is off
        if not metric_id:
            if settings.metric_id_required:
                return _ValidationError(
                    exception=exc.InvalidMetricId,
                    message="Metric ID is a required parameter but was not given",
                    relevant_parameters=self.required_parameters,
                )
            else:
                return

        allowed_metric_ids = [gbd_constants.metrics.NUMBER, gbd_constants.metrics.RATE]
        if metric_id not in allowed_metric_ids:
            return _ValidationError(
                exception=exc.InvalidMetricId,
                message=(
                    f"Metric ID must be either {gbd_constants.metrics.NUMBER} (number), "
                    f"or {gbd_constants.metrics.RATE} (rate); got {metric_id}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _CrossValValidation(_Validation):
    """Checks that cross validation is not run with density cutoffs."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.DENSITY_CUTOFFS, parameters.HOLDOUTS]

    def validate(self) -> Optional[_ValidationError]:
        density_cutoffs = self.parameters[parameters.DENSITY_CUTOFFS]
        holdouts = self.parameters[parameters.HOLDOUTS]
        if density_cutoffs and holdouts:
            return _ValidationError(
                exception=exc.CrossValidationWithDensityCutoffs,
                message="Running cross validation with density cutoffs is not allowed",
                relevant_parameters=self.required_parameters,
            )


class _RakeLogitValidation(_Validation):
    """Checks that logit raking is only used with logit models."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.RAKE_LOGIT, parameters.DATA_TRANSFORM]

    def validate(self) -> Optional[_ValidationError]:
        rake_logit = self.parameters[parameters.RAKE_LOGIT]
        data_transform = self.parameters[parameters.DATA_TRANSFORM]
        if rake_logit and data_transform != stgpr_schema.TransformType.logit.name:
            return _ValidationError(
                exception=exc.InvalidTransformWithLogitRaking,
                message=f"Cannot specify logit raking with transform {data_transform}",
                relevant_parameters=self.required_parameters,
            )


class _DataSpecificationValidation(_Validation):
    """Checks that `path_to_data` or (`crosswalk_version_id` + `bundle_id`) is specified."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [
        parameters.CROSSWALK_VERSION_ID,
        parameters.BUNDLE_ID,
        parameters.PATH_TO_DATA,
    ]

    def validate(self) -> Optional[_ValidationError]:
        crosswalk_version_id = self.parameters[parameters.CROSSWALK_VERSION_ID]
        bundle_id = self.parameters[parameters.BUNDLE_ID]
        path_to_data = self.parameters[parameters.PATH_TO_DATA]

        # Either (crosswalk_version_id and bundle_id) OR path_to_data must be provided.
        # Fail if nothing is passed or some combination of crosswalk_version_id/bundle_id and
        # path_to_data is given.
        if not (
            ((crosswalk_version_id and bundle_id) and not path_to_data)
            or (not (crosswalk_version_id or bundle_id) and path_to_data)
        ):
            return _ValidationError(
                exception=exc.InvalidDataSpecification,
                message=(
                    f"Must specify either (crosswalk_version_id and bundle_id) or "
                    f"path_to_data. Got crosswalk_version_id {crosswalk_version_id}, "
                    f"bundle_id {bundle_id}, path_to_data {path_to_data}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _SingleCustomInputValidation(_Validation):
    """Checks that only one of custom stage 1 or custom covariates is present."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [
        parameters.PATH_TO_CUSTOM_STAGE_1,
        parameters.PATH_TO_CUSTOM_COVARIATES,
    ]

    def validate(self) -> Optional[_ValidationError]:
        path_to_custom_stage_1 = self.parameters[parameters.PATH_TO_CUSTOM_STAGE_1]
        path_to_custom_covariates = self.parameters[parameters.PATH_TO_CUSTOM_COVARIATES]
        if path_to_custom_stage_1 and path_to_custom_covariates:
            return _ValidationError(
                exception=exc.TwoCustomInputs,
                message="Cannot use both custom covariates and custom stage 1",
                relevant_parameters=self.required_parameters,
            )


class _CustomStage1Validation(_Validation):
    """Checks that exactly one of custom stage 1 and stage 1 model formula is provided."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [
        parameters.PATH_TO_CUSTOM_STAGE_1,
        parameters.STAGE_1_MODEL_FORMULA,
    ]

    def validate(self) -> Optional[_ValidationError]:
        path_to_custom_stage_1 = self.parameters[parameters.PATH_TO_CUSTOM_STAGE_1]
        stage_1_model_formula = self.parameters[parameters.STAGE_1_MODEL_FORMULA]
        if not path_to_custom_stage_1 and not stage_1_model_formula:
            return _ValidationError(
                exception=exc.BothOrNeitherCustomStage1AndFormulaProvided,
                message=(
                    "You must provide either a stage 1 model formula or path to custom stage "
                    "1 inputs. Found neither"
                ),
                relevant_parameters=self.required_parameters,
            )
        if path_to_custom_stage_1 and stage_1_model_formula:
            return _ValidationError(
                exception=exc.BothOrNeitherCustomStage1AndFormulaProvided,
                message=(
                    "You must provide either a stage 1 model formula or path to custom stage "
                    "1 inputs. Found both"
                ),
                relevant_parameters=self.required_parameters,
            )


class _DuplicateHyperparameterValidation(_Validation):
    """Checks that hyperparameters do not have duplicates (if not data density model)."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [
        parameters.DENSITY_CUTOFFS,
        parameters.ST_LAMBDA,
        parameters.ST_OMEGA,
        parameters.ST_ZETA,
        parameters.GPR_SCALE,
    ]

    def validate(self) -> Optional[_ValidationError]:
        density_cutoffs = self.parameters[parameters.DENSITY_CUTOFFS]
        st_lambda = self.parameters[parameters.ST_LAMBDA]
        st_omega = self.parameters[parameters.ST_OMEGA]
        st_zeta = self.parameters[parameters.ST_ZETA]
        gpr_scale = self.parameters[parameters.GPR_SCALE]

        if density_cutoffs:
            return

        hyperparameters_to_validate = {
            parameters.ST_LAMBDA: st_lambda,
            parameters.ST_OMEGA: st_omega,
            parameters.ST_ZETA: st_zeta,
            parameters.GPR_SCALE: gpr_scale,
        }
        bad_hyperparameters = [
            param
            for param, param_value in hyperparameters_to_validate.items()
            if len(param_value) != len(set(param_value))
        ]
        if bad_hyperparameters:
            hyperparam_names = ", ".join(hyperparameters_to_validate.keys())
            bad_hyperparam_names = ", ".join(bad_hyperparameters)
            return _ValidationError(
                exception=exc.DuplicateHyperparameters,
                message=(
                    f"{hyperparam_names} must not have any duplicates for non-data density "
                    "models as it would result in identical hyperparameter sets. Found "
                    f"duplicate values for the following: {bad_hyperparam_names}"
                ),
                relevant_parameters=bad_hyperparameters,
            )


class _HyperparameterCountValidation(_Validation):
    """Checks that hyperparameters have length density_cutoffs + 1."""

    dependencies: List[Type[_Validation]] = [_UniqueDensityCutoffsValidation]
    required_parameters: List[str] = [
        parameters.DENSITY_CUTOFFS,
        parameters.ST_LAMBDA,
        parameters.ST_OMEGA,
        parameters.ST_ZETA,
        parameters.GPR_SCALE,
    ]

    def validate(self) -> Optional[_ValidationError]:
        density_cutoffs = self.parameters[parameters.DENSITY_CUTOFFS]
        st_lambda = self.parameters[parameters.ST_LAMBDA]
        st_omega = self.parameters[parameters.ST_OMEGA]
        st_zeta = self.parameters[parameters.ST_ZETA]
        gpr_scale = self.parameters[parameters.GPR_SCALE]

        if not density_cutoffs:
            return

        hyperparameters_to_validate = {
            parameters.ST_LAMBDA: st_lambda,
            parameters.ST_OMEGA: st_omega,
            parameters.ST_ZETA: st_zeta,
            parameters.GPR_SCALE: gpr_scale,
        }
        bad_hyperparameters = [
            param
            for param, param_value in hyperparameters_to_validate.items()
            if len(param_value) != len(density_cutoffs) + 1
        ]
        if bad_hyperparameters:
            hyperparam_names = ", ".join(hyperparameters_to_validate.keys())
            bad_hyperparam_names = ", ".join(bad_hyperparameters)
            return _ValidationError(
                exception=exc.InvalidHyperparameterCount,
                message=(
                    f"{hyperparam_names} must all have length {len(density_cutoffs) + 1} "
                    "(number of density cutoffs + 1). Found an invalid number of "
                    f"hyperparameters for the following: {bad_hyperparam_names}"
                ),
                relevant_parameters=bad_hyperparameters,
            )


class _AgeOmegaValidation(_Validation):
    """Checks that multiple omegas are not cross-validated in a single-age model."""

    dependencies: List[Type[_Validation]] = [_UniqueDensityCutoffsValidation]
    required_parameters: List[str] = [
        parameters.DENSITY_CUTOFFS,
        parameters.HOLDOUTS,
        parameters.ST_LAMBDA,
        parameters.ST_OMEGA,
        parameters.ST_ZETA,
        parameters.GPR_SCALE,
        parameters.PREDICTION_AGE_GROUP_IDS,
    ]

    def validate(self) -> Optional[_ValidationError]:
        density_cutoffs = self.parameters[parameters.DENSITY_CUTOFFS]
        holdouts = self.parameters[parameters.HOLDOUTS]
        st_lambda = self.parameters[parameters.ST_LAMBDA]
        st_omega = self.parameters[parameters.ST_OMEGA]
        st_zeta = self.parameters[parameters.ST_ZETA]
        gpr_scale = self.parameters[parameters.GPR_SCALE]
        age_group_ids = self.parameters[parameters.PREDICTION_AGE_GROUP_IDS]

        model_type = model_type_utils.determine_model_type(
            density_cutoffs, holdouts, st_lambda, st_omega, st_zeta, gpr_scale
        )
        if (
            model_type
            in (
                stgpr_schema.ModelType.oos_selection,
                stgpr_schema.ModelType.in_sample_selection,
            )
            and len(age_group_ids) == 1
            and len(st_omega) != 1
        ):
            return _ValidationError(
                exception=exc.InvalidOmegaCrossValidation,
                message=(
                    "You are running a model with only one age group, but you're trying to "
                    "run cross validation for different omegas. This is extremely "
                    "inefficient for no added benefit"
                ),
                relevant_parameters=[
                    parameters.ST_OMEGA,
                    parameters.PREDICTION_AGE_GROUP_IDS,
                ],
            )


class _ModelableEntityValidation(_Validation):
    """Checks that ME exists, is not the generic ST-GPR ME, and has model type ST-GPR."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.MODELABLE_ENTITY_ID, parameters.RELEASE_ID]

    def validate(self) -> Optional[_ValidationError]:
        modelable_entity_id = self.parameters[parameters.MODELABLE_ENTITY_ID]
        if not db_stgpr.modelable_entity_exists(modelable_entity_id, self.stgpr_session):
            return _ValidationError(
                exception=exc.NoModelableEntityFound,
                message=f"Modelable entity ID {modelable_entity_id} does not exist",
                relevant_parameters=self.required_parameters,
            )
        if modelable_entity_id == modelable_entities.GENERIC_STGPR_ME_ID:
            return _ValidationError(
                exception=UserWarning,
                message=(
                    f"Generic ST-GPR ME {modelable_entity_id} was passed. This ME is only "
                    "for one-off analyses, test models, or models that also use a covariate "
                    "ID. It is not possible to save results for this ME, and it should be "
                    "used with care"
                ),
                relevant_parameters=self.required_parameters,
                warning=True,
            )

        model_type = db_stgpr.get_modelable_entity_model_type(
            modelable_entity_id, self.stgpr_session
        )
        if model_type != MeModelType.stgpr:
            return _ValidationError(
                exception=exc.InvalidModelType,
                message=(
                    f"Modelable entity {modelable_entity_id} has model type "
                    f"{model_type.name}, but ST-GPR requires modelable entities to have "
                    f"model type {MeModelType.stgpr.name}."
                ),
                relevant_parameters=self.required_parameters,
            )


class _CovariateIdValidation(_Validation):
    """Checks that covariate ID exists.

    If the covariate MEs feature is live, then we ignore covariate_id in the config: modelers
    must use an ME linked to a covariate. If covariate_id is still present in the config, then
    raise a warning saying that covariate_id will be ignored.

    Once covariate MEs feature has been live for some time, we can remove this validation
    entirely and just ignore covariate_id when reading the config.
    """

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.COVARIATE_ID, parameters.RELEASE_ID]

    def validate(self) -> Optional[_ValidationError]:
        covariate_id = self.parameters[parameters.COVARIATE_ID]
        release_id = self.parameters[parameters.RELEASE_ID]
        covariate_mes_feature_enabled = bool(
            ihme_cc_rules_client.RulesManager(
                release_id=release_id,
                research_area=ResearchAreas.COVARIATES,
                tool=Tools.SHARED_FUNCTIONS,
            ).get_rule_value(Rules.FEATURE_COVARIATE_MES)
        )
        if covariate_mes_feature_enabled:
            if covariate_id:
                return _ValidationError(
                    exception=UserWarning,
                    message=(
                        f"Config contains entry covariate_id {covariate_id}, but the "
                        "covariate_id parameter is no longer supported. This covariate will "
                        "not be linked to your ST-GPR model; please remove covariate_id from "
                        "your config and use a modelable entity linked to a covariate instead"
                    ),
                    relevant_parameters=self.required_parameters,
                    warning=True,
                )
            else:
                return

        if covariate_id and not db_stgpr.covariate_exists(covariate_id, self.stgpr_session):
            return _ValidationError(
                exception=exc.NoCovariateFound,
                message=f"Covariate ID {covariate_id} does not exist",
                relevant_parameters=self.required_parameters,
            )


class _CovariateNameShortsValidation(_Validation):
    """Checks that covariate name shorts exist."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.GBD_COVARIATES]

    def validate(self) -> Optional[_ValidationError]:
        gbd_covariates = self.parameters[parameters.GBD_COVARIATES]
        valid_gbd_covariates = covariate_client_utils.get_covariates()[
            "covariate_name_short"
        ].tolist()
        gbd_covariates_has_errors = [
            covariate_name_short not in valid_gbd_covariates
            for covariate_name_short in gbd_covariates
        ]
        invalid_covariates = ", ".join(
            [
                str(covariate_name_short)
                for has_error, covariate_name_short in zip(
                    gbd_covariates_has_errors, gbd_covariates
                )
                if has_error
            ]
        )
        if any(gbd_covariates_has_errors):
            return _ValidationError(
                exception=exc.NoCovariateNameShortFound,
                message=(
                    f"Some covariate short names do not exist, got non-existent "
                    f"covariate_name_shorts: {invalid_covariates}"
                ),
                relevant_parameters=self.required_parameters,
                gbd_covariates_has_errors=gbd_covariates_has_errors,
            )


class _CovariateMvidsValidation(_Validation):
    """Checks the covariate MVIDs given have corresponding covariate names and are valid."""

    dependencies: List[Type[_Validation]] = [_CovariateNameShortsValidation]
    required_parameters: List[str] = [
        parameters.GBD_COVARIATES,
        parameters.GBD_COVARIATE_MODEL_VERSION_IDS,
        parameters.RELEASE_ID,
    ]

    def validate(self) -> Optional[_ValidationError]:
        gbd_covariates = self.parameters[parameters.GBD_COVARIATES]
        gbd_covariate_mvids = self.parameters[parameters.GBD_COVARIATE_MODEL_VERSION_IDS]
        release_id = self.parameters[parameters.RELEASE_ID]

        if not gbd_covariate_mvids:
            return

        # 1) Can't specify covariate MVIDs without also giving covariate names
        if not gbd_covariates:
            return _ValidationError(
                exception=exc.MisspecifiedCovariateModelVersionId,
                message=(
                    f"Covariate model versions provided ({gbd_covariate_mvids}) but "
                    f"{parameters.GBD_COVARIATES} not given."
                ),
                relevant_parameters=[
                    parameters.GBD_COVARIATE_MODEL_VERSION_IDS,
                    parameters.GBD_COVARIATES,
                ],
            )

        # 2) Covariate names and covariate MVIDs given are same length
        if len(gbd_covariates) != len(gbd_covariate_mvids):
            return _ValidationError(
                exception=exc.MisspecifiedCovariateModelVersionId,
                message=(
                    "GBD covariates and covariate model version ids must match in length but "
                    f"did not.\nGBD covariate(s): {gbd_covariates}\n"
                    f"GBD covariate model version id(s): {gbd_covariate_mvids}"
                ),
                relevant_parameters=[
                    parameters.GBD_COVARIATE_MODEL_VERSION_IDS,
                    parameters.GBD_COVARIATES,
                ],
            )

        # 3) Covariate MVIDs are either ids or 'best'
        gbd_covariates_has_errors = [
            not isinstance(mvid, int) and mvid != parameters.BEST_MVID
            for mvid in gbd_covariate_mvids
        ]
        if any(gbd_covariates_has_errors):
            invalid_mvids = ", ".join(
                [
                    str(mvid)
                    for has_error, mvid in zip(gbd_covariates_has_errors, gbd_covariate_mvids)
                    if has_error
                ]
            )
            return _ValidationError(
                exception=exc.MisspecifiedCovariateModelVersionId,
                message=(
                    "Covariate model version ids must either be an integer or "
                    f"'{parameters.BEST_MVID}', got incorrect model versions IDs: "
                    f"{invalid_mvids}"
                ),
                relevant_parameters=[parameters.GBD_COVARIATES],
                gbd_covariates_has_errors=gbd_covariates_has_errors,
            )

        # 4) Covariate MVIDs exist and match the covariate name short AND are approved
        return _validate_covariate_mvids_exist_and_are_approved(
            release_id=release_id,
            covariate_name_shorts=gbd_covariates,
            covariate_model_version_ids=gbd_covariate_mvids,
            stgpr_session=self.stgpr_session,
            covariates_session=self.covariates_session,
        )


class _AgeGroupIdsValidation(_Validation):
    """Checks that all prediction age group ids are valid."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.PREDICTION_AGE_GROUP_IDS]

    def validate(self) -> Optional[_ValidationError]:
        age_group_ids = self.parameters[parameters.PREDICTION_AGE_GROUP_IDS]
        invalid_ids = db_stgpr.get_invalid_age_group_ids(age_group_ids, self.stgpr_session)
        if invalid_ids:
            return _ValidationError(
                exception=exc.InvalidAgeGroupIds,
                message=(
                    f"{len(invalid_ids)} invalid prediction age group id(s) given: "
                    f"{', '.join([str(i) for i in invalid_ids])}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _SexIdsValidation(_Validation):
    """Checks that all prediction sex ids are valid."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.PREDICTION_SEX_IDS]

    def validate(self) -> Optional[_ValidationError]:
        sex_ids = self.parameters[parameters.PREDICTION_SEX_IDS]
        invalid_ids = db_stgpr.get_invalid_sex_ids(sex_ids, self.stgpr_session)
        if invalid_ids:
            return _ValidationError(
                exception=exc.InvalidSexIds,
                message=(
                    f"{len(invalid_ids)} invalid prediction sex id(s) given: "
                    f"{', '.join([str(i) for i in invalid_ids])}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _YearIdsExistValidation(_Validation):
    """Checks that all prediction year ids are valid.

    Only validates the passed start and end years; if those years are valid, then so is every
    year between them.
    """

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.YEAR_START, parameters.YEAR_END]

    def validate(self) -> Optional[_ValidationError]:
        year_start = self.parameters[parameters.YEAR_START]
        year_end = self.parameters[parameters.YEAR_END]

        # if year_end is None, value is filled after parameter validation
        ids_to_check = [year_start, year_end] if year_end else [year_start]
        invalid_ids = db_stgpr.get_invalid_year_ids(ids_to_check, self.stgpr_session)

        if invalid_ids:
            return _ValidationError(
                exception=exc.InvalidYearIds,
                message=(
                    f"Invalid year start and/or year end given: "
                    f"{', '.join([str(i) for i in invalid_ids])}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _YearEndYearStartValidation(_Validation):
    """Checks that year_end is greater or equal to year_start."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.YEAR_START, parameters.YEAR_END]

    def validate(self) -> Optional[_ValidationError]:
        year_start = self.parameters[parameters.YEAR_START]
        year_end = self.parameters[parameters.YEAR_END]

        if not year_end:
            return

        if year_end < year_start:
            return _ValidationError(
                exception=exc.InvalidYearIds,
                message=(
                    "year start must not be greater than year end, was given year start "
                    f"{year_start} and year end {year_end}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _YearEndMinimumValueValidation(_Validation):
    """Checks that year_end is greater or equal to the terminal year of the GBD release ID."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.RELEASE_ID, parameters.YEAR_END]

    def validate(self) -> Optional[_ValidationError]:
        release_id = self.parameters[parameters.RELEASE_ID]
        year_end = self.parameters[parameters.YEAR_END]

        if not year_end:
            return

        min_year = gbd_estimation_years.estimation_years_from_release_id(release_id)[-1]
        if year_end < min_year:
            return _ValidationError(
                exception=exc.InvalidYearEnd,
                message=(
                    "year end must be equal to or greater than the terminal estimation year "
                    f"for the GBD release. For GBD release ID {release_id}, the final "
                    f"estimation year is {min_year}, was passed year end {year_end}"
                ),
                relevant_parameters=[parameters.YEAR_END],
            )


class _CrosswalkVersionValidation(_Validation):
    """Checks that crosswalk version exists."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.CROSSWALK_VERSION_ID]

    def validate(self) -> Optional[_ValidationError]:
        crosswalk_version_id = self.parameters[parameters.CROSSWALK_VERSION_ID]
        if crosswalk_version_id and not db_stgpr.crosswalk_version_exists(
            crosswalk_version_id, self.epi_session
        ):
            return _ValidationError(
                exception=exc.NoCrosswalkVersionFound,
                message=f"Crosswalk version {crosswalk_version_id} does not exist",
                relevant_parameters=self.required_parameters,
            )


class _GprAmplitudeValidation(_Validation):
    """Checks that either amplitude or some combo of the gpr amplitude parameters given."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [
        parameters.CUSTOM_AMPLITUDE,
        parameters.GPR_AMP_CUTOFF,
        parameters.GPR_AMP_FACTOR,
        parameters.GPR_AMP_METHOD,
    ]

    def validate(self) -> Optional[_ValidationError]:
        custom_amplitude = self.parameters[parameters.CUSTOM_AMPLITUDE]
        gpr_amp_cutoff = self.parameters[parameters.GPR_AMP_CUTOFF]
        gpr_amp_factor = self.parameters[parameters.GPR_AMP_FACTOR]
        gpr_amp_method = self.parameters[parameters.GPR_AMP_METHOD]

        if custom_amplitude and (gpr_amp_cutoff or gpr_amp_factor or gpr_amp_method):
            return _ValidationError(
                exception=exc.InvalidGprAmplitudeCombination,
                message=(
                    "Custom amplitude and some combination of gpr_amp_cutoff/gpr_amp_factor/"
                    "gpr_amp_method provided, which is inconsistent. Either provide only "
                    "amplitude or some combination of the gpr amplitude parameters."
                ),
                relevant_parameters=self.required_parameters,
            )


class _BundleShapeValidation(_Validation):
    """Checks that passed bundle is of shape 'stgpr'."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.BUNDLE_ID]

    def validate(self) -> Union[_ValidationError, None]:
        bundle_id = self.parameters[parameters.BUNDLE_ID]

        if not bundle_id:
            return

        bundle_shape_id = db_stgpr.get_bundle_shape_id(bundle_id, session=self.epi_session)
        if bundle_shape_id != BundleShape.stgpr.value:
            shape_map = {shape.value: shape.name for shape in BundleShape}
            return _ValidationError(
                exception=exc.BundleIsNotStgprShape,
                message=(
                    f"Bundle ID {bundle_id} has shape {shape_map[bundle_shape_id]} and is not "
                    f"of type stgpr. Please provide a bundle_id that is of the stgpr shape."
                ),
                relevant_parameters=self.required_parameters,
            )


class _LinkedBundleValidation(_Validation):
    """Checks that passed bundle is linked to passed crosswalk."""

    dependencies: List[Type[_Validation]] = [
        _CrosswalkVersionValidation,
        _BundleShapeValidation,
    ]
    required_parameters: List[str] = [parameters.CROSSWALK_VERSION_ID, parameters.BUNDLE_ID]

    def validate(self) -> Optional[_ValidationError]:
        crosswalk_version_id = self.parameters[parameters.CROSSWALK_VERSION_ID]
        bundle_id = self.parameters[parameters.BUNDLE_ID]
        if not crosswalk_version_id:
            return

        associated_bundle_id = db_stgpr.get_linked_bundle_id(
            crosswalk_version_id, self.epi_session
        )
        if bundle_id != associated_bundle_id:
            return _ValidationError(
                exception=exc.BundleDoesNotMatchCrosswalkVersion,
                message=(
                    f"Bundle ID {bundle_id} provided in the config does not match bundle ID "
                    f"{associated_bundle_id} associated with crosswalk version ID "
                    f"{crosswalk_version_id}"
                ),
                relevant_parameters=self.required_parameters,
            )


class _GbdAgeGroupsValidation(_Validation):
    """Checks that age groups are part of the GBD age group set and warns otherwise."""

    dependencies: List[Type[_Validation]] = [_ReleaseIdValidation]
    required_parameters: List[str] = [
        parameters.PREDICTION_AGE_GROUP_IDS,
        parameters.RELEASE_ID,
    ]

    def validate(self) -> Optional[_ValidationError]:
        prediction_age_group_ids = self.parameters[parameters.PREDICTION_AGE_GROUP_IDS]
        release_id = self.parameters[parameters.RELEASE_ID]

        active_age_group_ids = db_queries.get_age_metadata(
            release_id=release_id, session=self.shared_vip_session
        )[columns.AGE_GROUP_ID].tolist()
        active_age_group_ids.append(gbd_constants.age.ALL_AGES)

        difference = set(prediction_age_group_ids).difference(active_age_group_ids)
        if difference:
            return _ValidationError(
                exception=UserWarning,
                message=(
                    f"Using non-standard prediction age group IDs {difference} for GBD "
                    f"release ID {release_id}. Standard age group set contains IDs: "
                    f"{sorted(active_age_group_ids)}"
                ),
                relevant_parameters=[
                    parameters.PREDICTION_AGE_GROUP_IDS,
                    parameters.RELEASE_ID,
                ],
                warning=True,
            )


class _CustomAgeVectorValidation(_Validation):
    """Checks that the custom age vector, if given, is the same length as prediction ages."""

    dependencies: List[Type[_Validation]] = [_AgeGroupIdsValidation]
    required_parameters: List[str] = [
        parameters.PREDICTION_AGE_GROUP_IDS,
        parameters.ST_CUSTOM_AGE_VECTOR,
    ]

    def validate(self) -> Optional[_ValidationError]:
        age_group_ids = self.parameters[parameters.PREDICTION_AGE_GROUP_IDS]
        custom_age_vector = self.parameters[parameters.ST_CUSTOM_AGE_VECTOR]

        if not custom_age_vector:
            return

        if len(age_group_ids) != len(custom_age_vector):
            return _ValidationError(
                exception=exc.InvalidCustomAgeVector,
                message=(
                    "If provided, custom age vector must have the same length as prediction "
                    f"age group ids.\nst_custom_age_vector (length {len(custom_age_vector)}):"
                    f" {custom_age_vector}\nprediction_age_group_id (length "
                    f"{len(age_group_ids)}): {age_group_ids}"
                ),
                relevant_parameters=[
                    parameters.PREDICTION_AGE_GROUP_IDS,
                    parameters.ST_CUSTOM_AGE_VECTOR,
                ],
            )


class _AgeExpandValidation(_Validation):
    """Checks that prediction_age_group_ids and age_expand are valid.

    If age_expand is passed, validates:
        1) Only one age group in prediction_age_group_ids to be split
        2) Passed age_group_id is an aggregate
        3) Passed age_expand contains all most detailed age groups for splitting
    """

    dependencies: List[Type[_Validation]] = [_AgeGroupIdsValidation]
    required_parameters: List[str] = [
        parameters.PREDICTION_AGE_GROUP_IDS,
        parameters.AGE_EXPAND,
        parameters.RELEASE_ID,
    ]

    def validate(self) -> Optional[_ValidationError]:
        prediction_age_group_id = self.parameters[parameters.PREDICTION_AGE_GROUP_IDS]
        age_expand = self.parameters[parameters.AGE_EXPAND]
        release_id = self.parameters[parameters.RELEASE_ID]

        if not age_expand:
            return

        # Validate only 1 aggregate age group is being split if age_expand is passed
        if len(prediction_age_group_id) != 1:
            return _ValidationError(
                exception=exc.InvalidAgeExpand,
                message=(
                    "Expanding results to child age groups requires arg "
                    f"'{parameters.PREDICTION_AGE_GROUP_IDS}' to be a single aggregate "
                    f"age group, was given: {prediction_age_group_id}"
                ),
                relevant_parameters=[
                    parameters.PREDICTION_AGE_GROUP_IDS,
                    parameters.AGE_EXPAND,
                ],
            )
        # Pull the set of most detailed age groups for the given aggregate
        agetree = dbtrees.agetree(
            prediction_age_group_id[0], release_id=release_id, session=self.shared_vip_session
        )
        if len(agetree.leaves()) <= 1:
            return _ValidationError(
                exception=exc.InvalidAgeExpand,
                message=(
                    "Age group expansion requested for an age_group_id that is not an "
                    "aggregate. This is not allowed. Check that your data contain a single, "
                    f"aggregate age_group_id, was given age group {prediction_age_group_id[0]}"
                ),
                relevant_parameters=[
                    parameters.PREDICTION_AGE_GROUP_IDS,
                    parameters.AGE_EXPAND,
                ],
            )

        if (
            gbd_constants.age.BIRTH in age_expand
            and gbd_constants.age.BIRTH not in agetree.node_ids
        ):
            # Need to add birth prevalence age group if provided
            agetree.add_node(gbd_constants.age.BIRTH, None, agetree.root.id)

        # Validate all age groups in age_expand exist and are most-detailed
        age_set_diff = set(age_expand) - set([int(n.id) for n in agetree.leaves()])
        if age_set_diff:
            return _ValidationError(
                exception=exc.InvalidAgeExpand,
                message=(
                    "The following IDs in the age_expand list are not part of the most "
                    f"detailed components for aggregate age group "
                    f"{prediction_age_group_id[0]}. These age groups are not allowed: "
                    f"{', '.join([str(age) for age in age_set_diff])}"
                ),
                relevant_parameters=[
                    parameters.PREDICTION_AGE_GROUP_IDS,
                    parameters.AGE_EXPAND,
                ],
            )


class _SexExpandValidation(_Validation):
    """Checks that prediction_sex_ids and sex_expand are valid.

    If sex_expand is passed, validates:
        1) prediction_sex_ids is both-sex (3)
        2) sex_expand is equal to sex_ids [1, 2]
    """

    dependencies: List[Type[_Validation]] = [_SexIdsValidation]
    required_parameters: List[str] = [parameters.PREDICTION_SEX_IDS, parameters.SEX_EXPAND]

    def validate(self) -> Optional[_ValidationError]:
        prediction_sex_id = self.parameters[parameters.PREDICTION_SEX_IDS]
        sex_expand = self.parameters[parameters.SEX_EXPAND]

        if not sex_expand:
            return

        sex_tree = dbtrees.sextree()
        # If sex_expand is not equal to both-sex, raise
        if prediction_sex_id[0] != sex_tree.root.id:
            return _ValidationError(
                exception=exc.InvalidSexExpand,
                message=(
                    "Expanding results to child sex_ids requires arg "
                    f"'{parameters.PREDICTION_SEX_IDS}' to be both-sex (sex_id 3), was given "
                    f"{prediction_sex_id}"
                ),
                relevant_parameters=[parameters.PREDICTION_SEX_IDS, parameters.SEX_EXPAND],
            )

        sex_tree_leaves = [int(n.id) for n in sex_tree.leaves()]
        sex_set_equal = set(sex_expand) == set(sex_tree_leaves)
        if not sex_set_equal:
            return _ValidationError(
                exception=exc.InvalidSexExpand,
                message=(
                    f"Both-sex model results can only be expanded to both sex_ids "
                    f"{gbd_constants.sex.MALE} and {gbd_constants.sex.FEMALE}, was given "
                    f"sex_id {prediction_sex_id}"
                ),
                relevant_parameters=[parameters.PREDICTION_SEX_IDS, parameters.SEX_EXPAND],
            )


class _PregnancyAgeGroupIdsValidation(_Validation):
    """Checks that all prediction age group ids are valid for a pregnancy model."""

    dependencies: List[Type[_Validation]] = [_AgeGroupIdsValidation]
    required_parameters: List[str] = [
        parameters.PREDICTION_AGE_GROUP_IDS,
        parameters.IS_PREGNANCY_ME,
    ]

    def validate(self) -> Optional[_ValidationError]:
        prediction_age_group_ids = self.parameters[parameters.PREDICTION_AGE_GROUP_IDS]
        is_pregnancy_me = self.parameters[parameters.IS_PREGNANCY_ME]

        if is_pregnancy_me:
            invalid_ids = [
                id
                for id in prediction_age_group_ids
                if id not in demographics.PREGNANCY_AGE_GROUP_IDS
            ]
            if invalid_ids:
                return _ValidationError(
                    exception=exc.InvalidPregnancyAgeGroupIds,
                    message=(
                        f"{len(invalid_ids)} invalid prediction age group id(s) given for "
                        f"pregnancy model, invalid age group id(s) are: "
                        f"{', '.join([str(i) for i in invalid_ids])}"
                    ),
                    relevant_parameters=self.required_parameters,
                )


class _PregnancySexIdsValidation(_Validation):
    """Checks that prediction sex IDs are valid for a pregnancy model."""

    dependencies: List[Type[_Validation]] = [_SexIdsValidation]
    required_parameters: List[str] = [
        parameters.PREDICTION_SEX_IDS,
        parameters.IS_PREGNANCY_ME,
    ]

    def validate(self) -> Optional[_ValidationError]:
        sex_ids = self.parameters[parameters.PREDICTION_SEX_IDS]
        invalid_ids = list(set(sex_ids) - {gbd_constants.sex.FEMALE})
        is_pregnancy_me = self.parameters[parameters.IS_PREGNANCY_ME]

        if is_pregnancy_me:
            if invalid_ids:
                return _ValidationError(
                    exception=exc.InvalidPregnancySexIds,
                    message=(
                        f"{len(invalid_ids)} invalid prediction sex id(s) given for "
                        f"pregnancy model, invalid sex id(s) are: "
                        f"{', '.join([str(i) for i in invalid_ids])}"
                    ),
                    relevant_parameters=self.required_parameters,
                )


class _PregnancyYearStartValidation(_Validation):
    """Checks that prediction year IDs are valid for a pregnancy model."""

    dependencies: List[Type[_Validation]] = []
    required_parameters: List[str] = [parameters.YEAR_START, parameters.IS_PREGNANCY_ME]

    def validate(self) -> Optional[_ValidationError]:
        year_start = self.parameters[parameters.YEAR_START]
        is_pregnancy_me = self.parameters[parameters.IS_PREGNANCY_ME]

        if is_pregnancy_me:
            if year_start < demographics.PREGNANCY_YEAR_START:
                return _ValidationError(
                    exception=exc.InvalidPregnancyYearStart,
                    message=(
                        f"Pregnancy population estimates are only provided "
                        f"for {demographics.PREGNANCY_YEAR_START}+. Invalid year start: "
                        f"{year_start} was found"
                    ),
                    relevant_parameters=self.required_parameters,
                )


def _validate_covariate_mvids_exist_and_are_approved(
    release_id: int,
    covariate_name_shorts: List[str],
    covariate_model_version_ids: List[Union[int, str]],
    stgpr_session: orm.Session,
    covariates_session: orm.Session,
) -> Optional[_ValidationError]:
    """Validates that given MVIDs both exist and are approved.

    Sub-validation of _CovariateMvidsValidation. Assumes covariate_name_shorts and
    covariate_model_version_ids are the same length and have the expected data types.
    """
    # Loop through covariates and run validations
    covariate_short_covariate_mvid_pairs = list(
        zip(covariate_name_shorts, covariate_model_version_ids)
    )
    covariate_ids = db_stgpr.get_covariate_ids_by_name(covariate_name_shorts, stgpr_session)
    gbd_covariates_has_misspecified_errors = [
        mvid != parameters.BEST_MVID
        and not db_stgpr.covariate_model_version_exists(
            covariate_ids[covariate_name], mvid, covariates_session
        )
        for (covariate_name, mvid) in covariate_short_covariate_mvid_pairs
    ]
    if any(gbd_covariates_has_misspecified_errors):
        invalid_mvids = ", ".join(
            [
                str(mvid)
                for has_error, mvid in zip(
                    gbd_covariates_has_misspecified_errors, covariate_model_version_ids
                )
                if has_error
            ]
        )
        return _ValidationError(
            exception=exc.MisspecifiedCovariateModelVersionId,
            message=(
                f"Specified covariate model versions include non-existent model version IDs. "
                f"Received non-existent IDs: {invalid_mvids}"
            ),
            relevant_parameters=[parameters.GBD_COVARIATES],
            gbd_covariates_has_errors=gbd_covariates_has_misspecified_errors,
        )

    gbd_covariates_not_approved_errors = [
        mvid != parameters.BEST_MVID
        and not db_stgpr.covariate_model_version_is_approved(
            mvid, release_id, covariates_session
        )
        for (covariate_name, mvid) in covariate_short_covariate_mvid_pairs
    ]
    if any(gbd_covariates_not_approved_errors):
        invalid_mvids = ", ".join(
            [
                str(mvid)
                for has_error, mvid in zip(
                    gbd_covariates_not_approved_errors, covariate_model_version_ids
                )
                if has_error
            ]
        )
        return _ValidationError(
            exception=exc.MisspecifiedCovariateModelVersionId,
            message=(
                f"Specified covariate model versions include non-approved model version IDs. "
                f"Received non-approved IDs: {invalid_mvids}"
            ),
            relevant_parameters=[parameters.GBD_COVARIATES],
            gbd_covariates_has_errors=gbd_covariates_not_approved_errors,
        )


_ALL_VALIDATION_CLASSES: Final[List[Type[_Validation]]] = [
    _ReleaseIdValidation,
    _DescriptionValidation,
    _HoldoutsValidation,
    _DrawsValidation,
    _UniqueDensityCutoffsValidation,
    _MetricIdValidation,
    _CrossValValidation,
    _RakeLogitValidation,
    _DataSpecificationValidation,
    _SingleCustomInputValidation,
    _CustomStage1Validation,
    _DuplicateHyperparameterValidation,
    _HyperparameterCountValidation,
    _AgeOmegaValidation,
    _ModelableEntityValidation,
    _CovariateIdValidation,
    _CovariateNameShortsValidation,
    _CovariateMvidsValidation,
    _AgeGroupIdsValidation,
    _SexIdsValidation,
    _YearIdsExistValidation,
    _YearEndYearStartValidation,
    _YearEndMinimumValueValidation,
    _CrosswalkVersionValidation,
    _GprAmplitudeValidation,
    _BundleShapeValidation,
    _LinkedBundleValidation,
    _GbdAgeGroupsValidation,
    _CustomAgeVectorValidation,
    _AgeExpandValidation,
    _SexExpandValidation,
    _PregnancySexIdsValidation,
    _PregnancyAgeGroupIdsValidation,
    _PregnancyYearStartValidation,
]


def run_validations(
    parameters: Dict[str, Any],
    raise_on_failure: bool,
    stgpr_session: orm.Session,
    epi_session: orm.Session,
    covariates_session: orm.Session,
    shared_vip_session: orm.Session,
) -> Optional[List[_ValidationError]]:
    """Runs all of the parameter validations."""
    validation_graph = {v: set(v.dependencies) for v in _ALL_VALIDATION_CLASSES}
    sorted_graph = cast(Iterator[Set[Type[_Validation]]], toposort.toposort(validation_graph))

    for batch in sorted_graph:
        failed_in_batch: List[_ValidationError] = []
        validations = [
            Validation(
                parameters=parameters,
                stgpr_session=stgpr_session,
                epi_session=epi_session,
                covariates_session=covariates_session,
                shared_vip_session=shared_vip_session,
            )
            for Validation in batch
        ]
        # Toposorted validations within a batch aren't ordered, so sort them
        # alphabetically by validation class name so that validation errors are
        # returned/printed in a consistent order.
        sorted_validations = sorted(validations, key=lambda v: type(v).__name__)

        for validation in sorted_validations:
            required_params_set = set(validation.required_parameters)
            actual_params_set = set(parameters.keys())
            difference = required_params_set - actual_params_set
            if difference:
                raise RuntimeError(
                    f"Required parameter(s) {difference} missing for parameter validation "
                    f"{type(validation).__name__}"
                )

            validation_error = validation.validate()
            if validation_error:
                failed_in_batch.append(validation_error)

        if failed_in_batch:
            if raise_on_failure:
                # If there are errors, raise them.
                error_str = _format_error_string(failed_in_batch, warnings=False)
                if error_str:
                    raise exc.ParameterValidationError(error_str)

                # Otherwise there are only warnings; warn then return success.
                warning_str = _format_error_string(failed_in_batch, warnings=True)
                if warning_str:
                    warnings.warn(warning_str)

            return [validation_error for validation_error in failed_in_batch]


def _format_error_string(
    validation_errors: List[_ValidationError], warnings: bool
) -> Optional[str]:
    errors_or_warnings = (
        [error for error in validation_errors if error.warning]
        if warnings
        else [error for error in validation_errors if not error.warning]
    )
    if not errors_or_warnings:
        return

    num_errors = len(errors_or_warnings)
    error_or_warning = "warning" if warnings else "error"
    was_were = "was"
    plural = ""
    if num_errors > 1:
        was_were = "were"
        plural = "s"
    error_base = (
        f"\nThere {was_were} {num_errors} {error_or_warning}{plural} during parameter "
        "validation:\n"
    )
    error_messages = "\n\n".join(
        f"{error.exception.__name__} ({i+1}/{num_errors}):\n" f"{error.message}"
        for i, error in enumerate(errors_or_warnings)
    )
    return error_base + error_messages
