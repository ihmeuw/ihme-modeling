"""ST-GPR custom exceptions.

Inheritance structure:

ValueError
    NotFoundError (from cc-errors)
    UnprocessableEntityError (from cc-errors)
        ParameterValueError
        ConfigValueError
"""

import cc_errors

# 404 ERRORS - API cannot find requested ID


class FailedToQueryCovariateService(cc_errors.NotFoundError):
    """Failure to query the covariate service."""


class NoStgprVersionFound(cc_errors.NotFoundError):
    """No ST-GPR version ID found."""


class NoDataStageFound(cc_errors.NotFoundError):
    """No data stage ID found."""


# 422 ERRORS - Parameter validations, rules validations, incorrect argument combinations


class ModelIterationDoesNotMatchStgprVersion(cc_errors.UnprocessableEntityError):
    """Model iteration id does not match ST-GPR version id."""


class ModelCantRun(cc_errors.UnprocessableEntityError):
    """ST-GPR model cannot run for GBD release."""


class ModelQuotaMet(cc_errors.UnprocessableEntityError):
    """ST-GPR model quota has been met, no more models can be registered."""


class CantDetermineModelType(cc_errors.UnprocessableEntityError):
    """Cannot determine model type."""


class NoBestCovariateModelFound(cc_errors.UnprocessableEntityError):
    """Not best model for a GBD covariate found."""


class NoCrosswalkVersionFound(cc_errors.UnprocessableEntityError):
    """No crosswalk found associated with bundle."""


class NoModelIterationProvided(cc_errors.UnprocessableEntityError):
    """No model iteration given for estimate/statistics retrieval if needed."""


class SqlSanitationError(ValueError):
    """SQL sanitation failed."""


class ModelWasDeleted(cc_errors.UnprocessableEntityError):
    """ST-GPR model was deleted and request cannot be completed."""


# PARAMETER ERRORS: SUBCLASS OF 422 ERRORS


class ParameterValidationError(cc_errors.UnprocessableEntityError):
    """Base class for parameter validation errors."""


class ReleaseIdError(ParameterValidationError):
    """Invalid or unspecified release id."""


class ParameterValueError(ParameterValidationError):
    """Illegal value given for an ST-GPR parameter."""


class InvalidCustomAgeVector(ParameterValidationError):
    """Parameter st_custom_age_vector is invalid."""


class InvalidAgeExpand(ParameterValueError):
    """Parameter age_expand is invalid."""


class InvalidSexExpand(ParameterValueError):
    """Parameter sex_expand is invalid."""


class DescriptionTooLong(ParameterValueError):
    """Model description is too long."""


class InvalidHoldoutNumber(ParameterValueError):
    """Invalid holdout number."""


class InvalidDrawNumber(ParameterValueError):
    """Invalid number of draws."""


class NonUniqueDensityCutoffs(ParameterValueError):
    """Density cutoffs are not all unique values."""


class InvalidMetricId(ParameterValueError):
    """Metric id given is invalid."""


class CrossValidationWithDensityCutoffs(ParameterValueError):
    """Cross validation run with density cutoffs."""


class InvalidTransformWithLogitRaking(ParameterValueError):
    """Invalid tranform with logit raking."""


class InvalidPathToData(ParameterValueError):
    """Path to data is not a valid file path."""


class InvalidDataSpecification(ParameterValueError):
    """Data specified is not either path_to_data OR a crosswalk_version."""


class TwoCustomInputs(ParameterValueError):
    """Both custom stage 1 and custom covariate are supplied."""


class BothOrNeitherCustomStage1AndFormulaProvided(ParameterValueError):
    """Both custom stage 1 and stage 1 formula provided or neither."""


class DuplicateHyperparameters(ParameterValueError):
    """One or more hyperparameters have duplicates."""


class InvalidHyperparameterCount(ParameterValueError):
    """Invalid hyperparameter count."""


class InvalidOmegaCrossValidation(ParameterValueError):
    """Multiple omegas given in cross validation with a single age group."""


class NoModelableEntityFound(ParameterValueError):
    """No modelable entity found."""


class InvalidModelType(ParameterValueError):
    """Modelable entity's model type is not ST-GPR."""


class NoCovariateFound(ParameterValueError):
    """No covariate found."""


class NoCovariateNameShortFound(ParameterValueError):
    """No covariate name short found."""


class MisspecifiedCovariateModelVersionId(ParameterValueError):
    """Covariate model version id was mis-specified."""


class BundleDoesNotMatchCrosswalkVersion(ParameterValueError):
    """Bundle id does not match crosswalk version id."""


class BundleIsNotStgprShape(ParameterValueError):
    """Bundle id is not of stgpr shape."""


class InvalidAgeGroupIds(ParameterValueError):
    """Invalid prediction age group ids found."""


class InvalidSexIds(ParameterValueError):
    """Invalid prediction sex ids found."""


class InvalidPregnancyAgeGroupIds(ParameterValueError):
    """Invalid prediction age group ids found for pregnancy model."""


class InvalidPregnancySexIds(ParameterValueError):
    """Invalid prediction sex ids found for pregnancy model."""


class InvalidPregnancyYearStart(ParameterValueError):
    """Invalid prediction year start found for pregnancy model."""


class InvalidYearIds(ParameterValueError):
    """Invalid prediction year ids found."""


class InvalidYearEnd(ParameterValueError):
    """Invalid year end found, value not greater or equal to terminal release year."""


class InvalidGprAmplitudeCombination(ParameterValueError):
    """Invalid combination of GPR amplitude parameters."""


# CONFIG ERRORS: SUBCLASS OF 422 ERRORS


class ConfigValueError(cc_errors.UnprocessableEntityError):
    """Illegal value given in an ST-GPR config or related."""


class InvalidPathToConfig(ConfigValueError):
    """Path to config is not a valid file path."""


class EmptyConfig(ConfigValueError):
    """Config is empty."""


class InvalidModelIndex(ConfigValueError):
    """Invalid model index id."""


class MissingModelIndexColumn(ConfigValueError):
    """Model index id column is missing in config."""


class InvalidParameterType(ConfigValueError):
    """Parameter in config has the wrong type."""


class ConfigMissingRequiredParameters(ConfigValueError):
    """Config is missing required parameter."""
