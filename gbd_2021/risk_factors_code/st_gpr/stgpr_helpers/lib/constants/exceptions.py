"""ST-GPR custom exceptions.

Inheritance structure:

ValueError
    NotFoundError
    UnprocessableEntityError
        ParameterValueError
        ConfigValueError
"""

# 404 ERRORS - API cannot find requested ID


class NotFoundError(ValueError):
    """Parent class for 404 resource not found errors."""


class NoStgprVersionFound(NotFoundError):
    """No ST-GPR version ID found."""


class NoDataStageFound(NotFoundError):
    """No data stage ID found."""


# 422 ERRORS - Variations of model misspecifications


class UnprocessableEntityError(ValueError):
    """Parent class for 422 unprocessable entity errors."""


class ModelIterationDoesNotMatchStgprVersion(UnprocessableEntityError):
    """Model iteration id does not match ST-GPR version id."""


class ModelCantRun(UnprocessableEntityError):
    """ST-GPR modek cannot run for GBD round and decomp step."""


class ModelableEntityCantRun(UnprocessableEntityError):
    """Modelable entity cannot run."""


class ParametersCantChange(UnprocessableEntityError):
    """Parameters cannot change for the ST-GPR model but did."""


class CantDetermineModelType(UnprocessableEntityError):
    """Cannot determine model type."""


class NoBestCovariateModelFound(UnprocessableEntityError):
    """Not best model for a GBD covariate found."""


class NoCrosswalkVersionFound(UnprocessableEntityError):
    """No crosswalk found associated with bundle."""


class NoModelIterationProvided(UnprocessableEntityError):
    """No model iteration given for estimate/statistics retrieval if needed."""


class SqlSanitationError(ValueError):
    """SQL sanitation failed."""


# PARAMETER ERRORS: SUBCLASS OF 422 ERRORS


class ParameterValueError(UnprocessableEntityError):
    """Illegal value given for an ST-GPR parameter."""


class DescriptionTooLong(ParameterValueError):
    """Model description is too long."""


class InvalidHoldoutNumber(ParameterValueError):
    """Invalid holdout number."""


class InvalidDrawNumber(ParameterValueError):
    """Invalid number of draws."""


class NonUniqueDensityCutoffs(ParameterValueError):
    """Density cutoffs are not all unique values."""


class CrossValidationWithDensityCutoffs(ParameterValueError):
    """Cross validation run with density cutoffs."""


class InvalidTransformWithLogitRaking(ParameterValueError):
    """Invalid tranform with logit raking."""


class PathToDataNotAllowed(ParameterValueError):
    """Path to data not allowed for decomp step."""


class InvalidPathToData(ParameterValueError):
    """Path to data is not a valid file path."""


class InvalidDataSpecification(ParameterValueError):
    """Data specified is not either path_to_data OR a crosswalk_version."""


class TwoCustomInputs(ParameterValueError):
    """Both custom stage 1 and custom covariate are supplied."""


class BothOrNeitherCustomStage1AndFormulaProvided(ParameterValueError):
    """Both custom stage 1 and stage 1 formula provided or neither."""


class InvalidHyperparameterCount(ParameterValueError):
    """Invalid hyperparameter count."""


class InvalidOmegaCrossValidation(ParameterValueError):
    """Multiple omegas given in cross validation with a single age group."""


class NoModelableEntityFound(ParameterValueError):
    """No modelable entity found."""


class NoCovariateFound(ParameterValueError):
    """No covariate found."""


class NoCovariateNameShortFound(ParameterValueError):
    """No covariate name short found."""


class BundleDoesNotMatchCrosswalkVersion(ParameterValueError):
    """Bundle id does not match crosswalk version id."""


# CONFIG ERRORS: SUBCLASS OF 422 ERRORS


class ConfigValueError(UnprocessableEntityError):
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
