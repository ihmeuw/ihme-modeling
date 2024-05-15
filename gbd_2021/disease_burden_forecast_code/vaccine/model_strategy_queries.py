from fhs_lib_year_range_manager.lib.year_range import YearRange

from fhs_pipeline_vaccine.lib import model_strategy


def get_vaccine_model(
    vaccine: str, model_type: str, years: YearRange, gbd_round_id: int
) -> model_strategy.ModelParameters:
    """Pulling the necessary processor model to modify.

    Args:
        vaccine (str): which vaccine model
        model_type (str): what type of model to modify
        years (YearRange): years to enter
        gbd_round_id (int): which gbd round

    Returns:
         model_strategy.ModelParameters

    Return the default model parameters for the given vaccine model,
    with the "years" and "gbd_round_id" set to the given ones everywhere in
    the parameters.
    """
    model_parameters = model_strategy.MODEL_PARAMETERS[vaccine][model_type]
    model_parameters = _update_processor_years(model_parameters, years)
    model_parameters = _update_processor_gbd_round_id(model_parameters, gbd_round_id)
    return model_parameters


def _update_processor_years(
    model_parameters: model_strategy.ModelParameters, years: YearRange
) -> model_strategy.ModelParameters:
    """Updating the years for the processor.

    Args:
        model_parameters (model_strategy.ModelParameters): model_parameters
        years (YearRange): years to enter

    Returns:
         model_strategy.ModelParameters

    Return the default model parameters for the given vaccine model,
    with the "years" and "gbd_round_id" set to the given ones everywhere in
    the parameters.
    """
    model_parameters.processor.years = years

    if model_parameters.covariates:
        for cov_name in model_parameters.covariates.keys():
            model_parameters.covariates[cov_name].years = years

    return model_parameters


def _update_processor_gbd_round_id(
    model_parameters: model_strategy.ModelParameters, gbd_round_id: int
) -> model_strategy.ModelParameters:
    """Updating the gbd round id for the processor.

    Args:
        model_parameters (model_strategy.ModelParameters): model parameters
            to update
        gbd_round_id (int): gbd round id to insert

    Returns:
        model_strategy.ModelParameters

    gbd_round_id is entered as ``None`` in the processor for the dependent
    variable and covariates so it needs to be updated here
    """
    model_parameters.processor.gbd_round_id = gbd_round_id

    if model_parameters.covariates:
        for cov_name in model_parameters.covariates.keys():
            model_parameters.covariates[cov_name].gbd_round_id = gbd_round_id

    return model_parameters