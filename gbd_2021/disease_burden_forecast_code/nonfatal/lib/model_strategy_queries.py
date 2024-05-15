"""This module has query functions that give nonfatal modeling strategies and their params.
"""
from fhs_lib_database_interface.lib.constants import (
    DBConnectionParameterConstants,
    DimensionConstants,
    FHSDBConstants,
)
from fhs_lib_database_interface.lib.db_session import create_db_session
from fhs_lib_database_interface.lib.fhs_lru_cache import fhs_lru_cache
from fhs_lib_database_interface.lib.query.model_strategy import STAGE_STRATEGY_IDS
from fhs_lib_database_interface.lib.strategy_set.strategy import get_cause_set
from fhs_lib_year_range_manager.lib.year_range import YearRange
from tiny_structured_logger.lib import fhs_logging

from fhs_pipeline_nonfatal.lib import model_parameters

logger = fhs_logging.get_logger()

model_strategy_module = None


def _real_model_strategy_module() -> object:
    """Import and return the "real" implementation of model_strategy.

    Used to load the real implementation late, so that tests can install their own mock
    version, without actually running the "import" statement (since that import fails on some
    platforms).
    """
    import fhs_pipeline_nonfatal.lib.model_strategy

    return fhs_pipeline_nonfatal.lib.model_strategy


@fhs_lru_cache(1)
def get_cause_model(
    acause: str, stage: str, years: YearRange, gbd_round_id: int
) -> model_parameters.ModelParameters:
    r"""Gets modeling parameters associated with the given cause-stage.

    Finds the model appropriate and
    1) get cause strategies associated with stage.
    2) find which strategy cause falls under
    3) Get the modeling parameters associated with that strategy and return them.

    Args:
        acause (str): The cause that is being modeled. e.g. ``cvd_ihd``
        stage (str): Stage being forecasted, e.g. "yld_yll".
        years (YearRange): Forecasting timeseries
        gbd_round_id (int): The numeric ID of GBD round associated with the past data.

    Returns:
        model_parameters.ModelParameters named tuple containing the following:
            Model (Model): Class, i.e. un-instantiated from
            ``fhs_pipeline_nonfatal.lib.model.py``

            processor (Processor):  The pre/post process strategy of the cause-stage, i.e.
            instance of a class defined in ``fhs_lib_data_transformation.lib.processing.py``.

            covariates (dict[str, Processor]] | None): Maps each needed covariate, i.e.
            independent variable, to it's respective preprocess strategy, i.e. instance of a
            class defined in ``fhs_lib_data_transformation.lib.processing.py``.

            fixed_effects (dict[str, str] | None): List of covariates to calculate fixed
            effect coefficient estimates for. e.g.:
            {"haq": [-float('inf'), float('inf'), "edu": [0, 4.7]}

            fixed_intercept (str | None): To restrict the fixed intercept to be positive or
            negative, pass "positive" or "negative", respectively. "unrestricted" says to
            estimate a fixed effect intercept that is not restricted to positive or negative.

            random_effects (dict[str, list[str]] | None): A dictionary mapping covariates to
            the dimensions that their random slopes will be estimated for and the standard
            deviation of the gaussian prior on their variance. Of the form
            ``dict[covariate, list[dimension]]``. e.g.:
            {"haq": (["location_id", "age_group_id"], None),
            "education": (["location_id"], 4.7)}

            indicators (dict[str, list[str]] | None): A dictionary mapping indicators to the
            dimensions that they are indicators on. e.g.:
            {"ind_age_sex": ["age_group_id", "sex_id"], "ind_loc": ["location_id"]}

            spline (dict): A dictionary mapping covariates to the spline parameters that will
            be used for them of the form {covariate: SplineParams(degrees_of_freedom,
            constraints)} Each key must be a covariate. The degrees_of_freedom int represents
            the degrees of freedom on that spline. The constraint string can be "center"
            indicating to apply a centering constraint or a 2-d array defining general linear
            constraints.
            node_models (list[CovModel]):
            A list of NodeModels (e.g. StudyModel, OverallModel), each of
            which has specifications for cov_models.
            study_id_cols (Union[str, List[str]]): The columns to use in the `col_study_id`
            argument to MRBRT ``load_df`` function. If it is a list of strings, those columns
            will be concatenated together (e.g. ["location_id", "sex_id"] would yield columns
            with values like ``{location_id}_{sex_id}``). This is done since MRBRT can
            currently only use the one ``col_study_id`` column for random effects.
            scenario_quantiles (dict | None): Whether to use quantiles of the stage two model
            when predicting scenarios. Dictionary of quantiles to use is passed in e.g.:
            {-1: dict(sdi=0.85), 0: None, 1: dict(sdi=0.15), 2: None,}

    Raises:
        ValueError: If the given stage does NOT have any cause-strategy IDs, or if the given
            acause/stage/gbd-round-id combo does not have a modeling strategy associated with
            it.
    """
    with create_db_session(
        db_name=FHSDBConstants.FORECASTING_DB_NAME,
        server_conn_key=DBConnectionParameterConstants.DEFAULT_SERVER_CONN_KEY,
    ) as session:
        try:
            strategy_ids = list(STAGE_STRATEGY_IDS[stage].keys())
        except KeyError:
            raise ValueError(f"{stage} does not have available strategy IDs")

        model_strategy_name = None
        for strategy_id in strategy_ids:
            cause_strategy_set = list(
                get_cause_set(
                    session=session,
                    gbd_round_id=gbd_round_id,
                    strategy_id=strategy_id,
                )[DimensionConstants.ACAUSE].unique()
            )
            if acause in cause_strategy_set:
                # ``STAGE_STRATEGY_IDS[stage][strategy_id]`` is a ``StrategyModelSource``
                # instance, and we only need its model attribute.
                model_strategy_name = STAGE_STRATEGY_IDS[stage][strategy_id].model
                break  # Model strategy found

    if not model_strategy_name:
        raise ValueError(
            f"acause={acause}, stage={stage}, gbd_round_id={gbd_round_id}"
            f"does not have a model strategy associated with it."
        )
    else:
        logger.info(
            f"acause={acause}, stage={stage}, gbd_round_id={gbd_round_id} "
            f"has the {model_strategy_name} model strategy associated with it."
        )

    model_strategy = model_strategy_module or _real_model_strategy_module()
    model_parameters = model_strategy.MODEL_PARAMETERS[
        stage
    ][model_strategy_name]

    model_parameters = _update_processor_years(model_parameters, years)
    model_parameters = _update_processor_gbd_round_id(model_parameters, gbd_round_id)

    return model_parameters


def _update_processor_years(
    model_parameters: model_parameters.ModelParameters, years: YearRange
) -> model_parameters.ModelParameters:
    """Update the model_params.processor.

    If ``years`` is entered as ``None`` in the procesor for the dependent
    variable and covariates so it needs to be updated here.

    Args:
        model_parameters (model_parameters.ModelParameters): named tuple containing a processor
        years (YearRange): year range to update processor to

    Returns:
        model_parameters.ModelParameters: model parameters where the processor years have been
            updated
    """
    if model_parameters:
        model_parameters.processor.years = years

        if model_parameters.covariates:
            for cov_name in model_parameters.covariates.keys():
                model_parameters.covariates[cov_name].years = years

    return model_parameters


def _update_processor_gbd_round_id(
    model_parameters: model_parameters.ModelParameters, gbd_round_id: int
) -> model_parameters.ModelParameters:
    """Update gbd_round_id of input model_parameters.processor.

    ``gbd_round_id`` is entered as ``None`` in the procesor for the dependent
    variable and covariates so it needs to be updated here.

    Args:
        model_parameters (model_parameters.ModelParameters): model parameters to update
        gbd_round_id (int): gbd round

    Returns:
        model_parameters.ModelParameters
    """
    if model_parameters:
        model_parameters.processor.gbd_round_id = gbd_round_id

        if model_parameters.covariates:
            for cov_name in model_parameters.covariates.keys():
                model_parameters.covariates[cov_name].gbd_round_id = gbd_round_id

    return model_parameters
