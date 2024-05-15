r"""This module has query functions that give nonfatal modeling strategies and
their parameters.
"""
import logging

from migration import model_strategy


LOGGER = logging.getLogger(__name__)

MIG_LIMETR_STAGES = ["migration"]

def get_mig_model(stage, years, gbd_round_id):
    r"""Gets modeling parameters associated with the given migration stage.
    Technically not a query right now, but functions like get_cause_model.

    Note:
        Update ``FILEPATH.MODEL_PARAMETERS`` to
        reflect changes in model specification.
    Args:
        stage (str):
            Stage being forecasted, e.g. "temperature".
        years (fbd_core.YearRange):
            Forecasting timeseries
    Returns:
        Model (Model):
            Class, i.e. un-instantiated from
            ``FILEPATH.model.py``
        processor (Processor):
            The pre/post process strategy of the cause-stage, i.e. instance of
            a class defined in ``FILEPATH.processing.py``.
        covariates (dict[str, Processor]] | None):
            Maps each needed covariate, i.e. independent variable, to it's
            respective preprocess strategy, i.e. instance of a class defined in
            ``FILEPATH.processing.py``.
        node_models (list[CovModel]):
            A list of NodeModels (e.g. StudyModel, OverallModel), each of
            which has specifications for cov_models.
        study_id_cols (Union[str, List[str]]):
            The columns to use in the `col_study_id` argument to MRBRT
            ``load_df`` function. If it is a list of strings, those
            columns will be concatenated together (e.g. ["location_id",
            "sex_id"] would yield columns with values like
            ``{location_id}_{sex_id}``). This is done since MRBRT can
            currently only use the one ``col_study_id`` column for random
            effects.
        scenario_quantiles (dict | None):
                Whether to use quantiles of the stage two model when predicting
                scenarios. Dictionary of quantiles to use is passed in
                e.g.::

                    {
                        -1: dict(sdi=0.85),
                        0: None,
                        1: dict(sdi=0.15),
                        2: None,
                    }
        fixed_effects (dict[str, str] | None):
                List of covariates to calculate fixed effect coefficient
                estimates for.
                e.g.::

                    {"haq": [-float('inf'), float('inf'), "edu": [0, 4.7]}

        fixed_intercept (str | None):
            To restrict the fixed intercept to be positive or negative, pass
            "positive" or "negative", respectively. "unrestricted" says to
            estimate a fixed effect intercept that is not restricted to
            positive or negative.
        random_effects (dict[str, list[str]] | None):
            A dictionary mapping covariates to the dimensions that their
            random slopes will be estimated for and the standard deviation of
            the gaussian prior on their variance. Of the form
            ``dict[covariate, list[dimension]]``. e.g.::

                {"haq": (["location_id", "age_group_id"], None),
                 "education": (["location_id"], 4.7)}

            **NOTE** that random intercepts should be included here -- effects
            that aren't associated with covariates will be assumed to be random
            intercepts.
        indicators (dict[str, list[str]] | None):
            A dictionary mapping indicators to the dimensions that they are
            indicators on. e.g.::

                {"ind_age_sex": ["age_group_id", "sex_id"],
                 "ind_loc": ["location_id"]}
        spline (dict[str, SplineTuple]):
            A dictionary mapping covariates to the spline parameters that
            will be used for them of the form
            {covariate: SplineParams(degrees_of_freedom, constraints)}
            Each key must be a covariate.
            The degrees_of_freedom int represents the degrees of freedom
            on that spline.
            The constraint string can be "center" indicating to apply a
            centering constraint or a 2-d array defining general linear
            constraints.
    Raises:
        ValueError:
            If the given stage does NOT have any cause-strategy IDs
    """
    if stage in MIG_LIMETR_STAGES:
        model_strategy_name = "LimeTr"
    else:
        err_msg = (
            f"stage={stage} does not have a model strategy associated with it")
        LOGGER.error(err_msg)
        raise ValueError(err_msg)

    model_parameters = (
        model_strategy.MODEL_PARAMETERS[stage][model_strategy_name])

    model_parameters = _update_processor_years(model_parameters, years)

    model_parameters = _update_processor_gbd_round_id(model_parameters, gbd_round_id)

    return model_parameters


def _update_processor_gbd_round_id(model_parameters, gbd_round_id):
    """``gbd_round_id`` is entered as ``None`` in the procesor for the dependent
    variable and covariates so it needs to be updated here"""
    if model_parameters:
        model_parameters.processor.gbd_round_id = gbd_round_id

        if model_parameters.covariates:
            for cov_name in model_parameters.covariates.keys():
                model_parameters.covariates[cov_name].gbd_round_id = gbd_round_id

    return model_parameters


def _update_processor_years(model_parameters, years):
    """``years`` is entered as ``None`` in the procesor for the dependent
    variable and covariates so it needs to be updated here"""
    if model_parameters:
        model_parameters.processor.years = years

        if model_parameters.covariates:
            for cov_name in model_parameters.covariates.keys():
                model_parameters.covariates[cov_name].years = years

    return model_parameters
