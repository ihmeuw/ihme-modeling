from collections import namedtuple

ModelParameters = namedtuple(
    "ModelParameters",
    (
        "Model, "
        "processor, "
        "covariates, "
        "fixed_effects, "
        "fixed_intercept, "
        "random_effects, "
        "indicators, "
        "spline, "
        "predict_past_only, "
        "node_models, "
        "study_id_cols, "
        "scenario_quantiles, "
        "omega_selection_strategy, "
    ),
)
