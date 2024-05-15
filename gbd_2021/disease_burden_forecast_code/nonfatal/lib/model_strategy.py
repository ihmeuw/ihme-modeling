r"""This module is where nonfatal modeling strategies and their parameters are managed/defined.

**Modeling parameters include:**

* pre/post processing strategy (i.e. processor object)
* covariates
* fixed-effects
* fixed-intercept
* random-effects
* indicators

Currently, the LimeTr model for ratios is described as follows:

.. Math::
    E[log({Y}_{l,a,s,y})] = \beta_{1}SDI + \gamma_{l,a,s}SDI + \alpha_{l,a,s}

where :math:`E[\log{Y}]` is the expected log ratio, :math:`\beta_{1}` is the
fixed coefficient on SDI across time, :math:`\gamma_{l,a,s}` is the random
coefficient on SDI across time for each location-age-sex combination, and
:math:`\alpha_{l,a,s}` is the location-age-sex-specific random intercept.
The random slope has a Gaussian prior with mean 0 and standard deviation of
0.001.

The LimeTr model for the indicators (i.e. prevalence or incidence) is simpler:

.. Math::
    E[logit({Y}_{l,a,s,y})] = \beta_{1}SDI + \alpha_{l,a,s}

where :math:`E[logit({Y}_{l,a,s,y})]` is the expected value of
logit(prevalence or incidence).
"""
from fhs_lib_data_aggregation.lib import aggregation_methods
from fhs_lib_data_transformation.lib import processing
from fhs_lib_data_transformation.lib.constants import ProcessingConstants
from fhs_lib_database_interface.lib.query.model_strategy import ModelStrategyNames
from fhs_lib_model.lib.arc_method import omega_selection_strategy as oss
from fhs_lib_model.lib.arc_method.arc_method import ArcMethod
from fhs_lib_model.lib.limetr import LimeTr, RandomEffect
from frozendict import frozendict

from fhs_pipeline_nonfatal.lib.constants import StageConstants
from fhs_pipeline_nonfatal.lib.model_parameters import ModelParameters

MODEL_PARAMETERS = frozendict(
    {
        # Indicators:
        StageConstants.PREVALENCE: frozendict(
            {
                ModelStrategyNames.ARC.value: ModelParameters(
                    Model=ArcMethod,
                    processor=processing.LogitProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=True,
                        bias_adjust=False,
                        intercept_shift=None,
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates=None,
                    fixed_effects=None,
                    fixed_intercept=None,
                    random_effects=None,
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=oss.adjusted_zero_biased_omega_distribution,
                ),
                ModelStrategyNames.LIMETREE.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogitProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "sdi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"sdi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
                ModelStrategyNames.LIMETREE_BMI.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogitProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "bmi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"bmi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
                ModelStrategyNames.NONE.value: None,
                ModelStrategyNames.SPECTRUM.value: None,
            }
        ),
        StageConstants.INCIDENCE: frozendict(
            {
                ModelStrategyNames.ARC.value: ModelParameters(
                    Model=ArcMethod,
                    processor=processing.LogProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=True,
                        bias_adjust=False,
                        intercept_shift=None,
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates=None,
                    fixed_effects=None,
                    fixed_intercept=None,
                    random_effects=None,
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=oss.use_smallest_omega_within_threshold,
                ),
                ModelStrategyNames.LIMETREE.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "sdi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"sdi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
                ModelStrategyNames.NONE.value: None,
                ModelStrategyNames.SPECTRUM.value: None,
            }
        ),
        StageConstants.YLD: frozendict({ModelStrategyNames.NONE.value: None}),
        # Ratios:
        StageConstants.MI_RATIO: frozendict(
            {
                ModelStrategyNames.LIMETREE.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "sdi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"sdi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                        "sdi": RandomEffect(["location_id", "age_group_id", "sex_id"], 0.001),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
            }
        ),
        StageConstants.MP_RATIO: frozendict(
            {
                ModelStrategyNames.LIMETREE.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogitProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "sdi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"sdi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                        "sdi": RandomEffect(["location_id", "age_group_id", "sex_id"], 0.001),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
            }
        ),
        StageConstants.PI_RATIO: frozendict(
            {
                ModelStrategyNames.LIMETREE.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "sdi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"sdi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                        "sdi": RandomEffect(["location_id", "age_group_id", "sex_id"], 0.001),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
            }
        ),
        StageConstants.YLD_YLL_RATIO: frozendict(
            {
                ModelStrategyNames.LIMETREE.value: ModelParameters(
                    Model=LimeTr,
                    processor=processing.LogProcessor(
                        years=None,
                        gbd_round_id=None,
                        remove_zero_slices=True,
                        no_mean=False,
                        bias_adjust=False,
                        intercept_shift="unordered_draw",
                        age_standardize=False,
                        tolerance=ProcessingConstants.MAXIMUM_PRECISION,
                    ),
                    covariates={
                        "sdi": processing.NoTransformProcessor(
                            years=None,
                            gbd_round_id=None,
                        )
                    },
                    fixed_effects={"sdi": [-float("inf"), float("inf")]},
                    fixed_intercept=None,
                    random_effects={
                        "location_age_sex_intercept": RandomEffect(
                            ["location_id", "age_group_id", "sex_id"], None
                        ),
                        "sdi": RandomEffect(["location_id", "age_group_id", "sex_id"], 0.001),
                    },
                    indicators=None,
                    spline=None,
                    predict_past_only=False,
                    node_models=None,
                    study_id_cols=None,
                    scenario_quantiles=None,
                    omega_selection_strategy=None,
                ),
            }
        ),
    }
)


STAGE_AGGREGATION_METHODS = frozendict(
    {
        StageConstants.PREVALENCE: aggregation_methods.comorbidity,
        StageConstants.INCIDENCE: aggregation_methods.summation,
        StageConstants.YLD: aggregation_methods.summation,
        StageConstants.DALY: aggregation_methods.summation,
        StageConstants.DEATH: aggregation_methods.summation,
        StageConstants.YLL: aggregation_methods.summation,
    }
)
