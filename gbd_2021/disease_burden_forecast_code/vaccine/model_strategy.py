from collections import namedtuple

from fhs_lib_data_transformation.lib import processing
from fhs_lib_database_interface.lib.query.model_strategy import ModelStrategyNames
from fhs_lib_model.lib.arc_method.arc_method import ArcMethod
from fhs_lib_model.lib.limetr import LimeTr

from fhs_pipeline_vaccine.lib.constants import ModelConstants

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
    ),
)

MODEL_PARAMETERS = {
    "mcv1": {
        ModelStrategyNames.ARC.value: ModelParameters(
            Model=ArcMethod,
            processor=processing.LogProcessor(
                years=None,
                offset=ModelConstants.DEFAULT_OFFSET,
                no_mean=True,
                intercept_shift="unordered_draw",
                gbd_round_id=6,
            ),
            covariates=None,
            fixed_effects=None,
            fixed_intercept=None,
            random_effects=None,
            indicators=None,
            spline=None,
            predict_past_only=False,
        ),
        ModelStrategyNames.LIMETREE.value: ModelParameters(
            Model=LimeTr,
            processor=processing.LogitProcessor(
                years=None,
                offset=ModelConstants.DEFAULT_OFFSET,
                remove_zero_slices=True,
                intercept_shift="unordered_draw",
                gbd_round_id=6,
            ),
            covariates={"sdi": processing.NoTransformProcessor(gbd_round_id=None, years=None)},
            fixed_effects={"sdi": [-float("inf"), float("inf")]},
            fixed_intercept="unrestricted",
            random_effects=None,
            indicators=None,
            spline=None,
            predict_past_only=False,
        ),
        ModelStrategyNames.NONE.value: None,
    },
    "dtp3": {
        ModelStrategyNames.ARC.value: ModelParameters(
            Model=ArcMethod,
            processor=processing.LogProcessor(
                years=None,
                offset=ModelConstants.DEFAULT_OFFSET,
                no_mean=True,
                intercept_shift="unordered_draw",
                gbd_round_id=6,
            ),
            covariates=None,
            fixed_effects=None,
            fixed_intercept=None,
            random_effects=None,
            indicators=None,
            spline=None,
            predict_past_only=False,
        ),
        ModelStrategyNames.LIMETREE.value: ModelParameters(
            Model=LimeTr,
            processor=processing.LogitProcessor(
                years=None,
                offset=ModelConstants.DEFAULT_OFFSET,
                remove_zero_slices=True,
                intercept_shift="unordered_draw",
                gbd_round_id=6,
            ),
            covariates={"sdi": processing.NoTransformProcessor(gbd_round_id=None, years=None)},
            fixed_effects={"sdi": [-float("inf"), float("inf")]},
            fixed_intercept="unrestricted",
            random_effects=None,
            indicators=None,
            spline=None,
            predict_past_only=False,
        ),
        ModelStrategyNames.NONE.value: None,
    },
}