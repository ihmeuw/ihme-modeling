r"""This module is where migration modeling strategies and their parameters are
managed/defined.

**Modeling parameters include:**

* pre/post processing strategy (i.e. processor object)
* covariates
* fixed-effects
* fixed-intercept
* random-effects
* indicators
"""

from collections import namedtuple
from enum import Enum
from frozendict import frozendict

from fhs_lib_model.lib import model
from fhs_lib_data_transformation.lib import processing


class Covariates(Enum):
    """Covariates uses for modeling migration"""
    POPULATION = "population"
    DEATH = "death"

VALID_COVARIATES = tuple(cov.value for cov in Covariates)


ModelParameters = namedtuple(
    "ModelParameters", (
        "Model, "
        "processor, "
        "covariates, "
        "fixed_effects, "
        "fixed_intercept, "
        "random_effects, "
        "indicators, "
        "spline, "
        "predict_past_only, "
        )
    )
MODEL_PARAMETERS = frozendict({
    # Indicators:
    "migration": frozendict({
        "LimeTr": ModelParameters(
            Model=model.LimeTr,
            processor=processing.NoTransformProcessor(years=None, gbd_round_id=None),
            covariates={"population": processing.NoTransformProcessor(years=None, gbd_round_id=None),
                        "death": processing.NoTransformProcessor(years=None, gbd_round_id=None),
                        },
            fixed_effects={"population": [-float('inf'), float('inf')],
                           "death": [-float('inf'), float('inf')],
                           },
            fixed_intercept='unrestricted', 
            random_effects={
                        "location_intercept": model.RandomEffect(
                            ["location_id"], None
                        ),
                    },
            indicators=None,
            spline=None,
            predict_past_only=False,
            ),
        })
    })
