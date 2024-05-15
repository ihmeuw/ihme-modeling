"""CCF modeling strategies and their parameters for MRBRT.

**Modeling parameters include:**

* pre/post processing strategy (i.e. processor object)
* covariates
* cov_models
* study_id columns
"""
from collections import namedtuple
from enum import Enum

from fhs_lib_data_transformation.lib import processing
from fhs_lib_model.lib import model
from mrtool import LinearCovModel
from stagemodel import OverallModel

from fhs_pipeline_fertility.lib.constants import ModelConstants, StageConstants


class Covariates(Enum):
    """Covariates used for modeling fertility."""

    EDUCATION = "education"
    MET_NEED = "met_need"


VALID_COVARIATES = tuple(cov.value for cov in Covariates)


ModelParameters = namedtuple(
    "ModelParameters",
    (
        "Model, "
        "processor, "
        "covariates, "
        "node_models, "
        "study_id_cols, "
        "scenario_quantiles, "
    ),
)

MODEL_PARAMETERS = {
    StageConstants.CCF: {
        StageConstants.CCF: ModelParameters(
            Model=model.MRBRT,
            processor=processing.LogitProcessor(
                years=None,
                offset=ModelConstants.log_logit_offset,  # 1e-8
                gbd_round_id=None,
                age_standardize=False,
                remove_zero_slices=True,
                intercept_shift="mean",
            ),
            covariates={
                "education": processing.NoTransformProcessor(
                    years=None, gbd_round_id=None, no_mean=True
                ),
                "met_need": processing.NoTransformProcessor(
                    years=None, gbd_round_id=None, no_mean=True
                ),
                "u5m": processing.NoTransformProcessor(
                    years=None, gbd_round_id=None, no_mean=True
                ),
                "urbanicity": processing.NoTransformProcessor(
                    years=None, gbd_round_id=None, no_mean=True
                ),
            },
            node_models=[
                OverallModel(
                    cov_models=[
                        LinearCovModel("intercept", use_re=False),
                        LinearCovModel(
                            "education",
                            use_re=False,
                            use_spline=True,
                            spline_knots=ModelConstants.knot_placements,
                            spline_knots_type="frequency",
                            spline_degree=3,
                            spline_l_linear=True,
                            spline_r_linear=True,
                        ),
                        LinearCovModel("met_need", use_re=False, use_spline=False),
                        LinearCovModel("u5m", use_re=False, use_spline=False),
                        LinearCovModel("urbanicity", use_re=False, use_spline=False),
                    ]
                ),
            ],
            study_id_cols="location_id",
            scenario_quantiles={0: None},
        ),
    }
}
