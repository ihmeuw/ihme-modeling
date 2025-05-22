import numpy as np
import pytest
from numpy.testing import assert_approx_equal

from pydisagg import models
from pydisagg.models.disagg_model import DisaggModel

# Need to test
# from pydisagg.disaggregate import split_datapoint

model_list = [
    models.RateMultiplicativeModel(),
    models.LogOddsModel(),
    models.LMOModel(5),
]


@pytest.mark.parametrize("model", model_list)
def test_model_consistency(model: DisaggModel):
    populations = np.array([2, 5])
    measured_total = 4.8
    measurement_SE = 1
    rate_pattern = np.array([0.2, 0.4])

    split_result = model.split_to_counts(
        measured_total,
        rate_pattern,
        populations,
    )
    beta = model.fit_beta(measured_total, rate_pattern, populations)
    split_SE_vals = model.count_split_standard_errors(
        beta,
        rate_pattern,
        populations,
        measurement_SE,
    )
    assert_approx_equal(measured_total, np.sum(split_result))
    assert_approx_equal(measurement_SE, np.sum(split_SE_vals))
