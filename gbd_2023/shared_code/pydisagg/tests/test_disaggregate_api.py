import numpy as np
import pytest
from numpy.testing import assert_approx_equal, assert_array_equal
import pandas as pd

from pydisagg import models
from pydisagg.disaggregate import split_datapoint, split_dataframe

model_list = [
    models.RateMultiplicativeModel(),
    models.LogOddsModel(),
    models.LMOModel(5),
]


# 1. Test split_datapoint with invalid output_type
def test_split_datapoint_invalid_output_type():
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, 0.4])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern, output_type="invalid")


# 2. Test split_datapoint without observed_total_se
@pytest.mark.parametrize("model", model_list)
def test_split_datapoint_no_se(model):
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, 0.4])
    result = split_datapoint(4.8, populations, rate_pattern, model=model)
    assert result is not None


# 3. Test split_datapoint with pattern_covariance
@pytest.mark.parametrize("model", model_list)
def test_split_datapoint_with_covariance(model):
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, 0.4])
    covariance = np.array([[0.1, 0.02], [0.02, 0.1]])
    result, SE = split_datapoint(
        4.8,
        populations,
        rate_pattern,
        1,
        model=model,
        pattern_covariance=covariance,
    )
    assert result is not None
    assert SE is not None


# 4. Test split_dataframe with mismatched index
def test_split_dataframe_mismatched_index():
    groups_to_split = ["A", "B"]
    obs_df = pd.DataFrame(
        {
            "demographic_id": [1, 2],
            "pattern_id": [1, 1],
            "obs": [100, 150],
            "A": [1, 0],
            "B": [0, 1],
        }
    )

    pop_df = pd.DataFrame({"A": [50, 75], "B": [50, 75]}, index=[3, 4])

    rate_df = pd.DataFrame({"A": [0.2, 0.3], "B": [0.4, 0.5]}, index=[1, 1])

    with pytest.raises(KeyError):
        split_dataframe(groups_to_split, obs_df, pop_df, rate_df)


# 5. Test split_dataframe with empty dataframes
def test_split_dataframe_empty():
    groups_to_split = ["A", "B"]
    obs_df = pd.DataFrame(
        columns=["demographic_id", "pattern_id", "obs", "A", "B"]
    )
    pop_df = pd.DataFrame(columns=["A", "B"])
    rate_df = pd.DataFrame(columns=["A", "B"])

    result = split_dataframe(groups_to_split, obs_df, pop_df, rate_df)
    assert result.empty


def test_split_datapoint_output_type_rate():
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, 0.4])
    result = split_datapoint(4.8, populations, rate_pattern, output_type="rate")
    assert result is not None
    assert len(result) == 2


def test_split_datapoint_with_observed_total_se():
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, 0.4])
    observed_total_se = 1.0
    result, se = split_datapoint(
        4.8, populations, rate_pattern, observed_total_se=observed_total_se
    )
    assert result is not None
    assert se is not None


def test_split_datapoint_negative_observed_total():
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, 0.4])
    with pytest.raises(ValueError):
        split_datapoint(-4.8, populations, rate_pattern)


def test_split_datapoint_zero_population():
    populations = np.array([0, 5])
    rate_pattern = np.array([0.2, 0.4])
    result = split_datapoint(4.8, populations, rate_pattern)
    assert result is not None


def test_split_datapoint_negative_population():
    populations = np.array([-2, 5])
    rate_pattern = np.array([0.2, 0.4])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern)


def test_split_datapoint_zero_rate_pattern():
    populations = np.array([2, 5])
    rate_pattern = np.array([0.0, 0.4])
    result = split_datapoint(4.8, populations, rate_pattern)
    assert result is not None


def test_split_datapoint_empty_populations():
    populations = np.array([])
    rate_pattern = np.array([0.2, 0.4])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern)


def test_split_datapoint_empty_rate_pattern():
    populations = np.array([2, 5])
    rate_pattern = np.array([])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern)


def test_split_datapoint_mismatched_lengths():
    populations = np.array([2, 5, 7])
    rate_pattern = np.array([0.2, 0.4])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern)


def test_split_datapoint_non_numeric_populations():
    populations = np.array([2, "a"])
    rate_pattern = np.array([0.2, 0.4])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern)


def test_split_datapoint_non_numeric_rate_pattern():
    populations = np.array([2, 5])
    rate_pattern = np.array([0.2, "b"])
    with pytest.raises(ValueError):
        split_datapoint(4.8, populations, rate_pattern)
