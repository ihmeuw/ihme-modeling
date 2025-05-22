import pytest
import pandas as pd
from pydisagg.ihme.splitter import (
    SexSplitter,
    SexDataConfig,
    SexPatternConfig,
    SexPopulationConfig,
)


# Step 1: Setup Fixtures
@pytest.fixture
def sex_data_config():
    return SexDataConfig(
        index=["age_group_id", "year_id", "location_id"],
        val="val",
        val_sd="val_sd",
    )


@pytest.fixture
def sex_pattern_config():
    return SexPatternConfig(by=["age_group_id", "year_id"])


@pytest.fixture
def sex_population_config():
    return SexPopulationConfig(
        index=["age_group_id", "year_id", "location_id"],
        sex="sex_id",
        sex_m=1,
        sex_f=2,
        val="population",
    )


@pytest.fixture
def valid_data():
    return pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "location_id": [10, 20, 10, 20],
            "sex_id": [3, 3, 3, 3],
            "val": [100, 200, 150, 250],
            "val_sd": [10, 20, 15, 25],
        }
    )


@pytest.fixture
def valid_pattern():
    return pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "pattern_val": [1.5, 2.0, 1.2, 1.8],
            "pattern_val_sd": [0.1, 0.2, 0.15, 0.25],
        }
    )


@pytest.fixture
def invalid_pattern_missing_columns():
    return pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "pattern_val": [1.5, 2.0, 1.2, 1.8],
            # Missing pattern_val_sd
        }
    )


@pytest.fixture
def duplicated_index_pattern():
    return pd.DataFrame(
        {
            "age_group_id": [1, 1, 1, 1],
            "year_id": [2000, 2000, 2000, 2000],
            "pattern_val": [1.5, 2.0, 1.2, 1.8],
            "pattern_val_sd": [0.1, 0.2, 0.15, 0.25],
        }
    )


@pytest.fixture
def pattern_with_nan():
    return pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "pattern_val": [1.5, None, 1.2, 1.8],
            "pattern_val_sd": [0.1, 0.2, None, 0.25],
        }
    )


@pytest.fixture
def pattern_with_non_positive():
    return pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "pattern_val": [-1.5, 0, -1.2, 0],
            "pattern_val_sd": [0.1, 0.2, 0.15, 0.25],
        }
    )


@pytest.fixture
def sex_splitter(sex_data_config, sex_pattern_config, sex_population_config):
    return SexSplitter(
        data=sex_data_config,
        pattern=sex_pattern_config,
        population=sex_population_config,
    )


# Step 2: Write Tests for parse_data
def test_parse_data_missing_columns(sex_splitter, valid_data):
    """Test parse_data raises an error when columns are missing."""
    invalid_data = valid_data.drop(columns=["val"])
    with pytest.raises(KeyError):
        sex_splitter.parse_data(invalid_data)


def test_parse_data_duplicated_index(sex_splitter, valid_data):
    """Test parse_data raises an error on duplicated index."""
    duplicated_data = pd.concat([valid_data, valid_data])
    with pytest.raises(ValueError):
        sex_splitter.parse_data(duplicated_data)


def test_parse_data_with_nan(sex_splitter, valid_data):
    """Test parse_data raises an error when there are NaN values."""
    nan_data = valid_data.copy()
    nan_data.loc[0, "val"] = None
    with pytest.raises(ValueError):
        sex_splitter.parse_data(nan_data)


def test_parse_data_non_positive(sex_splitter, valid_data):
    """Test parse_data raises an error for non-positive values in val or val_sd."""
    non_positive_data = valid_data.copy()
    non_positive_data.loc[0, "val"] = -10
    with pytest.raises(ValueError):
        sex_splitter.parse_data(non_positive_data)


def test_parse_data_valid(sex_splitter, valid_data):
    """Test that parse_data works correctly on valid data."""
    parsed_data = sex_splitter.parse_data(valid_data)
    assert not parsed_data.empty
    assert "val" in parsed_data.columns
    assert "val_sd" in parsed_data.columns


# Step 3: Write Tests for parse_pattern
def test_parse_pattern_missing_columns(
    sex_splitter, valid_data, invalid_pattern_missing_columns
):
    """Test parse_pattern raises an error when required columns are missing."""
    with pytest.raises(ValueError):
        sex_splitter.parse_pattern(
            valid_data, invalid_pattern_missing_columns, model="rate"
        )


def test_parse_pattern_non_positive(
    sex_splitter, valid_data, pattern_with_non_positive
):
    """Test parse_pattern raises an error for non-positive values in pattern."""
    with pytest.raises(ValueError):
        sex_splitter.parse_pattern(
            valid_data, pattern_with_non_positive, model="rate"
        )


# Step 4: Write Tests for parse_population
def test_parse_population_valid(sex_splitter, valid_data):
    """Test parse_population raises an error when population data is missing for some sex_id values."""
    valid_population = pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "location_id": [10, 20, 10, 20],
            "sex_id": [1, 2, 1, 2],  # No sex_id = 3 here
            "population": [1000, 1100, 1200, 1300],
        }
    )

    # Ensure that the function raises an error due to unmatched sex_id values
    with pytest.raises(ValueError):
        sex_splitter.parse_population(valid_data, valid_population)


def test_parse_population_missing_columns(sex_splitter, valid_data):
    """Test parse_population raises an error when required columns are missing."""
    invalid_population = pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "location_id": [10, 20, 10, 20],
            "population": [1000, 1100, 1200, 1300],
            # Missing 'sex_id' column
        }
    )
    with pytest.raises(KeyError):
        sex_splitter.parse_population(valid_data, invalid_population)


def test_parse_population_with_nan(sex_splitter, valid_data):
    """Test parse_population raises an error when there are NaN values."""
    invalid_population = pd.DataFrame(
        {
            "age_group_id": [1, 1, 2, 2],
            "year_id": [2000, 2000, 2001, 2001],
            "location_id": [10, 20, 10, 20],
            "sex_id": [1, 2, 1, 2],
            "population": [
                1000,
                None,
                1200,
                1300,
            ],  # NaN value in the population
        }
    )
    with pytest.raises(ValueError):
        sex_splitter.parse_population(valid_data, invalid_population)


def test_parse_population_duplicated_index(sex_splitter, valid_data):
    """Test parse_population raises an error when population index is duplicated."""
    duplicated_population = pd.DataFrame(
        {
            "age_group_id": [1, 1, 1, 1],
            "year_id": [2000, 2000, 2000, 2000],
            "location_id": [10, 20, 10, 20],
            "sex_id": [1, 2, 1, 2],
            "population": [1000, 1100, 1200, 1300],
        }
    )
    with pytest.raises(ValueError):
        sex_splitter.parse_population(valid_data, duplicated_population)
