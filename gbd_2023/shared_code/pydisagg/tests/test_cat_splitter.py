import pytest
import pandas as pd
from pydisagg.ihme.splitter import (
    CatSplitter,
    CatDataConfig,
    CatPatternConfig,
    CatPopulationConfig,
)

# Step 1: Setup Fixtures


@pytest.fixture
def cat_data_config():
    return CatDataConfig(
        index=[
            "study_id",
            "year_id",
            "location_id",
            "sub_category",
        ],  # Include 'sub_category' in index
        cat_group="sub_category",
        val="val",
        val_sd="val_sd",
    )


@pytest.fixture
def cat_pattern_config():
    return CatPatternConfig(
        by=["year_id", "location_id"],
        cat="sub_category",
        val="pattern_val",
        val_sd="pattern_val_sd",
    )


@pytest.fixture
def cat_population_config():
    return CatPopulationConfig(
        index=["year_id", "location_id", "sub_category"],
        val="population",
    )


@pytest.fixture
def valid_data():
    return pd.DataFrame(
        {
            "study_id": [1, 2, 3],
            "year_id": [2000, 2000, 2001],
            "location_id": [10, 20, 10],
            "sub_category": [
                ["A1", "A2"],  # List of sub_categories
                ["B1", "B2"],
                ["C1", "C2"],
            ],
            "val": [100, 200, 150],
            "val_sd": [10, 20, 15],
        }
    )


@pytest.fixture
def valid_pattern():
    return pd.DataFrame(
        {
            "year_id": [2000, 2000, 2000, 2000, 2001, 2001],
            "location_id": [10, 10, 20, 20, 10, 10],
            "sub_category": ["A1", "A2", "B1", "B2", "C1", "C2"],
            "pattern_val": [0.6, 0.4, 0.7, 0.3, 0.55, 0.45],
            "pattern_val_sd": [0.06, 0.04, 0.07, 0.03, 0.055, 0.045],
        }
    )


@pytest.fixture
def valid_population():
    return pd.DataFrame(
        {
            "year_id": [2000, 2000, 2000, 2000, 2001, 2001],
            "location_id": [10, 10, 20, 20, 10, 10],
            "sub_category": ["A1", "A2", "B1", "B2", "C1", "C2"],
            "population": [5000, 3000, 7000, 3000, 5500, 4500],
        }
    )


@pytest.fixture
def cat_splitter(cat_data_config, cat_pattern_config, cat_population_config):
    return CatSplitter(
        data=cat_data_config,
        pattern=cat_pattern_config,
        population=cat_population_config,
    )


# Step 2: Write Tests for parse_data


def test_parse_data_duplicated_index(cat_splitter, valid_data):
    """Test parse_data raises an error on duplicated index."""
    duplicated_data = pd.concat([valid_data, valid_data])
    with pytest.raises(ValueError, match="has duplicated index"):
        cat_splitter.parse_data(duplicated_data, positive_strict=True)


def test_parse_data_valid(cat_splitter, valid_data):
    """Test that parse_data works correctly on valid data."""
    parsed_data = cat_splitter.parse_data(valid_data, positive_strict=True)
    assert not parsed_data.empty
    assert "val" in parsed_data.columns
    assert "val_sd" in parsed_data.columns


# Step 3: Write Tests for parse_pattern


def test_parse_pattern_valid(cat_splitter, valid_data, valid_pattern):
    """Test that parse_pattern works correctly on valid data."""
    parsed_data = cat_splitter.parse_data(valid_data, positive_strict=True)
    parsed_pattern = cat_splitter.parse_pattern(
        parsed_data, valid_pattern, model="rate"
    )
    assert not parsed_pattern.empty
    # The pattern columns are renamed with prefix 'cat_pat_'
    assert (
        f"{cat_splitter.pattern.prefix}{cat_splitter.pattern.val}"
        in parsed_pattern.columns
    )
    assert (
        f"{cat_splitter.pattern.prefix}{cat_splitter.pattern.val_sd}"
        in parsed_pattern.columns
    )


# Step 4: Write Tests for parse_population


def test_parse_population_missing_columns(
    cat_splitter, valid_data, valid_pattern, valid_population
):
    """Test parse_population raises an error when population columns are missing."""
    invalid_population = valid_population.drop(columns=["population"])
    parsed_data = cat_splitter.parse_data(valid_data, positive_strict=True)
    parsed_pattern = cat_splitter.parse_pattern(
        parsed_data, valid_pattern, model="rate"
    )
    with pytest.raises(KeyError, match="has missing columns"):
        cat_splitter.parse_population(parsed_pattern, invalid_population)


def test_parse_population_with_nan(
    cat_splitter, valid_data, valid_pattern, valid_population
):
    """Test parse_population raises an error when there are NaN values."""
    invalid_population = valid_population.copy()
    invalid_population.loc[0, "population"] = None
    parsed_data = cat_splitter.parse_data(valid_data, positive_strict=True)
    parsed_pattern = cat_splitter.parse_pattern(
        parsed_data, valid_pattern, model="rate"
    )
    with pytest.raises(ValueError, match="has NaN values"):
        cat_splitter.parse_population(parsed_pattern, invalid_population)


def test_parse_population_valid(
    cat_splitter, valid_data, valid_pattern, valid_population
):
    """Test that parse_population works correctly on valid data."""
    parsed_data = cat_splitter.parse_data(valid_data, positive_strict=True)
    parsed_pattern = cat_splitter.parse_pattern(
        parsed_data, valid_pattern, model="rate"
    )
    parsed_population = cat_splitter.parse_population(
        parsed_pattern, valid_population
    )
    assert not parsed_population.empty
    # The population column is renamed with prefix 'cat_pop_'
    pop_col = f"{cat_splitter.population.prefix}{cat_splitter.population.val}"
    assert pop_col in parsed_population.columns


# Step 5: Write Tests for the split method


def test_split_valid(cat_splitter, valid_data, valid_pattern, valid_population):
    """Test that the split method works correctly on valid data."""
    result = cat_splitter.split(
        data=valid_data,
        pattern=valid_pattern,
        population=valid_population,
        model="rate",
        output_type="rate",
    )
    assert not result.empty
    assert "split_result" in result.columns
    assert "split_result_se" in result.columns


def test_split_with_invalid_output_type(
    cat_splitter, valid_data, valid_pattern, valid_population
):
    """Test that the split method raises an error with an invalid output_type."""
    with pytest.raises(ValueError, match="Invalid output_type"):
        cat_splitter.split(
            data=valid_data,
            pattern=valid_pattern,
            population=valid_population,
            model="rate",
            output_type="invalid_output",
        )


def test_split_with_missing_population(cat_splitter, valid_data, valid_pattern):
    """Test that the split method raises an error when population data is missing."""
    with pytest.raises(
        KeyError, match="Parsing Population has missing columns"
    ):
        cat_splitter.split(
            data=valid_data,
            pattern=valid_pattern,
            population=pd.DataFrame(),  # Empty population data
            model="rate",
            output_type="rate",
        )


def test_split_with_non_matching_categories(
    cat_splitter, valid_data, valid_pattern, valid_population
):
    """Test that the split method raises an error when categories don't match."""
    invalid_population = valid_population.copy()
    invalid_population["sub_category"] = ["X1", "X2", "X1", "X2", "X1", "X2"]
    with pytest.raises(
        ValueError,
        match="After merging with population, there were NaN values created",
    ):
        cat_splitter.split(
            data=valid_data,
            pattern=valid_pattern,
            population=invalid_population,
            model="rate",
            output_type="rate",
        )
