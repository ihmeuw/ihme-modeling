import pytest
import pandas as pd
import numpy as np
from pydisagg.ihme.splitter import (
    AgeSplitter,
    AgeDataConfig,
    AgePatternConfig,
    AgePopulationConfig,
)
from pydisagg.ihme.validator import (
    validate_positive,
    validate_noindexdiff,
)


@pytest.fixture
def data():
    np.random.seed(123)
    return pd.DataFrame(
        dict(
            uid=range(10),
            sex_id=[1] * 5 + [2] * 5,
            location_id=[1, 2] * 5,
            year_id=[2010] * 10,
            age_start=[0, 5, 10, 17, 20] * 2,
            age_end=[12, 10, 22, 21, 25] * 2,
            val=5.0,
            val_sd=1.0,
        )
    )


@pytest.fixture
def pattern():
    np.random.seed(123)
    pattern_df1 = pd.DataFrame(
        dict(
            sex_id=[1] * 5 + [2] * 5,
            age_start=[0, 5, 10, 15, 20] * 2,
            age_end=[5, 10, 15, 20, 25] * 2,
            age_group_id=list(range(5)) * 2,
            draw_0=np.random.rand(10),
            draw_1=np.random.rand(10),
            draw_2=np.random.rand(10),
            year_id=[2010] * 10,
            location_id=[1] * 10,
        )
    )
    pattern_df2 = pattern_df1.copy()
    pattern_df2["location_id"] = 2
    return pd.concat([pattern_df1, pattern_df2]).reset_index(drop=True)


@pytest.fixture
def population():
    np.random.seed(123)
    sex_id = pd.DataFrame(dict(sex_id=[1, 2]))
    year_id = pd.DataFrame(dict(year_id=[2010]))
    location_id = pd.DataFrame(dict(location_id=[1, 2]))
    age_group_id = pd.DataFrame(dict(age_group_id=range(5)))

    population = (
        sex_id.merge(location_id, how="cross")
        .merge(age_group_id, how="cross")
        .merge(year_id, how="cross")
    )
    population["population"] = 1000
    return population


@pytest.fixture
def splitter(data, pattern, population):
    pattern_config = AgePatternConfig(
        by=["sex_id", "location_id", "year_id"],
        age_key="age_group_id",
        age_lwr="age_start",
        age_upr="age_end",
        draws=["draw_0", "draw_1", "draw_2"],
    )
    data_config = AgeDataConfig(
        index=["uid", "sex_id", "location_id", "year_id"],
        age_lwr="age_start",
        age_upr="age_end",
        val="val",
        val_sd="val_sd",
    )
    population_config = AgePopulationConfig(
        index=["sex_id", "location_id", "age_group_id", "year_id"],
        val="population",
    )
    return AgeSplitter(
        data=data_config, pattern=pattern_config, population=population_config
    )


def test_parse_data_success(splitter, data):
    # Should pass without exceptions
    parsed_data = splitter.parse_data(data, positive_strict=True)
    assert isinstance(parsed_data, pd.DataFrame)
    assert not parsed_data.empty


def test_parse_pattern_success(splitter, data, pattern):
    # Ensure the pattern parsing works correctly with valid input
    parsed_pattern = splitter.parse_pattern(data, pattern, positive_strict=True)
    assert isinstance(parsed_pattern, pd.DataFrame)
    assert not parsed_pattern.empty


def test_parse_data_invalid_intervals(splitter, data):
    # Introduce invalid interval (age_start >= age_end)
    data.loc[0, "age_start"] = 20
    data.loc[0, "age_end"] = 10
    with pytest.raises(ValueError):
        splitter.parse_data(data, positive_strict=True)


def test_parse_pattern_pat_coverage(splitter, data, pattern):
    # Modify pattern intervals so they don't cover the data intervals
    pattern.loc[0, "age_start"] = 50
    pattern.loc[0, "age_end"] = 60
    with pytest.raises(ValueError):
        splitter.parse_pattern(data, pattern, positive_strict=True)


def test_validate_positive():
    df = pd.DataFrame({"val_sd": [-1.0, -0.5, 0.0, 1.0, 2.0]})

    with pytest.raises(ValueError):
        validate_positive(df, ["val_sd"], "Test Pattern", strict=True)


def test_validate_noindexdiff():
    # Create two DataFrames with different indices
    df_ref = pd.DataFrame(
        {
            "uid": [0, 1, 2],
            "sex_id": [1, 1, 2],
            "location_id": [1, 1, 2],
            "year_id": [2010, 2010, 2011],
            "age_start": [0, 5, 10],
        }
    )

    df = pd.DataFrame(
        {
            "uid": [0, 1],
            "sex_id": [1, 1],
            "location_id": [1, 1],
            "year_id": [2010, 2010],
            "age_start": [0, 5],
        }
    )

    with pytest.raises(ValueError):
        validate_noindexdiff(
            df_ref, df, ["uid", "sex_id", "location_id", "year_id"], "Test Data"
        )


def test_parse_data_missing_columns(splitter, data):
    # Remove a required column
    data = data.drop(columns=["val"])
    with pytest.raises(KeyError):
        splitter.parse_data(data, positive_strict=True)


def test_parse_pattern_missing_columns(splitter, pattern):
    # Remove a required column
    pattern = pattern.drop(columns=["draw_0"])
    with pytest.raises(KeyError):
        splitter.parse_pattern(pattern, pattern, positive_strict=True)


# def test_parse_data_invalid_data_types(splitter, data):
#     # Introduce invalid data type
#     data["val"] = "invalid"
#     with pytest.raises(ValueError):
#         splitter.parse_data(data, positive_strict=True)


def test_parse_pattern_invalid_data_types(splitter, pattern):
    # Introduce invalid data type
    pattern["draw_0"] = "invalid"
    with pytest.raises(ValueError):
        splitter.parse_pattern(pattern, pattern, positive_strict=True)
