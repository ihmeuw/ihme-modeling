import numpy as np
import pandas as pd
import pytest

from pydisagg.ihme.validator import (
    validate_columns,
    validate_index,
    validate_interval,
    validate_noindexdiff,
    validate_nonan,
    validate_pat_coverage,
    validate_positive,
    validate_set_uniqueness,
    validate_realnumber,
)


# Test functions
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
    return pd.DataFrame(
        dict(
            sex_id=[1] * 5 + [2] * 5,
            age_start=[0, 5, 10, 15, 20] * 2,
            age_end=[5, 10, 15, 20, 25] * 2,
            age_group_id=list(range(5)) * 2,
            draw_0=np.random.rand(10),
            draw_1=np.random.rand(10),
            draw_2=np.random.rand(10),
            year_id=[2010] * 10,
            location_id=[1, 2] * 5,
        )
    )


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


# Tests for validate_columns
def test_validate_columns_missing(population):
    with pytest.raises(KeyError):
        validate_columns(
            population.drop(columns=["population"]),
            ["sex_id", "location_id", "age_group_id", "year_id", "population"],
            "population",
        )


def test_validate_columns_no_missing(population):
    # All columns are present; should pass
    validate_columns(
        population,
        ["sex_id", "location_id", "age_group_id", "year_id", "population"],
        "population",
    )


# Tests for validate_index
def test_validate_index_missing(population):
    with pytest.raises(ValueError):
        validate_index(
            pd.concat([population, population]),
            ["sex_id", "location_id", "age_group_id", "year_id"],
            "population",
        )


def test_validate_index_no_duplicates(population):
    # Ensure DataFrame has no duplicate indices; should pass
    validate_index(
        population,
        ["sex_id", "location_id", "age_group_id", "year_id"],
        "population",
    )


# Tests for validate_nonan
def test_validate_nonan(population):
    with pytest.raises(ValueError):
        validate_nonan(population.assign(population=np.nan), "population")


def test_validate_nonan_no_nan(population):
    # No NaN values; should pass
    validate_nonan(population, "population")


# Tests for validate_positive
def test_validate_positive_strict(population):
    with pytest.raises(ValueError):
        validate_positive(
            population.assign(population=0),
            ["population"],
            "population",
            strict=True,
        )


def test_validate_positive_not_strict(population):
    with pytest.raises(ValueError):
        validate_positive(
            population.assign(population=-1),
            ["population"],
            "population",
            strict=False,
        )


def test_validate_positive_no_error(population):
    validate_positive(population, ["population"], "population", strict=True)
    validate_positive(population, ["population"], "population", strict=False)


# Tests for validate_interval
def test_validate_interval_lower_equal_upper(data):
    with pytest.raises(ValueError):
        validate_interval(
            data.assign(age_end=data["age_start"]),
            "age_start",
            "age_end",
            ["uid"],
            "data",
        )


def test_validate_interval_lower_greater_than_upper(data):
    with pytest.raises(ValueError):
        validate_interval(
            data.assign(age_end=0), "age_start", "age_end", ["uid"], "data"
        )


def test_validate_interval_positive(data):
    validate_interval(data, "age_start", "age_end", ["uid"], "data")


# Tests for validate_noindexdiff
@pytest.fixture
def merged_data_pattern(data, pattern):
    return pd.merge(
        pattern,
        data,
        on=["sex_id", "age_start", "age_end", "location_id"],
        how="outer",
    )


def test_validate_noindexdiff_merged_positive(merged_data_pattern, population):
    # Positive test case: no index difference
    validate_noindexdiff(
        population,
        merged_data_pattern.dropna(subset=["sex_id", "location_id"]),
        ["sex_id", "location_id"],
        "merged_data_pattern",
    )


def test_validate_noindexdiff_merged_negative(data, pattern):
    # Negative test case: index difference exists
    data_with_pattern = data.merge(
        pattern,
        on=["sex_id", "age_start", "age_end", "location_id", "year_id"],
        how="left",
    )
    # Introduce an index difference
    data_with_pattern = data_with_pattern.drop(index=0)
    with pytest.raises(ValueError):
        validate_noindexdiff(
            data,
            data_with_pattern,
            data.columns.tolist(),
            "merged_data_pattern",
        )


# Tests for validate_pat_coverage
@pytest.mark.parametrize(
    "bad_data_with_pattern",
    [
        # gap in pattern
        pd.DataFrame(
            dict(
                pat_lwr=[0, 1, 2, 2, 4, 5],
                pat_upr=[1, 2, 5, 3, 5, 7],
            )
        ),
        # overlap in pattern
        pd.DataFrame(
            dict(
                pat_lwr=[0, 0, 2, 2, 3, 5],
                pat_upr=[1, 2, 5, 3, 5, 7],
            )
        ),
        # doesn't cover head
        pd.DataFrame(
            dict(
                pat_lwr=[1, 2, 3, 2, 3, 5],
                pat_upr=[2, 3, 5, 3, 5, 7],
            )
        ),
        # doesn't cover tail
        pd.DataFrame(
            dict(
                pat_lwr=[0, 1, 2, 2, 3, 5],
                pat_upr=[1, 2, 4, 3, 5, 7],
            )
        ),
    ],
)
def test_validate_pat_coverage_failure(bad_data_with_pattern):
    bad_data_with_pattern["group_id"] = [1, 1, 1, 2, 2, 2]
    bad_data_with_pattern["lwr"] = [0, 0, 0, 3, 3, 3]
    bad_data_with_pattern["upr"] = [5, 5, 5, 7, 7, 7]

    with pytest.raises(ValueError):
        validate_pat_coverage(
            bad_data_with_pattern,
            "lwr",
            "upr",
            "pat_lwr",
            "pat_upr",
            ["group_id"],
            "pattern",
        )


# Tests for validate_realnumber
def test_validate_realnumber_positive():
    df = pd.DataFrame({"col1": [1, 2.5, -3.5, 4.2], "col2": [5.1, 6, 7, 8]})
    # Should pass without exceptions
    validate_realnumber(df, ["col1", "col2"], "df")


def test_validate_realnumber_zero():
    df = pd.DataFrame({"col1": [1, 2, 0, 4], "col2": [5, 6, 7, 8]})
    with pytest.raises(
        ValueError, match="df has non-real or zero values in: \\['col1'\\]"
    ):
        validate_realnumber(df, ["col1"], "df")


def test_validate_realnumber_nan():
    df = pd.DataFrame({"col1": [1, 2, 3, np.nan], "col2": [5, 6, 7, 8]})
    with pytest.raises(
        ValueError, match="df has non-real or zero values in: \\['col1'\\]"
    ):
        validate_realnumber(df, ["col1"], "df")


def test_validate_realnumber_non_numeric():
    df = pd.DataFrame({"col1": [1, 2, 3, "a"], "col2": [5, 6, 7, 8]})
    with pytest.raises(
        ValueError, match="df has non-real or zero values in: \\['col1'\\]"
    ):
        validate_realnumber(df, ["col1"], "df")


def test_validate_realnumber_infinite():
    df = pd.DataFrame({"col1": [1, 2, 3, np.inf], "col2": [5, 6, 7, 8]})
    # np.inf is not a finite real number
    with pytest.raises(
        ValueError, match="df has non-real or zero values in: \\['col1'\\]"
    ):
        validate_realnumber(df, ["col1"], "df")


# Tests for validate_set_uniqueness
def test_validate_set_uniqueness_positive():
    df = pd.DataFrame(
        {"col1": [[1, 2, 3], ["a", "b", "c"], [True, False], [1.1, 2.2, 3.3]]}
    )
    # Should pass without exceptions
    validate_set_uniqueness(df, "col1", "df")


def test_validate_set_uniqueness_negative():
    df = pd.DataFrame(
        {"col1": [[1, 2, 2], ["a", "b", "a"], [True, False], [1.1, 2.2, 1.1]]}
    )
    with pytest.raises(
        ValueError,
        match="df has rows in column 'col1' where list elements are not unique.",
    ):
        validate_set_uniqueness(df, "col1", "df")


def test_validate_set_uniqueness_empty_lists():
    df = pd.DataFrame({"col1": [[], [], []]})
    # Should pass; empty lists have no duplicates
    validate_set_uniqueness(df, "col1", "df")


def test_validate_set_uniqueness_single_element_lists():
    df = pd.DataFrame({"col1": [[1], ["a"], [True]]})
    # Should pass; single-element lists can't have duplicates
    validate_set_uniqueness(df, "col1", "df")


def test_validate_set_uniqueness_mixed_types_with_duplicates():
    df = pd.DataFrame({"col1": [[1, "1", 1.0], [True, 1, 1.0], [2, 2, 2]]})
    with pytest.raises(ValueError):
        validate_set_uniqueness(df, "col1", "df")
