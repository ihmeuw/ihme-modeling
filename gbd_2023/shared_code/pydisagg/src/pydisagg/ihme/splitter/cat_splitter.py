# cat_splitter.py

from typing import Any, List, Literal
import numpy as np
import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel

from pydisagg.disaggregate import split_datapoint
from pydisagg.models import RateMultiplicativeModel, LogOddsModel
from pydisagg.ihme.schema import Schema
from pydisagg.ihme.validator import (
    validate_columns,
    validate_index,
    validate_noindexdiff,
    validate_nonan,
    validate_positive,
    validate_set_uniqueness,
)


class CatDataConfig(Schema):
    """
    Configuration schema for categorical data DataFrame.
    """

    index: List[str]
    cat_group: str
    val: str
    val_sd: str

    @property
    def columns(self) -> List[str]:
        return self.index + [self.cat_group, self.val, self.val_sd]

    @property
    def val_fields(self) -> List[str]:
        return [self.val, self.val_sd]


class CatPatternConfig(Schema):
    """
    Configuration schema for the pattern DataFrame.
    """

    by: List[str]
    cat: str
    draws: List[str] = []
    val: str = "mean"
    val_sd: str = "std_err"
    prefix: str = "cat_pat_"

    @property
    def index(self) -> List[str]:
        return self.by + [self.cat]

    @property
    def columns(self) -> List[str]:
        return self.index + self.val_fields + self.draws

    @property
    def val_fields(self) -> List[str]:
        return [self.val, self.val_sd]


class CatPopulationConfig(Schema):
    """
    Configuration for the population DataFrame.
    """

    index: List[str]
    val: str
    prefix: str = "cat_pop_"

    @property
    def columns(self) -> List[str]:
        return self.index + [self.val]

    @property
    def val_fields(self) -> List[str]:
        return [self.val]


class CatSplitter(BaseModel):
    """
    Class for splitting categorical data based on pattern and population data.
    """

    data: CatDataConfig
    pattern: CatPatternConfig
    population: CatPopulationConfig

    def model_post_init(self, __context: Any) -> None:
        """
        Perform extra validation after model initialization.
        """
        if not set(self.pattern.index).issubset(self.data.index):
            raise ValueError(
                "The pattern's match criteria must be a subset of the data."
            )
        if not set(self.population.index).issubset(
            self.data.index + self.pattern.index
        ):
            raise ValueError(
                "The population's match criteria must be a subset of the data and the pattern."
            )
        if self.pattern.cat not in self.population.index:
            raise ValueError(
                "The 'target' column in the population must match the 'target' column in the data."
            )

    def parse_data(self, data: DataFrame, positive_strict: bool) -> DataFrame:
        """
        Parse and validate the input data DataFrame.
        """
        name = "While parsing data"

        # Validate required columns
        validate_columns(data, self.data.columns, name)

        # Ensure that 'cat_group' column contains lists
        data[self.data.cat_group] = data[self.data.cat_group].apply(
            lambda x: x if isinstance(x, list) else [x]
        )

        # Validate that every list in 'cat_group' contains unique elements
        validate_set_uniqueness(data, self.data.cat_group, name)

        # Explode the 'cat_group' column and rename it to match the pattern's 'cat'
        data = data.explode(self.data.cat_group).rename(
            columns={self.data.cat_group: self.pattern.cat}
        )

        # Validate index after exploding
        validate_index(data, self.data.index, name)
        validate_nonan(data, name)
        validate_positive(
            data, [self.data.val_sd], name, strict=positive_strict
        )

        return data

    def _merge_with_pattern(
        self,
        data: DataFrame,
        pattern: DataFrame,
    ) -> DataFrame:
        """
        Merge data with pattern DataFrame.
        """
        data_with_pattern = data.merge(
            pattern, on=self.pattern.index, how="left"
        )

        validate_nonan(
            data_with_pattern[
                [
                    f"{self.pattern.prefix}{col}"
                    for col in self.pattern.val_fields
                ]
            ],
            "After merging with pattern, there were NaN values created. This indicates that your pattern does not cover all the data.",
        )

        return data_with_pattern

    def parse_pattern(
        self, data: DataFrame, pattern: DataFrame, model: str
    ) -> DataFrame:
        """
        Parse and merge the pattern DataFrame with data.
        """
        name = "While parsing pattern"

        try:
            val_cols = self.pattern.val_fields
            if not all(col in pattern.columns for col in val_cols):
                if not self.pattern.draws:
                    raise ValueError(
                        f"{name}: Must provide draws for pattern if pattern.val and "
                        "pattern.val_sd are not available."
                    )
                validate_columns(pattern, self.pattern.draws, name)
                pattern[self.pattern.val] = pattern[self.pattern.draws].mean(
                    axis=1
                )
                pattern[self.pattern.val_sd] = pattern[self.pattern.draws].std(
                    axis=1
                )

            validate_columns(pattern, self.pattern.columns, name)
        except KeyError as e:
            raise KeyError(
                f"{name}: Missing columns in the pattern. Details:\n{e}"
            )

        pattern_copy = pattern.copy()
        pattern_copy = pattern_copy[
            self.pattern.index + self.pattern.val_fields
        ]
        rename_map = {
            col: f"{self.pattern.prefix}{col}"
            for col in self.pattern.val_fields
        }
        pattern_copy.rename(columns=rename_map, inplace=True)

        # Merge with pattern
        data_with_pattern = self._merge_with_pattern(data, pattern_copy)

        # Validate index differences after merging
        validate_noindexdiff(
            data,
            data_with_pattern,
            self.data.index,
            name,
        )

        return data_with_pattern

    def parse_population(
        self, data: DataFrame, population: DataFrame
    ) -> DataFrame:
        name = "Parsing Population"
        validate_columns(population, self.population.columns, name)

        population = population[self.population.columns].copy()

        validate_index(population, self.population.index, name)
        validate_nonan(population, name)

        rename_map = {
            self.population.val: f"{self.population.prefix}{self.population.val}"
        }
        population.rename(columns=rename_map, inplace=True)

        data_with_population = self._merge_with_population(data, population)

        # Ensure the prefixed population column exists
        pop_col = f"{self.population.prefix}{self.population.val}"
        if pop_col not in data_with_population.columns:
            raise KeyError(
                f"Expected column '{pop_col}' not found in merged data."
            )

        validate_nonan(
            data_with_population[[pop_col]],
            "After merging with population, there were NaN values created. This indicates that your population data does not cover all the data.",
        )
        return data_with_population

    def _merge_with_population(
        self, data: DataFrame, population: DataFrame
    ) -> DataFrame:
        """
        Merge data with population DataFrame.
        """
        data_with_population = data.merge(
            population, on=self.population.index, how="left"
        )

        return data_with_population

    def _process_group(
        self, group: DataFrame, model: str, output_type: str
    ) -> DataFrame:
        """
        Process a group of data for splitting.
        """
        observed_total = group[self.data.val].iloc[0]
        observed_total_se = group[self.data.val_sd].iloc[0]

        if len(group) == 1:
            # No need to split, assign the observed values
            group["split_result"] = observed_total
            group["split_result_se"] = observed_total_se
            group["split_flag"] = 0  # Not split
        else:
            # Need to split among multiple targets
            bucket_populations = group[
                f"{self.population.prefix}{self.population.val}"
            ].values
            rate_pattern = group[
                f"{self.pattern.prefix}{self.pattern.val}"
            ].values
            pattern_sd = group[
                f"{self.pattern.prefix}{self.pattern.val_sd}"
            ].values
            pattern_covariance = np.diag(pattern_sd**2)

            if model == "rate":
                splitting_model = RateMultiplicativeModel()
            elif model == "logodds":
                splitting_model = LogOddsModel()

            # Determine whether to normalize by population for the output type
            pop_normalize = output_type == "rate"

            # Perform splitting
            split_result, split_se = split_datapoint(
                observed_total=observed_total,
                bucket_populations=bucket_populations,
                rate_pattern=rate_pattern,
                model=splitting_model,
                output_type=output_type,
                normalize_pop_for_average_type_obs=pop_normalize,
                observed_total_se=observed_total_se,
                pattern_covariance=pattern_covariance,
            )

            # Assign results back to the group
            group["split_result"] = split_result
            group["split_result_se"] = split_se
            group["split_flag"] = 1  # Split

        return group

    def split(
        self,
        data: DataFrame,
        pattern: DataFrame,
        population: DataFrame,
        model: Literal["rate", "logodds"] = "rate",
        output_type: Literal["rate", "count"] = "rate",
    ) -> DataFrame:
        """
        Split the input data based on a specified pattern and population model.
        """
        # Validate model and output_type
        if model not in ["rate", "logodds"]:
            raise ValueError(f"Invalid model: {model}")
        if output_type not in ["rate", "count"]:
            raise ValueError(f"Invalid output_type: {output_type}")

        if self.population.prefix_status == "prefixed":
            self.population.remove_prefix()
        if self.pattern.prefix_status == "prefixed":
            self.pattern.remove_prefix()

        # Parsing input data, pattern, and population
        data = self.parse_data(data, positive_strict=True)
        data = self.parse_pattern(data, pattern, model)
        data = self.parse_population(data, population)

        # Determine grouping columns
        group_cols = self.data.index[:-1]  # Exclude 'location_id' from grouping

        # Process groups using regular groupby
        final_split_df = (
            data.groupby(group_cols, group_keys=False)
            .apply(lambda group: self._process_group(group, model, output_type))
            .reset_index(drop=True)
        )

        return final_split_df
