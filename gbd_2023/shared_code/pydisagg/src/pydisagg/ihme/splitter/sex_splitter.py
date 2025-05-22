from typing import Any

import numpy as np
import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel
from scipy.special import expit  # type: ignore
from typing import Literal
from pydisagg.disaggregate import split_datapoint
from pydisagg.ihme.schema import Schema
from pydisagg.ihme.validator import (
    validate_columns,
    validate_index,
    validate_noindexdiff,
    validate_nonan,
    validate_positive,
    validate_realnumber,
)
from pydisagg.models import RateMultiplicativeModel
from pydisagg.models import LogOddsModel


class SexPatternConfig(Schema):
    by: list[str]
    draws: list[str] = []
    val: str = "ratio_f_to_m"
    val_sd: str = "ratio_f_to_m_se"
    prefix: str = "sex_pat_"

    @property
    def index(self) -> list[str]:
        return self.by

    @property
    def columns(self) -> list[str]:
        return self.index + [
            self.val,
            self.val_sd,
        ]

    @property
    def val_fields(self) -> list[str]:
        return [
            "val",
            "val_sd",
        ]


class SexPopulationConfig(Schema):
    index: list[str]
    sex: str
    sex_m: str | int
    sex_f: str | int
    val: str
    prefix: str = "sex_pop_"

    @property
    def columns(self) -> list[str]:
        return self.index + [self.sex, self.val]

    @property
    def val_fields(self) -> list[str]:
        return ["val"]


class SexDataConfig(Schema):
    index: list[str]
    val: str
    val_sd: str

    @property
    def columns(self) -> list[str]:
        return list(set(self.index + [self.val, self.val_sd]))


class SexSplitter(BaseModel):
    data: SexDataConfig
    pattern: SexPatternConfig
    population: SexPopulationConfig

    def model_post_init(self, __context: Any) -> None:
        """Extra validation of all the index."""
        if not set(self.pattern.by).issubset(self.data.index):
            raise ValueError("pattern.by must be a subset of data.index")
        if not set(self.population.index).issubset(
            self.data.index + self.pattern.index
        ):
            raise ValueError(
                "population.index must be a subset of data.index + pattern.index"
            )

    def _merge_with_pattern(
        self, data: DataFrame, pattern: DataFrame
    ) -> DataFrame:
        data_with_pattern = data.merge(
            pattern, on=self.pattern.by, how="left"
        ).dropna()
        return data_with_pattern

    def get_population_by_sex(self, population, sex_value):
        return population[population[self.population.sex] == sex_value][
            self.population.index + [self.population.val]
        ].copy()

    def parse_data(self, data: DataFrame) -> DataFrame:
        name = "While parsing data"

        # Validate core columns first
        try:
            validate_columns(data, self.data.columns, name)
        except KeyError as e:
            raise KeyError(
                f"{name}: Missing columns in the input data. Details:\n{e}"
            )

        if self.population.sex not in data.columns:
            raise KeyError(
                f"{name}: Missing column '{self.population.sex}' in the input data."
            )

        try:
            validate_index(data, self.data.index, name)
        except ValueError as e:
            raise ValueError(f"{name}: Duplicated index found. Details:\n{e}")

        try:
            validate_nonan(data, name)
        except ValueError as e:
            raise ValueError(f"{name}: NaN values found. Details:\n{e}")

        try:
            validate_positive(data, [self.data.val, self.data.val_sd], name)
        except ValueError as e:
            raise ValueError(
                f"{name}: Non-positive values found in 'val' or 'val_sd'. Details:\n{e}"
            )

        return data

    def parse_pattern(
        self, data: DataFrame, pattern: DataFrame, model: str
    ) -> DataFrame:
        name = "While parsing pattern"

        try:
            if not all(
                col in pattern.columns
                for col in [self.pattern.val, self.pattern.val_sd]
            ):
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

        pattern = pattern[self.pattern.columns].copy()

        try:
            validate_index(pattern, self.pattern.index, name)
        except ValueError as e:
            raise ValueError(
                f"{name}: Duplicated index found in the pattern. Details:\n{e}"
            )

        try:
            validate_nonan(pattern, name)
        except ValueError as e:
            raise ValueError(
                f"{name}: NaN values found in the pattern. Details:\n{e}"
            )

        if model == "rate":
            try:
                validate_positive(
                    pattern, [self.pattern.val, self.pattern.val_sd], name
                )
            except ValueError as e:
                raise ValueError(
                    f"{name}: Non-positive values found in 'val' or 'val_sd'. Details:\n{e}"
                )
        elif model == "logodds":
            try:
                validate_realnumber(pattern, [self.pattern.val_sd], name)
            except ValueError as e:
                raise ValueError(
                    f"{name}: Invalid real number values found. Details:\n{e}"
                )

        pattern_copy = pattern.copy()
        rename_map = self.pattern.apply_prefix()
        pattern_copy.rename(columns=rename_map, inplace=True)

        data_with_pattern = self._merge_with_pattern(data, pattern_copy)

        # Validate index differences after merging
        validate_noindexdiff(data, data_with_pattern, self.data.index, name)

        return data_with_pattern

    def parse_population(
        self, data: DataFrame, population: DataFrame
    ) -> DataFrame:
        name = "While parsing population"

        # Step 1: Validate population columns
        try:
            validate_columns(population, self.population.columns, name)
        except KeyError as e:
            raise KeyError(
                f"{name}: Missing columns in the population data. Details:\n{e}"
            )

        # Step 2: Get male and female populations and rename columns
        male_population = self.get_population_by_sex(
            population, self.population.sex_m
        )
        female_population = self.get_population_by_sex(
            population, self.population.sex_f
        )

        male_population.rename(
            columns={self.population.val: "m_pop"}, inplace=True
        )
        female_population.rename(
            columns={self.population.val: "f_pop"}, inplace=True
        )

        # Step 3: Merge population data with main data
        data_with_population = self._merge_with_population(
            data, male_population, "m_pop"
        )
        data_with_population = self._merge_with_population(
            data_with_population, female_population, "f_pop"
        )

        # Step 4: Validate the merged data columns
        try:
            validate_columns(data_with_population, ["m_pop", "f_pop"], name)
        except KeyError as e:
            raise KeyError(
                f"{name}: Missing population columns after merging. Details:\n{e}"
            )

        # Step 5: Validate for NaN values in the merged columns using validate_nonan
        try:
            validate_nonan(data_with_population, name)
        except ValueError as e:
            raise ValueError(
                f"{name}: NaN values found in the population data. Details:\n{e}"
            )

        # Step 6: Validate index differences
        try:
            validate_noindexdiff(
                data, data_with_population, self.data.index, name
            )
        except ValueError as e:
            raise ValueError(
                f"{name}: Index differences found between data and population. Details:\n{e}"
            )

        # Ensure the columns are in the correct numeric type (e.g., float64)
        data_with_population["m_pop"] = data_with_population["m_pop"].astype(
            "float64"
        )
        data_with_population["f_pop"] = data_with_population["f_pop"].astype(
            "float64"
        )

        return data_with_population

    def _merge_with_population(
        self, data: DataFrame, population: DataFrame, pop_col: str
    ) -> DataFrame:
        keep_cols = self.population.index + [pop_col]
        population_temp = population[keep_cols]
        data_with_population = data.merge(
            population_temp,
            on=self.population.index,
            how="left",
        )

        # Ensure the merged population columns are standard numeric types
        if pop_col in data_with_population.columns:
            data_with_population[pop_col] = data_with_population[
                pop_col
            ].astype("float64")

        return data_with_population

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

        This function splits the input data into male and female groups based on
        the population and pattern data provided. It can operate using either a
        "rate" or "logodds" model for splitting. If the `sex_id` in a row is
        already for a specific sex (male or female), the row's values are
        inherited and not split. Otherwise, the function splits the row's data
        and calculates corresponding standard errors.

        Parameters
        ----------
        data : pd.DataFrame
            The input data to be split. This should include information on the
            observed total value (`self.data.val`) and the associated standard
            error (`self.data.val_sd`), as well as sex identifiers.
        pattern : pd.DataFrame
            The pattern data used to inform the split. This includes pattern
            values (`self.pattern.val`) and associated standard errors
            (`self.pattern.val_sd`) which will be applied during the split.
        population : pd.DataFrame
            Population data that provides the population sizes for male and
            female groups. This is required for proportional splitting of the
            data.
        model : {"rate", "logodds"}, optional
            The model to be used for splitting the data, by default "rate".
            - "rate": Splits based on a multiplicative rate model.
            - "logodds": Splits based on a log-odds model.
        output_type : {"rate", "count"}, optional
            The type of output expected, by default "rate".
            - "rate": The output will be normalized by population.
            - "count": The output will not be normalized by population.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the following columns:
            - `sex_split_result`: The split value for male and female groups.
            - `sex_split_result_se`: The standard error associated with the
            split values.
            - `sex_split`: A flag (0 or 1) indicating whether the row was split.
        """

        # Ensure no prefixes in the pattern config
        if self.pattern.prefix_status == "prefixed":
            self.pattern.remove_prefix()
        if self.population.prefix_status == "prefixed":
            self.population.remove_prefix()

        # Parsing input data, pattern, and population
        data = self.parse_data(data)
        data = self.parse_pattern(data, pattern, model)
        data = self.parse_population(data, population)

        # Determine whether to normalize by population for the output type
        pop_normalize = output_type == "rate"

        # Step 1: Handle rows where `sex_id` is already `sex_m` or `sex_f`
        mask_sex_m_or_f = data[self.population.sex].isin(
            [self.population.sex_m, self.population.sex_f]
        )

        # Create a copy for the final DataFrame where rows are not split
        final_df = data[mask_sex_m_or_f].copy()

        # Set the `sex_split_result`, `sex_split_result_se`, and `sex_split` columns for non-split rows
        final_df["sex_split_result"] = final_df[self.data.val]
        final_df["sex_split_result_se"] = final_df[self.data.val_sd]
        final_df["sex_split"] = 0  # Mark as not split

        # Step 2: Handle rows that need to be split (where `sex_id` is not `sex_m` or `sex_f`)
        split_data = data[~mask_sex_m_or_f].copy()

        # Calculate input patterns and splitting logic
        if model == "rate":
            input_patterns = np.vstack(
                [np.ones(len(split_data)), split_data[self.pattern.val].values]
            ).T
            splitting_model = RateMultiplicativeModel()
        elif model == "logodds":
            input_patterns = np.vstack(
                [
                    0.5 * np.ones(len(split_data)),
                    expit(split_data[self.pattern.val].values),
                ]
            ).T
            splitting_model = LogOddsModel()

        # Perform the split for all rows at once using vectorized operations
        split_results, SEs = zip(
            *split_data.apply(
                lambda row: split_datapoint(
                    observed_total=row[self.data.val],
                    bucket_populations=np.array([row["m_pop"], row["f_pop"]]),
                    rate_pattern=input_patterns[
                        split_data.index.get_loc(row.name)
                    ],
                    model=splitting_model,
                    output_type=output_type,
                    normalize_pop_for_average_type_obs=pop_normalize,
                    observed_total_se=row[self.data.val_sd],
                    pattern_covariance=np.diag(
                        [0, row[self.pattern.val_sd] ** 2]
                    ),
                ),
                axis=1,
            )
        )

        # Split results into male and female values
        split_results = np.array(split_results)
        SEs = np.array(SEs)

        # Step 3: Create male and female dataframes for the split data
        male_split_data = split_data.copy()
        male_split_data[self.population.sex] = self.population.sex_m
        male_split_data["sex_split_result"] = split_results[:, 0]
        male_split_data["sex_split_result_se"] = SEs[:, 0]
        male_split_data["sex_split"] = 1

        female_split_data = split_data.copy()
        female_split_data[self.population.sex] = self.population.sex_f
        female_split_data["sex_split_result"] = split_results[:, 1]
        female_split_data["sex_split_result_se"] = SEs[:, 1]
        female_split_data["sex_split"] = 1

        # Step 4: Combine the non-split rows and the split male/female rows
        final_split_df = pd.concat(
            [final_df, male_split_data, female_split_data], ignore_index=True
        )

        # Reindex columns
        final_split_df = final_split_df.reindex(
            columns=self.data.index
            + [
                col
                for col in final_split_df.columns
                if col not in self.data.index
            ]
        )

        # Clean up any prefixes added earlier
        self.pattern.remove_prefix()
        self.population.remove_prefix()

        return final_split_df
