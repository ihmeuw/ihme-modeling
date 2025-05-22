# cat_split_example.py

import numpy as np
import pandas as pd
from pandas import DataFrame

# Import CatSplitter and related classes
from pydisagg.ihme.splitter import (
    CatSplitter,
    CatDataConfig,
    CatPatternConfig,
    CatPopulationConfig,
)

# -------------------------------
# Example DataFrames
# -------------------------------

# Set a random seed for reproducibility
np.random.seed(42)

# Pre-split DataFrame with 3 rows
pre_split = pd.DataFrame(
    {
        "study_id": np.random.randint(1000, 9999, size=3),  # Unique study IDs
        "year_id": [2010, 2010, 2010],
        "location_id": [
            [1234, 1235, 1236],  # List of location_ids for row 1
            [2345, 2346, 2347],  # List of location_ids for row 2
            [3456],  # Single location_id for row 3 (no need to split)
        ],
        "mean": [0.2, 0.3, 0.4],
        "std_err": [0.01, 0.02, 0.03],
    }
)

# Create a list of all location_ids mentioned
all_location_ids = [
    1234,
    1235,
    1236,
    2345,
    2346,
    2347,
    3456,
    4567,
    5678,  # Additional location_ids
]

# Pattern DataFrame for all location_ids
data_pattern = pd.DataFrame(
    {
        "year_id": [2010] * len(all_location_ids),
        "location_id": all_location_ids,
        "mean": np.random.uniform(0.1, 0.5, len(all_location_ids)),
        "std_err": np.random.uniform(0.01, 0.05, len(all_location_ids)),
    }
)

# Population DataFrame for all location_ids
data_pop = pd.DataFrame(
    {
        "year_id": [2010] * len(all_location_ids),
        "location_id": all_location_ids,
        "population": np.random.randint(10000, 1000000, len(all_location_ids)),
    }
)

# Print the DataFrames
print("Pre-split DataFrame:")
print(pre_split)
print("\nPattern DataFrame:")
print(data_pattern)
print("\nPopulation DataFrame:")
print(data_pop)

# -------------------------------
# Configurations
# -------------------------------

# Adjusted configurations to match the modified CatSplitter
data_config = CatDataConfig(
    index=[
        "study_id",
        "year_id",
        "location_id",
    ],  # Include 'location_id' in the index
    cat_group="location_id",
    val="mean",
    val_sd="std_err",
)

pattern_config = CatPatternConfig(
    by=["year_id"],
    cat="location_id",
    val="mean",
    val_sd="std_err",
)

population_config = CatPopulationConfig(
    index=["year_id", "location_id"],
    val="population",
)

# Initialize the CatSplitter with the updated configurations
splitter = CatSplitter(
    data=data_config,
    pattern=pattern_config,
    population=population_config,
)

# Perform the split
try:
    final_split_df = splitter.split(
        data=pre_split,
        pattern=data_pattern,
        population=data_pop,
        model="rate",
        output_type="rate",
    )
    # Sort the final DataFrame for better readability
    final_split_df.sort_values(by=["study_id", "location_id"], inplace=True)
    print("\nFinal Split DataFrame:")
    print(final_split_df)
except Exception as e:
    print(f"Error during splitting: {e}")
