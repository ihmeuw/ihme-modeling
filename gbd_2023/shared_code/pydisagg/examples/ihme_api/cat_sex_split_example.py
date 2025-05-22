import pandas as pd
import numpy as np

# Import CatSplitter and configurations from your module
from pydisagg.ihme.splitter import (
    CatSplitter,
    CatDataConfig,
    CatPatternConfig,
    CatPopulationConfig,
)

# Set a random seed for reproducibility
np.random.seed(42)

# -------------------------------
# 1. Create and Update data_df
# -------------------------------

# Existing data_df DataFrame
data_df = pd.DataFrame(
    {
        "seq": [303284043, 303284062, 303284063, 303284064, 303284065],
        "location_id": [78, 130, 120, 30, 141],
        "mean": [0.5] * 5,
        "standard_error": [0.1] * 5,
        "year_id": [2015, 2019, 2018, 2017, 2016],
    }
)

# Adding the 'sex' column with a list [1, 2] for each row
data_df["sex"] = [[1, 2]] * len(data_df)

# Sort data_df for clarity
data_df_sorted = data_df.sort_values(by=["location_id"]).reset_index(drop=True)

# Display the sorted data_df
print("data_df:")
print(data_df_sorted)

# -------------------------------
# 2. Create and Update pattern_df_final
# -------------------------------

pattern_df = pd.DataFrame(
    {
        "location_id": [78, 130, 120, 30, 141],
        "mean": [0.5] * 5,
        "standard_error": [0.1] * 5,
        "year_id": [2015, 2019, 2018, 2017, 2016],
    }
)

# Create DataFrame for sex=1
pattern_df_sex1 = pattern_df.copy()
pattern_df_sex1["sex"] = 1  # Assign sex=1
pattern_df_sex1["mean"] += np.random.normal(0, 0.01, size=len(pattern_df_sex1))
pattern_df_sex1["standard_error"] += np.random.normal(
    0, 0.001, size=len(pattern_df_sex1)
)
pattern_df_sex1["mean"] = pattern_df_sex1["mean"].round(6)
pattern_df_sex1["standard_error"] = pattern_df_sex1["standard_error"].round(6)

# Create DataFrame for sex=2
pattern_df_sex2 = pattern_df.copy()
pattern_df_sex2["sex"] = 2  # Assign sex=2
pattern_df_sex2["mean"] += np.random.normal(0, 0.01, size=len(pattern_df_sex2))
pattern_df_sex2["standard_error"] += np.random.normal(
    0, 0.001, size=len(pattern_df_sex2)
)
pattern_df_sex2["mean"] = pattern_df_sex2["mean"].round(6)
pattern_df_sex2["standard_error"] = pattern_df_sex2["standard_error"].round(6)

pattern_df_final = pd.concat(
    [pattern_df_sex1, pattern_df_sex2], ignore_index=True
)

# Sort pattern_df_final for clarity
pattern_df_final_sorted = pattern_df_final.sort_values(
    by=["location_id", "sex"]
).reset_index(drop=True)

print("\npattern_df_final:")
print(pattern_df_final_sorted)

# -------------------------------
# 3. Create and Update population_df
# -------------------------------

population_df = pd.DataFrame(
    {
        "location_id": [30, 30, 78, 78, 120, 120, 130, 130, 141, 141],
        "year_id": [2017] * 2
        + [2015] * 2
        + [2018] * 2
        + [2019] * 2
        + [2016] * 2,
        "sex": [1, 2] * 5,  # Sexes 1 and 2
        "population": [
            39789,
            40120,
            10234,
            10230,
            30245,
            29870,
            19876,
            19980,
            50234,
            49850,
        ],
    }
)

# Sort population_df for clarity
population_df_sorted = population_df.sort_values(
    by=["location_id", "sex"]
).reset_index(drop=True)

# Display the sorted population_df
print("\npopulation_df:")
print(population_df_sorted)

# -------------------------------
# 4. Configure and Run CatSplitter
# -------------------------------

# Data configuration
data_config = CatDataConfig(
    index=[
        "seq",
        "location_id",
        "year_id",
        "sex",
    ],  # Include 'sex' in the index
    cat_group="sex",
    val="mean",
    val_sd="standard_error",
)

# Pattern configuration
pattern_config = CatPatternConfig(
    by=["location_id", "year_id"],
    cat="sex",
    val="mean",
    val_sd="standard_error",
)

# Population configuration
population_config = CatPopulationConfig(
    index=["location_id", "year_id", "sex"],  # Include 'sex' in the index
    val="population",
)

# Initialize the CatSplitter
splitter = CatSplitter(
    data=data_config, pattern=pattern_config, population=population_config
)

# Perform the split
try:
    final_split_df = splitter.split(
        data=data_df,
        pattern=pattern_df_final,
        population=population_df,
        model="rate",
        output_type="rate",
    )

    # Sort the final DataFrame by 'seq' and then by 'sex'
    final_split_df.sort_values(by=["seq", "sex"], inplace=True)

    print("\nFinal Split DataFrame:")
    print(final_split_df)
except Exception as e:
    print(f"Error during splitting: {e}")
