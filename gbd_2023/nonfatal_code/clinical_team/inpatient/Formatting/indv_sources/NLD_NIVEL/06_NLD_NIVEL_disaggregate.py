import pandas as pd

from inpatient.Formatting.indv_sources.NLD_NIVEL import constants_NLD_NIVEL

adjusted_data = pd.read_csv(constants_NLD_NIVEL.adjusted_path)
intermediate_data = pd.read_csv(constants_NLD_NIVEL.intermediate_path)
unadjusted_data = pd.read_csv(constants_NLD_NIVEL.unadjusted_path)

# Rename the 'cases' column
adjusted_data.rename(columns={"cases": "adjusted_cases"}, inplace=True)
intermediate_data.rename(columns={"cases": "intermediate_cases"}, inplace=True)
unadjusted_data.rename(columns={"cases": "unadjusted_cases"}, inplace=True)

# Combine adjusted and intermediate data
join_columns = [
    "cause_code",
    "code_name",
    "measure",
    "age_group_id",
    "age_start",
    "age_end",
    "year",
    "sex",
    "sample_size",
]
df = pd.merge(adjusted_data, intermediate_data, on=join_columns, how="left")

# Subset unadjusted_data to rows with unique combinations not found in df
unique_unadjusted_data = (
    pd.merge(unadjusted_data, df, how="left", indicator=True)
    .query('_merge == "left_only"')
    .drop(columns=["_merge"])
)

unique_unadjusted_data.drop(columns=["adjusted_cases", "intermediate_cases"], inplace=True)

unique_unadjusted_data.rename(columns={"age_group_id": "age_group_id_granular"}, inplace=True)

# Step 1: Define a function that maps the granular ID to its group ID based on the dictionary


def map_age_group_id(granular_id):
    for group_id, granular_ids in constants_NLD_NIVEL.age_group_id_mappings.items():
        if granular_id in granular_ids:
            return group_id
    return None  # Return None or a default value if no matching group ID is found


# Step 2: Use the .apply() method on the 'age_group_id_granular' column
# to create the new 'age_group_id' column
unique_unadjusted_data["age_group_id"] = unique_unadjusted_data["age_group_id_granular"].apply(
    map_age_group_id
)

# Step 3: Merge expanded_df with unique_unadjusted_data
result_df = df.merge(
    unique_unadjusted_data,
    on=["age_group_id", "cause_code", "code_name", "measure", "year", "sex"],
    how="left",
)

# Ensure that 'adjusted_cases' and 'intermediate_cases' are not zero to avoid division by zero
valid_cases = result_df[
    (result_df["adjusted_cases"] > 0) & (result_df["intermediate_cases"] > 0)
]

# Calculate the ratio of 'adjusted_cases' to 'intermediate_cases'
valid_cases["ratio"] = valid_cases["adjusted_cases"] / valid_cases["intermediate_cases"]

# Multiply 'unadjusted_cases' by the ratio where 'unadjusted_cases'
# is not null/NaN and ratio is valid.
# Using .loc to avoid SettingWithCopyWarning and ensure we are
# modifying the DataFrame as intended.
result_df.loc[valid_cases.index, "unadjusted_cases"] = (
    valid_cases["unadjusted_cases"] * valid_cases["ratio"]
)

# Check if 'unadjusted_cases' is not null before attempting to replace values
result_df.loc[pd.notna(result_df["unadjusted_cases"]), "age_group_id"] = result_df[
    "age_group_id_granular"
]
result_df.loc[pd.notna(result_df["unadjusted_cases"]), "age_start_x"] = result_df[
    "age_start_y"
]
result_df.loc[pd.notna(result_df["unadjusted_cases"]), "age_end_x"] = result_df["age_end_y"]
result_df.loc[pd.notna(result_df["unadjusted_cases"]), "adjusted_cases"] = result_df[
    "unadjusted_cases"
]
result_df.loc[pd.notna(result_df["unadjusted_cases"]), "sample_size_x"] = result_df[
    "sample_size_y"
]

# After replacing, drop columns that are no longer needed.
columns_to_drop = [
    "intermediate_cases",
    "unadjusted_cases",
    "age_group_id_granular",
    "age_start_y",
    "age_end_y",
    "sample_size_y",
]
result_df.drop(columns=columns_to_drop, inplace=True)

result_df.rename(columns={"age_start_x": "age_start"}, inplace=True)
result_df.rename(columns={"age_end_x": "age_end"}, inplace=True)
result_df.rename(columns={"sample_size_x": "sample_size"}, inplace=True)
result_df.rename(columns={"adjusted_cases": "cases"}, inplace=True)

result_df.to_csv(
    f"{constants_NLD_NIVEL.filedir}/prepped_outputs"
    "/NLD_NIVEL_PCD_IHME_long-format_adjusted-disaggregated.csv",
    index=False,
)
