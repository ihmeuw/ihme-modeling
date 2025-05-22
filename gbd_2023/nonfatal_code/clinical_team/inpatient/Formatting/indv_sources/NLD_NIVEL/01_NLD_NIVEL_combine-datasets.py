import numpy as np
import pandas as pd

from inpatient.Formatting.indv_sources.NLD_NIVEL import constants_NLD_NIVEL

# List of years to iterate over
years = [str(year) for year in range(2011, 2023)]

# Create a Pandas ExcelWriter object with the output file path
with pd.ExcelWriter(constants_NLD_NIVEL.output_file_path, engine="openpyxl") as writer:
    # Loop through each year
    for year in years:
        # Read the specific yearly sheet from each file
        df1 = pd.read_excel(constants_NLD_NIVEL.file_path1, sheet_name=year)
        df2 = pd.read_excel(constants_NLD_NIVEL.file_path2, sheet_name=year)

        # Creating a mask to identify rows with at least one NaN value in the specified columns
        rows_with_some_missing = (
            df2[constants_NLD_NIVEL.specified_age_bins].isnull().any(axis=1)
        )

        # Creating another mask to identify rows that do not have all values missing
        # (i.e., at least one non-NaN value)
        rows_not_all_missing = (
            df2[constants_NLD_NIVEL.specified_age_bins].notnull().any(axis=1)
        )

        # Combining the masks to identify rows with some but not all values missing
        rows_with_partial_values = df2[rows_with_some_missing & rows_not_all_missing]

        # Identify all columns that start with 'age_'
        age_columns = [col for col in df2.columns if col.startswith("age_")]

        # Identifying rows with some missing values in 'age_...' columns
        rows_with_some_missing = df2[age_columns].isnull().any(axis=1) & df2[
            age_columns
        ].notnull().any(axis=1)

        # For each of the age-related columns
        for column in age_columns:
            # Use .loc with rows_with_some_missing to target specific rows and column
            # Replace NaNs in those rows of df2 with values from df1, matching by index
            df2.loc[rows_with_some_missing, column] = df2.loc[
                rows_with_some_missing, column
            ].fillna(df1[column])

        # Iterate over each group
        for group in constants_NLD_NIVEL.raw_age_bin_groups:
            # Check if all values in a group are not NA (i.e., the group is fully populated)
            fully_populated = df2[group].notnull().all(axis=1)

            # Check if any value in the group is NA (i.e., the group is partially populated)
            partially_populated = df2[group].isnull().any(axis=1)

            # Exclude rows with "P70_I" and "P70_P" in the "ICPC" column
            exclusion_criteria = ~df2["ICPC"].isin(["P70_I", "P70_P"])

            # Update mask to identify rows where the group is partially populated
            # and do not have "P70_I" or "P70_P" in the "ICPC" column
            mask = partially_populated & ~fully_populated & exclusion_criteria

            # For rows where the group is partially populated, set values to NA
            for column in group:
                df2.loc[mask, column] = np.nan

        # For each wider age bin, check if all corresponding narrower bins are fully populated
        for wider_bin, narrower_bins in constants_NLD_NIVEL.raw_age_bin_mapping.items():
            # Check if all values in the narrower age bins are not NA (i.e., fully populated)
            fully_populated = df2[narrower_bins].notnull().all(axis=1)

            # For rows where the narrower age bins are fully populated,
            # set the wider bin value to NA
            df2.loc[fully_populated, wider_bin] = np.nan

        df2.to_excel(writer, sheet_name=year, index=False)

    df_denom = pd.read_excel(
        constants_NLD_NIVEL.file_path1, sheet_name="denominators 2011-2022"
    )
    df_desc = pd.read_excel(constants_NLD_NIVEL.file_path3, sheet_name="selection Nivel")

    df_denom.to_excel(writer, sheet_name="denominators 2011-2022", index=False)
    df_desc.to_excel(writer, sheet_name="selection Nivel", index=False)
