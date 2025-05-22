import pandas as pd

from inpatient.Formatting.indv_sources.NLD_NIVEL import (
    constants_NLD_NIVEL,
    functions_NLD_NIVEL,
)

# Load the Excel file
xls = pd.ExcelFile(constants_NLD_NIVEL.read_path)

last_sheet_name = xls.sheet_names[-1]  # Get the name of the last sheet
df_last_sheet = pd.read_excel(
    xls, sheet_name=last_sheet_name
)  # Read the last sheet into a DataFrame for later use

# Prepare a dictionary to collect DataFrames
dfs_modified = {}

# Process each sheet
for sheet_name in xls.sheet_names[:-1]:
    df = pd.read_excel(xls, sheet_name=sheet_name)

    # Filtering rows with 'i_p' == 3 and either 'incidence' or 'prevalence'
    for icpc in df["ICPC"].unique():
        for sex in df["sex"].unique():
            # Filter for the specific ICPC and sex
            df_specific = df[(df["ICPC"] == icpc) & (df["sex"] == sex)]

            # Split into incidence and prevalence
            df_incidence = df_specific[df_specific["measure"] == "incidence"]
            df_prevalence = df_specific[df_specific["measure"] == "prevalence"]

            # Loop through the age bin columns to perform the calculations
            for col in constants_NLD_NIVEL.all_bins_columns:
                # Check if there are matching rows in both incidence and prevalence DataFrames
                if not df_incidence.empty and not df_prevalence.empty:
                    # Half the incidence value
                    half_incidence_value = df_incidence[col] / 2

                    # Add half the incidence value to the prevalence where 'i_p' == 3
                    df_prevalence.loc[
                        df_prevalence["i_p"] == 3, col
                    ] -= half_incidence_value.values

            # Combine back the modified prevalence and the incidence DataFrames
            df_combined = pd.concat([df_incidence, df_prevalence])

            # Replace the original segment in the DataFrame
            df.update(df_combined)

    # Store the modified DataFrame
    dfs_modified[sheet_name] = df

# Assuming the year_strings list exists and corresponds to the sheets' names or years
for sheet_name, df_sheet in dfs_modified.items():
    # Apply the adjusted function without passing the year as a separate argument
    dfs_modified[sheet_name] = functions_NLD_NIVEL.adjust_values_based_on_population(
        df_sheet, df_last_sheet
    )

# Write the modified DataFrames to a new Excel file
with pd.ExcelWriter(constants_NLD_NIVEL.write_path, engine="openpyxl") as writer:
    for sheet_name, df_sheet in dfs_modified.items():
        df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)
    df_last_sheet.to_excel(writer, sheet_name="Population Denominators", index=False)

print("Process complete! The modified file has been saved.")
