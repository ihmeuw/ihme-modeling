import numpy as np
import pandas as pd

from inpatient.Formatting.indv_sources.NLD_NIVEL import constants_NLD_NIVEL


def read_data(year_str: str, denom_str: str, desc_str: str) -> pd.DataFrame:
    """Reads in the sheet identified by the "year_str" year string,
    as well as the sheets containing identifying data into separate
    dataframes.

    Args:
        year_str (str): Year string identifying year of data to be processed.
        denom_str (str): String identifying sheet containing population denominators.
        desc_str (str): String identifying sheet containing cause code descriptions.

    Returns:
        pd.DataFrame: Current year string's data.
        pd.DataFrame: Population denominators.
        pd.DataFrame: Cause code descriptions.
    """
    df = pd.read_excel(constants_NLD_NIVEL.filepath, sheet_name=year_str)
    df_denom = pd.read_excel(constants_NLD_NIVEL.filepath, sheet_name=denom_str)
    df_desc = pd.read_excel(constants_NLD_NIVEL.filepath, sheet_name=desc_str)

    return df, df_denom, df_desc


def format_columns(df: pd.DataFrame, df_desc: pd.DataFrame) -> pd.DataFrame:
    """Creates the cause code description column, and moves the I/P
    from the end of the cause code to its own column. Additionally
    reads in the cause/age population denominators.

    Args:
        df (pd.DataFrame): Current year's data to be processed.
        df_desc (pd.DataFrame): Dataframe containing cause code descriptions.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    # Create a measure column with the last character of the ICPC code values.
    df["measure"] = df["ICPC"].str[-1]

    # Convert the measure column's characters to "incidence" and "prevalence".
    df["measure"] = df["measure"].replace({"I": "incidence", "P": "prevalence"})

    # Remove the last two characters from the ICPC column:
    df["ICPC"] = df["ICPC"].str[:-2]

    # Remove the third column from the DataFrame
    df_desc.drop(df_desc.columns[2], axis=1, inplace=True)

    # Merge 'df' with 'df_desc' based on matching ICPC codes, retaining all rows from 'df'
    df = pd.merge(
        df,
        df_desc[["ICPC code", "Description"]],
        left_on="ICPC",
        right_on="ICPC code",
        how="left",
    )

    # Now that the Description values are in your main df,
    # drop the 'ICPC code' column since it's no longer needed
    df.drop("ICPC code", axis=1, inplace=True)

    # Rearranging columns
    cols = df.columns.tolist()  # Get a list of all columns
    new_cols_order = (
        cols[:1] + cols[-2:] + cols[1:-2]
    )  # New order: first column, last two columns, then the rest

    # Reorder the dataframe
    df = df[new_cols_order]

    # Convert "sex" from "M"/"F" to integers
    df.rename(columns={"sex ": "sex"}, inplace=True)
    df["sex"] = df["sex"].map({"M": 1, "F": 2})

    return df


def format_denom(df_denom: pd.DataFrame):
    """Formats the population denominator dataframe in preparation
    for the value calculations.

    Args:
        df_denom (pd.DataFrame): Dataframe containing population denominators.
    """
    # Format the sex column
    df_denom.rename(columns={"sex ": "sex"}, inplace=True)
    df_denom["sex"] = df_denom["sex"].map({"M": 1, "F": 2})
    df_denom["sex"] = df_denom["sex"].astype(int)

    # Format the age group column
    df_denom.rename(columns={"age category": "age"}, inplace=True)
    df_denom["age"] = df_denom["age"].str.replace("-", "_", regex=False)
    df_denom["age"] = df_denom["age"].str.replace(r" years$", "", regex=True)
    df_denom["age"] = df_denom["age"].str.replace(r" years and older$", "_plus", regex=True)

    # Remove duplicate age bin rows from denominator sheet
    df_denom = df_denom.drop_duplicates().reset_index(drop=True)

    # Subset denominator sheet to male and female
    df_denom_male = df_denom[df_denom["sex"] == 1]
    df_denom_female = df_denom[df_denom["sex"] == 2]

    return df_denom, df_denom_male, df_denom_female


def format_values(df: pd.DataFrame):
    """Formats column names and cell data for processing.

    Args:
        df (pd.DataFrame): Current year's data to be processed.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    df.rename(columns=lambda x: x.replace("age_", ""), inplace=True)
    df.rename(columns=lambda x: x.replace("_up", "_plus"), inplace=True)

    df = df.sort_values(by=["sex"], kind="stable").reset_index(drop=True)

    # Convert all NaN values to 0.0 floats
    df.fillna(0.0, inplace=True)

    # Function to check and convert cells containing "<" to -1.0
    def convert_cell(cell):
        if isinstance(cell, str) and "<" in cell:
            return -1.0
        return cell

    # Apply the conversion function to all cells in the DataFrame
    df = df.applymap(convert_cell)

    return df


def format_gender(df: pd.DataFrame):
    """Separates out male and female data for processing. Creates
    distinct dataframes for data and descriptive columns.

    Args:
        df (pd.DataFrame): Current year's data to be processed.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    df_male = df[df["sex"] == 1].reset_index(drop=True)
    df_female = df[df["sex"] == 2].reset_index(drop=True)

    # Drop first five columns for calculations
    df_male_values = df_male.iloc[:, 5:]
    df_female_values = df_female.iloc[:, 5:]

    # Save the first five columns for re-adding
    df_male = df_male.iloc[:, :5]
    df_female = df_female.iloc[:, :5]

    return df_male, df_female, df_male_values, df_female_values


def calc_values(
    df_male: pd.DataFrame,
    df_female: pd.DataFrame,
    df_male_values: pd.DataFrame,
    df_female_values: pd.DataFrame,
    df_denom_male: pd.DataFrame,
    df_denom_female: pd.DataFrame,
    year_str: str,
):
    """Formats the population denominator dataframe and merges its
    data onto the current year's dataframe.

    Args:
        df (pd.DataFrame): Current year's data to be processed.
        df_denom (pd.DataFrame): Dataframe containing population denominators.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    df_denom_male.set_index("age", inplace=True)
    df_denom_female.set_index("age", inplace=True)

    df_male_values = df_male_values.mul(df_denom_male[int(year_str)], axis=1)
    df_female_values = df_female_values.mul(df_denom_female[int(year_str)], axis=1)

    df_male_values = df_male_values / 1000
    df_female_values = df_female_values / 1000

    # Round values to whole numbers
    df_male_values = df_male_values.round(0)
    df_female_values = df_female_values.round(0)

    def replace_values(x):
        if x < 0:
            return "<0.1"
        elif x == 0:
            return np.nan
        else:
            return x

    df_male_values = df_male_values.applymap(replace_values)
    df_female_values = df_female_values.applymap(replace_values)

    df_male = pd.merge(df_male, df_male_values, left_index=True, right_index=True, how="left")
    df_female = pd.merge(
        df_female, df_female_values, left_index=True, right_index=True, how="left"
    )

    df = pd.concat([df_male, df_female], ignore_index=True)

    # df_values = df_values / 1000

    return df


def format_ip_spec(df: pd.DataFrame):
    """Merges on incidnce/prevalence adjustment and sex-specificity
    data from description sheet.

    Args:
        df (pd.DataFrame): Current year's data to be processed.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    # Re-read unmodified description sheet
    df_desc = pd.read_excel(
        constants_NLD_NIVEL.filepath, sheet_name=constants_NLD_NIVEL.desc_str
    )

    # Standardize and subset columns
    df_desc.rename(columns={"ICPC code": "ICPC"}, inplace=True)
    df_desc.rename(columns={"I or P": "i_p"}, inplace=True)
    df_desc.rename(columns={"sex-specific?": "sex_spec"}, inplace=True)

    df_desc = df_desc[["ICPC", "i_p", "sex_spec"]]

    # Drop cause codes that reappear in residual categories
    df_desc.drop_duplicates(inplace=True)

    # Standardize I/P and sex specification values
    df_desc["i_p"] = df_desc["i_p"].map({"I": 1, "P": 2, "P-I/2": 3, "I&P": 3})

    df_desc["sex_spec"] = df_desc["sex_spec"].map({"M": 1, "F": 2, "B": 3})

    # Combine filtered sheet with current year's data
    df = pd.merge(df, df_desc[["ICPC", "i_p"]], left_on="ICPC", right_on="ICPC", how="left")

    # Prepare data for aggregation by replacing strings
    df["0_4"] = df["0_4"].replace({"<0.1": 0})

    for bin in constants_NLD_NIVEL.wider_bins_columns:
        df[bin] = df[bin].replace({"<0.1": 0})

    for bin in constants_NLD_NIVEL.narrower_bins_columns:
        df[bin] = df[bin].replace({"<0.1": 0})

    return df


def calc_partial_groups(df: pd.DataFrame):
    """Creates a temporary 50_59 age bin for disaggregating wide bins
    where only half their granular bins existed in raw data (e.g.
    Dementia).

    Args:
        df (pd.DataFrame): Current year's data to be processed.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    df["50_59"] = np.nan

    mask = pd.Series([False] * len(df), index=df.index)

    for group in constants_NLD_NIVEL.age_bin_groups:
        # For each group, calculate the number of non-NA values per row
        non_na_count = df[group].notnull().sum(axis=1)

        # A group is partially populated if its non-NA count is between 1
        # and the length of the group minus 1
        group_mask = (non_na_count > 0) & (non_na_count < len(group))

        # Update the overall mask to include these conditions
        mask = mask | group_mask

    df.loc[mask, "50_59"] = df.loc[mask, "50_69"] - (
        df.loc[mask, "60_64"] + df.loc[mask, "65_69"]
    )

    # Use the mask to filter the DataFrame
    return df, mask


def adjust_values_based_on_population(df: pd.DataFrame, df_denom: pd.DataFrame):
    """Disaggregates the temporary 50_59 age bin based on the populations
    values in the denominator sheet. Adjusts prevalence on these values
    since this occurs after the prevalence adjustments.

    Args:
        df (pd.DataFrame): Current year's data to be processed.
        df_denom (pd.DataFrame): Dataframe containing population denominators.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    # Assuming the 'year' column exists and each DataFrame only contains one unique year value
    year = df["year"].iloc[
        0
    ]  # This retrieves the first 'year' value, which should be consistent across the DataFrame

    # Ensure the year is formatted as an integer, then convert to string to match the
    # population DataFrame column names
    year_column = int(
        year
    )  # Convert the year to an integer first to remove any decimal, then to a string

    # For each sex, calculate the new values
    for sex in [1, 2]:
        # Extract population data for the current sex
        df_pop_sex = df_denom[df_denom["sex"] == sex]

        # Retrieve population values for the specific year
        # Ensure to use .get() or check if the column exists to avoid KeyError
        if year_column in df_pop_sex.columns:
            pop_50_54 = (
                df_pop_sex[df_pop_sex["age"] == "50_54"][year_column].values[0]
                if "50_54" in df_pop_sex["age"].values
                else 0
            )
            pop_55_59 = (
                df_pop_sex[df_pop_sex["age"] == "55_59"][year_column].values[0]
                if "55_59" in df_pop_sex["age"].values
                else 0
            )

            total_pop_50_59 = pop_50_54 + pop_55_59
            if total_pop_50_59 > 0:  # To avoid division by zero
                proportion_50_54 = pop_50_54 / total_pop_50_59
                proportion_55_59 = pop_55_59 / total_pop_50_59

                # Adjust the DataFrame
                for index, row in df[df["sex"] == sex].iterrows():
                    if not pd.isnull(row["50_59"]):
                        df.at[index, "50_54"] = row["50_59"] * proportion_50_54
                        df.at[index, "55_59"] = row["50_59"] * proportion_55_59

                        # Clear '50_69' for affected rows
                        if "50_69" in df.columns:
                            df.at[index, "50_69"] = np.nan

                    if pd.isnull(row["50_59"]) and row["ICPC"] == "P70":
                        index_list = df.index.tolist()
                        current_position = index_list.index(index)
                        previous_index = index_list[current_position - 1]

                        incidence_50_54 = df.at[previous_index, "50_54"]
                        incidence_55_59 = df.at[previous_index, "55_59"]

                        # Check existence and calculate the adjusted prevalence
                        if "50_54" in df.columns and not pd.isnull(incidence_50_54):
                            df.at[index, "50_54"] = row["50_54"] - 0.5 * incidence_50_54
                        if "55_59" in df.columns and not pd.isnull(incidence_55_59):
                            df.at[index, "55_59"] = row["55_59"] - 0.5 * incidence_55_59

        else:
            print(f"Year column '{year_column}' not found in population data.")

    return df


def unadjusted_values_based_on_population(df: pd.DataFrame, df_denom: pd.DataFrame):
    """Disaggregates the temporary 50_59 age bin based on the populations
    values in the denominator sheet. Doesn't adjust prevalence on these values
    since this occurs in the intermediate file harmonized for Dementia.

    Args:
        df (pd.DataFrame): Current year's data to be processed.
        df_denom (pd.DataFrame): Dataframe containing population denominators.

    Returns:
        pd.DataFrame: Current year's formatted data.
    """
    # Assuming the 'year' column exists and each DataFrame only contains one unique year value
    year = df["year"].iloc[
        0
    ]  # This retrieves the first 'year' value, which should be consistent across the DataFrame

    # Ensure the year is formatted as an integer, then convert to string to match the
    # population DataFrame column names
    year_column = int(
        year
    )  # Convert the year to an integer first to remove any decimal, then to a string

    # For each sex, calculate the new values
    for sex in [1, 2]:
        # Extract population data for the current sex
        df_pop_sex = df_denom[df_denom["sex"] == sex]

        # Retrieve population values for the specific year
        # Ensure to use .get() or check if the column exists to avoid KeyError
        if year_column in df_pop_sex.columns:
            pop_50_54 = (
                df_pop_sex[df_pop_sex["age"] == "50_54"][year_column].values[0]
                if "50_54" in df_pop_sex["age"].values
                else 0
            )
            pop_55_59 = (
                df_pop_sex[df_pop_sex["age"] == "55_59"][year_column].values[0]
                if "55_59" in df_pop_sex["age"].values
                else 0
            )

            total_pop_50_59 = pop_50_54 + pop_55_59
            if total_pop_50_59 > 0:  # To avoid division by zero
                proportion_50_54 = pop_50_54 / total_pop_50_59
                proportion_55_59 = pop_55_59 / total_pop_50_59

                # Adjust the DataFrame
                for index, row in df[df["sex"] == sex].iterrows():
                    if not pd.isnull(row["50_59"]):
                        df.at[index, "50_54"] = row["50_59"] * proportion_50_54
                        df.at[index, "55_59"] = row["50_59"] * proportion_55_59

                        # Clear '50_69' for affected rows
                        if "50_69" in df.columns:
                            df.at[index, "50_69"] = np.nan

        else:
            print(f"Year column '{year_column}' not found in population data.")

    return df
