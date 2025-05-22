import pandas as pd

from inpatient.Formatting.indv_sources.NLD_NIVEL import (
    constants_NLD_NIVEL,
    functions_NLD_NIVEL,
)


def process_year_data(year_str, denom_str, desc_str):
    # Read the data for the given year
    df, df_denom, df_desc = functions_NLD_NIVEL.read_data(year_str, denom_str, desc_str)

    # Process the data
    df = functions_NLD_NIVEL.format_columns(df, df_desc)
    df_denom, df_denom_male, df_denom_female = functions_NLD_NIVEL.format_denom(df_denom)
    df = functions_NLD_NIVEL.format_values(df)
    df_male, df_female, df_male_values, df_female_values = functions_NLD_NIVEL.format_gender(
        df
    )
    df = functions_NLD_NIVEL.calc_values(
        df_male,
        df_female,
        df_male_values,
        df_female_values,
        df_denom_male,
        df_denom_female,
        year_str,
    )
    df = functions_NLD_NIVEL.format_ip_spec(df)
    df, mask = functions_NLD_NIVEL.calc_partial_groups(df)

    df_final = functions_NLD_NIVEL.unadjusted_values_based_on_population(df, df_denom)

    return df_final, df_denom


# Use Pandas ExcelWriter to write each DataFrame to a separate sheet
with pd.ExcelWriter(
    f"{constants_NLD_NIVEL.filedir}/prepped_outputs"
    "/NLD_NIVEL_PCD_IHME_wide-format_unadjusted.xlsx"
) as writer:
    # Placeholder for storing the df_denom from the last iteration
    last_df_denom = None

    for year_str in constants_NLD_NIVEL.year_strings:
        # Process the data for the current year
        df_year, df_denom = process_year_data(
            year_str, constants_NLD_NIVEL.denom_str, constants_NLD_NIVEL.desc_str
        )

        # Write the processed DataFrame to a new sheet named after the current year
        df_year.to_excel(writer, sheet_name=year_str, index=False)

        # Update last_df_denom for later use
        last_df_denom = df_denom

    # After looping through all years, write the df_denom to its own sheet
    # Assuming df_denom does not change significantly across years,
    # or you're okay with using the last year's version
    if last_df_denom is not None:
        last_df_denom.to_excel(writer, sheet_name="Population Denominators", index=False)
