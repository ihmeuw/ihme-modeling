import numpy as np
import pandas as pd

from inpatient.Formatting.indv_sources.NLD_NIVEL import (
    constants_NLD_NIVEL,
    functions_NLD_NIVEL,
)

outpaths = {
    "intermediate": constants_NLD_NIVEL.outpath,
    "harmonized": constants_NLD_NIVEL.outpath2,
}

years = [str(year) for year in range(2011, 2023)]
runtype = ["intermediate", "harmonized"]

for run in runtype:
    outpath_for_current_run = outpaths[run]
    # Create a Pandas ExcelWriter object with the output file path
    with pd.ExcelWriter(outpath_for_current_run, engine="openpyxl") as writer:
        # Loop through each year
        for year in years:

            year_str = year

            # Read the data for the given year
            df, df_denom, df_desc = functions_NLD_NIVEL.read_data(
                year_str, constants_NLD_NIVEL.denom_str, constants_NLD_NIVEL.desc_str
            )

            # Process the data
            df = functions_NLD_NIVEL.format_columns(df, df_desc)
            df_denom, df_denom_male, df_denom_female = functions_NLD_NIVEL.format_denom(
                df_denom
            )
            df = functions_NLD_NIVEL.format_values(df)
            (
                df_male,
                df_female,
                df_male_values,
                df_female_values,
            ) = functions_NLD_NIVEL.format_gender(df)
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

            # Subset to rows which will contain aggregations
            df_subset = df[df["i_p"] == 3].copy()
            df_rest = df[df["i_p"] != 3].copy()

            df_wider = df_subset.dropna(
                subset=constants_NLD_NIVEL.wider_bins_columns, how="all"
            )
            df_narrower = df_subset.dropna(
                subset=constants_NLD_NIVEL.narrower_bins_columns, how="all"
            )

            # Initialize mismatch_details
            mismatch_details = {}

            def check_matching_rows(row, opposite_df):
                """Compares wide and narrow agebin sets to identify
                rows with partial/mismatching sets.

                Args:
                    row (pd.DataFrame): Current row being compared against
                    opposite_df (pd.DataFrame): Matching index row from
                    comparator dataframe.
                """
                # Ensure this line is present to modify the global variable
                global mismatch_details
                matches = opposite_df[
                    (opposite_df["ICPC"] == row["ICPC"]) & (opposite_df["sex"] == row["sex"])
                ]
                for _, match_row in matches.iterrows():
                    if match_row["measure"] != row["measure"]:
                        for (
                            wider_bin,
                            narrower_bins,
                        ) in constants_NLD_NIVEL.age_bin_mappings.items():
                            current_row_has_data_in_bin = not row[narrower_bins].isna().all()
                            matching_row_has_data_in_bin = (
                                not match_row[narrower_bins].isna().all()
                            )
                            if current_row_has_data_in_bin != matching_row_has_data_in_bin:
                                if row.name not in mismatch_details:
                                    mismatch_details[row.name] = []
                                mismatch_details[row.name].append(wider_bin)

            # Apply the checking function
            df_wider.apply(check_matching_rows, args=(df_narrower,), axis=1)
            df_narrower.apply(check_matching_rows, args=(df_wider,), axis=1)

            # Loop over the mappings to update wider bins and replace narrower bins
            # with NaN for identified mismatched rows
            for index, affected_mappings in mismatch_details.items():
                if index in df_subset.index:
                    for wider_bin in affected_mappings:  # Only iterate over affected mappings
                        narrower_bins = constants_NLD_NIVEL.age_bin_mappings[wider_bin]

                        # Check if all values in the narrower bins are NaN
                        if df_subset.loc[index, narrower_bins].isna().all():
                            # Skip to the next wider bin without summing
                            # or modifying narrower bins
                            continue
                        else:
                            # Proceed with the aggregation logic for
                            # the identified narrower bins.
                            # Sum the values for the current index in
                            # the affected narrower bins, ignoring NaNs.
                            sum_narrower_bins = df_subset.loc[index, narrower_bins].sum(
                                min_count=1
                            )
                            existing_value = df_subset.at[index, wider_bin]
                            if not pd.isna(existing_value):
                                df_subset.at[index, wider_bin] += sum_narrower_bins
                            else:
                                df_subset.at[index, wider_bin] = sum_narrower_bins
                            # Replace the values in the narrower bins with NaN
                            # for the current index
                            for narrower_bin in narrower_bins:
                                df_subset.at[index, narrower_bin] = np.nan

            df = pd.concat([df_subset, df_rest]).sort_index()

            if run == "harmonized":
                # Change above to df to run this:
                df = functions_NLD_NIVEL.unadjusted_values_based_on_population(df, df_denom)

            df.to_excel(writer, sheet_name=year_str, index=False)

        df_denom.to_excel(writer, sheet_name="Population Denominators", index=False)
