import pandas as pd
import numpy as np

from clinical_info.Upload.uncertainty import Uncertainty


class MaternalRatioError(Exception):
    pass


class MaternalRatioBuilder:
    def __init__(self, logger):
        self.logger = logger

    def ratio_bundles(self, df, ratios):
        self.logger.log(f"building ratios for {ratios}")

        ratio_dfs = []
        for bundle_id, (num_bid, den_bid) in ratios.items():
            # TODO - Don't hardcode these estimates...
            for estimate_id in [7, 8, 9]:
                estimate_mask = df.estimate_id == estimate_id
                num_df = df[(df.bundle_id == num_bid) & estimate_mask]
                den_df = df[(df.bundle_id == den_bid) & estimate_mask]
                self.logger.log(f"Source col present : {'source' in df.columns}")
                tmp_df = create_ratios(
                    bundle_id=bundle_id, numerator_df=num_df, denominator_df=den_df
                )

                self.logger.log(
                    f"{bundle_id}: {num_bid} / {den_bid} "
                    f"- Estimate: {estimate_id}. "
                    f"Total of {len(tmp_df)} rows"
                )

                ratio_dfs.append(tmp_df)

        ratio_df = pd.concat(ratio_dfs)
        del tmp_df
        del ratio_dfs

        # If we don't create any ratio bundles then we can't run the
        # uncerainty code.
        if len(ratio_df) == 0:
            return None

        self.logger.log(f"creating uncertainty for {len(ratio_df)} rows")
        uc = Uncertainty(ratio_df)
        uc.fill()
        ratio_df = uc.df

        return ratio_df


def create_ratios(bundle_id, numerator_df, denominator_df):
    merge_columns = [
        "age_group_id",
        "estimate_id",
        "location_id",
        "measure_id",
        "merged_nid",
        "sex_id",
        "year_end",
        "year_start",
        "representative_id",
    ]

    # Test to see if we have all the required columns
    required_columns = ["mean", "standard_error", "sample_size"] + merge_columns
    missing_columns_num = [
        col for col in required_columns if col not in numerator_df.columns
    ]
    missing_columns_den = [
        col for col in required_columns if col not in denominator_df.columns
    ]
    if missing_columns_num:
        raise MaternalRatioError(
            f"Numerator is missing the following columns: " f"{missing_columns_num}"
        )
    if missing_columns_den:
        raise MaternalRatioError(
            f"Numerator is missing the following columns: " f"{missing_columns_den}"
        )

    # Merge both dataframes
    suffix = ["_num", "_den"]
    ratio_df = pd.merge(numerator_df, denominator_df, on=merge_columns, suffixes=suffix)

    # Test that sample_sizes are the same across age/sex/location/year
    assert (ratio_df.sample_size_num != ratio_df.sample_size_den).sum() == 0, (
        "We expect the sample size to be the same across " "age/sex/loc/year/source"
    )

    # X is the numerator, Y is the denominator
    MEAN_x = ratio_df["mean_num"]
    MEAN_y = ratio_df["mean_den"]
    SE_x = ratio_df["standard_error_num"]
    SE_y = ratio_df["standard_error_den"]

    # Calculate the mean and SE
    ratio = MEAN_x / MEAN_y
    SE = np.sqrt(
        (MEAN_x ** 2 / MEAN_y ** 2)
        * ((SE_x ** 2 / MEAN_x ** 2) + (SE_y ** 2 / MEAN_y ** 2))
    )

    # Copy the numerator/denominator df and replace it's values with
    # ratio estimates
    ratio_df["mean"] = ratio
    ratio_df["standard_error"] = SE
    ratio_df["bundle_id"] = bundle_id
    ratio_df["sample_size"] = ratio_df.sample_size_num

    # Drop all columns with _num or _den.
    cols_to_drop = {
        col for col in ratio_df.columns if col.endswith("_den") or col.endswith("_num")
    }
    ratio_df = ratio_df.drop(columns=cols_to_drop)

    # Add these columns back
    columns = [
        "upper",
        "lower",
        "cases",
        "effective_sample_size",
        "uncertainty_type",
        "uncertainty_type_value",
        "uncertainty_type_id",
    ]
    for col in columns:
        ratio_df[col] = None

    # There are some bad ratios being generated when we divide by zero
    # so we will drop them.
    ratio_df["mean"].replace(np.inf, np.nan, inplace=True)
    ratio_df = ratio_df[~ratio_df["mean"].isnull()]

    return ratio_df
