import pandas as pd
import numpy as np
import sys
from RegressionModifier import RegressionModifier


class X59RegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        has_60 = prop_df['wgt_group_name'].str.contains("60-74")
        has_75 = prop_df['wgt_group_name'].str.contains("75+")
        df = prop_df.loc[has_60 | has_75]

        # clean up the ages so we can reference directly
        df['wgname_clean'] = df['wgt_group_name'].str.replace(
            "Virgin Islands,", "Virgin Islands")
        df['age'] = df['wgname_clean'].apply(lambda x: x.split(',')[2].strip())

        # find the causes to zero out in age 60-74 & age 75+
        cause_ages = pd.read_excel(
            "{}/cause_ages_to_zero_out.xlsx".format(
                x59_mod.fetch_path_to_work_dir()
            )
        )
        causes_60 = set(
            cause_ages.loc[cause_ages['restriction'] == "over 60", 'acause'])
        causes_75 = set(
            cause_ages.loc[cause_ages['restriction'] == "over 75", 'acause'])
        causes_75 = causes_60.union(causes_75)

        df.loc[(df['target_codes'].isin(causes_60)) &
               (df['age'] == "60-74"), 'wgt'] = 0

        df.loc[(df['target_codes'].isin(causes_75)) &
               (df['age'] == "75+"), 'wgt'] = 0

        # rescale weights
        df['wgt_sum'] = df.groupby(['wgt_group_name'])['wgt'].transform(np.sum)
        df['wgt'] = df['wgt'] / df['wgt_sum']

        # make sure everything sums to 1
        assert np.allclose(df.groupby('wgt_group_name').wgt.sum(), 1)

        return df


if __name__ == "__main__":
    work_folder = "2017_04_20_x59_bad_old_ladies"
    x59_mod = X59RegressionModifier(2559, work_folder)
    x59_mod.generate_new_proportions()
    x59_mod.update_proportions_in_engine_room()