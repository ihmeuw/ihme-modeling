import pandas as pd
import numpy as np
from RegressionModifier import RegressionModifier


class BrazilunsptrnsprtRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        brazil_weights = prop_df.loc[
            prop_df['wgt_group_name'].str.contains('Brazil')]

        # read in old weights
        new_weights = pd.read_excel(
            "{}/extracted_weights.xlsx".format(self.work_dir))

        # reshape long by target code
        new_weights = new_weights.set_index('wgt_group_name').unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'target_codes', 0: 'new_wgt'})

        # make sure new weights and old weights have the same weight group
        # names and target codes
        assert set(brazil_weights['wgt_group_name']) == set(
            new_weights['wgt_group_name'])
        assert set(brazil_weights['target_codes']) == set(
            new_weights['target_codes'])

        # merge new weights onto old weights and assert that there are none
        # missing
        brazil_weights = brazil_weights.merge(new_weights, how='left')
        assert brazil_weights['wgt'].notnull().values.all()

        brazil_weights = brazil_weights.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # make sure everything sums to 1
        assert np.allclose(
            brazil_weights.groupby('wgt_group_name').wgt.sum(), 1)

        return brazil_weights


if __name__ == "__main__":
    version_id = 2563
    brazil_mod = BrazilunsptrnsprtRegressionModifier(
        version_id,
        '2017_05_11_brazil_unspecified_transport_2_direct')
    brazil_mod.generate_new_proportions()

    assert version_id != 23
    brazil_mod.update_proportions_in_engine_room()
