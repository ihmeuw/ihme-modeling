import pandas as pd
import numpy as np
from RegressionModifier import RegressionModifier

class RussiaRtiRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        russia_weights = prop_df.loc[
            prop_df['wgt_group_name'].str.contains('Russia')]

        # read in old weights / formatting column names
        new_weights = pd.read_excel(
            "{}/extracted_weights.xlsx".format(self.work_dir))
        new_weights['wgt_group_name'] = new_weights['Country'] + ', ' + new_weights['Sex'] + ', ' + new_weights['Age']
        new_weights = new_weights.drop(['Age', 'Country', 'Sex'], axis=1)

        # reshape long by target code and adjusting for wgt_groups who don't sum to 1
        new_weights = new_weights.set_index('wgt_group_name').unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'target_codes', 0: 'new_wgt'})

        new_weights['wgt_total'] = new_weights.groupby('wgt_group_name')['new_wgt'].transform('sum')
        new_weights['new_wgt'] = new_weights['new_wgt'] / new_weights['wgt_total']
        new_weights = new_weights.drop(['wgt_total'], axis=1)

        # make sure new weights and old weights have the same weight group
        # names and target codes
        assert set(russia_weights['wgt_group_name']) == set(
            new_weights['wgt_group_name'])
        assert set(russia_weights['target_codes']) == set(
            new_weights['target_codes'])

        # merge new weights onto old weights and assert that there are none
        # missing
        russia_weights = russia_weights.merge(new_weights, how='left')
        assert russia_weights['wgt'].notnull().values.all()

        russia_weights = russia_weights.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})


        # make sure everything sums to 1
        assert np.allclose(
            russia_weights.groupby('wgt_group_name').wgt.sum(), 1)
        
        return russia_weights

if __name__ == "__main__":
	russia_mod = RussiaRtiRegressionModifier(2981, "2018_02_19_russia_stroke_direct")
	russia_mod.generate_new_proportions()
	russia_mod.update_proportions_in_engine_room()