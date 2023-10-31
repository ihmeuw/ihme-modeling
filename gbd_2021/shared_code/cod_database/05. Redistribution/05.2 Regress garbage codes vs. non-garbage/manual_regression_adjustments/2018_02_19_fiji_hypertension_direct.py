import pandas as pd
import numpy as np
from RegressionModifier import RegressionModifier

class FijiRtiRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        fiji_weights = prop_df.loc[
            prop_df['wgt_group_name'].str.contains('Fiji')]

        #read in old weights
        new_weights = pd.read_excel(
            "{}/extracted_weights.xlsx".format(self.work_dir))

        #formatting column names
        new_weights['Sex'] = new_weights['Sex'].str.strip(' ')
        new_weights['Age'] = new_weights['Age'].str.strip(' ')
        new_weights['Country'] = new_weights['Country'].str.strip(' ')
        new_weights['wgt_group_name'] = new_weights['Country'] + ', ' + new_weights['Sex'] + ', ' + new_weights['Age']
        new_weights = new_weights.drop(['Age', 'Country', 'Sex'], axis=1)

        # reshape long by target code and adjusting for weight groups that don't sum to 1
        new_weights = new_weights.set_index('wgt_group_name').unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'target_codes', 0: 'new_wgt'})
        
        new_weights['wgt_total'] = new_weights.groupby('wgt_group_name')['new_wgt'].transform('sum')
        new_weights['new_wgt'] = new_weights['new_wgt'] / new_weights['wgt_total']
        new_weights = new_weights.drop(['wgt_total'], axis=1)

        # make sure new weights and old weights have the same weight group
        # names and target codes
        assert set(fiji_weights['wgt_group_name']) == set(
            new_weights['wgt_group_name'])
        assert set(fiji_weights['target_codes']) == set(
            new_weights['target_codes'])

        # merge new weights onto old weights and assert that there are none
        # missing
        fiji_weights = fiji_weights.merge(new_weights, how='left')
        assert fiji_weights['wgt'].notnull().values.all()

        fiji_weights = fiji_weights.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # make sure everything sums to 1
        assert np.allclose(
            fiji_weights.groupby('wgt_group_name').wgt.sum(), 1)
        
        return fiji_weights


if __name__ == "__main__":
    fiji_mod = FijiRtiRegressionModifier(2982, "2018_02_19_fiji_hypertension_direct")
    fiji_mod.generate_new_proportions()
    fiji_mod.update_proportions_in_engine_room()