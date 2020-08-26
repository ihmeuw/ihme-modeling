import pandas as pd
import RegressionModifier


class ChinaMesoRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        # restrict the old proportions to weight group names that have china
        # in the name
        china_weights = prop_df.loc[
            prop_df['wgt_group_name'].str.contains("China")]
        
        # zero out meso weights
        is_meso = (china_weights['target_codes'] == "neo_meso")

        # multiply china, meso weights by 0
        china_weights['new_wgt'] = china_weights['wgt'] * (1 - is_meso)        
        
        # scale up
        # scaling up china weights where it is non-meso
        china_weights['scale_up'] = 1*(~is_meso)
        
        # sum up the residuals only (scale them up to 1 - sum of fixed weights)
        # fixed weights are the weights for which scale_up == 0
        china_weights['residual_wgt'] = china_weights['new_wgt'] * china_weights['scale_up']
        china_weights['residual_sum'] = china_weights.groupby('wgt_group_name')['residual_wgt'].transform(np.sum)
        # these weights get scaled up to wgt/residual sum so that the sum of new_wgt 
        # where scale_up == 1 is equal to residual_sum
        china_weights.loc[china_weights['scale_up'] == 1, 'new_wgt'] = china_weights['new_wgt'] / china_weights['residual_sum']

        assert np.allclose(china_weights.groupby('wgt_group_name').wgt.sum(), 1)
        assert np.allclose(china_weights.groupby('wgt_group_name').new_wgt.sum(), 1)
        china_weights = china_weights.drop(['scale_up', 'residual_wgt', 'residual_sum'], axis=1)
        # check that no weights are null
        assert china_weights['new_wgt'].notnull().values.all()

        # swap the new weight with the old
        china_weights = china_weights.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # return
        return china_weights


if __name__ == "__main__":
	work_folder = "2017_04_03_china_zero_meso_in_throx"
	china_mod = ChinaMesoRegressionModifier(2546, work_folder)
	china_mod.generate_new_proportions()
   	china_mod.update_proportions_in_engine_room()
