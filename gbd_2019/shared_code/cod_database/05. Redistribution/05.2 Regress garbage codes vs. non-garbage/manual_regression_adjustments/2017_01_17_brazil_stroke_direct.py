import pandas as pd
import RegressionModifier


class BrazilStrokeRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        # restrict the old proportions to weight group names that have brazil
        # in the name
        brazil_weights = prop_df.loc[
            prop_df['wgt_group_name'].str.contains("Brazil")]
        new_weights = pd.read_excel(
            "{}/extracted_weights.xlsx".format(self.work_dir))

        # reshape long
        new_weights = new_weights.set_index("wgt_group_name").unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'target_codes', 0: 'new_wgt'})

        # make sure that the new weights and the brazil weights match on weight
        # group name and target codes
        assert set(brazil_weights['wgt_group_name']) == set(
            new_weights['wgt_group_name'])
        assert set(brazil_weights['target_codes']) == set(
            new_weights['target_codes'])

        # merge the new weights in and verify all weights have a match
        brazil_weights = brazil_weights.merge(new_weights, how='left')
        assert brazil_weights['wgt'].notnull().values.all()

        # swap the new weight with the old
        brazil_weights = brazil_weights.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # set the new props attribute, restricting to columns we need to upload
        return brazil_weights


if __name__ == "__main__":
    braz_mod = BrazilStrokeRegressionModifier(
        2377, "2017_01_17_brazil_stroke_direct")
    braz_mod.generate_new_proportions()
    braz_mod.update_proportions_in_engine_room()
