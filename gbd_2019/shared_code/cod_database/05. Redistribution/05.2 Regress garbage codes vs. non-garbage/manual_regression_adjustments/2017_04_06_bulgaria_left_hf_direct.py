import pandas as pd
import RegressionModifier


class BulgariaLeftHfRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        # restrict the old proportions to weight group names that have brazil
        # in the name
        df = prop_df.loc[
            prop_df['wgt_group_name'].str.contains("Bulgaria")]
        new_weights = pd.read_excel(
            "{}/extracted_weights.xlsx".format(self.work_dir))

        # reshape long
        new_weights = new_weights.set_index("target_codes").unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'wgt_group_name', 0: 'new_wgt'})
        # heart failure redistribution to iron deficiency anemia is two-staged
        new_weights.loc[
            new_weights['target_codes'] == "nutrition_iron",
            'target_codes'] = "reg_gc_left_hf_anemia"

        # make sure that the new weights and the brazil weights match on weight
        # group name and target codes
        assert set(df['wgt_group_name']) == set(
            new_weights['wgt_group_name'])
        assert set(df['target_codes']) == set(
            new_weights['target_codes'])

        # merge the new weights in and verify all weights have a match
        df = df.merge(new_weights, how='left')
        assert df['wgt'].notnull().values.all()

        # swap the new weight with the old
        df = df.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # set the new props attribute, restricting to columns we need to upload
        return df


if __name__ == "__main__":
    bulg_mod = BulgariaLeftHfRegressionModifier(
        2547, "2017_04_06_bulgaria_left_hf_direct")
    bulg_mod.generate_new_proportions()
    bulg_mod.update_proportions_in_engine_room()