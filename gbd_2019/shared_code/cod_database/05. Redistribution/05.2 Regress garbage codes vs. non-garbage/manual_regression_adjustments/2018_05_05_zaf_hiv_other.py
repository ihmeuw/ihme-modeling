import pandas as pd
import numpy as np
from RegressionModifier import RegressionModifier

class zafRtiRegressionModifier(RegressionModifier):
    def custom_modify_proportions(self, prop_df):
        zaf_weights = prop_df.loc[
                    prop_df['wgt_group_name'].str.contains('Southern Sub-Saharan Africa')]

        # read in old weights / formatting column names
        new_weights = pd.read_excel(
            "{}/extracted_weights.xlsx".format(self.work_dir))
        new_weights['wgt_group_name'] = new_weights['Country'] + ', ' + new_weights['Sex'] + ', ' + new_weights['Age'] + ', ' + new_weights['Year']
        new_weights = new_weights.drop(['Age', 'Country', 'Sex', 'Year'], axis=1)

        # reshape long by target code and adjusting for wgt_groups who don't sum to 1
        new_weights = new_weights.set_index('wgt_group_name').unstack()
        new_weights = new_weights.reset_index().rename(
            columns={'level_0': 'target_codes', 0: 'new_wgt'})

        new_weights['wgt_total'] = new_weights.groupby('wgt_group_name')['new_wgt'].transform('sum')
        new_weights['new_wgt'] = new_weights['new_wgt'] / new_weights['wgt_total']
        new_weights = new_weights.drop(['wgt_total'], axis=1)

        # make sure new weights and old weights have the same weight group
        # names and target codes
        assert set(zaf_weights['wgt_group_name']) == set(
            new_weights['wgt_group_name'])
        assert set(zaf_weights['target_codes']) == set(
            new_weights['target_codes'])

        # merge new weights onto old weights and assert that there are none
        # missing
        zaf_weights = zaf_weights.merge(new_weights, how='left')
        assert zaf_weights['wgt'].notnull().values.all()

        zaf_weights = zaf_weights.rename(
            columns={'wgt': 'old_wgt', 'new_wgt': 'wgt'})

        # make sure everything sums to 1
        assert np.allclose(
            zaf_weights.groupby('wgt_group_name').wgt.sum(), 1)
        
        return zaf_weights

def create_new_props(old):
	new = pd.read_csv("FILEPATH")

	new.drop(['shared_package_wgt_id', 'wgt_old', 'pct_diff', 'Unnamed: 0'], axis = 1, inplace = True)
	new.rename(columns={'wgt_new':'wgt'}, inplace = True)

	out = old.merge(new, on = ['wgt_group_name', 'target_codes'], how = 'left', suffixes = ('_old', '_new'))
	out.loc[out.wgt_new.notnull(), 'wgt_old'] = out.wgt_new
	out.drop('wgt_new', axis = 1, inplace = True)
	out.rename(columns={'wgt_old':'wgt'}, inplace = True)
	assert out.wgt.notnull().all()
	assert out.notnull().values.all()

	#assert same wgt groups
	assert set(old['wgt_group_name']) == set(
	    out['wgt_group_name'])
	assert set(old['target_codes']) == set(
	    out['target_codes'])

	#breaking wgt_group_name into columns
	out['region'] = out['wgt_group_name'].apply(lambda x: x.split(',')[0].strip())
	out['sex'] = out['wgt_group_name'].apply(lambda x: x.split(',')[1].strip())
	out['age'] = out['wgt_group_name'].apply(lambda x: x.split(',')[2].strip())
	out['year_id'] = out['wgt_group_name'].apply(lambda x: x.split(',')[3].strip())
	assert old.notnull().values.all()
	out = out[['wgt_group_name', 'region', 'sex', 'age', 'year_id', 'target_codes', 'wgt']]

	#one more check all the wgt_groups sum to one
	test = out.groupby('wgt_group_name', as_index = False)['wgt'].sum()
	assert test.wgt.apply(lambda x: np.allclose(x, 1)).all()

	return out


def upload(new):
	#2429 is shared_package_id; does not change with updates
	reg_up = RegressionUplaoder(new, 2429, "Update to Southern SSA proportions",
									['region', 'sex', 'age', 'year_id'], package_type_id = 5)
	reg_up.upload_to_db()

	

if __name__ == "__main__":
	#2678 is shared_package_version_id, at the time of upload. It is different now
	zaf_mod = zafRtiRegressionModifier(2678, "2018_05_05_south_africa_hiv_other")
	old = zaf_mod.fetch_old_package_proportions()
	new = create_new_props(old)
	upload(new)