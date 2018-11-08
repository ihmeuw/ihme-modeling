import pandas as pd
import numpy as np

def manually_reallocate_gbdcauses(df):
	#storing incoming deaths to assure totals remain constant after reallocation
	orig_deaths = df.deaths.sum()

	#reading in cleaned prop_df
	path = 'FILEPATH'
	prop_df = pd.read_excel(path)

	#merge prop_df onto existing data frame -- changing code_ids to target and scaling deaths by prop where merge successful
	df = df.merge(prop_df, on = ['sex_id', 'cause_id', 'age_group_id'], how = 'left')

	df.loc[df.target.notnull(), 'code_id'] = df['target']
	assert df.code_id.notnull().all()

	df.loc[df.prop.notnull(), 'deaths'] = df['deaths'] * df['prop']
	assert np.allclose(df.deaths.sum(), orig_deaths)

	#fixing cause_ids where merge successful
	df.loc[df.code_id == 94985, 'cause_id'] = 743
	df.loc[df.code_id == 156952, 'cause_id'] = 532
	df.loc[df.code_id == 156953, 'cause_id'] = 545
	df.loc[df.code_id == 100373, 'cause_id'] = 298

	df.drop(['target', 'prop'], axis = 1, inplace = True)
	assert df.notnull().values.all()

	return df