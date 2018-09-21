from db_queries import get_population
from db_queries import get_location_metadata
import pandas as pd


def tidy_split(df, column, sep='|', keep=False):
    """
    Split the values of a column and expand so the new DataFrame has one split
    value per row. Filters rows where the column is missing.

    Params
    ------
    df : pandas.DataFrame
        dataframe with the column to split and expand
    column : str
        the column to split and expand
    sep : str
        the string used to split the column's values
    keep : bool
        whether to retain the presplit value as it's own row

    Returns
    -------
    pandas.DataFrame
        Returns a dataframe with the same columns as `df`.
        Source : http://stackoverflow.com/a/39946744
    """
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df



df = pd.read_csv('FILEPATH')
no_split = df[df['population_split'] != 'Yes']

# only keep locations we need to population split
df = df[df['population_split'] == 'Yes']
df = df.rename(index=str, columns={'iso'				 : 'iso3', 
									'start_date_y_3'	 :'year_id', 
									'populations_tosplit':'split_locs',
									'total_deaths'		 :'numkilled'})
df.loc[~(df['split_locs'] == 'National'), 'needs_agg'] = 1

# get all location_ids, parent_ids, and iso3 for estimate locations
locations = get_location_metadata(location_set_id=35)

# locations = locations[locations['is_estimate'] == 1]
keep_cols = ['location_id', 'ihme_loc_id', 'path_to_top_parent', 'is_estimate', 'most_detailed']
locations = locations[keep_cols]

# make lists of lowest level estimate locs for each nation. Store them in a dictionary with
# the relevant nation as key. The dictionary is mostly for reference.
nation_iso_list = df.iso3.unique().tolist()
nation_iso_list.append('SSD')
nat_to_lowest = {}
for nation in nation_iso_list:
	subs = locations.loc[(locations['most_detailed'] == 1) &
						 (locations['is_estimate']   == 1) &
						 (locations['ihme_loc_id'].str.contains(nation)), 'ihme_loc_id'].unique().tolist()
	nat_to_lowest[nation] = subs
	# replace 'national' with the relevant list
	df.loc[(df['split_locs'] == 'National') &
		   (df['iso3'] == nation), 'split_locs'] = ', '.join(subs)

pop_locs = [x for sublist in nat_to_lowest.values() for x in sublist] + nation_iso_list

# list of GBR_4749 children, aka UTLHAs (utlas for short...)
utlas = locations.loc[(locations.path_to_top_parent.str.contains('4749')) & 
                      (locations['most_detailed'] == 1) &
                      (locations['is_estimate']   == 1),  'ihme_loc_id'].unique().tolist()
utlas_and_wales = ['GBR_4636'] + utlas
df.loc[(df['split_locs'] == 'GBR_4636, GBR_4749'), 'split_locs'] = ', '.join(utlas_and_wales)
# want pops for subnats above and SDN and SSD
ids = locations.loc[locations['ihme_loc_id'].isin(pop_locs), 'location_id'].unique().tolist()


# get pops for locations we want & make special aggregates
pops = get_population(location_id=ids, year_id=-1, status='recent')

special_aggs = {'sudan'		:['SDN', 'SSD'],
				'bra_subset':['BRA_4750', 'BRA_4752', 'BRA_4771', 'BRA_4763', 'BRA_4753', 'BRA_4770', 'BRA_4776', 'BRA_4759', 'BRA_4767', 'BRA_4754', 'BRA_4755', 'BRA_4769', 'BRA_4764', 'BRA_4766', 'BRA_4751', 'BRA_4773'],
				'gbr_subset': utlas_and_wales,
				'zaf_subset':['ZAF_487', 'ZAF_486', 'ZAF_484', 'ZAF_488', 'ZAF_485']
}


# make new rows for each subnational using convenience function above. Need to reindex, as they're
# duplicated by the tidy_split.
df = tidy_split(df, 'split_locs', sep=', ', keep=True)
df['parent_iso3'] = df['iso3']
df = df.reset_index()
df.loc[~(df['split_locs'].str.contains(',')), 'iso3'] = df['split_locs']

# add location_ids to df
df = pd.merge(df, 
			  locations, 
			  how='left',
			  left_on='iso3',
			  right_on='ihme_loc_id')

# add populations to df
assert pops[['location_id', 'year_id']].duplicated().sum() == 0
df = pd.merge(df, 
			  pops, 
			  how='left', 
			  on= ['location_id', 'year_id'])
# slim down our df
df = df[['cause', 'iso3', 'year_id', 'location_id', 'parent_iso3', 'numkilled', 'population', 'split_locs', 'needs_agg']]

# make special population aggregates
pops = pd.merge(pops, locations, how='left', on='location_id')
pops = pops[['location_id', 'year_id', 'sex_id', 'population', 'ihme_loc_id']]
aggs = pops.copy()
aggs['iso3'] = ''
aggs['split_locs'] = ''
for k, v in special_aggs.items():
	aggs.loc[aggs['ihme_loc_id'].isin(v), 'iso3'] = k

aggs = aggs[~(aggs['iso3'] == '')]
for k, v in special_aggs.items():
	aggs.loc[aggs['iso3'] == k, 'split_locs'] = ', '.join(v)

aggs = aggs.groupby(['iso3', 'year_id', 'split_locs'])['population'].sum().reset_index()
aggs = aggs.rename(index=str, columns={'population': 'agg_pop'})
aggs['needs_agg'] = 1

# merge for all subnats
just_pops = df[['iso3', 'year_id', 'population']]
just_pops = just_pops.rename(index=str, columns={'population':'national_pop'})
df = pd.merge(df, 
			  just_pops, 
			  how='left', 
			  left_on=['parent_iso3', 'year_id'], 
			  right_on=['iso3', 'year_id'])

# merge for special aggs
df = df.drop('iso3_y',1)
df = df.rename(index=str, columns={'iso3_x':'iso3'})
for k, v in special_aggs.items():
	df.loc[(df['iso3'].isin(v)) & (df['needs_agg'] == 1), 'parent_iso3'] = k

df = pd.merge(df,
			  aggs,
			  how='left',
			  left_on=['parent_iso3', 'year_id', 'needs_agg'],
			  right_on=['iso3', 'year_id', 'needs_agg'])

df.loc[df['agg_pop'].notnull(), 'national_pop'] = df['agg_pop']
df['weight'] = df['population'] / df['national_pop']
df['numkilled'] = df['numkilled'] * df['weight']
df = df.drop_duplicates()

# collapse and save
df = df.drop('iso3_y',1)
df = df.rename(index=str, columns={'iso3_x':'iso3'})
df = df[~df.split_locs_x.str.contains(",")]             # remove original rows, we only want ones with distributed deaths.
df = df[['cause', 'iso3', 'year_id', 'location_id', 'numkilled']]
df['source'] = 'Gideon'
df = df[df.iso3.str.contains('SSD|SDN|_')]              # this might be redundant now...
df = df.groupby(['iso3', 'year_id', 'cause', 'location_id', 'source']).sum().reset_index()

df.to_csv('FILEPATH')
no_split.to_csv('FILEPATH')

print("All done!")

# merge with self to get parent pops associated with subnationals,
# then compute weight = sub_pop / national_pop
# and multiply deaths by weight to get distributed deaths.
