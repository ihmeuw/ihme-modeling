import pandas as pd

from db_queries import get_cod_data
from db_queries import get_location_metadata

gbd_year = 2019
needed_years = range(1980,(gbd_year+1)) # years used in dataframe

lm = get_location_metadata(location_set_id=22, gbd_round_id=6)

df = get_cod_data(cause_id='618', gbd_round_id=7, decomp_step='step2') # grab the cod data for other hemog
df = df[df['data_type']=='Vital Registration'] # subset cod data to only include VR sources

df = df.merge(lm, on='location_id', how='left')
df = df[df['developed']==u'1']

df = df[['cause_id', 'location_id', 'year', 'age_group_id', 'sex', 'rate']] #subset relevant columns of data
df = df.groupby(['location_id', 'year', 'age_group_id', 'sex']).mean() #take the mean CSMR across location/year/age/sex combos
df = df.reset_index()

pooled_dfs = [] #make an empty list to fill with dataframes of pooled years

'''
loop through each year in the years list, 
1) define the set of years being pooled to this year
2) grab chunks of the df used for pooling
3) take the mean across these years
4) once pooled, update the entry in the year column
5) add the pooled year dataframe to the list
'''
for y in needed_years:
    df1 = df[df['year'].isin(range((y-4), (y+1)))]
    df1 = df1.groupby(['age_group_id', 'sex']).mean()
    df1 = df1.reset_index()
    df1['year']=y
    pooled_dfs.append(df1)
pooled = pd.concat(pooled_dfs) # concatenate all of the pooled dataframes into one big dataframe
pooled.drop('location_id', axis=1, inplace=True)

pooled.rename(columns={'sex': 'sex_id', 'year':'year_id'}, inplace=True) #rename some columns according to IHME conventions

print("Saving...")

pooled.to_stata('FILEPATH', write_index=False) # export to DTA file
