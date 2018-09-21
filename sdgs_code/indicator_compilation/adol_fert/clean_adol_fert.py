import pandas as pd
import sys
import os

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

def agg_fertility(fert_df, pop_df):
    # aggregate
    assert set(fert_df.age_group_id.values) == {7, 8}, 'incorrect ages present'
    df = fert_df.merge(pop_df, how='left')
    assert len(df.loc[df.population.isnull()]) == 0, 'problem with merge'
    df['births'] = df['asfr'] * df['population']
    df['age_group_id'] = 162
    df = df.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id', 'draw'], as_index=False)['births', 'population'].sum()
    df['fertility'] = df['births'] / df['population']

    # reshape wide
    df['draw'] = 'draw_' + df['draw'].astype(str)
    df['measure_id'] = 19
    df['metric_id'] = 3
    df = pd.pivot_table(df,
                        values='fertility',
                        index=['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'metric_id'],
                        columns='draw')
    df = df.reset_index()
    return df[['location_id', 'year_id', 'age_group_id', 'sex_id', 'measure_id', 'metric_id'] + dw.DRAW_COLS]

# get locations
locsdf = qry.get_sdg_reporting_locations()

# read past
print 'prepping past file...'
past_df = pd.read_csv(dw.ADOL_FERT_PAST_FILE)
past_df = past_df.loc[past_df.location_id.isin(locsdf.location_id.values)]

pop_df = qry.get_pops()
pop_df = pop_df.loc[(pop_df.age_group_id.isin([7, 8])) & \
                    (pop_df.sex_id == 2)]
past_df = agg_fertility(past_df, pop_df)

print 'writing...'
try:
    if not os.path.exists(dw.ADOL_FERT_DIR):
        os.makedirs(dw.ADOL_FERT_DIR)
except OSError:
    pass
past_df.to_hdf("{d}/asfr_clean.h5".format(d=dw.ADOL_FERT_DIR),
          key="data", format="table",
          data_columns=['location_id', 'year_id'])

print 'done'
