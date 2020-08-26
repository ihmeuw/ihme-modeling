import numpy as np
import pandas as pd
from db_queries import get_location_metadata


def get_regions_dict(location_set_id=21, gbd_round_id=6):
    regions_df = (get_location_metadata(location_set_id=location_set_id,
                                        gbd_round_id=gbd_round_id)
                  .loc[:, ['location_id', 'region_id']])
    regions_dict = dict(zip(regions_df['location_id'], regions_df['region_id']))
    return regions_dict

def fill_war_cis(war_df):
    ci_template = war_df.loc[((war_df['source_id'] == "4") | (war_df['source_id'] == "7")) &
                             (war_df['best'].notnull()) &
                             (war_df['high'].notnull()) &
                             (war_df['low'].notnull()),
                             ['location_id', 'best', 'low', 'high']]
    ci_template = ci_template.loc[(ci_template['low'] < ci_template['best']) &
                                  (ci_template['high'] > ci_template['best'])]
    regions_dict = get_regions_dict()
    ci_template['region_id'] = ci_template['location_id'].map(regions_dict)
    war_df['region_id'] = war_df['location_id'].map(regions_dict)
    region_avg = (ci_template.loc[:, ['region_id', 'best', 'high', 'low']]
                  .groupby(by=['region_id'])
                  .mean()
                  .reset_index(drop=False))
    region_avg['hi_best_ratio'] = region_avg['high'] / region_avg['best']
    region_avg['lo_best_ratio'] = region_avg['low'] / region_avg['best']
    region_avg = region_avg[['region_id', 'hi_best_ratio', 'lo_best_ratio']]
    war_df = pd.merge(left=war_df,
                      right=region_avg,
                      on=['region_id'],
                      how='left')
    war_df['high_est'] = war_df['best'] * war_df['hi_best_ratio']
    war_df['low_est'] = war_df['best'] * war_df['lo_best_ratio']
    war_df.loc[war_df['low'].isnull(), 'low'] = war_df.loc[war_df['low'].isnull(), 'low_est']
    war_df.loc[war_df['high'].isnull(), 'high'] = war_df.loc[war_df['high'].isnull(), 'high_est']
    war_df = war_df.drop(labels=['low_est', 'high_est', 'hi_best_ratio', 'lo_best_ratio'],
                         axis=1, errors='ignore')
    return (war_df, region_avg)


def fill_disaster_cis(disaster_df, cis):
    regions_dict = get_regions_dict()
    disaster_df['region_id'] = disaster_df['location_id'].map(regions_dict)
    disaster_df = pd.merge(left=disaster_df,
                           right=cis,
                           on=['region_id'],
                           how='left')
    disaster_df['high_est'] = disaster_df['best'] * disaster_df['hi_best_ratio']
    disaster_df['low_est'] = disaster_df['best'] * disaster_df['lo_best_ratio']
    disaster_df.loc[disaster_df['low'].isnull(),
                    'low'] = disaster_df.loc[disaster_df['low'].isnull(), 'low_est']
    disaster_df.loc[disaster_df['high'].isnull(),
                    'high'] = disaster_df.loc[disaster_df['high'].isnull(), 'high_est']
    disaster_df = disaster_df.drop(labels=['low_est', 'high_est', 'hi_best_ratio',
                                           'lo_best_ratio', 'region_id'],
                                   axis=1, errors='ignore')
    return disaster_df


def generate_upper_lower(df):

    df.loc[((df['low'] == 0) &
            (df['best'] != 0)
            ), 'low'] = np.nan
    df.loc[(df['low'] > df['high']), 'low'] = np.nan
    df.loc[(df['low'] > df['best']), 'low'] = np.nan
    df.loc[(df['high'] < df['best']), 'high'] = np.nan

    war_causes = [724,  
                  855,  
                  851,  
                  854,  
                  945]  
    war_df = df.loc[df['cause_id'].isin(war_causes), :]
    disaster_df = df.loc[~df['cause_id'].isin(war_causes), :]
    (war_df, ci_ratio_standard) = fill_war_cis(war_df)
    disaster_df = fill_disaster_cis(disaster_df, cis=ci_ratio_standard)
    filled = pd.concat([war_df, disaster_df])
    filled.loc[filled.low.isnull(), "low"] = filled.loc[filled.low.isnull(), "best"] * .9
    filled.loc[filled.high.isnull(), "high"] = filled.loc[filled.high.isnull(), "best"] * 1.1
    for col in ['best', 'high', 'low']:
        filled = filled.loc[filled[col].notnull(), :]
        filled.loc[filled[col] < 0, col] = 0
    filled = filled.rename(columns={'best': 'val', 'low': 'lower', 'high': 'upper'})
    return filled
