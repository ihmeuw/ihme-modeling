
# Imports (general)
import argparse
import numpy as np
import os
import sys
import pandas as pd
from os.path import join
from time import sleep
import getpass
# Imports (this folder)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities.create_tracker import create_tracker
sys.path.append('FILEPATH'.format(getpass.getuser()))
from cod_prep.downloaders.locations import get_current_location_hierarchy
from db_queries import (get_location_metadata as lm,
                        get_population as get_pops)



def korean_war_split_sides_override(df):
    korean_war_years = [1950,1951,1952,1953]
    new_df = df.query('side_a != "North Korea;;China, Russia (Soviet Union)"')
    war_df = df.query('side_a == "North Korea;;China, Russia (Soviet Union)"')
    to_add_df = pd.DataFrame()

    for year in korean_war_years:
        df1 = war_df[war_df['year'] == year]
        PRK_low = df1[df1['location_id'] == 7]['best'].min()
        KOR_low = df1[df1['location_id'] == 68]['best'].min() 
        PRK_max = df1[df1['location_id'] == 7]['best'].max()
        KOR_max = df1[df1['location_id'] == 68]['best'].max() 
        df1_PRK = df1[(df1['best'] == PRK_low) & (df1['location_id'] == 7)]
        df1_KOR = df1[(df1['best'] == KOR_low) & (df1['location_id'] == 68)] 
        
        prk = df1[(df1['best'] == PRK_max) & (df1['location_id'] == 7)]
        kor = df1[(df1['best'] != KOR_low) & (df1['location_id'] == 68)]
        
        
        kor_percent = 748218 / 1807839.0
        prk_percent = 1059621 / 1807839.0
        
        rus_deaths = 350 /4.0
        chn_deaths = 110000/4.0
        us_deaths = 33642.0/4.0 
        uk_deaths = 1078 /4.0
        nzl_deaths = 33/8.0 
        turk_deaths = 717/4.0
        can_deaths = 291/4.0
        fra_deaths = 288/4.0
        aus_deaths = 281/4.0
        greece_deaths = 169/4.0
        col_deaths = 140/4.0
        eth_deaths = 120/4.0
        neth_deaths = 111/4.0
        thailand_deaths = 114/4.0
        bel_deaths = 97/4.0
        phil_deaths = 92/4.0
        total_a = (rus_deaths + chn_deaths)
        total_b = (us_deaths + uk_deaths + nzl_deaths + turk_deaths + can_deaths + fra_deaths + aus_deaths +
                   greece_deaths + col_deaths + eth_deaths + neth_deaths + nzl_deaths + thailand_deaths + bel_deaths + phil_deaths)

        total_korean_deaths_best = df1['best'].sum() - total_a - total_b
        total_korean_deaths_high = df1['high'].sum() - total_a - total_b
        total_korean_deaths_low = df1['low'].sum() - total_a - total_b
        
        kor['best'] = total_korean_deaths_best * kor_percent
        kor['high'] = total_korean_deaths_high * kor_percent
        kor['low'] = total_korean_deaths_low * kor_percent
        prk['best'] = total_korean_deaths_best * prk_percent
        prk['high'] = total_korean_deaths_high * prk_percent
        prk['low'] = total_korean_deaths_low * prk_percent
        
        df1 = prk.append(kor)
        to_add_df = to_add_df.append(df1)
        
        PRK_split = pd.DataFrame()
        df1_PRK.drop(['high','low'], axis =1, inplace=True)
        #North Korea side split
        side_a = ["Russian Federation"]
        locs = get_current_location_hierarchy()
        side_a = pd.DataFrame(data = side_a, columns = {"location_name"})
        side_a = pd.merge(side_a, locs[['location_name','location_id']],how='left')
        side_a = list(locs[locs['parent_id'].isin(side_a['location_id'])]['location_id'])

        df1_PRK['best'] = rus_deaths/(len(side_a))
        for loc_id in side_a:
            df1_PRK['location_id'] = loc_id
            PRK_split = PRK_split.append(df1_PRK)
        
        side_a = ["China"]
        locs = get_current_location_hierarchy()
        side_a = pd.DataFrame(data = side_a, columns = {"location_name"})
        side_a = pd.merge(side_a, locs[['location_name','location_id']],how='left')
        side_a = list(locs[locs['parent_id'].isin(side_a['location_id'])]['location_id'])
        
        pop = pop = get_pops(location_id = side_a, year_id = year)
        pop['percent'] = pop['population'] / pop['population'].sum()

        
        for loc_id in side_a:
            df1_PRK['best'] = chn_deaths * pop.query("location_id == {}".format(loc_id))['percent'].iloc[0]
            df1_PRK['location_id'] = loc_id
            PRK_split = PRK_split.append(df1_PRK)
        to_add_df = to_add_df.append(PRK_split)

        
        side_b_us = ['United States']
        side_b_us = pd.DataFrame(data = side_b_us, columns = {"location_name"})
        side_b_us = pd.merge(side_b_us, locs[['location_name','location_id']],how='left')
        side_b_us = list(locs[locs['parent_id'].isin(side_b_us['location_id'])]['location_id'])

        side_b_uk = ['United Kingdom']
        side_b_uk = pd.DataFrame(data = side_b_uk, columns = {"location_name"})
        side_b_uk = pd.merge(side_b_uk, locs[['location_name','location_id']],how='left')
        side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk['location_id'])]['location_id'])
        side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk)]['location_id'])
        side_b_uk = list(locs[locs['parent_id'].isin(side_b_uk)]['location_id']) #goes 3 levels deep for children

        KOR_split = pd.DataFrame()
        df1_KOR.drop(['high','low'], axis =1, inplace=True)
        #PHIL
        df1_KOR['best'] = phil_deaths
        df1_KOR['location_id'] = 16
        KOR_split = KOR_split.append(df1_KOR)
        
        #BELGIUM
        df1_KOR['best'] = bel_deaths
        df1_KOR['location_id'] = 76
        KOR_split = KOR_split.append(df1_KOR)
        
        #THAILAND
        df1_KOR['best'] = thailand_deaths
        df1_KOR['location_id'] = 18
        KOR_split = KOR_split.append(df1_KOR)
        
        #NETHERLANDS
        df1_KOR['best'] = neth_deaths
        df1_KOR['location_id'] = 89
        KOR_split = KOR_split.append(df1_KOR)
        
        #ETHIOPIA
        df1_KOR['best'] = eth_deaths
        df1_KOR['location_id'] = 179
        KOR_split = KOR_split.append(df1_KOR)
        
        #COLOMBIA
        df1_KOR['best'] = col_deaths
        df1_KOR['location_id'] = 125
        KOR_split = KOR_split.append(df1_KOR)
        
        #GREECE
        df1_KOR['best'] = greece_deaths
        df1_KOR['location_id'] = 82
        KOR_split = KOR_split.append(df1_KOR)
        
        #AUSTRALIA
        df1_KOR['best'] = aus_deaths
        df1_KOR['location_id'] = 71
        KOR_split = KOR_split.append(df1_KOR)
        
        #FRANCE
        df1_KOR['best'] = fra_deaths
        df1_KOR['location_id'] = 80
        KOR_split = KOR_split.append(df1_KOR)
        
        #CANADA
        df1_KOR['best'] = can_deaths
        df1_KOR['location_id'] = 101
        KOR_split = KOR_split.append(df1_KOR)
        
        #Turkey
        df1_KOR['best'] = turk_deaths 
        df1_KOR['location_id'] = 155
        KOR_split = KOR_split.append(df1_KOR)
        
        #NZL - New Zealand Maori population
        df1_KOR['best'] = nzl_deaths 
        df1_KOR['location_id'] = 44850
        KOR_split = KOR_split.append(df1_KOR)
        
        #NZL - New Zealand non-Maori population
        df1_KOR['best'] = nzl_deaths
        df1_KOR['location_id'] = 44851
        KOR_split = KOR_split.append(df1_KOR)
        
        #UNITED KINGDOM
        df1_KOR['best'] = uk_deaths/(len(side_b_uk)) 
        for loc_id in side_b_uk:
            df1_KOR['location_id'] = loc_id
            KOR_split = KOR_split.append(df1_KOR)

        #UNITED STATES    
        df1_KOR['best'] = us_deaths/(len(side_b_us)) 
        df1_KOR['nid'] = 350248 
        for loc_id in side_b_us:
            df1_KOR['location_id'] = loc_id
            KOR_split = KOR_split.append(df1_KOR)
        to_add_df = to_add_df.append(KOR_split)
        
        
    new_df = new_df.append(to_add_df)
    assert int(new_df['best'].sum()) == int(df['best'].sum()) 
    return new_df



def sudan_war_split(df):
    total = round(df['best'].sum())
    final = df.query("location_id != 522 | event_type != 'B1' | year < 1983 | year > 1996")
    sudan = df.query("location_id == 522 & event_type == 'B1' & year >= 1983 & year <= 1996")
    south_sudan = sudan.copy()
    south_sudan['location_id'] = 435
    to_add = sudan.append(south_sudan)
    locations = sorted(to_add['location_id'].unique())
    years = sorted(to_add['year'].unique())
    
    pop = get_pops(location_id=locations, year_id=years, status="recent")
    pop['total_pop'] = pop.groupby(["year_id"])['population'].transform(sum)
    pop['population_rate'] = pop['population'] / pop['total_pop'] 
    pop = pop.rename(columns={'year_id':"year"})
    
    to_add = pd.merge(to_add, pop[['location_id','year','population_rate']], how='left',on=['location_id','year'])
    to_add['best'] = to_add['best'] * to_add['population_rate']
    
    columns = df.columns
    to_add = to_add[columns]
    final = final.append(to_add)
    
    assert round(final['best'].sum()) == total
    return final

if __name__=="__main__":
    # Read input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--infile",type=str,
                        help="The CSV file that needs to be split by sides")
    parser.add_argument("-o","--outfile",type=str,
                        help="The CSV file for model ready after overrides"
                             "saved.")
    parser.add_argument("-n","--encoding",type=str,
                        help="Encoding for all CSV files")
    cmd_args = parser.parse_args()
    # Pull data 
    df = pd.read_csv(cmd_args.infile, encoding=cmd_args.encoding)

    df = korean_war_split_sides_override(df)

    df = sudan_war_split(df)
   
    df.to_csv(cmd_args.outfile,index=False,encoding=cmd_args.encoding)
    