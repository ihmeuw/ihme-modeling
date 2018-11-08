#==============================================================================
#  02_csmr script calculate CSMR input for First ever {subtype} stroke w CSMR models:
#  1. Get populations for all age groups, sexes, years, and locations 
#  2. Calculate global deaths due to {subtype} stroke using outpuf from step 1
#  and Dismod-generated CSMR
#  3. Calculate {subtype} proportion within all stroke global deaths
#  4. Calculate {subtype} CoDCorrected death rates
#  5. Calculate CSMR from output of step 3 & 4
#==============================================================================
import pandas as pd
import sys
from get_draws.api import get_draws
from db_tools.ezfuncs import query
from db_queries import get_population
from db_queries import get_location_metadata
from elmo import run
from itertools import izip, product
import multiprocessing as mp
import itertools
import stroke_fns

# bring in args
out_dir, year, isch_me, ich_me, sah_me, chronic_me_isch, chronic_me_ich, chronic_me_sah = sys.argv[1:9]

# Check ages
def checkAges(df):
    ages = df.age_group_id.unique()
    if 235 in ages:
        df = df[df.age_group_id.isin(ages2)]
        df[['age_group_id']] = df[['age_group_id']].replace(to_replace=235,value=33)
    elif 33 in ages:
        df = df[df.age_group_id.isin(ages1)]
    return df

#==============================================================================
#  1. Get populations for all age groups, sexes, years, and locations 
#==============================================================================
locations = stroke_fns.get_locations()

all_pop = get_population(location_id=locations,year_id=year,gbd_round_id=5,
            sex_id=[1,2], age_group_id=-1) #-1
ages1 = range(2,21) + [30,31,32,33]
ages2 = range(2,21) + [30,31,32,235]
all_pop = checkAges(all_pop)

#==============================================================================
#  2. Calculate global deaths due to {subtype} stroke using outpuf from step 1
#  and Dismod-generated CSMR
#==============================================================================
def calcGlobalDeaths(params):
    me = params[0]
    #geo = params[1]
    sex = params[1] 
    
    csmr = get_draws('modelable_entity_id', me, 'epi', location_id=locations, year_id=year, sex_id=sex, gbd_round_id=5)
    csmr = csmr[csmr['measure_id']==15]
    csmr = checkAges(csmr)    
    csmr = csmr[all_cols]
    return csmr

#==============================================================================
# 3. Calculate {subtype} proportion within all stroke global deaths
#==============================================================================
def calcGlobalProps(all_df):
    #all_df.drop('population', axis=1, inplace=True)
    all_df_global = all_df.groupby(['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id']).sum().reset_index()
    
    all_df_global = all_df_global[keep_cols+['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id']]
    all_df_global = all_df_global.sort_values(by=['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id'], ascending=False).reset_index(drop=True)
    all_df_global = all_df_global.apply(pd.to_numeric)   
    all_df_global = all_df_global.set_index(['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id'])    
    
    sum_all_df_global = all_df_global.groupby(['sex_id', 'age_group_id', 'cause_id']).sum().reset_index() # 11am
    sum_all_df_global = sum_all_df_global[keep_cols+['sex_id', 'age_group_id', 'cause_id']]
    tmp_me = pd.DataFrame(list(product(sexes, ages1, me_list, cause_list)), columns=['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id'])
    sum_all_df_global = sum_all_df_global.merge(tmp_me, on=['sex_id', 'age_group_id', 'cause_id'], how='inner')
    sum_all_df_global = sum_all_df_global.sort_values(by=['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id'], ascending=False).reset_index(drop=True)
    sum_all_df_global = sum_all_df_global.dropna()
    sum_all_df_global = sum_all_df_global.apply(pd.to_numeric)
    sum_all_df_global = sum_all_df_global.set_index(['sex_id', 'age_group_id', 'modelable_entity_id', 'cause_id'])
    
    all_df_prop = all_df_global / sum_all_df_global
    # all_df_prop = all_df_prop.dropna(subset=['draw_0'], inplace=True)
    all_df_prop = all_df_prop.dropna()
    all_df_prop = all_df_prop.drop_duplicates(keep='first')
    
    # make proportion tableau
    all_df_prop['year_id']=year
    all_df_prop = all_df_prop.apply(pd.to_numeric)  
    all_df_prop.to_csv('%s/stroke_csmr_prop_%s.csv' % (out_dir, year), index=True, encoding='utf-8')
    return all_df_prop

#==============================================================================
#  4. Calculate {subtype} CoDCorrected death rates (latest)
#==============================================================================
def pullDeaths(geo):
    codcorrect = get_draws(['cause_id', 'cause_id', 'cause_id'],[495, 496, 497],'codcorrect',location_id=geo,
                           year_id=year, measure_id=1, gbd_round_id=5,version_id=86, num_workers=2)
    codcorrect = codcorrect[codcorrect.sex_id.isin(sexes)]
    codcorrect = codcorrect[codcorrect['measure_id']==1]
    codcorrect = checkAges(codcorrect)
    return codcorrect
    

#==============================================================================
#  5. Calculate CSMR from output of step 2 & 3
#==============================================================================
def calcFinalCSMR(df1, df2):
    all_csmr = df1.merge(df2, on=['age_group_id','sex_id', 'cause_id', 'year_id'], how='inner')
    for i in range(0, 1000):
        all_csmr['rate_%s' % i] = (all_csmr['draw_%s_x' % i] * all_csmr['draw_%s_y' % i])
    keep_cols = (['rate_%s' % i for i in range(0, 1000)])
    index_cols = ['location_id', 'sex_id', 'age_group_id','year_id']
    new_index_cols = index_cols + ['modelable_entity_id']
    all_cols = keep_cols + new_index_cols
    all_csmr = all_csmr[all_cols].set_index(new_index_cols)
    # get mean, upper, lower
    all_csmr = stroke_fns.get_summary_stats(all_csmr, new_index_cols, 'mean')
    return all_csmr

#==============================================================================
# EXECUTE SCRIPT
#==============================================================================
if __name__ == "__main__":
    # get ready for loop
    keep_cols = (['draw_%s' % i for i in range(0, 1000)])
    index_cols = ['location_id', 'sex_id', 'age_group_id','year_id']
    all_cols = keep_cols + index_cols + ['modelable_entity_id']
    sexes = [1,2]
    
    all_csmr = pd.DataFrame()
    me_list = [isch_me, chronic_me_isch, ich_me, chronic_me_ich, sah_me, chronic_me_sah]
    cause_list = [495, 495, 496, 496, 497, 497]
    paramlist = list(itertools.product(me_list,sexes))
    
    pool = mp.Pool(processes=4)
    tmp_csmr = pool.map(calcGlobalDeaths, paramlist)
    all_csmr = all_csmr.append(tmp_csmr)
    print 'Global Deaths calculations complete'
    
    # merge with pop
    all_df = all_csmr.merge(all_pop, on=index_cols, how='inner')
    
    # turn rates to deaths
    for i in range(0, 1000):
        all_df['draw_%s' % i] = (all_df['draw_%s' % i] * all_df['population'])
    
    # add cause_id #add subarachnoid
    all_df.loc[all_df.modelable_entity_id.isin([isch_me, chronic_me_isch]),'cause_id'] = 495
    all_df.loc[all_df.modelable_entity_id.isin([ich_me, chronic_me_ich]), 'cause_id'] = 496
    all_df.loc[all_df.modelable_entity_id.isin([sah_me, chronic_me_sah]), 'cause_id'] = 497
    
    all_df_prop = calcGlobalProps(all_df)
    all_df_prop = all_df_prop.reset_index()
    print 'Global Proportion calculations complete'
    
    # # calculate CoDCorrect death rates
    all_death = pd.DataFrame()
    
    tmp_death = pool.map(pullDeaths, locations)
    all_deaths = all_death.append(tmp_death)
    # merge with pop
    all_death_rates = all_deaths.merge(all_pop, on=index_cols, how='inner')
    
    # turn cod deaths into rates
    for i in range(0, 1000):
        all_death_rates['draw_%s' % i] = (all_death_rates['draw_%s' % i] / all_death_rates['population'])
    all_death_rates.drop('population', axis=1, inplace=True)
    print 'CoD death rates calculations complete'
    all_death_rates = all_death_rates.apply(pd.to_numeric)  
        
    # Calculate final CSMR
    final_df = calcFinalCSMR(all_death_rates, all_df_prop)
    final_df = final_df.reset_index()
    print "Columns in final_df %s" % final_df.columns
            
    #print pool.map(writeCSV, me_list)
    for me in me_list:
        subtype = final_df.loc[final_df['modelable_entity_id'] == int(me)]
        subtype.to_csv('%s/step_2_output_from_%s_%s.csv' % (out_dir, me, year), index=False, encoding='utf-8')