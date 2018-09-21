import pandas as pd
import sys
import stroke_fns
from transmogrifier.draw_ops import get_draws
from db_tools.ezfuncs import query
from db_queries import get_population
from db_queries import get_location_metadata


# bring in args
out_dir, year, isch_me, hem_me, chronic_me_isch, chronic_me_hem = sys.argv[1:7]

# get ready for loop
locations = stroke_fns.get_locations()
keep_cols = (['draw_%s' % i for i in range(0, 1000)])
index_cols = ['location_id', 'sex_id', 'age_group_id','year_id']
all_cols = keep_cols + index_cols
ages1 = range(2,21) + [30,31,32,33]
ages2 = range(2,21) + [30,31,32,235]
# ages1 = [22,27]
# ages2 = [22,27]

all_acute_isch_list = []
all_acute_hem_list = []
all_chronic_isch_list = []
all_chronic_hem_list = []

count = 0
location_count = len(locations)
for geo in locations:
	# get acute isch
	csmr_isch = get_draws('modelable_entity_id', isch_me, 'epi', location_ids=geo,
			year_ids=year, sex_ids=[1,2], gbd_round_id=4)
	csmr_isch = csmr_isch[csmr_isch['measure_id']==15]
	isch_ages = csmr_isch.age_group_id.unique()
	if 235 in isch_ages:
		csmr_isch = csmr_isch[csmr_isch.age_group_id.isin(ages2)]
		csmr_isch[['age_group_id']] = csmr_isch[['age_group_id']].replace(to_replace=235,value=33)
	elif 33 in isch_ages:
		csmr_isch = csmr_isch[csmr_isch.age_group_id.isin(ages1)]
	csmr_isch = csmr_isch[all_cols]
	
	#get acute hem
	csmr_hem = get_draws('modelable_entity_id', hem_me, 'epi', location_ids=geo, 
			year_ids=year, sex_ids=[1,2], gbd_round_id=4)
	csmr_hem = csmr_hem[csmr_hem['measure_id']==15]
	hem_ages = csmr_hem.age_group_id.unique()
	if 235 in hem_ages:
		csmr_hem = csmr_hem[csmr_hem.age_group_id.isin(ages2)]
		csmr_hem[['age_group_id']] = csmr_hem[['age_group_id']].replace(to_replace=235,value=33)
	elif 33 in hem_ages:
		csmr_hem = csmr_hem[csmr_hem.age_group_id.isin(ages1)]
	csmr_hem = csmr_hem[all_cols]
	print csmr_hem.shape
	# get chronic isch
	chronic_isch = get_draws('modelable_entity_id', chronic_me_isch, 'epi', location_ids=geo,
			year_ids=year, sex_ids=[1,2], gbd_round_id=4)
	chronic_isch = chronic_isch[chronic_isch['measure_id']==15]
	chronic_isch_ages = chronic_isch.age_group_id.unique()
	if 235 in chronic_isch_ages:
		chronic_isch = chronic_isch[chronic_isch.age_group_id.isin(ages2)]
		chronic_isch[['age_group_id']] = chronic_isch[['age_group_id']].replace(to_replace=235,value=33)
	elif 33 in chronic_isch_ages:
		chronic_isch = chronic_isch[chronic_isch.age_group_id.isin(ages1)]
	chronic_isch = chronic_isch[all_cols]
	
	# get chronic hem
	chronic_hem = get_draws('modelable_entity_id', chronic_me_hem, 'epi', location_ids=geo, 
			year_ids=year, sex_ids=[1,2], gbd_round_id=4)
	chronic_hem = chronic_hem[chronic_hem['measure_id']==15]
	chronic_hem_ages = chronic_hem.age_group_id.unique()
	if 235 in chronic_hem_ages:
		chronic_hem = chronic_hem[chronic_hem.age_group_id.isin(ages2)]
		chronic_hem[['age_group_id']] = chronic_hem[['age_group_id']].replace(to_replace=235,value=33)
	elif 33 in chronic_hem_ages:
		chronic_hem = chronic_hem[chronic_hem.age_group_id.isin(ages1)]
	chronic_hem = chronic_hem[all_cols]	
	
	count += 1
	print ("loop {} of {} for epi-draws".format(count,location_count))
	print chronic_hem.shape
	# append together all the lcations
	all_acute_isch_list.append(csmr_isch)
	all_acute_hem_list.append(csmr_hem)
	all_chronic_isch_list.append(chronic_isch)
	all_chronic_hem_list.append(chronic_hem)
all_acute_isch = pd.concat(all_acute_isch_list)
all_acute_hem = pd.concat(all_acute_hem_list)
all_chronic_isch = pd.concat(all_chronic_isch_list)
all_chronic_hem = pd.concat(all_chronic_hem_list)

# get populations
all_pop = get_population(location_id=locations,year_id=year,gbd_round_id=4,
			sex_id=[1,2], age_group_id=-1) #-1
pop_ages = all_pop.age_group_id.unique()
if 235 in pop_ages:
	all_pop = all_pop[all_pop.age_group_id.isin(ages2)]
	all_pop[['age_group_id']] = all_pop[['age_group_id']].replace(to_replace=235,value=33)
elif 33 in pop_ages:
	all_pop = all_pop[all_pop.age_group_id.isin(ages1)]

# turn rates to deaths
all_acute_isch = all_acute_isch.merge(all_pop, on=index_cols, how='inner')
all_acute_hem = all_acute_hem.merge(all_pop, on=index_cols, how='inner')
all_chronic_isch = all_chronic_isch.merge(all_pop, on=index_cols, how='inner')
all_chronic_hem = all_chronic_hem.merge(all_pop, on=index_cols, how='inner')

for i in range(0, 1000):
    all_acute_isch['draw_%s' % i] = (all_acute_isch['draw_%s' % i] *
                                     all_acute_isch['population'])
    all_acute_hem['draw_%s' % i] = (all_acute_hem['draw_%s' % i] *
                                    all_acute_hem['population'])
    all_chronic_isch['draw_%s' % i] = (all_chronic_isch['draw_%s' % i] *
                                  all_chronic_isch['population'])
    all_chronic_hem['draw_%s' % i] = (all_chronic_hem['draw_%s' % i] *
                                  all_chronic_hem['population'])
print "all_chronic_hem"								  
print all_chronic_hem.head(n=2)
# collapse to global
all_acute_isch.drop('population', axis=1, inplace=True)
all_acute_hem.drop('population', axis=1, inplace=True)
all_chronic_isch.drop('population', axis=1, inplace=True)
all_chronic_hem.drop('population', axis=1, inplace=True)

all_acute_isch_global = all_acute_isch.groupby(
    ['sex_id', 'age_group_id']).sum().reset_index()
all_acute_hem_global = all_acute_hem.groupby(
    ['sex_id', 'age_group_id']).sum().reset_index()
all_chronic_isch_global = all_chronic_isch.groupby(
    ['sex_id', 'age_group_id']).sum().reset_index()
all_chronic_hem_global = all_chronic_hem.groupby(
    ['sex_id', 'age_group_id']).sum().reset_index()
print "all_chronic_hem_global"	
print all_chronic_hem_global.head(n=2)

all_acute_isch_global.set_index(['sex_id', 'age_group_id'], inplace=True)
all_acute_hem_global.set_index(['sex_id', 'age_group_id'], inplace=True)
all_chronic_isch_global.set_index(['sex_id', 'age_group_id'], inplace=True)
all_chronic_hem_global.set_index(['sex_id', 'age_group_id'], inplace=True)

acute_isch_prop = all_acute_isch_global / (all_acute_isch_global + all_chronic_isch_global)
acute_hem_prop = all_acute_hem_global / (all_acute_hem_global + all_chronic_hem_global)
chronic_isch_prop = all_chronic_isch_global / (all_acute_isch_global + all_chronic_isch_global)
chronic_hem_prop = all_chronic_hem_global / (all_acute_hem_global + all_chronic_hem_global)

acute_isch_prop.drop('location_id', axis=1, inplace=True)
acute_hem_prop.drop('location_id', axis=1, inplace=True)
chronic_isch_prop.drop('location_id', axis=1, inplace=True)
chronic_hem_prop.drop('location_id', axis=1, inplace=True)

acute_isch_prop.drop('year_id', axis=1, inplace=True)
acute_hem_prop.drop('year_id', axis=1, inplace=True)
chronic_isch_prop.drop('year_id', axis=1, inplace=True)
chronic_hem_prop.drop('year_id', axis=1, inplace=True)
acute_isch_prop['cause_id'] = 495
acute_hem_prop['cause_id'] = 496
chronic_isch_prop['cause_id'] = 495
chronic_hem_prop['cause_id'] = 496
print "chronic_hem_prop"
print chronic_hem_prop.head(n=2)

# make tableau from proportion
acute_isch_prop.to_csv('%s/acute_isch_prop_%s.csv' % (out_dir, year),
                    index=True, encoding='utf-8')
acute_hem_prop.to_csv('%s/acute_hem_prop_%s.csv' % (out_dir, year),
                    index=True, encoding='utf-8')
chronic_isch_prop.to_csv('%s/chronic_isch_prop_%s.csv' % (out_dir, year),
                      index=True, encoding='utf-8')
chronic_hem_prop.to_csv('%s/chronic_hem_prop_%s.csv' % (out_dir, year),
                       index=True, encoding='utf-8')


# get codcorrect
codcorrect_list = []

count = 0
location_count = len(locations)
for geo in locations:
    codcorrect = get_draws(['cause_id', 'cause_id'],[495, 496],'codcorrect',location_ids=geo,year_ids=year, measure_ids=1, metric_ids=3, gbd_round_id=4,status='latest')
    print codcorrect.shape
    codcorrect = codcorrect[codcorrect['measure_id']==1]
    #codcorrect = codcorrect[all_cols]
    codcorrect_list.append(codcorrect)
    count += 1
    print ("loop {} of {} for codcorrect-draws".format(count,location_count))
codcorrect = pd.concat(codcorrect_list)

cc_ages = codcorrect.age_group_id.unique()
if 235 in cc_ages:
	codcorrect = codcorrect[codcorrect.age_group_id.isin(ages2)]
	codcorrect[['age_group_id']] = codcorrect[['age_group_id']].replace(to_replace=235,value=33)
elif 33 in cc_ages:
	codcorrect = codcorrect[codcorrect.age_group_id.isin(ages1)]


# turn cod deaths into rates
codcorrect = codcorrect.merge(all_pop, on=index_cols, how='inner')

print "Codcorrect"
print codcorrect.head(n=2)

for i in range(0, 1000):
    codcorrect['draw_%s' % i] = (codcorrect['draw_%s' % i] /
                                 codcorrect['population'])
codcorrect.drop('population', axis=1, inplace=True)

# get rate chronic and rate acute
chronic_isch_prop.reset_index(inplace=True)
chronic_hem_prop.reset_index(inplace=True)
acute_hem_prop.reset_index(inplace=True)
acute_isch_prop.reset_index(inplace=True)
print acute_isch_prop.shape
rate_chronic_isch = codcorrect.merge(chronic_isch_prop,
							on=['age_group_id','sex_id', 'cause_id'],
                            how='inner')
rate_chronic_hem = codcorrect.merge(chronic_hem_prop,
							on=['age_group_id','sex_id', 'cause_id'],
                            how='inner')							
rate_acute_hem = codcorrect.merge(acute_hem_prop,
                            on=['age_group_id','sex_id', 'cause_id'], 
							how='inner')
rate_acute_isch = codcorrect.merge(acute_isch_prop,
                            on=['age_group_id','sex_id', 'cause_id'], 
							how='inner')
print "rate_acute_isch"
print rate_acute_isch.head(n=2)
for i in range(0, 1000):
    rate_chronic_isch['rate_%s' % i] = (rate_chronic_isch['draw_%s_x' % i] *
                                   rate_chronic_isch['draw_%s_y' % i])
    rate_chronic_hem['rate_%s' % i] = (rate_chronic_hem['draw_%s_x' % i] *
                                   rate_chronic_hem['draw_%s_y' % i])								   
    rate_acute_hem['rate_%s' % i] = (rate_acute_hem['draw_%s_x' % i] *
                                     rate_acute_hem['draw_%s_y' % i])
    rate_acute_isch['rate_%s' % i] = (rate_acute_isch['draw_%s_x' % i] *
                                      rate_acute_isch['draw_%s_y' % i])

keep_cols = (['rate_%s' % i for i in range(0, 1000)])
all_cols = keep_cols + index_cols
rate_chronic_isch = rate_chronic_isch[all_cols].set_index(index_cols)
rate_chronic_hem = rate_chronic_hem[all_cols].set_index(index_cols)
rate_acute_hem = rate_acute_hem[all_cols].set_index(index_cols)
rate_acute_isch = rate_acute_isch[all_cols].set_index(index_cols)

# get mean, upper, lower
rate_chronic_isch = stroke_fns.get_summary_stats(rate_chronic_isch, index_cols, 'mean')
rate_chronic_isch = rate_chronic_isch.reset_index()

rate_chronic_hem = stroke_fns.get_summary_stats(rate_chronic_hem, index_cols, 'mean')
rate_chronic_hem = rate_chronic_hem.reset_index()

rate_acute_isch = stroke_fns.get_summary_stats(rate_acute_isch, index_cols, 'mean')
rate_acute_isch = rate_acute_isch.reset_index()

rate_acute_hem = stroke_fns.get_summary_stats(rate_acute_hem, index_cols, 'mean')
rate_acute_hem = rate_acute_hem.reset_index()

# save output by year
rate_chronic_isch.to_csv('%s/rate_chronic_isch_%s.csv' % (out_dir, year),
                    index=False, encoding='utf-8')
rate_chronic_hem.to_csv('%s/rate_chronic_hem_%s.csv' % (out_dir, year),
                    index=False, encoding='utf-8')
rate_acute_hem.to_csv('%s/rate_acute_hem_%s.csv' % (out_dir, year),
                      index=False, encoding='utf-8')
rate_acute_isch.to_csv('%s/rate_acute_isch_%s.csv' % (out_dir, year),
                       index=False, encoding='utf-8')