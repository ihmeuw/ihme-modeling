import stroke_fns
import sys
import pandas as pd
from itertools import izip
from transmogrifier.draw_ops import get_draws
import db_queries

# bring in args
if len(sys.argv)>7:
# step 3 call
    dismod_dir, out_dir, year, model_vers1, model_vers2, me1, me2 = sys.argv[1:8]
else:
# step 1 call
    out_dir, year, model_vers1, model_vers2, me1, me2 = sys.argv[1:7]

year = int(year)
model_vers1 = int(model_vers1)
model_vers2 = int(model_vers2)
me1 = int(me1)
me2 = int(me2)
print out_dir
print year
print model_vers1
print model_vers2
print me1
print me2
print "... SURVIVORSHIP YEAR %s" % year

# get ready for loop
ages1 = range(2,21) + [30,31,32,33]
ages2 = range(2,21) + [30,31,32,235]
modelable_entity_ids = [me1,me2]
models = [model_vers1, model_vers2]
columns = stroke_fns.filter_cols()
locations = stroke_fns.get_locations()
sexes = [1,2]
loops = (len(locations) * 2) * 2

all_df = pd.DataFrame()
#all_df_list = []

count = 0
for me, mv in izip(modelable_entity_ids, models):
    for geo in locations:
        for sex in sexes:
			print 'On loop %s of %s' % (count, loops)
			draws = get_draws('modelable_entity_id', me, 'epi', location_ids=geo, 
					year_ids=year, sex_ids=sex, gbd_round_id=4)
			d_ages = draws.age_group_id.unique()
			if 235 in d_ages:
				draws = draws[draws.age_group_id.isin(ages2)]
				draws[['age_group_id']] = draws[['age_group_id']].replace(to_replace=235,value=33)
			elif 33 in d_ages:
				draws = draws[draws.age_group_id.isin(ages1)]
				
			# pull out incidence and EMR into seperate dfs
			incidence = draws[draws.measure_id == 6]
			emr = draws[draws.measure_id == 9]

			# keep only age_group_id and draw columns
			incidence = incidence[columns]
			emr = emr[columns]

			# get 28 day survivorship
			emr.set_index('age_group_id', inplace=True)
			fatality = emr / (12 + emr)
			survivorship = 1 - fatality

			# multiply incidence by 28 day survivorship
			incidence.set_index('age_group_id', inplace=True)
			final_incidence = incidence * survivorship

			# add back on identifying columns
			final_incidence['location_id'] = geo
			final_incidence['sex_id'] = sex
			final_incidence['model_version_id'] = mv

			# append to master dataset
			all_df = all_df.append(final_incidence)
			#all_df_list.append(final_incidence)
			count += 1

#all_df = pd.concat(all_df_list)
all_df.reset_index(inplace=True)
all_df['year_id'] = year
print all_df.head(10)

# add together ischemic and hemorrhagic
index_cols = ['model_version_id','location_id', 'age_group_id', 'year_id', 'sex_id']
summed_all_df = all_df.groupby(index_cols).sum().reset_index()

# get mean, upper, lower
#summed_all_df.drop('model_version_id', axis=1, inplace=True)
summed_all_df.set_index(index_cols, inplace=True)
final = summed_all_df.transpose().describe(
    percentiles=[.025, .975]).transpose()[['mean', '2.5%', '97.5%']]
final.rename(
    columns={'2.5%': 'lower', '97.5%': 'upper'}, inplace=True)
final.index.rename(['model_version_id','location_id', 'age_group_id', 'year_id', 'sex_id'],
                   inplace=True)
final.reset_index(inplace=True)
final_isch = final.loc[final.model_version_id==model_vers1]
final_hem = final.loc[final.model_version_id==model_vers2]

# output
final_isch.to_csv('%s/input_%s_%s.csv' %
             (out_dir, model_vers1, year),
             index=False, encoding='utf-8')
final_hem.to_csv('%s/input_%s_%s.csv' %
             (out_dir, model_vers2, year),
             index=False, encoding='utf-8')