import stroke_fns
import sys
import pandas as pd
from itertools import izip
# from transmogrifier.draw_ops import get_draws
from get_draws.api import get_draws
import db_queries
import multiprocessing as mp
import itertools

out_dir, year, isch_mv, ich_mv, sah_mv, isch_me, ich_me, sah_me, step = sys.argv[1:10]

year = int(year)
isch_mv = int(isch_mv)
ich_mv = int(ich_mv)
sah_mv = int(sah_mv)
isch_me = int(isch_me)
ich_me = int(ich_me)
sah_me = int(sah_me)
step = int(step)
print "... SURVIVORSHIP YEAR %s" % year
print "... STEP %s" % step
# Function to calculate incidence of surviving 28 days after an acute event
def calcSurvInc(params):
    me = params[0][0]
    mv = params[0][1]
    geo = params[1]
    sex = params[2] 

    #print 'On loop %s of %s' % (count, loops)
    print 'Preparing to get_draws for %s' % me
    draws = get_draws('modelable_entity_id', me, 'epi', location_id=geo, year_id=year, sex_id=sex, gbd_round_id=5)
    print 'Finish get_draws, starting data processing'
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

    # multiply incidence by 28 day survivorship:q
	
    incidence.set_index('age_group_id', inplace=True)
    final_incidence = incidence * survivorship

    # add back on identifying columns
    final_incidence['location_id'] = geo
    final_incidence['sex_id'] = sex
    final_incidence['modelable_entity_id'] = me

    return final_incidence

if __name__ == "__main__":
    ages1 = range(2,21) + [30,31,32,33]
    ages2 = range(2,21) + [30,31,32,235]
    columns = stroke_fns.filter_cols()
    print "Pulling location data .. "
    locations = stroke_fns.get_locations()
    print "All locations pulled .."
    sexes = [1,2]
    loops = (len(locations) * 2) * 2
    print "Number of locations pulled %s" % len(locations)
    #main(step)    
    all_df = pd.DataFrame()
    #all_df_list = []
    
    #count = 0
    
    isch_list = [isch_me, isch_mv]
    ich_list = [ich_me, ich_mv]
    sah_list = [sah_me, sah_mv]
    print "Checkpoint 1"
    paramlist = list(itertools.product([isch_list, ich_list, sah_list],locations, sexes))
    print "paramlist created"
    print paramlist
    pool = mp.Pool(processes=4)
    final_incidence = pool.map(calcSurvInc, paramlist)
    print "Survival incidence calculated"
    all_df = all_df.append(final_incidence)
    #pool.join()
    
    all_df.reset_index(inplace=True)
    all_df['year_id'] = year
    print all_df.head(10)

    # add together iscichic and ichorrhagic
    index_cols = ['modelable_entity_id','location_id', 'age_group_id', 'year_id', 'sex_id']
    summed_all_df = all_df.groupby(index_cols).sum().reset_index()

    # get mean, upper, lower
    #summed_all_df.drop('modelable_entity_id', axis=1, inplace=True)
    summed_all_df.set_index(index_cols, inplace=True)
    final = summed_all_df.transpose().describe(percentiles=[.025, .975]).transpose()[['mean', '2.5%', '97.5%']]
    final.rename(
    columns={'2.5%': 'lower', '97.5%': 'upper'}, inplace=True)
    final.index.rename(['modelable_entity_id','location_id', 'age_group_id', 'year_id', 'sex_id'], inplace=True)
    final.reset_index(inplace=True)
    final_isch = final.loc[final.modelable_entity_id==isch_me]
    final_ich = final.loc[final.modelable_entity_id==ich_me]
    final_sah = final.loc[final.modelable_entity_id==sah_me]
    
    # output
    final_isch.to_csv('%s/step_%s_output_from_%s_%s.csv' % (out_dir, step, isch_me, year),
             index=False, encoding='utf-8')
    final_ich.to_csv('%s/step_%s_output_from_%s_%s.csv' % (out_dir, step, ich_me, year),
             index=False, encoding='utf-8')
    final_sah.to_csv('%s/step_%s_output_from_%s_%s.csv' % (out_dir, step, sah_me, year),
             index=False, encoding='utf-8')		