# Python launcher code for all Nonfatal Stroke Custom Modeling

import pandas as pd
import stroke_fns
import subprocess
import db_queries
from db_tools.ezfuncs import query
import sys
from elmo import run
from itertools import izip

#step=1
step = sys.argv[1]
step = int(step)

print "\n////////////////STARTING STROKE CODE\\\\\\\\\\\\\\\\n"

# set i/o parameters
time_now = stroke_fns.get_time()
dismod_dir = 'FILEPATH'
out_dir = stroke_fns.check_dir('FILEPATH/%s' % time_now)
yearvals = [1990, 1995, 2000, 2005, 2010, 2016]

# /////////////////////////STEP 1\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
# set target me_id
total_me_isch = 1832
total_me_hem = 1844
total_bd_isch = 787
total_bd_hem = 785

# get modelable entity id of dismod models currently running/most recent
isch_me = 3952
isch_bd = 454
hem_me = 3953
hem_bd = 455

# wait on dismod models running and return model versions
isch_mv, hem_mv = stroke_fns.wait_dismod(isch_me, hem_me)
print isch_mv, hem_mv

if step == 1:
    print "\n///////////STARTING SURVIVORSHIP (STEP 1)\\\\\\\\"
    # run 01_survivorship
    	
    for year in yearvals:
        call = ('qsub -cwd -N "surv_%s" -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/01_survivorship.py "%s" "%s" "%s" "%s" "%s" "%s"' % (
                    year, out_dir, year, isch_mv, hem_mv, isch_me, hem_me))
        subprocess.call(call, shell=True)

    # # wait for 01_survivorship to completely finish
    stroke_fns.wait('surv', 300)

    # concatenate all of the year-separate outputs from 01
    models = [hem_mv, isch_mv]
    total_mes = [total_me_hem, total_me_isch]
    total_bds = [total_bd_hem, total_bd_isch]
	
    for model, total_me, total_bd in izip(models, total_mes, total_bds):
		epi_input = pd.DataFrame()
		for year in yearvals:
		    df = pd.read_csv('%s/input_%s_%s.csv' % (out_dir, model, year))
		    epi_input = epi_input.append(df)
		raw_df = epi_input.copy(deep=True)
		
        # add on necessary columns for epi uploader
		epi_input = stroke_fns.add_uploader_cols(epi_input)
		epi_input['nid'] = 239169
		epi_input['measure_id'] = 6
		epi_input['modelable_entity_id'] = total_me
		epi_input['bundle_id'] = total_bd
		
		#fill in row_nums
		epi_input = stroke_fns.assign_row_nums(epi_input, total_bd)
		epi_input['measure'] = 'incidence'
		epi_input.drop(['age_group_id', 'year_id'], axis=1, inplace=True)
		epi_input.rename(columns = {'sex_id':'sex'}, inplace=True)
		sexes = epi_input['sex']
		sexes = sexes.apply(stroke_fns.sex_fix)
		epi_input['sex'] = sexes
		writer = pd.ExcelWriter('%s/epi_input_%s.xlsx'%(out_dir, total_bd), engine='xlsxwriter')
		epi_input.to_excel(writer, sheet_name='extraction', index=False, encoding='utf-8')
		writer.save()

    upload = False	
    if upload == True:
        for bundle in total_bds:
		    call = ('qsub -cwd -N "upload_%s" -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/03_upload.py "%s" "%s" "%s" ' % (
                    bundle,bundle, dismod_dir, out_dir))
        subprocess.call(call, shell=True)
        
        # df = run.upload_epi_data(total_bd_hem, '%s/epi_input_%s.xlsx'%(out_dir, total_bd_hem));
        # print df
        # df = run.upload_epi_data(total_bd_isch, '%s/epi_input_%s.xlsx'%(out_dir, total_bd_isch))
        # print df	

# /////////////////////////STEP 2\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
# set target me_ids
acute_isch_csmr_me = 9310
acute_isch_csmr_bd = 514
acute_hem_csmr_me = 9311
acute_hem_csmr_bd = 515
chronic_isch_csmr_me = 10837
chronic_isch_csmr_bd = 786
chronic_hem_csmr_me = 10836
chronic_hem_csmr_bd = 784

if step == 2:

    # wait on dismod model running and return model version
    # note: two inputs into the prev step are also inputs to this step
    chronic_isch_mv = stroke_fns.wait_dismod(total_me_isch)
    chronic_hem_mv = stroke_fns.wait_dismod(total_me_hem)

    print chronic_isch_mv
    print chronic_hem_mv 
    
    # chronic_hem_mv

    # run 02_csmr
    for year in yearvals:
        call = ('qsub -cwd -N "step2_csmr_%s" -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/02_csmr.py "%s" "%s" "%s" "%s" "%s" "%s"' % (
                    year, out_dir, year, isch_me, hem_me, total_me_isch, total_me_hem))
        subprocess.call(call, shell=True)

    #wait for 02_csmr to completely finish
    stroke_fns.wait('step2_csmr', 300)

    # concatenate all of the year-separate outputs from 02
    total_mes_2 = [acute_isch_csmr_me, acute_hem_csmr_me, chronic_isch_csmr_me, chronic_hem_csmr_me]
    total_bds_2 = [acute_isch_csmr_bd, acute_hem_csmr_bd, chronic_isch_csmr_bd, chronic_hem_csmr_bd]
    models_2 = ['acute_isch', 'acute_hem', 'chronic_isch', 'chronic_hem']
    
    for model, total_me, total_bd in izip(models_2, total_mes_2, total_bds_2):
		csmr_input = pd.DataFrame()
		for year in yearvals:
		    df = pd.read_csv('%s/rate_%s_%s.csv' % (out_dir, model, year))
		    csmr_input = csmr_input.append(df)
		raw_df = csmr_input.copy(deep=True)
		
        # add on necessary columns for epi uploader
		csmr_input = stroke_fns.add_uploader_cols(csmr_input)
		csmr_input['nid'] = 238131
		csmr_input['measure_id'] = 15
		csmr_input['modelable_entity_id'] = total_me
		csmr_input['bundle_id'] = total_bd
		
		#fill in row_nums
		csmr_input = stroke_fns.assign_row_nums(csmr_input, total_bd)
		csmr_input['measure'] = 'mtspecific'
		csmr_input.drop(['age_group_id', 'year_id'], axis=1, inplace=True)
		csmr_input.rename(columns = {'sex_id':'sex'}, inplace=True)
		sexes = csmr_input['sex']
		sexes = sexes.apply(stroke_fns.sex_fix)
		csmr_input['sex'] = sexes
		writer = pd.ExcelWriter('%s/csmr_input_%s.xlsx'%(out_dir, total_bd), engine='xlsxwriter')
		csmr_input.to_excel(writer, sheet_name='extraction', index=False, encoding='utf-8')
		writer.save()

    upload = False	
    if upload == True:
        for bundle in total_bds_2:
		    call = ('qsub -cwd -N "upload_%s" -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/03_upload.py "%s" "%s" "%s" ' % (
                    bundle,bundle, dismod_dir, out_dir))
        subprocess.call(call, shell=True)

        # # upload using the epi uploader
        # df = run.upload_epi_data(acute_isch_csmr_bd, '%s/acute_isch_input_%s.xlsx'%(out_dir, acute_isch_csmr_bd))
        # df = run.upload_epi_data(acute_hem_csmr_bd, '%s/acute_hem_input_%s.xlsx'%(out_dir, acute_hem_csmr_bd))
        # df = run.upload_epi_data(chronic_isch_csmr_bd, '%s/chronic_isch_input_%s.xlsx'%(out_dir, chronic_isch_csmr_bd))
        # df = run.upload_epi_data(chronic_hem_csmr_bd, '%s/chronic_hem_input_%s.xlsx'%(out_dir, chronic_hem_csmr_bd))

if step == 3:
# ihme_general
    # /////////////////////////STEP 3\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
    # wait on dismod models running and return model versions
    isch_csmr_mv, hem_csmr_mv = stroke_fns.wait_dismod(
    acute_isch_csmr_me, acute_hem_csmr_me)
    # run 01_survivorship for Step 3
    for year in yearvals:
        call = ('qsub -cwd -N "step3_surv_%s" -P  proj_custom_models  '
                '-o FILEPATH '
                '-e FILEPATH '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/01_survivorship.py "%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (
                    year, dismod_dir, out_dir, year, isch_csmr_mv, 
                    hem_csmr_mv, acute_isch_csmr_me, acute_hem_csmr_me))
        subprocess.call(call, shell=True)

    # # wait for 01_survivorship to completely finish
    stroke_fns.wait('step3_surv', 300)

    # concatenate all of the year-separate outputs from 03
    csmr_mvs = [hem_csmr_mv, isch_csmr_mv]
    chronic_csmr_bds = [chronic_hem_csmr_bd, chronic_isch_csmr_bd]
    chronic_csmr_mes = [chronic_hem_csmr_me, chronic_isch_csmr_me]
	
	# concatenate all of the year-separate outputs from 03
    for csmr_mv, chronic_csmr_me, chronic_csmr_bd in izip(csmr_mvs, chronic_csmr_mes, chronic_csmr_bds):
        epi_input = pd.DataFrame()
        for year in yearvals:
		    df = pd.read_csv('%s/input_%s_%s.csv' %(out_dir, csmr_mv, year))
		    epi_input = epi_input.append(df)
        raw_df = epi_input.copy(deep=True)

		# add on necessary columns for epi uploader
        epi_input = stroke_fns.add_uploader_cols(epi_input)
        epi_input['nid'] = 239169
        epi_input['modelable_entity_id'] = chronic_csmr_me
        epi_input['measure_id'] = 6

		# fill in row_nums
        epi_input = stroke_fns.assign_row_nums(epi_input, chronic_csmr_bd)
        epi_input['measure'] = 'incidence'
        epi_input.drop(['age_group_id', 'year_id'], axis=1,inplace=True)
				   
        epi_input.rename(columns = {'sex_id':'sex'}, inplace=True)
        sexes = epi_input['sex']
        sexes = sexes.apply(stroke_fns.sex_fix)
        epi_input['sex'] = sexes
		
        if chronic_csmr_bd == 784:
		    writer = pd.ExcelWriter('%s/epi_input_784.xlsx'%out_dir, engine='xlsxwriter')
        else:
			writer = pd.ExcelWriter('%s/epi_input_786.xlsx'%out_dir, engine='xlsxwriter')
			
        epi_input.to_excel(writer, sheet_name='extraction', index=False, encoding='utf-8')
        writer.save()
		
    upload = False	
    if upload == True:
        for bundle in chronic_csmr_bds:
		    call = ('qsub -cwd -N "upload_%s" -P proj_custom_models '
                '-o FILEPATH '
                '-e FILEPATH '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/03_upload.py "%s" "%s" "%s" ' % (
                    bundle,bundle, dismod_dir, out_dir))
        subprocess.call(call, shell=True)
    # upload using the epi uploader
        #df = run.upload_epi_data(chronic_isch_csmr_bd, '%s/epi_input_%s.xlsx'% (out_dir, chronic_isch_csmr_bd))
        #print df
        #df = run.upload_epi_data(chronic_hem_csmr_bd, '%s/epi_input_%s.xlsx'%(out_dir, chronic_hem_csmr_bd))
        #print df	