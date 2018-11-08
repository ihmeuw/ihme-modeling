# Python launcher code for all Nonfatal Stroke Custom Modeling
# Since the modeler involved does not code in Python, this code
# is launched using the nonfatal_stroke_wrapper.sh

import pandas as pd
import stroke_fns
import subprocess
import db_queries
from db_tools.ezfuncs import query
import sys
from elmo import run
from itertools import izip
import multiprocessing as mp
import itertools

#step=1
step = sys.argv[1]
step = int(step)

print "\n////////////////STARTING STROKE CODE\\\\\\\\\\\\\\\\n"

# set i/o parameters
time_now = stroke_fns.get_time()
out_dir = stroke_fns.check_dir('FILEPATH/%s' % time_now)

yearvals = [1990, 1995, 2000, 2005, 2010, 2017]

# first ever stroke ids
isch_me = 3952
isch_bd = 454
ich_me = 3953
ich_bd = 455
sah_me = 18730
sah_bd = 2996

# chronic stroke ids
total_me_isch = 1832
total_bd_isch = 787
total_me_ich = 1844
total_bd_ich = 785
total_me_sah = 18732
total_bd_sah = 3002

# first ever stroke w csmr ids
acute_isch_csmr_me = 9310
acute_isch_csmr_bd = 514
acute_ich_csmr_me = 9311
acute_ich_csmr_bd = 515
acute_sah_csmr_me = 18731
acute_sah_csmr_bd = 2999

# chronic stroke w csmr ids
chronic_isch_csmr_me = 10837
chronic_isch_csmr_bd = 786
chronic_ich_csmr_me = 10836
chronic_ich_csmr_bd = 784
chronic_sah_csmr_me = 18733
chronic_sah_csmr_bd = 3005

def prepSheet(params):
    input_me = params[0]
    print input_me
    me_id = params[1]
    bd_id = params[2]
    print bd_id
    epi_input = pd.DataFrame()
    for year in yearvals:
        df = pd.read_csv('%s/step_%s_output_from_%s_%s.csv' % (out_dir, step, input_me, year))
        epi_input = epi_input.append(df)
    raw_df = epi_input.copy(deep=True)
    # add on necessary columns for epi uploader
    epi_input = stroke_fns.add_uploader_cols(epi_input)
    epi_input['nid'] = nid
    epi_input['measure_id'] = measure_id
    epi_input['modelable_entity_id'] = me_id
    epi_input['bundle_id'] = bd_id

    #fill in row_nums
    epi_input = stroke_fns.assign_row_nums(epi_input, bd_id)
    epi_input['measure'] = measure
    epi_input.drop(['age_group_id', 'year_id'], axis=1, inplace=True)
    epi_input.rename(columns = {'sex_id':'sex'}, inplace=True)
    sexes = epi_input['sex']
    sexes = sexes.apply(stroke_fns.sex_fix)
    epi_input['sex'] = sexes
    writer = pd.ExcelWriter('%s/step_%s_input_%s.xlsx'%(out_dir, step, bd_id), engine='xlsxwriter')
    epi_input.to_excel(writer, sheet_name='extraction', index=False, encoding='utf-8')
    writer.save()

# /////////////////////////STEP 1\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
if step == 1:
    print "\n///////////STARTING SURVIVORSHIP (STEP 1)\\\\\\\\"

    # wait on dismod models running and return model versions
    isch_mv, ich_mv, sah_mv = stroke_fns.wait_dismod(isch_me, ich_me, sah_me)
    print isch_mv, ich_mv, sah_mv

    # run 01_survivorship
    nid = 239169
    measure_id = 6
    measure = 'incidence'
    for year in yearvals:
        call = ('qsub -cwd -N "surv_%s" -P proj_custom_models '
                '-o FILEPATH/output '
                '-e FILEPATH/errors '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/01_survivorship.py "%s" "%s" "%s" "%s" "%s" "%s" "%s" "%s" "%s" ' % (
                    year, out_dir, year, isch_mv, ich_mv, sah_mv, isch_me, ich_me, sah_me, step))
        subprocess.call(call, shell=True)

    # # wait for 01_survivorship to completely finish
    stroke_fns.wait('surv', 300)

    # concatenate all of the year-separate outputs from 01
    #mvs = [ich_mv, isch_mv]
    #total_mes = [total_me_ich, total_me_isch]
    total_bds = [total_bd_ich, total_bd_isch, total_bd_sah]

    isch_list = [isch_me, total_me_isch, total_bd_isch]
    ich_list = [ich_me, total_me_ich, total_bd_ich]
    sah_list = [sah_me, total_me_sah, total_bd_sah]
    paramlist = [isch_list, ich_list, sah_list]
    #paramlist = list(itertools.product(mvs, total_mes, total_bds))
    pool = mp.Pool(processes=4)
    results = pool.map(prepSheet, paramlist)

    upload = True
    if upload == True:
        for bundle in total_bds:
            call = ('qsub -cwd -N "upload_%s" -P proj_custom_models '
                '-o FILEPATH/output '
                '-e FILEPATH/errors '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/03_upload.py "%s" "%s" "%s" ' % (
                    bundle, step, bundle, out_dir))
            subprocess.call(call, shell=True)

if step == 2: # (hopefully) final step 2
# wait on dismod model running and return model version
# note: two inputs into the prev step are also inputs to this step
    chronic_isch_mv = stroke_fns.wait_dismod(total_me_isch)
    chronic_ich_mv = stroke_fns.wait_dismod(total_me_ich)
    chronic_sah_mv = stroke_fns.wait_dismod(total_me_sah)

    print chronic_isch_mv
    print chronic_ich_mv
    print chronic_sah_mv

    nid = 238131
    measure_id = 15
    measure = 'mtspecific'

    # run 02_csmr
    for year in yearvals:
        call = ('qsub -cwd -N "step2_csmr_%s" -P proj_custom_models '
                '-o FILEPATH/output '
                '-e FILEPATH/errors '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/02_csmr.py "%s" "%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (
                    year, out_dir, year, isch_me, ich_me, sah_me, total_me_isch, total_me_ich, total_me_sah))
        subprocess.call(call, shell=True)

    #wait for 02_csmr to completely finish
    stroke_fns.wait('step2_csmr', 300)

    # concatenate all of the year-separate outputs from 02
    csmr_bds = [acute_isch_csmr_bd, acute_ich_csmr_bd, acute_sah_csmr_bd, chronic_isch_csmr_bd, chronic_ich_csmr_bd, chronic_sah_csmr_bd]
    acute_isch_csmr = [isch_me, acute_isch_csmr_me, acute_isch_csmr_bd]
    acute_ich_csmr = [ich_me, acute_ich_csmr_me, acute_ich_csmr_bd]
    acute_sah_csmr = [sah_me, acute_sah_csmr_me, acute_sah_csmr_bd]
    chronic_isch_csmr = [total_me_isch, chronic_isch_csmr_me, chronic_isch_csmr_bd]
    chronic_ich_csmr = [total_me_ich, chronic_ich_csmr_me, chronic_ich_csmr_bd]
    chronic_sah_csmr = [total_me_sah, chronic_sah_csmr_me, chronic_sah_csmr_bd]

    paramlist = [acute_isch_csmr, acute_ich_csmr, acute_sah_csmr, chronic_isch_csmr, chronic_ich_csmr, chronic_sah_csmr]
    #paramlist = list(itertools.product(mvs, total_mes, total_bds))
    pool = mp.Pool(processes=4)
    results = pool.map(prepSheet, paramlist)

    upload = True
    if upload == True:
        for bundle in csmr_bds:
            call = ('qsub -cwd -N "upload_%s" -P proj_custom_models '
                '-o FILEPATH/output '
                '-e FILEPATH/errors '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/03_upload.py "%s" "%s" "%s" ' % (
                    bundle, step, bundle, out_dir))
            subprocess.call(call, shell=True)

if step == 3:
    print "\n///////////STARTING SURVIVORSHIP (STEP 3)\\\\\\\\"
    nid = 239169
    measure_id = 6
    measure = 'incidence'

    # wait on dismod models running and returning model versions
    isch_csmr_mv, ich_csmr_mv, sah_csmr_mv = stroke_fns.wait_dismod(
    acute_isch_csmr_me, acute_ich_csmr_me, acute_sah_csmr_me)

    # run 01_survivorship for Step 3
    for year in yearvals:
        call = ('qsub -cwd -N "step3_surv_%s" -P  proj_custom_models  '
                '-o FILEPATH/output '
                '-e FILEPATH/errors '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/01_survivorship.py "%s" "%s" "%s" "%s" "%s" "%s" "%s" "%s" "%s"' % (
                    year, out_dir, year, isch_csmr_mv,
                    ich_csmr_mv, sah_csmr_mv, acute_isch_csmr_me, acute_ich_csmr_me, acute_sah_csmr_me, step))
        subprocess.call(call, shell=True)

    # # wait for 01_survivorship to completely finish
    stroke_fns.wait('step3_surv', 300)

    # concatenate all of the year-separate outputs from 03
    chronic_csmr_bds = [chronic_ich_csmr_bd, chronic_isch_csmr_bd, chronic_sah_csmr_bd]

    # prepare for parallelization
    isch_list = [acute_isch_csmr_me, chronic_isch_csmr_me, chronic_isch_csmr_bd]
    ich_list = [acute_ich_csmr_me, chronic_ich_csmr_me, chronic_ich_csmr_bd]
    sah_list = [acute_sah_csmr_me, chronic_sah_csmr_me, chronic_sah_csmr_bd]
    paramlist = [isch_list, ich_list, sah_list]
    pool = mp.Pool(processes=4)
    results = pool.map(prepSheet, paramlist)

    upload = True
    if upload == True:
        for bundle in chronic_csmr_bds:
            call = ('qsub -cwd -N "upload_%s" -P proj_custom_models '
                '-o FILEPATH/output '
                '-e FILEPATH/errors '
                '-l mem_free=40G -pe multi_slot 20 FILEPATH/cluster_shell.sh '
                'FILEPATH/03_upload.py "%s" "%s" "%s" ' % (
                    bundle, step, bundle, out_dir))
            subprocess.call(call, shell=True)
