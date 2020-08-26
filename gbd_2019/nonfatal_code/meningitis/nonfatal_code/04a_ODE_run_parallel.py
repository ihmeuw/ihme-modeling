"""
Date: 9/5/2017
Purpose: Launch the DisMod ODE Step for all outcome/locations/year/sex.
"""

import pandas as pd
import numpy as np
import os
import time
import db_queries as db
import shutil
import sys

##read in args from parent
in_dir = sys.argv[1]
in_dir_data = sys.argv[2]
in_dir_value = sys.argv[3]
in_dir_rate = sys.argv[4]
out_dir_ODE = sys.argv[5]

#arguments from parameter map
code_dir = # filepath
filename = "04a_ODE_run_parallel_parameter.csv"
param_map = pd.read_csv(os.path.join(code_dir, filename))

task_id = int(os.environ.get("SGE_TASK_ID")) - 1
location = param_map.loc[task_id, 'location_id']
outcome = param_map.loc[task_id, 'outcome']

gbd_round = 6


#############################################################################################################
#write code here
#############################################################################################################

##define functions 

def write_checks(outcome, location, dm_out_dir):
    ## create a finished.txt for check system 
    finished = []
    checks = os.path.join(out_dir_ODE,"prev_results", "checks")
    if not os.path.exists(checks):
        os.makedirs(checks)
    np.savetxt(os.path.join(checks,"finished_{}_{}.txt".format(outcome, location)), finished)

def run_dismod_ode(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out, verbose):
    cmd = '/usr/local/dismod_ode/bin/dismod_ode \\' + '''
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s''' %(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out)
    if verbose:
        print(cmd)
    flag = os.system(cmd)
    print('flag is %s' %flag)
    if flag != 0 :
        sys.exit('dismod_ode.py: dismod_ode command failed')

def produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,
                sample_out,draw_out, verbose):
    cmd =   '/usr/local/dismod_ode/bin/model_draw \\' + '''
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s''' %(draw_in,value_in,plain_in,rate_in,effect_in,sample_out,draw_out)
    if verbose:
        print(cmd)
    flag = os.system(cmd)
    if flag != 0 :
        sys.exit('draw_cpp.py: model_draw command failed')

def format_draws(filename,drawnum):
    """This takes the raw output of Brad's model_draw code and formats it according 
    to GBD file structure/naming conventions"""
    result = pd.read_csv(filename)
    num_rows = len(result.index)
    row_start = num_rows - drawnum
    result = result[row_start:num_rows]
    result = result.transpose()
    orig_cols = result.columns
    col_dict = {orig_cols[i]:'draw_'+str(i) for i in range(drawnum)}
    result = result.rename(columns=col_dict)
    result.index.name = 'age'
    return result

def execute(draw_in, data_in, value_in, plain_in, rate_in, effect_in,
        sample_out, draw_out, drawnum, drop_temp, verbose):
    
    # create filepath for sample_out csv
    info_out = '/dev/null'
    
    # run dismod engine
    run_dismod_ode(data_in,value_in,plain_in,rate_in,effect_in,sample_out,info_out,verbose)
        
    # ---------------------------------------------------------------------------
    # run model_draw (create draw_out.csv) for draws (ind dismod output format)
    produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,sample_out,draw_out,verbose)
    
    # ---------------------------------------------------------------------------
    # clean up
    if drop_temp:
        os.remove(sample_out)
        
    # return the result
    data = format_draws(draw_out,drawnum)
    return data

def write_results(final, outcome, out_dir_ODE, location):
    """Function to write the results that are appended from the DisMod ODE."""
    
    # define filepaths, clean up columns we need
    if outcome == "epilepsy":
        out_dir = os.path.join(out_dir_ODE, "save_results_epilepsy")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

    if outcome == "long_modsev":
        out_dir = os.path.join(out_dir_ODE, "save_results_long_modsev")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    
    del final['outcome']
    final.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id', 'measure_id'], inplace = True)
    del final['index']
    
    #final.to_hdf(filepath, "draws", mode = "w", format = "table", data_columns = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'measure_id']) 
    final.to_csv(os.path.join(out_dir, "{}.csv".format(str(location))), index = True)
    return final
    
def main(in_dir, in_dir_data, in_dir_value, in_dir_rate, out_dir_ODE, outcome, location):
    
    # do we want to drop the temp files and temp directories?
    drop_temp = True
    
    # how much do we want to see?
    verbose = True
    
    #create empty list to fill dfs
    alldata = []
    
    # get demographics to loop over
    dems = db.get_demographics(gbd_team = "epi", gbd_round_id = gbd_round)
    for year in dems['year_id']:
        for sex in dems['sex_id']: 
    
            # begin new data frame to save one outcome-year-sex
            
            print("Running DisMod ODE for {} {} {} {}".format(outcome, location, year, sex))
            
            # define filepaths that already exist
            data_in = os.path.join(in_dir_data, str(location), "{outcome}_{location}_{year}_{sex}.csv".format(
                outcome = outcome, 
                location = location,
                year = year, 
                sex = sex))
            value_in = os.path.join(in_dir_value, "value_in_{outcome}.csv".format(outcome = outcome))
            rate_in = os.path.join(in_dir_rate, str(location), str(year), str(sex), "{location}_{year}_{sex}_{outcome}.csv".format(
                location = location,
                year = year,
                sex = sex,
                outcome = outcome))
            draw_in = os.path.join(in_dir, "draw_in.csv")
            plain_in = os.path.join(in_dir, "plain_in.csv")
            #different effect_in files because epilepsy has remission
            if outcome == "long_modsev":
                effect_in = os.path.join(in_dir, "effect_in_long_modsev.csv")
            if outcome == "epilepsy":
                effect_in = os.path.join(in_dir, "effect_in_epilepsy.csv")
            
            # define filepaths that will be created here
            sample_out_dir = os.path.join(out_dir_ODE, "TEMP/tmp_{}_{}_{}_{}".format(outcome,  str(location), str(year), str(sex)))
            sample_out = os.path.join(sample_out_dir, "sample_out.csv")
            if not os.path.exists(sample_out_dir):
                os.makedirs(sample_out_dir)
                
            draw_out_dir = os.path.join(out_dir_ODE, "prev_results", outcome, str(location), str(year), str(sex))
            draw_out = os.path.join(draw_out_dir, "prevalence_{}_{}_{}_{}.csv".format(outcome, str(location), str(year), str(sex)))
            if not os.path.exists(draw_out_dir):
                os.makedirs(draw_out_dir)
                
            drawnum = 1000
            
            result = execute(draw_in, data_in, value_in, plain_in, rate_in, effect_in,
                sample_out, draw_out, drawnum, drop_temp, verbose)
            
            # format the results so that we have the identifying columns as a draw sheet 
            result["location_id"] = location
            result["year_id"] = year
            result["sex_id"] = sex
            result["outcome"] = outcome
            result["measure_id"] = 5
            
            #replace ages
            
            result.reset_index(inplace = True)
            result['age'] = result['age'].astype(float)
            result["age"] = result["age"].round(2).astype(str)
            ridiculous_am = {
            '0.0': 2, '0.01': 3, '0.1': 4, '1.0': 5, '5.0': 6, '10.0': 7,
            '15.0': 8, '20.0': 9, '25.0': 10, '30.0': 11, '35.0': 12,
            '40.0': 13, '45.0': 14, '50.0': 15, '55.0': 16, '60.0': 17,
            '65.0': 18, '70.0': 19, '75.0': 20, '80.0': 30, '85.0': 31,
            '90.0': 32, '95.0': 235}
            result["age"] = result["age"].replace(ridiculous_am).astype(int)
            result.rename(columns={"age": "age_group_id"}, inplace=True)
            
            #save individual files for 05b
            result.to_csv(draw_out, index = False) 
            
            if drop_temp == True:
                # will remove the directories starting at year and everything after. Keeps the e-code n-code directory for the durations of this job.
                temp_dir = os.path.join(out_dir_ODE, "TEMP/tmp_{}_{}_{}_{}".format(outcome,  str(location), str(year), str(sex)))
                shutil.rmtree(temp_dir)
            
            #concatenate 
            alldata.append(result)
            final = pd.concat(alldata)
            
            # reset the index before going into write-results function
            final.reset_index(inplace = True)
            
            # write the results to save_results folder
            write_results(final, outcome, out_dir_ODE, location)

            #write checks file
            write_checks(outcome, location, out_dir_ODE)
            
#run main function
main(in_dir, in_dir_data, in_dir_value, in_dir_rate, out_dir_ODE, outcome, location)
