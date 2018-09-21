"""
This script will run the dismod engine (just the engine, NOT the cascade), and
will output draws, summary stats, or both, that are properly formatted for GBD
processes. In other words, they are organized by files specified like 
[iso3]_[year]_[sex].csv, with columns age and draw0-draw999. The ages will be
the GBD age groups.

Arguments:
    
Outputs:
    csv [iso3]_[year]_[sex].csv: csv containing columns:
        int age: GBD age groups
        double draw0-draw999: values by draw
"""

import os
from sys import argv,path
import sys
import shutil
import pandas as pd

def run_dismod_ode(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out):
    cmd = 'FILEPATH \\' + '''
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s''' %(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out)
    print cmd
    flag = os.system(cmd)
    print 'flag is %s' %flag
    if flag != 0 :
        sys.exit('dismod_ode.py: dismod_ode command failed')
        
def produce_posterior_summ_stats(pred_in,value_in,plain_in,rate_in,effect_in,
                                sample_out,summary_out):
    """This function will use data_pred script to produce summary stats
    instead of draws. It is currently not used because it produces lower, 
    MEDIAN, and upper; rather than lower, MEAN, and upper."""
    cmd = 'FILEPATH \\' + '''
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s''' %(pred_in,value_in,plain_in,rate_in,effect_in,sample_out,
                            summary_out)
    print cmd
    flag = os.system(cmd)
    if flag != 0 :
        sys.exit('dismod_ode.py: data_pred command failed')

def produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,
                sample_out,draw_out):
    cmd =   'FILEPATH \\' + '''
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s \\
        %s''' %(draw_in,value_in,plain_in,rate_in,effect_in,sample_out,draw_out)
    print cmd
    flag = os.system(cmd)
    if flag != 0 :
        sys.exit('draw_cpp.py: model_draw command failed')
        
def format_draws(filename,drawnum):
    """This takes the raw output of model_draw code and formats it according
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
    result.to_csv(filename)
    return result
     
def main(draw_in,data_in, value_in, plain_in, rate_in, effect_in,
        temp_dir,draw_out,summary_out,drawnum,temp_suffix,drop_temp=True):
    
    # create filepath for sample_out csv
    sample_out = os.path.join(temp_dir,'sample_out_%s.csv' %temp_suffix)
    info_out = 'FILEPATH'
    
    # run dismod engine
    run_dismod_ode(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out)
        
    # ---------------------------------------------------------------------------
    # run model_draw (create draw_out.csv) for draws (ind dismod output format)
    produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,
                sample_out,draw_out)
    format_draws(draw_out,drawnum)
    
    # create summary stats if desired
    if summary_out:
        create_summary_stats(draw_out,summary_out)
    
    # ---------------------------------------------------------------------------
    # clean up
    if drop_temp:
        shutil.rmtree(temp_dir)
                
  
if __name__ == '__main__':
    homedir =      argv[1]
    draw_in =      argv[2]
    data_in =      argv[3]
    value_in =     argv[4]
    plain_in =     argv[5]
    rate_in =      argv[6]
    effect_in =    argv[7]
    temp_dir =     argv[8]
    temp_suffix =  argv[9]
    draw_out =     argv[10]
    pyHME_parent = argv[11]
    if len(argv) == 13:
        summary_out =  argv[12]
    else:
        summary_out = None

    
    path.append(pyHME_parent)
    from pyHME import params
    from pyHME.functions import create_summary_stats
    
    p = params.Params(incl_ages=False)
    
    os.chdir(homedir)
    
    main(draw_in,data_in, value_in, plain_in, rate_in, effect_in,
        temp_dir,draw_out,summary_out,p.drawnum,temp_suffix,drop_temp=False)
