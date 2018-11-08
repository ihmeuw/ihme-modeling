"""
Author: USERNAME
Date: 9/5/2017
Purpose: Launch the DisMod ODE Step for all location/ncode/platforms. This is parallelized
over ecode/year/sex.
"""

import pandas as pd
import numpy as np
import os
import time
import db_queries as db
import sys
from fbd_core import etl
from gbd_inj.inj_helpers import help, inj_info, paths


def run_dismod_ode(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out, verbose):
    cmd = 'FILEPATH' + '''
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
    cmd =   'FILEPATH' + '''
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
    """This takes the raw output of model_draw code and formats it according
    to GBD file structure/naming conventions"""
    result = pd.read_csv(filename)
    return result


def run_model_injuries(draw_in, data_in, value_in, plain_in, rate_in, effect_in, draw_out, n_draws):
    cmd = 'FILEPATH'.format(
        dr=draw_in,
        da=data_in,
        v=value_in,
        p=plain_in,
        r=rate_in,
        e=effect_in,
        dr_o=draw_out,
        n=n_draws
    )
    flag = os.system(cmd)
    if flag != 0:
        sys.exit('model_injuries command failed')
    return format_draws(draw_out, n_draws)


def execute(draw_in, data_in, value_in, plain_in, rate_in, effect_in,
        sample_out, draw_out, drawnum, drop_temp, verbose):
    
    # create filepath for sample_out csv
    info_out = 'FILEPATH'
    
    # run dismod engine
    run_dismod_ode(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out, verbose)
        
    # ---------------------------------------------------------------------------

    produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,
                sample_out,draw_out, verbose)
    
    # ---------------------------------------------------------------------------
    # clean up
    if drop_temp:
        os.remove(sample_out)
        
    # return the result
    data = format_draws(draw_out,drawnum)
    return data

def write_results(df, ecode, ncode, platform, year, version):
    """Function to write the results that are appended from the DisMod ODE."""
    out_dir = os.path.join("FILEPATH")
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    filename = "FILEPATH.nc".format(year)
    filepath = os.path.join(out_dir, filename)
    df = help.convert_to_age_group_id(df)
    
    df.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id', 'platform'], inplace = True)
    
    arr = etl.df_to_xr(df, wide_dim_name='draw', fill_value=np.nan)
    arr.to_netcdf(filepath)
    

def main(ecode, ncode, platform, year, version, flat_version):
    dems = db.get_demographics(gbd_team="epi", gbd_round_id=help.GBD_ROUND)
    dm_settings = os.path.join("FILEPATH")
    dm_dir = os.path.join("FILEPATH")
    metaloc = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)

    locations = help.ihme_loc_id_dict(metaloc, dems['location_id'])
    
    alldata = []
    
    value_in = os.path.join("FILEPATH.csv".format(ncode, platform))
    draw_in = os.path.join(dm_settings, "FILEPATH.csv")
    plain_in = os.path.join(dm_settings, "FILEPATH.csv")
    effect_in = os.path.join(dm_settings, "FILEPATH.csv")
    v_in = pd.read_csv(value_in)
    
    num_locs = len(locations)
    loc_pos = 0
    initime = help.start_timer()
    for locn in locations:
        loc_pos = loc_pos + 1

        for sex in [1,2]:

            print("Running DisMod ODE for location {} year {} sex {}".format(locations[locn], year, sex))

            start = help.start_timer()
            
            if float(v_in.loc[v_in['name']=='eta_incidence','value'][0]) == 0:
                print('eta incidence is 0, so all incidence should be 0 and we\'ll just make an all 0 df instead of '
                      'running the ODE')
                result = pd.DataFrame({'age_group_id': dems['age_group_id']})
                result = result.assign(**{d: 0 for d in help.drawcols()})
                result = help.convert_from_age_group_id(result)
            else:
                data_in = os.path.join("FILEPATH.csv".format(ncode, platform))
                
                # create the rate in filepath based on whether it has excess mortality or not
                if ncode in inj_info.EMR_NCODES:
                    rate_in_name = "FILEPATH.csv"
                else:
                    rate_in_name = "FILEPATH.csv"
                rate_in = os.path.join("FILEPATH")
                
                draw_out_dir = os.path.join("FILEPATH")
                draw_out = os.path.join("FILEPATH.csv".format(ncode, platform))
                if not os.path.exists(draw_out_dir):
                    try:
                        os.makedirs(draw_out_dir)
                    except OSError as e:
                        if e.errno != os.errno.EEXIST:
                            raise
                        pass
                
                result = run_model_injuries(draw_in, data_in, value_in, plain_in, rate_in, effect_in, draw_out, 1000)
                
            # format the results so that we have the identifying columns
            result['location_id'] = locn
            result['platform'] = platform
            
            result['year_id'] = year
            result['sex_id'] = sex
            
            alldata.append(result)
            help.end_timer(start)
            sys.stdout.flush()  # write to log file
        total_time = (time.time() - initime)/60.
        print('Completed {} of {} locations in {} minutes. Will take {} more minutes at this rate'.format(
            loc_pos, num_locs, total_time, (total_time/loc_pos)*(num_locs-loc_pos)))
        sys.stdout.flush()  # write to log file
        
    # concatenate all of the data together
    final = pd.concat(alldata)
    write_results(final, ecode, ncode, platform, year, version)
    print('Finished!')

if __name__ == '__main__':
    ecode = "inj_animal_nonven"
    ncode = "N40"
    version = 1
    year = 1995
    platform = "inpatient"
    repo = "FILEPATH"
    flat_version = 34
    
    main(ecode, ncode, platform, year, repo, version, flat_version)