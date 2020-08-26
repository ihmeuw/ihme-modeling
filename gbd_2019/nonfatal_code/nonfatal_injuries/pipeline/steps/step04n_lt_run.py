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
    cmd = 'FILEPATH/dismod_ode \\' + '''
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
    print(('flag is %s' %flag))
    if flag != 0 :
        sys.exit('dismod_ode.py: dismod_ode command failed')

def produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,
                sample_out,draw_out, verbose):
    cmd =   'FILEPATH/model_draw \\' + '''
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
    result = pd.read_csv(filename)
    return result


def run_model_injuries(draw_in, data_in, value_in, plain_in, rate_in, effect_in, draw_out, n_draws):
    cmd = 'FILEPATH/model_injuries {dr} {da} {v} {p} {r} {e} {dr_o} {n}'.format(
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
    info_out = '/dev/null'
    
    run_dismod_ode(data_in,value_in,plain_in,rate_in,
                            effect_in,sample_out,info_out, verbose)
        
    produce_draws(draw_in,value_in,plain_in,rate_in,effect_in,
                sample_out,draw_out, verbose)
    
    if drop_temp:
        os.remove(sample_out)
        
    data = format_draws(draw_out,drawnum)
    return data

def write_path(ecode, ncode, platform, year, decomp, version):
    return (paths.DATA_DIR /
            f"{decomp}/{inj_info.ECODE_PARENT[ecode]}/"
            f"{version}/ode/{ecode}/{ncode}/{platform}/prev_{year}.nc"
    )
    
def write_results(df, ecode, ncode, platform, year, decomp, version):
    version = version.rstrip()
    out_dir = os.path.join(paths.DATA_DIR, decomp, inj_info.ECODE_PARENT[ecode], str(version), "ode", str(ecode), str(ncode), platform)
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    
    df = help.convert_to_age_group_id(df)
    
    df.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id', 'platform'], inplace = True)
    
    arr = etl.df_to_xr(df, wide_dim_name='draw', fill_value=np.nan)

    filepath = write_path(ecode, ncode, platform, year, decomp, version)
    arr.to_netcdf(filepath)
    
def main(ecode, ncode, platform, year, decomp, version, flat_version):
    toc = time.time()

    dems = db.get_demographics(gbd_team="epi", gbd_round_id=help.GBD_ROUND)
    dm_settings = os.path.join(paths.SHARE_DIR, 'dismod_settings')
    version = version.rstrip()
    dm_dir = os.path.join(paths.DATA_DIR, decomp, inj_info.ECODE_PARENT[ecode], str(version), "dismod_ode", ecode)
    metaloc = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)

    filepath = write_path(ecode, ncode, platform, year, decomp, version)
    locations = help.ihme_loc_id_dict(metaloc, dems['location_id'])

    alldata = []
    value_in = os.path.join(dm_dir, "value_in", "value_in_{}_{}.csv".format(ncode, platform))
    draw_in = os.path.join(dm_settings, "draw_in.csv")
    plain_in = os.path.join(dm_settings, "plain_in.csv")
    effect_in = os.path.join(dm_settings, "effect_in.csv")

    v_in = pd.read_csv(value_in)
    
    num_locs = len(locations)
    loc_pos = 0
    initime = help.start_timer()
    for locn in locations:
        loc_pos = loc_pos + 1
        
        for sex in [1,2]:

            start = help.start_timer()
            
            if float(v_in.loc[v_in['name']=='eta_incidence','value'][0]) == 0:
                result = pd.DataFrame({'age_group_id': dems['age_group_id']})
                result = result.assign(**{d: 0 for d in help.drawcols()})
                result = help.convert_from_age_group_id(result)
            else:
                data_in = os.path.join(dm_dir, "data_in", locations[locn], str(year), str(sex), ecode, "data_in_{}_{}.csv".format(ncode, platform))
                
                if ncode in inj_info.EMR_NCODES:
                    rate_in_name = "rate_in_emr.csv"
                else:
                    rate_in_name = "rate_in_no_emr.csv"
                rate_in = os.path.join(paths.DATA_DIR, 'flats', str(flat_version), 'rate_in', str(year), str(sex), locations[locn], rate_in_name)
                
                draw_out_dir = os.path.join(dm_dir, "prev_results", locations[locn], str(year), str(sex))
                draw_out = os.path.join(draw_out_dir, "prevalence_{}_{}.csv".format(ncode, platform))
                if not os.path.exists(draw_out_dir):
                    try:
                        os.makedirs(draw_out_dir)
                    except OSError as e:
                        if e.errno != os.errno.EEXIST:
                            raise
                        pass

                result = run_model_injuries(draw_in, data_in, value_in, plain_in, rate_in, effect_in, draw_out, 1000)
                
            result['location_id'] = locn
            result['platform'] = platform
            
            result['year_id'] = year
            result['sex_id'] = sex
            
            alldata.append(result)
            help.end_timer(start)
            sys.stdout.flush()  # write to log file
        total_time = (time.time() - initime)/60.
        
    final = pd.concat(alldata)

    write_results(final, ecode, ncode, platform, year, decomp, version)
    tic = time.time()

