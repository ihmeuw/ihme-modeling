"""
Author: USERNAME
Date: 8/29/2017
Purpose: Setup for DisMod ODE Steps
"""

# SETUP ----------------------------------------------------------- #

# import packages
import xarray as xr
import pandas as pd
import db_queries as db
import os
import sys

# import specific modules from our scripts
from gbd_inj.inj_helpers import help, versions, load_measures, calculate_measures, inj_info, paths


def make_data_in(measures, ecode, version, ncode, platform, loc_dict, year_id, sex_id):
    # don't let stdev be more than half of the value
    df_list = []
    for m, ds in measures.items():
        ds['meas_stdev'] = ds['meas_stdev'].where(ds['meas_stdev'] <= ds['meas_value'] / 2, ds['meas_value'] / 2)
        if (ds['meas_stdev'] <= 0).values.any():
            try:
                minval = ds['meas_stdev'].values[(ds['meas_stdev']>0).values].min()
            except ValueError as e:
                print('Warning, the whole df is 0, setting stdev to be 1e-12')
                print(e)
                minval = .00000001
            #  year, sex, ecode, ncode, platform and dividing by 10000, or setting to 1e-12 if everything is 0
            ds['meas_stdev'] = ds['meas_stdev'].clip(min=minval/10000)
        frame = ds.to_dataframe().reset_index()[['location_id','year_id','sex_id','age_group_id','meas_value','meas_stdev']]
        frame['integrand'] = m
        df_list.append(frame)
    df = pd.concat(df_list)
    df = help.convert_from_age_group_id(df)

    for hierarchy in ["subreg", "region", "super"]:
        df[hierarchy] = "none"
    
    df["x_ones"] = 1

    # create age upper and lower
    df.rename(columns={'age': 'age_lower'}, inplace=True)
    df["age_upper"] = df["age_lower"] + 5.
    df.loc[df['age_lower'] == 0, 'age_upper'] = .01
    df.loc[df['age_lower'] == 0.01, 'age_upper'] = .1
    df.loc[df['age_lower'] == 0.1, 'age_upper'] = 1
    df.loc[df['age_lower'] == 1, 'age_upper'] = 5
    
    df.drop(['sex_id', 'year_id'], inplace=True, axis=1)
    df.set_index('location_id', inplace=True)
    
    for locn in loc_dict:
        dm_out_dir = os.path.join("FILEPATH")
        filename = "FILEPATH.csv".format(ncode, platform)
        filepath = os.path.join("FILEPATH")
        if not os.path.exists(filepath):
            try:
                os.makedirs(filepath)
            except OSError as e:
                if e.errno != os.errno.EEXIST:
                    raise
                pass
    
        # write the CSV for the DisMod ODE to read in
        df.loc[locn].to_csv(os.path.join(filepath, filename), index=False)
    
    return df[['integrand', 'meas_value']]
    

# 3. MAKE VALUE-IN FILES

def condense(df, ncode):
    """Collapses data over each integrand - incidence and excess mortality (mtexcess).
    
    Args:
        df: data frame to collapse -- can contain multiple demographic groups and even causes
    
    Returns:
        df: collapsed df with only one value for incidence and one for excess mortality (if the
            df contains excess mortality)
    """
    df = df.loc[df['meas_value'] > 0]

    if len(df.loc[df['integrand']=='incidence']) == 0:
        df.loc[len(df)] = {'integrand': 'incidence', 'meas_value': 0}
    if ncode in inj_info.EMR_NCODES and len(df.loc[df['integrand']=='mtexcess']) == 0:
        df.loc[len(df)] = {'integrand': 'mtexcess', 'meas_value': 0}
    df = df.groupby('integrand').median().reset_index()
    # we want eta to be 1% of the non-zero median of the integrand values
    df['meas_value'] = df['meas_value'] * 0.01
    df['name'] = "eta_" + df['integrand']
    df.drop('integrand', inplace = True, axis = 1)
    df.rename(columns = {'meas_value': 'value'}, inplace = True)
   
    return df

def get_value(ecode):
    """Pulls value-in parameters from the best initial DisMod model version.
    
    Args:
        ecode (str): acause
        repo (str): code repository that has the ME ids of the ecodes that will be used
    
    Returns:
        Value-in parameters from DisMod pandas cascade
    """
    me_id = help.get_me(ecode)
    model_version = db.get_best_model_versions(
        entity = "modelable_entity",
        ids = me_id,
        status = "best",
        gbd_round_id=help.GBD_ROUND)["model_version_id"].iloc[0].astype(str)
    filepath = os.path.join("FILEPATH.csv")
    
    value = pd.read_csv(filepath)
    cond = value['name'].str.contains("eta")
    value.drop(value[cond].index.values, inplace = True)
    value.loc[value['name'] == "data_like", 'value'] = "log_gaussian"
    
    return value

def make_value_in(df, ecode, ncode, platform, dm_out_dir):
    """Makes the value-in files for the DisMod ODE.
    
    Args:
        df: data frame with all data for a cause (e/n specific)
        ecode (str)
        ncode (str)
        dm_out_dir (str): directory for saving DisMod ODE inputs
        
    Returns:
        None
    """
    
    # format data frame
    raw = pd.concat(df)
    collapsed = condense(raw, ncode)
    
    # format value file and append to collapsed values
    value = get_value(ecode)
    result = collapsed.append(value)
    
    # output the results to value_in directory
    folder = os.path.join("FILEPATH")
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    filepath = os.path.join(folder, "FILEPATH.csv".format(ncode, platform))
    print(filepath)
    result.to_csv(filepath, index = False)
    

# RUN ----------------------------------------------------------------------------------------------------- #

def main(ecode, ncode, platform, version):
    
    start = help.start_timer()
    
    parent = inj_info.ECODE_PARENT[ecode]
    flat_version = versions.get_env(parent, version)
    
    # get demographics
    print("1. Getting demographic, location, and long-term probabilities...")
    dems = db.get_demographics(gbd_team = "epi", gbd_round_id=help.GBD_ROUND)
    metaloc = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)
    locations = help.ihme_loc_id_dict(metaloc, dems['location_id'])
    
    # get long-term probabilities that will be used and long-term standardized-mortality ratios
    lt_probs = calculate_measures.long_term_probs_combined(ncode=ncode)
    smr = load_measures.smr(ncode)
    
    # define DisMod ODE input directory
    dm_out_dir = os.path.join("FILEPATH")
    
    # make the sub-directory for data in files:
    folder = os.path.join("FILEPATH")
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    print("2. Looping through years and sexes to make rate-in and data-in files.")

    value_data = []

    for year in dems["year_id"]:
        for sex in dems["sex_id"]:
            measures = {}
            print('Working on year {} sex {}'.format(year, sex))

            incidence = calculate_measures.long_term_incidence(ecode, version, ncode, platform, year, sex, lt_probs)
            inc_mean = incidence.mean(dim='draw')
            # if the value is less then one in a trillion, set to 0. Otherwise, DisMod can have an overflow issue where
            #    it sets prevalence to 100%
            inc_summary = xr.merge([inc_mean.where(inc_mean > .000000000001, 0).rename('meas_value'),
                                    incidence.std(dim='draw').rename('meas_stdev')])
            measures['incidence'] = inc_summary
            if ncode in inj_info.EMR_NCODES:
                emr = calculate_measures.emr(smr, year, sex, flat_version)
                emr_summary = xr.merge([emr.mean(dim='draw').rename('meas_value'),
                                        emr.std(dim='draw').rename('meas_stdev')])
                measures['mtexcess'] = emr_summary
            
            print('Making data in')
            data = make_data_in(measures, ecode, version, ncode, platform, locations, year, sex)

            value_data.append(data)

            sys.stdout.flush()
                        
    print("Finished making data in files.")
    print("4. Now making the value-in file with the saved data from data in process...")
    
    make_value_in(value_data, ecode, ncode, platform, dm_out_dir)
    
    help.end_timer(start)
    

if __name__ == '__main__':
    ecode = "inj_falls"
    ncode = "N12"
    platform = "inpatient"
    repo = "FILEPATH"
    version = 13
    
    main(ecode, ncode, platform, repo, version)