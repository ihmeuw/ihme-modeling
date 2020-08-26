import xarray as xr
import pandas as pd
import db_queries as db
import os
import sys

from gbd_inj.inj_helpers import help, versions, load_measures, calculate_measures, inj_info, paths


def make_data_in(measures, ecode, decomp, version, ncode, platform, loc_dict, year_id, sex_id):
    df_list = []
    for m, ds in list(measures.items()):
        ds['meas_stdev'] = ds['meas_stdev'].where(ds['meas_stdev'] <= ds['meas_value'] / 2, ds['meas_value'] / 2)
        if (ds['meas_stdev'] <= 0).values.any():
            try:
                minval = ds['meas_stdev'].values[(ds['meas_stdev']>0).values].min()
            except ValueError as e:
                print('Warning, the whole df is 0, setting stdev to be 1e-12')
                print(e)
                minval = .00000001
            ds['meas_stdev'] = ds['meas_stdev'].clip(min=minval/10000)
        frame = ds.to_dataframe().reset_index()[['location_id','year_id','sex_id','age_group_id','meas_value','meas_stdev']]
        frame['integrand'] = m
        df_list.append(frame)
    df = pd.concat(df_list)
    df = help.convert_from_age_group_id(df)

    for hierarchy in ["subreg", "region", "super"]:
        df[hierarchy] = "none"
    
    df["x_ones"] = 1


    df.rename(columns={'age': 'age_lower'}, inplace=True)
    df["age_upper"] = df["age_lower"] + 5.

    df.loc[df['age_lower'] == 0, 'age_upper'] = .01
    df.loc[df['age_lower'] == 0.01, 'age_upper'] = .1
    df.loc[df['age_lower'] == 0.1, 'age_upper'] = 1
    df.loc[df['age_lower'] == 1, 'age_upper'] = 5
    
    df.drop(['sex_id', 'year_id'], inplace=True, axis=1)
    df.set_index('location_id', inplace=True)
    
    for locn in loc_dict:
        version = version.rstrip()
        dm_out_dir = os.path.join(paths.DATA_DIR, decomp, inj_info.ECODE_PARENT[ecode], str(version), "dismod_ode", ecode)
        filename = "data_in_{}_{}.csv".format(ncode, platform)
        filepath = os.path.join(dm_out_dir, "data_in", loc_dict[locn], str(year_id), str(sex_id), ecode)
        if not os.path.exists(filepath):
            try:
                os.makedirs(filepath)
            except OSError as e:
                if e.errno != os.errno.EEXIST:
                    raise
                pass
    
        df.loc[locn].to_csv(os.path.join(filepath, filename), index=False)
    
    return df[['integrand', 'meas_value']]
    

def condense(df, ncode):
    df = df.loc[df['meas_value'] > 0]
    if len(df.loc[df['integrand']=='incidence']) == 0:
        df.loc[len(df)] = {'integrand': 'incidence', 'meas_value': 0}
    if ncode in inj_info.EMR_NCODES and len(df.loc[df['integrand']=='mtexcess']) == 0:
        df.loc[len(df)] = {'integrand': 'mtexcess', 'meas_value': 0}
    df = df.groupby('integrand').median().reset_index()

    df['meas_value'] = df['meas_value'] * 0.01
    df['name'] = "eta_" + df['integrand']
    df.drop('integrand', inplace = True, axis = 1)
    df.rename(columns = {'meas_value': 'value'}, inplace = True)
   
    return df

def get_value(ecode, decomp):
    me_id = help.get_me(ecode)
    model_version = db.get_best_model_versions(
        entity = "modelable_entity",
        ids = me_id,
        status = "best",
        gbd_round_id=help.GBD_ROUND,
        decomp_step=decomp)["model_version_id"].iloc[0].astype(str)
    filepath = "FILEPATH"
    
    value = pd.read_csv(filepath)
    cond = value['name'].str.contains("eta")
    value.drop(value[cond].index.values, inplace = True)
    value.loc[value['name'] == "data_like", 'value'] = "log_gaussian"
    
    return value

def make_value_in(df, ecode, ncode, platform, dm_out_dir, decomp):
    raw = pd.concat(df)
    collapsed = condense(raw, ncode)
    
    value = get_value(ecode, decomp)
    result = collapsed.append(value)
    
    folder = os.path.join(dm_out_dir, "value_in")
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    filepath = os.path.join(folder, "value_in_{}_{}.csv".format(ncode, platform))
    print(filepath)
    result.to_csv(filepath, index = False)
    

def main(ecode, ncode, platform, decomp, version):
    parent = inj_info.ECODE_PARENT[ecode]
    flat_version = versions.get_env(parent, version)

    version = version.rstrip()

    dm_out_dir = os.path.join(paths.DATA_DIR, decomp, parent, str(version), "dismod_ode", ecode)
    folder = os.path.join(dm_out_dir, "value_in")
    filepath = os.path.join(folder, "value_in_{}_{}.csv".format(ncode, platform))
    
    dems = db.get_demographics(gbd_team = "epi", gbd_round_id=help.GBD_ROUND)
    metaloc = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)
    locations = help.ihme_loc_id_dict(metaloc, dems['location_id'])
    
    lt_probs = calculate_measures.long_term_probs_combined(ncode=ncode, decomp=decomp)
    smr = load_measures.smr(ncode)
    
    version = version.rstrip()
    dm_out_dir = os.path.join(paths.DATA_DIR, decomp, parent, str(version), "dismod_ode", ecode)
    
    
    folder = os.path.join(dm_out_dir, 'data_in')
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    
    value_data = []
    
    for year in dems["year_id"]:
        for sex in dems["sex_id"]:
            measures = {}

            incidence = calculate_measures.long_term_incidence(ecode, decomp, version, ncode, platform, year, sex, lt_probs)
            inc_mean = incidence.mean(dim='draw')
            inc_summary = xr.merge([inc_mean.where(inc_mean > .000000000001, 0).rename('meas_value'),
                                    incidence.std(dim='draw').rename('meas_stdev')])
            measures['incidence'] = inc_summary
            if ncode in inj_info.EMR_NCODES:
                emr = calculate_measures.emr(smr, year, sex, flat_version)
                emr_summary = xr.merge([emr.mean(dim='draw').rename('meas_value'),
                                        emr.std(dim='draw').rename('meas_stdev')])
                measures['mtexcess'] = emr_summary
            
            data = make_data_in(measures, ecode, decomp, version, ncode, platform, locations, year, sex)

            value_data.append(data)

    make_value_in(value_data, ecode, ncode, platform, dm_out_dir, decomp)
