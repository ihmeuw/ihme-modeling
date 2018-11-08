
# Imports (general)
import argparse
import numpy as np
import os
import subprocess as sp
import sys
import pandas as pd
from os.path import join
from shutil import copyfile
# Imports (this directory)
import data_exclusions as exclude
from prioritize import prioritize_dedupe_all
from uncertainty import generate_upper_lower
# Imports (from top-level)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utilities import qsub, save_tools


################################################################################
# HELPER FUNCTIONS
################################################################################
def use_original_vr(vr, split_db):
    initial = split_db['best'].sum()
    vr.loc[(vr['location_id'] >= 523) &
       (vr['location_id'] <= 573) &
       (vr['dataset'] == "VR") &
       (vr['cause_id'] == 945),"cause_id"] = 851
    vr.loc[(vr['location_id'] == 128) &
       (vr['year'] >= 1980) &
       (vr['year'] <= 1982) &
       (vr['dataset'] == "VR") &
       (vr['cause_id'] == 724),"cause_id"] = 855

    vr.loc[(vr['location_id'] == 33) &
       (vr['year'] >= 1990) &
       (vr['year'] <= 1993) &
       (vr['dataset'] == "VR") &
       (vr['cause_id'] == 724),"cause_id"] = 855

    vr.loc[(vr['location_id'] == 34) &
       (vr['year'] >= 1992) &
       (vr['year'] <= 1994) &
       (vr['dataset'] == "VR") &
       (vr['cause_id'] == 724),"cause_id"] = 855

    split_db_non_vr = split_db.loc[split_db.dataset != 'VR']
    split_db_vr = split_db.loc[split_db.dataset == 'VR']
    split_db_vr['prio'] = 1
    split_db_vr = split_db_vr.drop(labels=['dataset', 'best'], axis=1)

    vr_raw_format = vr[['year', 'location_id', 'cause_id', 
        'dataset', 'age_group_id', 'sex_id', 'deaths']].rename(columns={'year':'year_id', 'deaths':'best'})

    new_vr = vr_raw_format.merge(split_db_vr,
              on = ['year_id', 'location_id', 'cause_id', 'age_group_id', 'sex_id'],
              how = 'left')

    new_vr = new_vr.loc[new_vr.prio == 1]
    new_vr = new_vr.drop(labels=['prio'], axis=1)

    old_vr = split_db.loc[split_db.dataset == 'VR']

    df = old_vr.merge(
      new_vr, on=(list(set(old_vr.columns) - set(['best', 'low', 'high']))),
      how='outer', suffixes=('_old', '_new')
    )
    df = df.drop(['low_old', 'high_old', 'low_new', 'high_new'], axis=1)

    lyc_df = df.groupby(['location_id', 'year_id', 'cause_id', 'dataset'], as_index=False)[['best_old', 'best_new']].sum()
    lyc_df = lyc_df.query('abs(best_new - best_old) > 0')
    lyc_df['scale_factor'] = lyc_df['best_old'] / lyc_df['best_new']
    lyc_df = lyc_df.drop(['dataset', 'best_old', 'best_new'], axis = 1)

    scaled_new_vr = new_vr.merge(lyc_df, on = ['location_id', 'year_id', 'cause_id'], how = 'left')
    scaled_new_vr['scale_factor'] = scaled_new_vr['scale_factor'].fillna(1)

    scaled_new_vr['best'] = scaled_new_vr['best'] * scaled_new_vr['scale_factor']
    scaled_new_vr = scaled_new_vr.drop('scale_factor', axis = 1)


    final = pd.concat([split_db_non_vr, scaled_new_vr], ignore_index = True)
    np.allclose(final['best'].sum(), initial, atol=5)
    
    return final

def sorted_ls(path):
    #grabs the name of the most recent folder in a file path 
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))

def map_shocks_to_cause_ids(shocks_data, cause_map_df):

    cause_map_df = cause_map_df.loc[:,['event_code','cause_id']].drop_duplicates()
    cause_map_dict = dict(zip(cause_map_df['event_code'],cause_map_df['cause_id']))
    shocks_data['cause_id'] = shocks_data['event_type'].map(cause_map_dict)
    return shocks_data


def subset_to_modeling_columns(dataset):
    dataset = dataset.rename(columns={'deaths':'best'})
    # Subset columns
    data_cols = dataset.columns.tolist()
    match_cols = ['location_id','year','event_type','nid','dataset',
                  'cause_id','best','low','high',"event_name"]
    subset = dataset.loc[:,match_cols]
    assert subset.shape[1] > 0, "No matching columns were found..."
    return subset


def aggregate_vr_data(vr):

    # Drop null values for 'deaths'
    vr = vr.loc[vr['best'].notnull(),:]

    vr = vr.fillna(NA_FILL)
    # Drop age and sex identifiers
    vr = vr.drop(labels=['age_group_id','sex_id'],axis=1,errors='ignore')
    # Aggregate across age and sex values, grouping by all other identifiers
    agg_cols = [c for c in vr.columns if c!='best']
    vr_agg = (vr.groupby(by=agg_cols)
                .sum()
                .reset_index(drop=False))
    # Change the filled columns back to nulls
    for col in agg_cols:
        vr_agg[col] = vr_agg[col].replace({NA_FILL:np.nan})
    return vr_agg


def keep_only_complete(db):

    is_complete = (db['year'].notnull() &
                   db['location_id'].notnull() &
                   db['cause_id'].notnull() &
                   db['dataset'].notnull() &
                   db['best'].notnull())
    db = db.loc[is_complete,:]
    for col in ['year','location_id','cause_id']:
        db[col] = db[col].astype(np.int64)
    # Keep only realistic death counts
    for col in ['low','high']:
        db.loc[db[col]<0,col] = np.nan
    for col in ['best']:
        db = db.loc[db[col] >= 0,:]
    db = db.loc[(db['year']>=1950) & (db['year']<=2017)]
    return db



def run_age_sex_splitting(in_filepath, out_filepath, encoding):
    GBD_PYTHON_PATH = ("FILEPATH")
    splitting_code = join(os.path.dirname(os.path.abspath(__file__)),
                          'age_sex_split.py')
    args = ['--infile',in_filepath,'--outfile',out_filepath,'--encoding',encoding]
    sp.check_call([GBD_PYTHON_PATH,splitting_code]+args)
    split_data = pd.read_csv(out_filepath,encoding=encoding)
    return split_data



def gen_draws_save_results(in_filepath, cause_id, encoding, upload_only, mark_best,
                           message):

    base_dir = ''
    save_dir = join(base_dir,os.path.dirname(
                                  os.path.abspath(in_filepath)).split("/")[-1])
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    GBD_PYTHON_PATH = ("FILEPATH")

    if not upload_only:
        splitting_code = join(os.path.dirname(os.path.abspath(__file__)),
                              'draws2.py')
        program_args_a = ('--cause_id {} --infile {} --encoding {} --savedir {} '
                            .format(cause_id,in_filepath,encoding,save_dir))
        draws_jid = qsub.qsub(splitting_code,
                              program_args=program_args_a,
                              python_filepath=GBD_PYTHON_PATH,
                              slots=40,
                              mem_free_gb=70,
                              project="ADDRESS",
                              qsub_name="draws_{}".format(cause_id))
    else:
        draws_jid = None
    # ** SUBMIT THE SECOND JOB, WHERE THE RESULTS ARE SAVED **
    save_results_code = join(os.path.dirname(os.path.abspath(__file__)),
                             'save_shocks_results.py')
    all_years_dir = join(base_dir, "most_recent_all_years") 
    count_space_dir = ""
    program_args_b = '--cause_id {} --savedir {} --message {}'.format(cause_id,
                                                                  count_space_dir,message)
    if mark_best:
        program_args_b = "{} --best".format(program_args_b)
        draws_jid = qsub.qsub(save_results_code,
                              program_args=program_args_b,
                              python_filepath=GBD_PYTHON_PATH,
                              slots=50,
                              mem_free_gb=180,
                              hold_jids=draws_jid,
                              project="proj_shocks",
                              qsub_name="save_{}".format(cause_id))
    return None



################################################################################
# MAIN FUNCTION
################################################################################
def run_shocks_prioritization(db, vr, cause_map_df, out_dir, encoding):
    db = map_shocks_to_cause_ids(db,cause_map_df)
    # Split out ebola data: this does not need prioritization, so it can be 
    #  added near the end
    ebola_rows = db['dataset'].str.contains('ebola')
    ebola_db = (db.loc[ebola_rows,['year','location_id','cause_id',
                        'dataset','age_group_id','sex_id','best','low','high','nid']]
                  .dropna(subset=['location_id']))
    ebola_db['age_group_id'] = ebola_db['age_group_id'].replace({-1:22})
    db = db.loc[~ebola_rows,:]
    db = subset_to_modeling_columns(db)
    db = keep_only_complete(db)
    vr_agg = subset_to_modeling_columns(vr)
    vr_agg = aggregate_vr_data(vr_agg)

    db_vr_combined = pd.concat([db,vr_agg])
    db_vr_combined = keep_only_complete(db_vr_combined)
    # Run pre-prioritization data exclusions
    db_vr_combined = exclude.exclude_before_deduplication(db_vr_combined)
    save_tools.save_pandas(db_vr_combined,
                           filepath=join(out_dir,"FILEPATH"),
                           encoding=encoding)
    # Prioritize and deduplicate data
    print("*** PRIORITIZING ALL SOURCES ***")
    (prioritized_db, no_priority, dropped_db) = prioritize_dedupe_all(
                                        db=db_vr_combined,
                                        cause_map_df=cause_map_df)
    # Run post-prioritization data exclusions and save results
    prioritized_db = exclude.exclude_after_deduplication(prioritized_db)
    # Add ebola data back to the main db
    ebola_db['cause_id'] = 843
    ebola_db['event_name'] = "Ebola"
    prioritized_db = pd.concat([prioritized_db,ebola_db])
    prioritized_db = keep_only_complete(prioritized_db)
    # Format for age-sex splitting
    prioritized_db['sex_id'] = prioritized_db['sex_id'].fillna(3)
    prioritized_db['age_group_id'] = prioritized_db['age_group_id'].fillna(22)
    for col in ['location_id','year','cause_id','sex_id']:
        prioritized_db[col] = prioritized_db[col].astype(np.int32)

    dropped_vr = dropped_db[dropped_db['dataset'] == "VR"]
    prio_vr = prioritized_db[prioritized_db['dataset'] == "VR"]
    vr_to_pass_off = prio_vr.append(dropped_vr)

    save_tools.save_pandas(vr_to_pass_off,
                   filepath=join(out_dir,"FILEPATH"),
                   encoding=encoding)

    prioritized_db.loc[(prioritized_db['location_id'] == 156) &
       (prioritized_db['year'] == 1991) &
       (prioritized_db['dataset'] == "supplements_2015") &
       (prioritized_db['best'] == 6),"nid"] = 137015

    prioritized_db.loc[(prioritized_db['location_id'] == 153) &
       (prioritized_db['year'] == 2009) &
       (prioritized_db['dataset'] == "supplements_2014") &
       (prioritized_db['best'] == 17),"nid"] = 139435

    save_tools.save_pandas(prioritized_db,
                   filepath=join(out_dir,"FILEPATH"),
                   encoding=encoding)

    prioritized_db = (prioritized_db
                        .loc[:,['year','location_id','cause_id','dataset',
                                'age_group_id','sex_id','best','low','high']]
                        .rename(columns={'year':'year_id'}))
    save_tools.save_pandas(dropped_db,
                           filepath=join(out_dir,"FILEPATH"),
                           encoding=encoding)
    save_tools.save_pandas(prioritized_db,
                           filepath=join(out_dir,"FILEPATH"),
                           encoding=encoding)
    save_tools.save_pandas(no_priority,
                           filepath=join(out_dir,"FILEPATH"),
                           encoding=encoding)
    return prioritized_db


def run_shocks_model(vr, out_dir, encoding): 
    # Apply age-sex splitting
    print("Running age sex splitting")
    split_db = run_age_sex_splitting(
                          in_filepath=join(out_dir,"FILEPATH"),
                          out_filepath=join(out_dir,"FILEPATH"),
                          encoding=encoding)

    split_db = use_original_vr(vr, split_db)
    split_db.to_csv(join(out_dir, 'FILEPATH'), index = False, encoding=encoding)

    # Generate confidence intervals
    print("Generating confidence intervals")
    with_uncertainty = generate_upper_lower(split_db)

 
    agg_causes_map = (pd.read_excel("")
                        .loc[:,['cause_id','agg_cause_id']]
                        .drop_duplicates())
    with_uncertainty = (pd.merge(left=with_uncertainty,
                                right=agg_causes_map,
                                on=['cause_id'],
                                how='left')
                          .drop(labels=['cause_id'], axis=1)
                          .rename(columns={'agg_cause_id':'cause_id'}))
    index_cols = ['year_id','location_id','sex_id','age_group_id','cause_id']
    sum_cols = ['val','lower','upper']
    print("Num rows before summing by index: {}".format(with_uncertainty.shape[0]))
    with_uncertainty = (with_uncertainty
                            .loc[:,index_cols + sum_cols]
                            .groupby(by=index_cols)
                            .sum()
                            .reset_index())
    print("Num rows after summing by index: {}".format(with_uncertainty.shape[0]))
    save_tools.save_pandas(with_uncertainty,
                           filepath=join("FILEPATH"),
                           encoding=encoding)

    return with_uncertainty


if __name__ == "__main__":
    # Read input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--infile",type=str,
                        help="The filepath location of the formatted shocks data"
                             " to be used as input for modeling.")
    parser.add_argument("-v","--vrfile",type=str,
                        help="The filepath location of the pulled VR data.")
    parser.add_argument("-c","--causemap",type=str,
                        help="Path to the file linking shocks to GBD cause_ids.")
    parser.add_argument("-o","--outdir",type=str,
                        help="The filepath where all shocks modeling output "
                             "will be saved.")
    parser.add_argument("-e","--encoding",type=str,
                        help="Encoding that will be used to read and write all "
                             "CSV files.")
    parser.add_argument("-xp","--skip_priorization",type=int,
                        help="Skip the prioritization phase, reading it from the run "
                             "folder instead. [1=skip, 0=noskip]")
    cmd_args = parser.parse_args()
    out_dir = cmd_args.outdir
    encoding = cmd_args.encoding
    run_prioritization = (cmd_args.skip_priorization != 1)
    # Read input files
    db = pd.read_csv(cmd_args.infile, encoding=encoding)
    vr = pd.read_csv(cmd_args.vrfile, encoding=encoding)
    cause_map_df = pd.read_excel(cmd_args.causemap)
    # Run main shocks modeling function
    if run_prioritization:
      run_shocks_prioritization(
                       db=db,
                       vr=vr,
                       cause_map_df=cause_map_df,
                       out_dir=out_dir,
                       encoding=encoding)

      modeled_df = run_shocks_model(vr=vr, out_dir=out_dir, encoding=encoding)
    # else:
    modeled_df = pd.read_csv("")

    run_stamp = sorted_ls('')[-1]

    agg_vers = str(int(sorted_ls('')[-1]) + 1)
    all_cause_ids = modeled_df['cause_id'].astype(np.int32).unique().tolist()
    for cause_id in all_cause_ids:
        gen_draws_save_results(in_filepath=join(out_dir,"FILEPATH"),
                               cause_id=cause_id,
                               encoding=encoding,
                               upload_only=True,
                               mark_best=True, 
                               message="Final Run " + run_stamp + " Agg_V" + agg_vers)