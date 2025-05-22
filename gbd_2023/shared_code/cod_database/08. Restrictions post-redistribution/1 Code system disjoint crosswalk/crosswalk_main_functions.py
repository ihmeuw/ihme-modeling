import pandas as pd
import numpy as np
import shutil
import subprocess
import os
from cod_prep.utils import CodSchema
from cod_prep.claude.noise_reduction import NoiseReducer
from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders import (
    get_current_location_hierarchy,
    get_current_cause_hierarchy,
    get_datasets,
    get_nid_metadata,
)

def pull_claude_redistribution_data(CONF, CACHE_FILE_DATA_ORIG, OUT_FILE, nid_loc_years, demographics_to_run):

    shutil.rmtree(CACHE_FILE_DATA_ORIG, ignore_errors=True)
    shutil.rmtree(OUT_FILE, ignore_errors=True)

    id_cols = demographics_to_run.columns.tolist()
    value_cols = ["cf", "cf_se"]
    all_cols = id_cols + value_cols

    nid_meta_df = get_nid_metadata(force_rerun=False).loc[
        lambda d: d["project_id"] == CONF.get_id("project"), :
    ]
    assert not nid_meta_df.loc[:, ["nid", "extract_type_id"]].duplicated().any()

    df = get_claude_data(
        "redistribution",
        nid_extract_records=(
            nid_loc_years.loc[:, ["nid", "extract_type_id"]]
            .drop_duplicates()
            .to_records(index=False)
        ),
        force_rerun=False,
        lazy=False
    )

    original_shape = df.copy().shape[0]
    original_deaths = df.copy()['deaths'].sum()

    df = df.merge((df.groupby(CodSchema.infer_from_data(df).demo_cols)
     .agg({"deaths": "sum"})
     .reset_index()
     .rename(columns={"deaths": "sample_size"})
    ))

    df = df.merge(nid_meta_df.loc[:, ["nid", "extract_type_id", "data_type_id", "code_system_id"]])
    assert df.shape[0] == original_shape
    assert df['deaths'].sum() == original_deaths

    df = df.merge(demographics_to_run)

    original_shape = df.copy().shape[0]
    original_deaths = df.copy()['deaths'].sum()

    df = df.sort_values(by="year_id")

    df['cf'] = (df['deaths'] / df['sample_size']).replace(0, .00001)
    df['cf_se'] = NoiseReducer.get_wilson_std_err(df["cf"], df["sample_size"])
    
    return df

def launch_crosswalk_R_code(
    df, nid_loc_years, demographics_to_run, RUN_ID, R_CROSSWALK_PATH,
    CACHE_FILE_CROSSWALK_IN, CROSSWALK_OUT, CACHE_FILE_DATA_ORIG
    ):
    nid_loc_years = nid_loc_years.query("use_year == True")
    df = df.merge(nid_loc_years)
    
    id_cols = demographics_to_run.columns.tolist()
    value_cols = ["cf", "cf_se"]
    all_cols = id_cols + value_cols

    df = pd.merge(
        df.loc[df["code_system_id"] == 6, all_cols],
        df.loc[df["code_system_id"] == 1, all_cols],
        how="inner",
        on=id_cols,
        suffixes=("_icd9", "_icd10")
    )
    
    df = df.dropna()
    df.to_parquet(CACHE_FILE_CROSSWALK_IN)
    
    if os.path.exists(CROSSWALK_OUT):
        os.remove(CROSSWALK_OUT)

    death_df = pd.read_parquet(CACHE_FILE_DATA_ORIG)
    max_locs = 2
    execRscript = FILEPATH
    r_image = FILEPATH

    for cause_id in df.cause_id.unique():
        low_death_locs = []
        large_death_locs = [] 
        for location_id in df.query(f"cause_id == {cause_id}")['location_id'].unique():
            avg_deaths_per_year = (death_df.query(f"location_id == {location_id} & cause_id == {cause_id} & code_system_id==1")
                                   .groupby(['year_id'],as_index=False)['deaths'].sum()['deaths'].mean())
            if avg_deaths_per_year < 40:
                low_death_locs.append(location_id)
            else:
                large_death_locs.append(location_id)

        for i in range(int(len(low_death_locs) / max_locs) +1):
            submit_locs = low_death_locs[max_locs*i:max_locs + max_locs*i]
            prior = 100
            if submit_locs == []:
                pass
            else:
                len_submit_locs = len(submit_locs)
                submit_locs = str(submit_locs).strip("]").strip("[").replace(",","")
                subprocess.run(f"{execRscript} -i {r_image} -s {R_CROSSWALK_PATH} {RUN_ID} {cause_id} {prior} {len_submit_locs} {submit_locs}", shell=True)
                
        for i in range(int(len(large_death_locs) / max_locs) +1):
            submit_locs = large_death_locs[max_locs*i:max_locs + max_locs*i]
            prior = 1000
            if submit_locs == []:
                pass
            else:
                len_submit_locs = len(submit_locs)
                submit_locs = str(submit_locs).strip("]").strip("[").replace(",","")
                subprocess.run(f"{execRscript} -i {r_image} -s {R_CROSSWALK_PATH} {RUN_ID} {cause_id} {prior} {len_submit_locs} {submit_locs}", shell=True)
        

def apply_and_rake_results(df, WORK_DIR):
    adjustments = pd.read_parquet(FILEPATH)
    adjustments['code_system_id'] = 6
    adjustments = adjustments.drop_duplicates()
    
    df['is_gold_standard'] = True
    df['is_gold_standard'] = df['is_gold_standard'].where(df['code_system_id'] == 1, other=False)
    
    df = df[['nid','site_id',"extract_type_id",'location_id','year_id','age_group_id','sex_id','cause_id','is_gold_standard',
        'deaths','sample_size','cf']]

    df = df.merge(adjustments, how='left')
    df['adjustment_factor'].fillna(1, inplace=True)
    
    df['adjusted_cf'] = df['cf'] * df['adjustment_factor']
    df['adjusted_deaths'] = df['sample_size'] * df['adjusted_cf']
    df['adjusted_deaths'].where(df['adjusted_deaths'] > 0, other=0.0001, inplace=True)
    df = df.drop_duplicates()
    df.to_csv(FILEPATH, index=False)

    demo_cols = ['location_id','age_group_id','sex_id','cause_id','is_gold_standard','adjustment_factor']
    max_year = df.query("is_gold_standard != True").groupby(demo_cols, as_index=False)['year_id'].max()
    max_year = max_year.rename(columns={"year_id":"last_year_of_icd9"})

    df = df.merge(max_year, how='left')
    df['used_in_matched_pairs'] = df['year_id'] >= df['last_year_of_icd9'] - 3
    matched_pair_results = df.query("used_in_matched_pairs == True")
    max_deaths_per_demo = matched_pair_results.groupby(demo_cols, as_index=False)['deaths'].mean()

    max_deaths_per_demo['small_datapoint'] = max_deaths_per_demo['deaths'] <= 1
    max_deaths_per_demo['adjustment_deaths_number_for_small_observations'] = max_deaths_per_demo['deaths'] * (max_deaths_per_demo['adjustment_factor'] - 1)
    max_deaths_per_demo = max_deaths_per_demo.drop(['deaths','adjustment_factor'], axis=1)
    df = df.merge(max_deaths_per_demo, how='left')
    df['adjustment_deaths_number_for_small_observations'] = df['adjustment_deaths_number_for_small_observations'].where((df['deaths'] + df['adjustment_deaths_number_for_small_observations']) >= 0, other= 0.001 - df['deaths'])
    df['adjusted_deaths'] = df['adjusted_deaths'].where(df['small_datapoint'] == False, other=df['deaths'] + df['adjustment_deaths_number_for_small_observations'])
    df.to_csv(FILEPATH, index=False)

    df = df.drop(["last_year_of_icd9", "used_in_matched_pairs",'small_datapoint','adjustment_deaths_number_for_small_observations'], axis=1)
    cause_meta = get_current_cause_hierarchy()
    df = df.query("is_gold_standard == False")

    cause_meta = cause_meta.query("level >= 2")
    cause_meta['level_2_cause'] = cause_meta.apply(lambda x: int(x['path_to_top_parent'].split(',')[2]), axis=1)
    present_causes = cause_meta[cause_meta['cause_id'].isin(list(df.cause_id.unique()))]
    
    df = df.merge(present_causes[['cause_id','level_2_cause']])
    final = pd.DataFrame()
    all_vr_data = (
        get_claude_data(
            "redistribution",
            nid_extract_records=(
                df.loc[:, ["nid","extract_type_id"]]
                .drop_duplicates()
                .to_records(index=False)
            ),
            force_rerun=False,
            lazy=False,
        )
    )

    df['balancing_group'] = df['level_2_cause']
    df.loc[(
        (df['location_id'] == 183) &
        (df['level_2_cause'].isin([974, 491]))
    ), "balancing_group"] = "974, 491"


    for location_id in df.location_id.unique():
        loc_df = df.query(f"location_id == {location_id}") 
        for balancing_group in loc_df.balancing_group.unique():
            loc_cause_df = loc_df[loc_df["balancing_group"] == balancing_group]
            lvl_2_causes = str(balancing_group).split(',')
            lvl_2_causes = [eval(cause) for cause in lvl_2_causes]

            cause_universe_most_deatailed = cause_meta[(
                (cause_meta['parent_id'].isin(lvl_2_causes)) &
                (cause_meta['most_detailed'] == 1)
            )]['cause_id'].unique()
            cause_universe_not_deatailed = cause_meta[(
                (cause_meta['parent_id'].isin(lvl_2_causes)) &
                (cause_meta['most_detailed'] == 0)
            )]['cause_id'].unique()
            
            cause_universe_most_deatailed2 = cause_meta[cause_meta['parent_id'].isin(cause_universe_not_deatailed)].query("most_detailed == 1")['cause_id'].unique()
            cause_universe = np.append(cause_universe_most_deatailed, cause_universe_most_deatailed2)
            assert (cause_meta[cause_meta['cause_id'].isin(cause_universe)]['most_detailed'].unique() == 1).all()
            
            new_causes = cause_universe
            for c in loc_cause_df.cause_id.unique():
                new_causes = new_causes[new_causes != int(c)]
                
            loc_cause_df = loc_cause_df[loc_cause_df['cause_id'].isin(cause_universe)]
            loc_cause_df['change_in_deaths'] =  loc_cause_df['adjusted_deaths'] - loc_cause_df['deaths']
            loc_vr_data = all_vr_data.query(f"location_id == {location_id}")

            loc_vr_data = loc_vr_data[loc_vr_data['cause_id'].isin(new_causes)]
            loc_cause_df = loc_cause_df[['nid','extract_type_id','site_id','location_id','year_id','age_group_id','sex_id','cause_id','level_2_cause','deaths','adjusted_deaths']]

            loc_vr_data = loc_vr_data.merge(cause_meta[['cause_id','level_2_cause']].drop_duplicates(), how='left', on='cause_id')
            loc_vr_data['adjusted_deaths'] = loc_vr_data['deaths']
            loc_vr_data = loc_vr_data[['nid','extract_type_id','site_id','location_id','year_id','age_group_id','sex_id','cause_id','level_2_cause','deaths','adjusted_deaths']]

            loc_cause_df = pd.concat([loc_vr_data, loc_cause_df])

            total_deaths = loc_cause_df.groupby(['location_id','year_id','age_group_id','sex_id'], as_index=False)[['deaths','adjusted_deaths']].sum()
            total_deaths = total_deaths.rename(columns={"deaths":"total_deaths",
                                         "adjusted_deaths":"total_adjusted_deaths"})
            loc_cause_df = loc_cause_df.merge(total_deaths, how='left')
            loc_cause_df['squeeze_factor'] = loc_cause_df['total_deaths'] / loc_cause_df['total_adjusted_deaths']
            loc_cause_df['raked_deaths'] = loc_cause_df['adjusted_deaths'] * loc_cause_df['squeeze_factor']

            check_df = loc_cause_df.groupby(['location_id','year_id','age_group_id','sex_id'], as_index=False)[['deaths','raked_deaths']].sum()
            assert (np.isclose(check_df['deaths'], check_df['raked_deaths'])).all()
            final = pd.concat([final, loc_cause_df])
    final.to_csv(FILEPATH, index=False)

    return final

def save_final_results(final, RUN_ID, ROOT_DIR):
    final['raked_deaths'].fillna(0, inplace=True)
    final['change_in_deaths'] = final['raked_deaths'] - final['deaths']
    locs = get_current_location_hierarchy()
    causes = get_current_cause_hierarchy()
    locs['iso3'] = locs.apply(lambda x: x['ihme_loc_id'][:3], axis=1)
    causes = causes[['cause_id','cause_name']]
    causes2 = causes.rename(columns={"cause_id":"level_2_cause", "cause_name":"level_2_cause_name"})
    locs = locs[['location_id','location_name','iso3']]
    final = final.merge(causes, how='left')
    final = final.merge(causes2, how='left')
    final = final.merge(locs, how='left')
    accepted = pd.read_excel(FILEPATH)
    final = final.merge(accepted, how='inner')
    final = final[['location_id','cause_id','age_group_id','sex_id','year_id','extract_type_id','nid','site_id','change_in_deaths']]
    final.to_csv(f"FILEPATH", index=False)

    return final


