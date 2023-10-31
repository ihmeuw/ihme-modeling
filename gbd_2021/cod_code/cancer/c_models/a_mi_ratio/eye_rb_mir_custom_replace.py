''' 
Name: eye_rb_mir_custom_replace.py
Description: Replaces mortality MIR inputs with either CODEm or CoDCorrect mortality results for high-income locations only 
How to use: python eye_rb_mir_custom_replace.py <model_source> 
Arguments: model_source (str) : acceptable values [codcorrect, codem]

Contributors: USERNAME
'''

# libraries
import pandas as pd
import pdb 
from db_queries import get_location_metadata 
from get_draws.api import get_draws 

# globals 
MOR_DIR = 'FILEPATH'
INDIR = 'FILEPATH'
CANCER = 'neo_eye_rb'
CANCER_ID = 1009


def replace_with_codcorrect(mir_df : pd.DataFrame, locations_to_replace : list) -> pd.DataFrame: 
    ''' Takes MIR input and replaces mortality data points for selected locations with CoDCorrect mortality results 
    '''
    df_mor = pd.DataFrame()
    draw_cols_mor = ['deaths_{}'.format(i) for i in range(0,1000)] 
    keep_cols_mor = ['location_id','year_id','sex_id','age_group_id','codcorrect_deaths']
    missing_locs = [] 
    for l in locations_to_replace:
        try:
            tmp = pd.read_csv('{}/{}.csv'.format(MOR_DIR, l))

            # calculate mean 
            tmp['codcorrect_deaths'] = tmp[draw_cols_mor].mean(axis=1)
            tmp = tmp[keep_cols_mor]

            df_mor = df_mor.append(tmp)
        except: 
            missing_locs += [l]
    # create sex-combined 
    df_mor = df_mor.groupby(by=['location_id','year_id','age_group_id'])['codcorrect_deaths'].sum().reset_index()
    df_mor['sex_id'] = 3 

    print("WARNING: no mortality data points for these locations: {}".format(missing_locs))
    df = pd.merge(mir_df, df_mor, on=['location_id','year_id','sex_id','age_group_id'], how='left', indicator=True)

    df.loc[df['_merge'].eq('both'), 'deaths'] = df['codcorrect_deaths'] # replace mortality values for HICs 
    df['val'] = df['deaths'] / df['incident_cases'] # recalculate MIRs 

    # drop where MIR > 2, inf, or NA;
    df = df.loc[~((df['val'] > 2) | (df['val'].isna())), ]
    return df 


def replace_with_codem(mir_df : pd.DataFrame, locations_to_replace : list) -> pd.DataFrame: 
    ''' Takes MIR input and replaces mortality data points for selected location with CODEm results 
    '''
    df_mor = pd.DataFrame()
    draw_cols = ['draw_{}'.format(i) for i in range (0,1000)]
    missing_locs = [] 
    for l in locations_to_replace: 
        try:
            tmp_m = get_draws(gbd_id_type='cause_id', 
                            gbd_id= CANCER_ID,
                            source='codem',
                            measure_id=1, 
                            location_id=l,
                            metric_id=1, 
                            status='best',
                            gbd_round_id=7,
                            decomp_step='step3',
                            version_id=686987) #NOTE : best version for GBD2020
            tmp_f = get_draws(gbd_id_type='cause_id', 
                            gbd_id= CANCER_ID,
                            source='codem',
                            measure_id=1, 
                            location_id=l,
                            metric_id=1, 
                            status='best',
                            gbd_round_id=7,
                            decomp_step='step3',
                            version_id=686990) #NOTE : best version for GBD2020 
            tmp = tmp_m.append(tmp_f)

            tmp['codem_deaths'] = tmp[draw_cols].mean(axis=1)
            df_mor = df_mor.append(tmp) 
        except: 
            missing_locs += [l]

    # create 0-5 age group 
    df_0_5 = df_mor.loc[~df_mor['age_group_id'].eq(6), ]
    pdb.set_trace() 
    df_0_5 = df_0_5.groupby(by=['sex_id','year_id','location_id'])['codem_deaths'].sum().reset_index()
    df_0_5['age_group_id'] = 1 
    pdb.set_trace() 
    
    # create sex-combined 
    df_5_9 = df_mor.loc[df_mor['age_group_id'].eq(6), ]
    df_mor_agg = df_0_5.append(df_5_9)

    df_mor_agg = df_mor_agg.groupby(by=['location_id','year_id','age_group_id'])['codem_deaths'].sum().reset_index()
    df_mor_agg['sex_id'] = 3 

    print('WARNING: no mortality data points for these locations: {}'.format(missing_locs))
    df = pd.merge(mir_df, df_mor_agg, on=['location_id','year_id','sex_id','age_group_id'], how='left', indicator=True)

    df.loc[df['_merge'].eq('both'), 'deaths'] = df['codem_deaths']
    df['val'] = df['deaths'] / df['incident_cases']

    # drop where MIR > 2, inf, or NA
    drop_cond = ~((df['val'] > 2) | (df['val'].isna()))
    dropped_entries = df.loc[~drop_cond, ]
    final_df = df.loc[drop_cond, ]
    return final_df


def generate_new_mir_input(mor_source : str) -> None:
    ''' Takes final MI staged data (e.g pre-mir_prep), and replaces mortality data points
        with either CoDCorrect results or CoDEm results for high-income countries only 
    '''
    # load prepped MIR data; subset on just incidence data 
    mir = pd.read_csv('FILEPATH'.format(INDIR)) 
    mir_vr = pd.read_csv('FILEPATH'.format(INDIR)) 

    # list of high-income countries 
    loc_metadata = get_location_metadata(location_set_id=22, gbd_round_id =7)[['location_id','super_region_name','super_region_id']]
    HICs = loc_metadata.loc[loc_metadata['super_region_id'].eq(64), ] #id 64 = high-income 

    # subset to just eye; append VR and CR MIRs 
    df = mir.append(mir_vr)
    df = df.loc[df['acause'].eq(CANCER), ]
    orig_length = len(df)
    df.rename(columns={'cases' :'incident_cases'}, inplace=True)
       
    # get list of HICs in staged MIR 
    df_replace = pd.merge(df, HICs, on='location_id', how='left', indicator=True)
    df_replace = df_replace.loc[df_replace['_merge'].eq('both'), ]
    HICs_locs = df_replace['location_id'].unique().tolist()

    if mor_source == 'codcorrect': 
        new_mir_df = replace_with_codcorrect(df, HICs_locs)
    elif mor_source == 'codem': 
        new_mir_df = replace_with_codem(df, HICs_locs) 
    
    # save  
    new_mir_df.to_csv('FILEPATH.csv'.format(mor_source))
    print('data exported to {}'.format('FILEPATH'.format(mor_source)))
    return 


if __name__ == "__main__": 
    import sys 
    mortality_source = sys.argv[1]
    generate_new_mir_input(mortality_source)