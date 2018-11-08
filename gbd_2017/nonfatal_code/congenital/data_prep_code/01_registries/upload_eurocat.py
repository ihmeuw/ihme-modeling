import argparse 
import pandas as pd
import numpy as np
import os
from elmo.run import validate_input_sheet, upload_epi_data
from datetime import datetime
import re
from db_queries import get_ids, get_covariate_estimates

# pull in congenital bundle/model/cause mapping file
map_file = "FILEPATH"
map_df = pd.read_excel(map_file, header=0)
nbdpn_crosswalk_bundles = map_df.loc[((map_df.cause=='cong_heart')&
    (~map_df.description.str.startswith("Other", na=False)))|
    (map_df.fullmod_bundle.isin([618,620])), 'fullmod_bundle'].tolist()


def get_date_stamp():
    # get datestamp for output file
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:10]
    date = date_regex.sub('_', date_unformatted)
    return date

def get_envelope_bundles():
    envelope_bundles = map_df.loc[map_df.description.str.startswith("Total", na=False),'fullmod_bundle'].tolist()
    return envelope_bundles

def get_other_bundles():
    other_bundles = map_df.loc[map_df.description.str.startswith("Other", na=False),'fullmod_bundle'].tolist()
    return other_bundles

def get_chromo_acauses():
    return ['cong_chromo','cong_klinefelter','cong_downs','cong_turner']

def get_subcause_birth_prev_bundles(bundle_id, cause_name):
    '''Finds the subcause bundles of the given bundle_id'''
    if cause_name=='cong_chromo':
        subcause_bundles = map_df.loc[(map_df.cause.isin(get_chromo_acauses()))&
            (map_df.fullmod_bundle!=bundle_id)&
            (~map_df.description.str.startswith("Other", na=False)),'birthprev_bundle'].tolist()
    else:
        subcause_bundles = map_df.loc[(map_df.cause==cause_name)&
            (map_df.fullmod_bundle!=bundle_id)&
            (~map_df.description.str.startswith("Other", na=False)),'birthprev_bundle'].tolist()
    if '-' in subcause_bundles: subcause_bundles.remove('-')
    if None in subcause_bundles: subcause_bundles.remove(None)
    return subcause_bundles

def get_live_births_summaries(location_ids, year_ids):
    # best model_version_id at time of upload - 24083
    lvbrth_cov_id = 1106 # live births by sex covariate
    births = get_covariate_estimates(covariate_id=lvbrth_cov_id,gbd_round_id=5, 
        location_id=location_ids, year_id=year_ids, sex_id=[3,2,1])
    births = births[['location_id','year_id','sex_id','mean_value']]
    # Currently, the live births by sex covariate returns most_detailed 
    # sex but we need both sexes combined. The data needed to aggregate sex are 
    # contained in the returned dataframe.
    # Use that info to aggregate sex for this data adjustment
    both_sexes = births.copy()
    both_sexes.loc[:,'sex_id'] = 3
    both_sexes = both_sexes.groupby(['location_id','year_id','sex_id']).sum().reset_index()
    births = pd.merge(births, both_sexes[['location_id','year_id', 'mean_value']], 
        how='left', on=['location_id','year_id'], suffixes=['','_both'], 
        indicator=True)
    assert (births._merge=='both').all()
    births.drop('_merge', axis=1, inplace=True)
    births['birth_prop'] = births['mean_value'] / births['mean_value_both']
    # merge in 'sex' column because data in eurocat spreadsheet does not have
    # sex_ids
    sex_meta = get_ids('sex')
    births = pd.merge(births, sex_meta, on='sex_id')

    return births

def get_col_order(bundle_id):
    col_order = ['bundle_id', 'seq', 'nid', 'underlying_nid', 'location_id',
       'location_name', 'year_start', 'year_end', 'input_type', 'response_rate',
       'source_type', 'sex', 'age_start', 'age_end', 'age_demographer',
       'measure', 'cv_prenatal', 'cv_includes_chromos', 'cv_livestill', 
       'cv_topnotrecorded', 'cv_1facility_only', 'cv_low_income_hosp', 
       'cv_inpatient', 'cv_aftersurgery', 'cv_under_report', 'cv_excludes_chromos',
       'prenatal_top', 'prenatal_fd', 'prenatal_fd_definition', 'cause',
       'modelable_entity_name','field_citation_value',
       'file_path', 'page_num', 'table_num', 'ihme_loc_id',
       'smaller_site_unit', 'site_memo', 'case_name', 'case_definition',
       'case_diagnostics', 'group', 'specificity', 'group_review',
       'note_modeler', 'note_sr', 'year_issue', 'sex_issue', 'age_issue',
       'mean', 'lower', 'upper', 'measure_issue', 'measure_adjustment',
       'standard_error', 'cases', 'effective_sample_size', 'sample_size',
       'unit_type', 'unit_value_as_published', 'uncertainty_type',
       'uncertainty_type_value', 'representative_name', 'urbanicity_type',
       'recall_type', 'sampling_type', 'extractor', 'is_outlier',
       'recall_type_value', 'design_effect']
    if (bundle_id in get_envelope_bundles())&(int(bundle_id)!= 608): 
        col_order.extend(['prop_other', 'raw_cases'])
    elif (bundle_id in map_df.loc[map_df.cause.isin(['cong_turner',
        'cong_klinefelter']),'fullmod_bundle'].tolist()):
        col_order.extend(['birth_prop'])
    if (bundle_id in nbdpn_crosswalk_bundles):
        col_order.extend(['cv_eurocat_to_nbdpn'])
    if (bundle_id in [602, 620]):
        col_order.extend(['cv_registry'])

    return col_order

def get_eurocat_nids():
    #list of NIDs that correspond  to EUROCAT data
    eurocat_nids = [159930, 159937, 128835 , 159941, 163937, 159924, 159938, 
    159926, 159927, 159928, 159929, 159931, 159932, 159933, 159934, 159935, 
    159936, 159939, 159940, 159942, 163938, 163939, 159925, 128835]
    return eurocat_nids

def get_modelable_entity_name(bundle_id):
    me_id = map_df.loc[map_df.fullmod_bundle==bundle_id, 'fullmod_ME'].item()
    me_meta = get_ids('modelable_entity')
    me_name = me_meta.loc[me_meta.modelable_entity_id==me_id, 'modelable_entity_name'].item()
    return "Birth prevalence of {}".format(me_name.lower())

def get_prop_other(other_bundle_id, cause_name):
    #This file excludes taiwan MS data as taiwan data was added into GBD2017 separately and later
    fname = "FILENAME".format(cause_name, int(other_bundle_id))
    directory = "FILEPATH"
    other_props = pd.read_excel(os.path.join(directory,fname))
    other_props = other_props[['age_start','sex', 'cases', 'other_cases', 
    'prop_other']]
    # Currently, we calculate prop_other by most_detailed sex but the info
    # needed to aggregate sex are contained in the imported spreadsheet.
    # Use that info to aggregate sex for this data adjustment
    both_sexes = other_props.copy()
    both_sexes.drop('prop_other', axis=1, inplace=True)
    both_sexes.loc[:,'sex'] = 'Both'
    both_sexes = both_sexes.groupby(['age_start','sex']).sum().reset_index()
    both_sexes['prop_other'] = both_sexes['other_cases'] / both_sexes['cases']
    other_props = pd.concat([other_props, both_sexes])
    other_props.drop(['other_cases', 'cases'], axis=1, inplace=True)
    return other_props

def upload(bundle_id, destination_file, error_path):
    validate = validate_input_sheet(bundle_id, destination_file, error_path)
    assert (validate['status'].item()=='passed')
    status_df = upload_epi_data(bundle_id, destination_file)
    assert (status_df['request_status'].item()=='Successful')
    return status_df

def delete_old_eurocat(bundle_id, df, error_path):
    eurocat_nids = get_eurocat_nids()
    old_eurocat = df.loc[(df.nid.isin(eurocat_nids))|
        (df.underlying_nid.isin(eurocat_nids)), ['bundle_id', 'seq']]
    if not old_eurocat.empty:
        outdir = "FILEPATH"
        destination_file = os.path.join(
            outdir,'delete_eurocat_{}_{}.xlsx'.format(bundle_id, get_date_stamp()))
        old_eurocat.to_excel(destination_file, index=False, sheet_name="extraction")
        upload(bundle_id, destination_file, error_path)

def calc_envelope(df, bundle_id, cause_name):
    envelope = df.copy()
    # Standardize all columns that may present issues during the sum of 
    # multiple different bundles
    me_name = get_modelable_entity_name(bundle_id)
    envelope.loc[:,['case_name','modelable_entity_name']] = me_name
    # set cv_livestill, cv_topnotrecorded, and cv_subset 
    # to the max value of each; 
    # then change cv_subset to cv_under_report later in the script
    max_livestill, max_topnotrecorded, max_subset = envelope[['cv_livestill','cv_topnotrecorded','cv_subset']].max()
    envelope.loc[:,['cv_livestill','cv_topnotrecorded','cv_subset']] = max_livestill, max_topnotrecorded, max_subset
    # Add empty filler string so that pandas does not kick out NaNs during groupby
    filler = ''
    envelope.fillna(value=filler,inplace=True)
    # we'll deal with case definitions and note_SR in the collapse 
    groupby_cols = [c for c in df.columns if c not in ['bundle_id','mean','lower','upper','cases','prenatal_top','prenatal_fd','case_definition','note_SR']]
    envelope = envelope.groupby(groupby_cols ,as_index=False).agg(lambda x : x.sum() if (x.dtype=='float64' or x.dtype=='int64') else ', '.join(x))
    try:
        other_bundle_id = map_df.loc[(map_df.cause==cause_name)&(map_df.description.str.startswith("Other", na=False)),'fullmod_bundle'].item()
        other_props = get_prop_other(other_bundle_id,cause_name)
        envelope = pd.merge(envelope, other_props, on=['age_start', 'sex'], how='left', indicator=True)
        assert (envelope._merge=='both').all()
        envelope.drop('_merge', axis=1, inplace=True)
        envelope['raw_cases'] = envelope['cases']
        envelope['cases'] = (envelope['raw_cases']/(1-envelope['prop_other'])).astype('int64')
        envelope.to_csv("FILEPATH".format(cause_name), index=False, encoding='utf8')
    # only allow certain exceptions based on the acuases that have 'other'
    except ValueError:
        pass
    envelope.replace(to_replace=filler, value=np.nan, inplace=True, regex=True)
    return envelope

def handle_turner_klinefelter(df, bundle_id):
    # Remove the correction factor applied in GBD2016
    # by redoing the split by sex based on updated population numbers
    # and then let the uploader do the rest
    temp = df.copy()
    location_ids = df.location_id.unique().tolist()
    year_ids = df.year_end.unique().tolist()
    birth_props = get_live_births_summaries(location_ids, year_ids)
    birth_props.rename(columns={'year_id':'year_end'},inplace=True)
    if bundle_id==437: # Turner
        temp.loc[:,'sex'] = 'Female'
    elif bundle_id==438: # Klinefelter
        temp.loc[:,'sex'] = 'Male'
    sex_specific = pd.merge(temp, birth_props[['sex','location_id','year_end','birth_prop']], 
        how='left', on=['sex','location_id','year_end'], indicator=True)
    assert (sex_specific._merge=='both').all()
    sex_specific.drop('_merge', axis=1, inplace=True)
    sex_specific['sample_size'] = (sex_specific['sample_size']*sex_specific['birth_prop']).astype('int64')
    return sex_specific

def get_eurocat(bundle_id, birth_prev_bundle, env=False, cause_name=None, subcauses=None):
    eurocat = pd.read_excel("FILEPATH", sheet_name="extraction")
    if env:
        eurocat = eurocat.loc[eurocat.bundle_id.isin(subcauses),:]
        eurocat = calc_envelope(eurocat, bundle_id, cause_name)
    else:
        eurocat = eurocat.loc[eurocat.bundle_id.isin([bundle_id,birth_prev_bundle])]
    if bundle_id in [437,438]:
        eurocat = handle_turner_klinefelter(eurocat,bundle_id)
    eurocat.loc[eurocat.cv_includes_chromos==1,'cv_excludes_chromos'] = 0
    eurocat.loc[eurocat.cv_includes_chromos==0,'cv_excludes_chromos'] = 1
    eurocat['bundle_id'] = bundle_id
    eurocat.rename(columns={'cv_subset':'cv_under_report','note_SR':'note_sr', 
        'outlier_type_id': 'is_outlier'},inplace=True)
    if (bundle_id in nbdpn_crosswalk_bundles):
        eurocat.loc[:,'cv_eurocat_to_nbdpn'] = 1
    if (bundle_id in [602, 620]):
        eurocat.loc[:,'cv_registry'] = 1
    return eurocat

def run(bundle_id, cause_name, code_dir, fname):
    envelope_bundles = get_envelope_bundles()
    if bundle_id in envelope_bundles:
        env=True
        subcauses=get_subcause_birth_prev_bundles(bundle_id, cause_name)
    else: 
        env=False
        subcauses = None
    
    birth_prev_bundle = map_df.loc[map_df.fullmod_bundle==bundle_id,'birthprev_bundle'].item()
    latest_downloads = pd.read_csv(os.path.join(code_dir, fname))
    filepath = latest_downloads.loc[latest_downloads.bundle_id==bundle_id,'path'].item()
    df = pd.read_excel(filepath, sheet_name='extraction')
    eurocat = get_eurocat(bundle_id, birth_prev_bundle, env=env, 
        cause_name=cause_name, subcauses=subcauses)
    # Check that EUROCAT upload will not result in 0's added to
    # columns erroneously. The following columns are not present
    # in the pre-existing, compiled eurocat file. It is OK if these columns
    # are filled with 0's upon upload. Code will break if other columns
    # are found. Determine proper value for new columns found.
    possible_bundle_cols = ['bundle_name','correction_factor_1',
        'correction_factor_2','correction_factor_3','count','count2',
        'cv_england_hospital','cv_hosp_over1','cv_hosp_under1','cv_hospital',
        'cv_marketscan_all_2000','cv_marketscan_all_2010',
        'cv_marketscan_all_2012','cv_marketscan_inp_2000',
        'cv_marketscan_inp_2010','cv_marketscan_inp_2012','cv_no_still_births',
        'cv_under_report','egeoloc','encounter_anydx_ind_rate','file_name',
        'haqi_cf','inpt_primarydx_admit_rate','inpt_primarydx_ind_rate',
        'lower_encounter_anydx_ind_rate','lower_inpt_primarydx_admit_rate',
        'lower_inpt_primarydx_ind_rate','min_value','outlier_note',
        'underlying_field_citation_value','upper_encounter_anydx_ind_rate',
        'upper_inpt_primarydx_admit_rate','upper_inpt_primarydx_ind_rate',
        'cv_worldatlas_to_nbdpn','cv_icbdsr_to_nbdpn',
        'future_cv_1facility_only','future_cv_prenatal','median',
        'cv_diag_post_natal','mad3','future_cv_aftersurgery','mad_n1',
        'cv_diag_chromosomal','cv_bias','mad','prenatal_fd_def','cv_autopsy',
        'cv_echo_us','replace_me','mad_n2','mad2','mad1','source','correction',
        'year','encounter_anydx_ind_rate_mod',
        'upper_encounter_anydx_ind_rate_mod','correction_factor_3_mod',
        'lower_encounter_anydx_ind_rate_mod','cv_literature','cv_marketscan']
    missing_in_file = set(df.columns)-set(possible_bundle_cols)-set(eurocat.columns)
    missing_in_database = set(eurocat.columns)-set(df.columns)
    try:
        assert (set(df.columns)-set(eurocat.columns)).issubset(set(possible_bundle_cols))
    except AssertionError as e:
        e.args += ('Uh oh, extra column(s) somewhere', 
            'Unforseen columns in epi database but not in PRE-EXISTING COMPILED FILE: {}. Columns in PRE-EXISTING COMPILED FILE but not in epi database: {}.'.format(missing_in_file, missing_in_database))
        raise
    # make error_path two directories up from the download file
    error_path = os.path.dirname(os.path.dirname(os.path.dirname(filepath)))
    delete_old_eurocat(bundle_id, df, error_path)

    # Should have cases and sample size for all EUROCAT data, 
    # let the epi uploader calculate mean
    for column in ['mean', 'upper', 'lower']:
        eurocat['{}'.format(column)] = np.nan
    outdir = "FILEPATH"
    # filter for cong_chromo acause & cv_includes_chromos==1,
    # remove the covariate for cong_chromo
    destination_file = os.path.join(outdir, 'upload_eurocat_{}_{}.xlsx'.format(bundle_id,get_date_stamp()))
    eurocat = eurocat[get_col_order(bundle_id)]
    if cause_name in get_chromo_acauses():
        eurocat = eurocat.loc[eurocat.cv_includes_chromos==1,:]
        eurocat.drop(['cv_includes_chromos','cv_excludes_chromos'],axis=1, inplace=True)
    eurocat.to_excel(destination_file, sheet_name="extraction", index=False, encoding='utf8')
    upload(bundle_id, destination_file, error_path)

#############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle_id", help="the bundle we're going to upload to", type=int)
    parser.add_argument("cause_name", 
        help="the acause associated with the bundle we're working with")
    parser.add_argument("code_dir", 
        help="directory where csv containing the paths to all the downloaded bundle files lives")
    parser.add_argument("fname", help="name of final csv file")
    args = vars(parser.parse_args())
    
    # call function
    run(bundle_id=args['bundle_id'], cause_name=args['cause_name'], 
        code_dir=args['code_dir'], fname=args['fname'])