from __future__ import division
from elmo import run
import datetime
import glob
import os
import pandas as pd
from db_queries import get_location_metadata
import argparse 


date = str(datetime.date.today())


def cause_specifics(df, bundle):
    #delete hospital and marketscan 70+
    df = df[df.age_start < 70.]
    #change age_end to less than one if 1
    df.loc[df.age_end == 1, 'age_end'] = 0.999999

    # outlier hospital and marketscan 15+
    df.loc[df.age_start >= 15, 'is_outlier'] = 1

    # Have to do this step before changing any lower/uppers to zero
    # or else python will interpret the None rows unexpectedly and
    # throw them all out 
    if "cv_hospital" in df.columns:
        print "Hospital data!"
        df = df.loc[(df['mean'] <= 1.) & (df['upper'] <= 1.)]
        # if marketscan, check mean only
    else:
        print "Marketscan data!"
        df = df.loc[(df['mean'] <= 1.)]

    # outlier mean=0 and deal with upper/lower values
    df.loc[df['mean'] == 0, 'is_outlier'] = 1
    df.loc[df['mean'] == 0, 'lower'] = None
    df.loc[df['mean'] == 0, 'upper'] = None

    # outlier means
    df.loc[df['mean'] > 0.03, 'is_outlier'] = 1
    # if <1 mean is outside of threshold value, outlier all related location years
    if bundle == 435:
        subset = df.loc[(df.age_end < 1.) & (df['mean'] < 4./10000.), ['location_id', 'year_start', 'year_end']]
        subset.drop_duplicates(inplace=True)
        df = outlier_location_years(subset, df)
    if bundle == 602:
        subset = df.loc[(df.age_end < 1.) & (df['mean'] < 5./10000.), ['location_id', 'year_start', 'year_end']]
        subset.drop_duplicates(inplace=True)
        df = outlier_location_years(subset, df)
    if bundle == 616:
        subset = df.loc[(df.age_end < 1.) & (df['mean'] < 5./10000.), ['location_id', 'year_start', 'year_end']]
        subset.drop_duplicates(inplace=True)
        df = outlier_location_years(subset, df)

    # above threshold
    if bundle == 606:
        subset = df.loc[(df.age_end < 1.) & (df['mean'] > 5./1000.), ['location_id', 'year_start', 'year_end']]
        subset.drop_duplicates(inplace=True)
        df = outlier_location_years(subset, df)
        assert df.loc[(df.age_end < 1.) & (df['mean'] > 5./1000.), 'is_outlier'].all()==1

    # drop data
    if bundle == 437: df = df.loc[df.sex=="Female"]
    if bundle == 438: df = df.loc[df.sex=="Male"]
    if bundle == 610: 
        df = df.loc[df.age_start==0]
        df.loc[:,'age_end'] = 99
        df.loc[df.note_modeler.isnull(), 'note_modeler'] = ""
        df.loc[:,'note_modeler'] += "; age_end changed to birth for use in birth prevalence model"
    if bundle == 638: df = df.loc[df.age_start < 5.]
    return df
    
def outlier_location_years(subset, df):
    if not subset.empty:
        print "Outlier location years!"
        outlier_var = "x"
        subset['indicator'] = outlier_var
        df = df.merge(subset, how='left')
        df.loc[df.indicator == outlier_var, 'is_outlier'] = 1 
        df.drop('indicator', axis=1, inplace=True)
    return df

def hospital_data(df, bundle, cause_name):
    note = "fixes_{USERNAME}"

    # delete hospital data and reupload with new specifications
    # subset to hospital data
    hosp_delete = df.loc[(df.cv_hospital==1), ['bundle_id','seq']]
    destination_file = "{FILEPATH}".format(b=bundle, d=date)
    if not hosp_delete.empty:
        print destination_file
        hosp_delete.to_excel(destination_file, index=False, sheet_name="extraction")
        report = run.upload_epi_data(bundle, destination_file)
        #assert nothing in the report is wrong
        assert (report['request_status'].item()=='Successful')

    hosp_infile = "{FILEPATH}"
    fname = "{b}_v6_{DATE}.xlsx".format(b=bundle)
    hospital = pd.read_excel(os.path.join(hosp_infile, fname), header=0, sheet="extraction")

    # change to mean_3 (mean_0*correction_factor_3)
    for var in ['mean', 'lower', 'upper']:
        hospital.loc[hospital['{}_3'.format(var)].isnull(), '{}_3'.format(var)] = hospital['{}_0'.format(var)] * hospital['correction_factor_3']
        hospital.rename(columns={'{}_3'.format(var): var}, inplace=True)
    # add covariates
    hospital['cv_hospital'] = 1
    hospital['cv_hosp_under1'] = 0
    hospital['cv_hosp_over1'] = 0
    hospital.loc[hospital.age_start == 0, 'cv_hosp_under1'] = 1 
    hospital.loc[hospital.age_start > 0, 'cv_hosp_over1'] = 1
    #drop unneeded columns
    searchfor = ['_0', '_1','_2']
    cols = [c for c in hospital.columns if (searchfor[0] in c) or (searchfor[1] in c) or (searchfor[2] in c)]
    hospital.drop(cols, axis=1, inplace=True)
    # add note
    hospital['note_modeler'] = "Hospital data version 6.3, prepped {DATE} by {USERNAME}. Used Mean 3: inpatient and outpatient, after correction for multiple visits, all diagnoses"

    
    #Outlier locations
    out_loc_list_1 = ["Roraima", "Turkey", "Meghalaya", "Philippines"]
    hospital.loc[hospital['location_name'].isin(out_loc_list_1), "is_outlier"] = 1

    if (bundle==602 or bundle==604 or bundle==620 or bundle==624):
        out_loc_list_2 = ["Lithuania", "Poland", "Croatia", "Romania", "Slovakia", "Czech Republic", "Slovenia"]
        hospital.loc[hospital['location_name'].isin(out_loc_list_2), "is_outlier"] = 1

    hospital = cause_specifics(hospital, bundle)
    hosp_destination_file = "{FILEPATH}"
    print hosp_destination_file
    hospital.to_excel(hosp_destination_file, index=False, sheet_name="extraction")
    report = run.upload_epi_data(bundle, hosp_destination_file)
    #assert nothing in the report is wrong
    assert (report['request_status'].item()=='Successful')

def marketscan_data(df, bundle, cause_name):
    note = "fixes_{USERNAME}"

    # delete marketscan data and reupload with new specifications
    # subset to marketscan data
    searchfor = 'marketscan' #all marketscan data should have marketscan in the column names
    cols = [c for c in df.columns if (searchfor in c)]
    sub_list = []
    for c in cols:
        subset = df.loc[df['{}'.format(c)]==1, ['bundle_id','seq']]
        sub_list.append(subset)
    mrkt_delete = pd.concat(sub_list)

    if not mrkt_delete.empty:
        destination_file = "{FILEPATH}"
        mrkt_delete.to_excel(destination_file, index=False, sheet_name="extraction")
        report = run.upload_epi_data(bundle, destination_file)
        #assert nothing in the report is wrong
        assert (report['request_status'].item()=='Successful')

    mrkt_infile = "{FILEPATH}"
    fname = "ALL_{b}_v3_{DATE}.xlsx".format(b=bundle)
    market = pd.read_excel(os.path.join(mrkt_infile, fname), header=0, sheet="extraction")
    #drop everything with year_start 2000
    market = market[market.year_start != 2000]
    if bundle == 610: market.loc[:, 'is_outlier'] = 1
    #outlier Hawaii
    market.loc[market.location_name=="Hawaii", 'is_outlier'] = 1

    market = cause_specifics(market, bundle)
    mrkt_destination_file = "{FILEPATH}"
    print mrkt_destination_file
    market.to_excel(mrkt_destination_file, index=False, sheet_name="extraction")
    report = run.upload_epi_data(bundle, mrkt_destination_file)
    #assert nothing in the report is wrong
    assert (report['request_status'].item()=='Successful')

def other_data(df, bundle, cause_name):
    print "Other data!"
    
    metadata_set = 35
    round_id = 4

    meta = get_location_metadata(location_set_id=metadata_set, gbd_round_id=4)
    meta = meta[['location_id', 'parent_id']]
    china_df = df.merge(meta, how='left')
    china_loc_id = 6
    #filter china and subnats
    china_df = china_df[(china_df.parent_id==china_loc_id) | (china_df.location_id==china_loc_id)]
    #filter "mtwith" measure
    china_df = china_df.loc[(china_df.measure=="mtwith")]
    if not china_df.empty:
        print "china_df not empty!"
        china_df.loc[:, 'is_outlier'] = 1
        china_df.drop('parent_id', axis=1, inplace=True)

        destination_file = "{FILEPATH}"
        print destination_file
        china_df.to_excel(destination_file, index=False, sheet_name="extraction")
        report = run.upload_epi_data(bundle, destination_file)
        #assert nothing in the report is wrong
        assert (report['request_status'].item()=='Successful')


def adjust_data(bundle, cause_name):

    #delete all data to start over if needed, comment out if not
    bundle = int(bundle)
    #If export=True, save an excel file for the data retrieved in the bundle's download folder.
    df = run.get_epi_data(bundle, export=True)
    df = df[['bundle_id','seq']]
    destination_file = "{FILEPATH}"
    df.to_excel(destination_file, index=False, sheet_name="extraction")
    report = run.upload_epi_data(bundle, destination_file)
    assert (report['request_status'].item()=='Successful')

    # grab latest download if needed, comment out if not
    #download_path = "{FILEPATH}"
    #allFiles = glob.glob(os.path.join(download_path, "*.xlsx"))
    #request_list = []
    #for request in allFiles:
    #   underscore_index = request.rfind('_')
    #   file_ext_index= request.rfind('.')
    #   request_list.append(request[underscore_index+1:file_ext_index])
    #convert strings to int or the sort will be incorrect
    #request_list = map(int, request_list)
    #request_list.sort(reverse=True)
    #fname = "request_{}.xlsx".format(request_list[0])
    #print fname
    
    #Grab the recovery file if needed, comment out if not
    download_path = "{FILEPATH}"
    print fname
    re_up = pd.read_excel(os.path.join(download_path, fname), header=0, sheet="extraction")
    re_up['response_rate'] = None

    #reupload so we can start from the beginning
    destination_file = "{FILEPATH}"
    print destination_file
    re_up.to_excel(destination_file, index=False, sheet_name="extraction")
    report = run.upload_epi_data(bundle, destination_file)
    assert (report['request_status'].item()=='Successful')
    report_file = "{FILEPATH}"
    report.to_csv(report_file, encoding='utf-8')

    #df = pd.read_excel(os.path.join(download_path, fname), header=0, sheet="extraction") #use this for testing then comment out
    df = run.get_epi_data(bundle)
    hospital_data(df, bundle, cause_name)
    df = run.get_epi_data(bundle)
    assert (df.duplicated(subset=df.iloc[:,2:], keep=False).any()) == False
    marketscan_data(df, bundle, cause_name)
    df = run.get_epi_data(bundle)
    other_data(df, bundle, cause_name)
    

#############################################################################
# when called as a script
##############################################################################

if __name__ == '__main__':
    # parse arguments and set variables
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle_id", help="the bundle we're currently working with", type=int)
    parser.add_argument("cause_name", help="the acause we're currently working with (e.g. cong_heart")
    args = vars(parser.parse_args())
    
    # call function
    adjust_data(bundle=args['bundle_id'], cause_name=args['cause_name'])