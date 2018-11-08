
#########################################################################################################
# Author: 
# Date: 12/07/2017
# Purpose: process hospital data for meningitis cod proportions and upload
# Notes: edit version or change naming convention depending on how 
version = 'v6'
delete = 1
#########################################################################################################


##import modules
import pandas as pd
import db_queries as db
from elmo import run
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

## start code here ##
for me in range(10494,10498):
	
    ##import data and meningitis map file 
    data = pd.read_csv("FILEPATH".format(me))
    mapping = pd.read_csv("FILEPATH")
    
    #set etiology specific variables, change to integer from float
    bundle = mapping.loc[mapping['me'] == me, 'bundle'].values[0].astype(int)
    acause = mapping.loc[mapping['me'] == me, 'acause'].values[0]
    
    ##clean data
    data = data.loc[data.sample_size != 0]
    data.loc[data['mean'].isnull()==True, 'mean'] = 0
    #add outlier where mean is less than 1%
    data.loc[data['mean'] < 0.01, 'is_outlier'] = 1
    #create seq column
    data['seq'] = ""
    #try to replace source type to see if it will upload
    data['source_type'] = "Vital registration - sample"
    
    if delete == 1:
        ##download epi data to delete database
        print 'deleting {} bundle {} for {} upload'.format(acause, bundle, version)
        del_path = "FILEPATH".format(acause, bundle, version)
        epi_data = run.get_epi_data(bundle)
        epi_data = epi_data[['seq']]
        epi_data.to_excel(del_path, index = False, sheet_name = 'extraction')
        
        ##upload blank sheet
        upload = run.upload_epi_data(bundle, del_path)
    
    ##export data 
    print 'uploading {} bundle {} {}'.format(acause, bundle, version)
    upload_path = "FILEPATH".format(acause, bundle, me, version)
    data.to_excel(upload_path, index = False, sheet_name = 'extraction')
    upload = run.upload_epi_data(bundle, upload_path)