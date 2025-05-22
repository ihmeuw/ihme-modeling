import pandas as pd
import numpy as np
import re
from datetime import datetime
import incidence_subset as isub
import data
import sys
#Custom scripts repository
sys.path.append("FILEPATH")

################################################################################
## Author: USERNAME
## Date: Dec 14, 2017
## Last edited: May 20, 2019
## Description: Script which takes input data (incidence, prevalence, or csmr), 
## formats it, and divides it by LTBI (pulled from the model number).
## It then runs through a few formatting steps to prepare the data for upload 
## through the epi uploader.
##
## To use, simply change the input and output files, and the LTBI model number. 
## Next, on the cluster, in the GBD environment,
## run: 
## > source FILEPATH
## > cd FILEPATH
## > python divide_LTBI.py
## This should take 10-15 minutes, adjusting ages in can take several minutes.
################################################################################



if __name__ == "__main__":
    #Creates datetime tag for your output file
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:10]
    c_date = date_regex.sub('', date_unformatted)

    #Input filepath for the data you want to divide by LTBI (Inc, Prev, or CSMR)
    input_file = pd.read_excel('/FILEPATH/'
                               'step2_prev_screening_adj_inc_csmr_pre_ltbi.xlsx')
    #LTBI model version id
    model_version = 493040
    #Prepare data for dividing by creating GBD age-sex bins
    #input2 = data.adj_data_template(df=input_file)
    input2 = data.custom_adj_data_template(df=input_file)
    #Divide model by LTBI
    #joined = isub.join_to_latent(in_df=input2, model_version_id=model_version)
    joined = isub.join_to_latent_custom_ages(in_df=input2)
    #Save file (UPDATE FILENAME)
    joined.to_excel('FILEPATH/'
                    'step2_prev_screening_adj_inc_csmr_prev_{0}_{1}.xlsx'.format(model_version, c_date),
                    index=False, 
                    sheet_name='extraction',
                    encoding='utf8')




