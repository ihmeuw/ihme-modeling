## load in packages
import pandas as pd
import numpy as np
import sys as sys
import os as os
import requests
from io import StringIO
import copy as copy

home_dir = sys.argv[1]
write_dir = sys.argv[2]
index =  np.float(sys.argv[3])
proj = sys.argv[4]
os.chdir(home_dir+'code')
from extractor import gdoc_query, extractor
from cluster_utilities  import *

### take arguemnt and read in template reader
df_reader = pd.read_csv(write_dir+'inpatient_template_reader.csv')


## hard code path
data_path =write_dir

## loop thru everything.
func = extractor()
func.format_survey_info_1(df_reader.iloc[np.int(index)].copy().to_dict())

func.check_number_of_responses_2()

iso = func.reader['ISO3'][0]
visit_type = func.reader['type'][0]
title  = func.reader['title'][0]
nid = func.reader['nid'][0]
func.read_in_data_3()

print('done with data')

filename = iso + '_' + nid +'_' + title+'_'+visit_type +'_'+'raw_data.p'
filename = filename.lower()

func.merge_data_4()
func.all_data.to_pickle(data_path+filename)
print('done with all data')

func.get_gender_5()

func.get_age_6()

func.get_utilization_7()

func.get_respond_8()

func.get_surveyset_9()
func.get_recall_10(recall_rules= {4.3: [0., 15.],
                                 52 : [15, 999]})
func.clean_names_11()
func.get_numbers_12(path=data_path)
print('done prep')
os.chdir(home_dir+'/code')
if (func.collect.util_var.sum()>0) | ('util_var' not  in func.collect.columns) :
    call =qsub()

    print('done_call')
    ret= os.system(" ".join(call))
    print('submitted')
    assert ret==0, "Error in passing to R"
else:
    print(df_reader.ix[i, 'filepaths'], i)
    print('did not pass util_var test')