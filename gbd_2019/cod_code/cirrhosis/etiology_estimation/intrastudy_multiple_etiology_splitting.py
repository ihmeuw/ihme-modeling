############################################################################################################################
# Purpose: Etiology proportion splits for cirrhosis and liver cancer; 
# using intrastudy proportions to split multiple etiologies
############################################################################################################################

#import modules
from elmo import run
import pandas as pd
import sys
import numpy as np

############ manual inputs ##############################################
inpath = "FILEPATH"
outpath = "FILEPATH"

#for parent cause
acause = OBJECT
bundle_id = OBJECT
version = OBJECT
decomp = OBJECT
attempt = OBJECT

cause_map_path= "FILEPATH"
#########################################################################

##read in data, subset unnecessary columns
data = pd.read_excel(inpath)
data = data[['nid', 'location_id', 'sex', 'year_start', 'year_end', 'age_start', 'age_end', 'mean', 'upper', 'lower', 'cases', 'sample_size', 'case_name', 'id_var']]
data.loc[(data.nid == 210729) | (data.nid == 125862), 'prop_split'] = 1  
data.loc[(data.nid != 210729) & (data.nid != 125862), 'prop_split'] = 0
data['prop_split'] = data['prop_split'].apply(np.int64)

## read in cause map 
cause_map = pd.read_csv(cause_map_path)

#######################################
########## START CODE HERE ############
#######################################

##create a unique group index for nid, location, sex combinations 
data['group_index'] = pd.Categorical(data['sex'].astype(str) +
                                     data['nid'].astype(str) +
                                     data['year_start'].astype(str) +
                                     data['year_end'].astype(str) +
                                     data['age_start'].astype(str) +
                                     data['age_end'].astype(str) +
                                     data['location_id'].astype(str)).codes

##fill in mean and cases where missing
data.loc[data['cases'].isnull(), 'cases'] = data['mean']*data['sample_size']
data['mean'] = data['cases']/data['sample_size']

##gen total cases and proportions of each row, subtract total to get %unknown
data['total_cases'] = data['cases'].groupby(data['group_index']).transform(sum)
data.loc[data['prop_split'] == 1, 'total_cases'] = data['sample_size']

data['%unknown'] = 1 - (data['total_cases']/data['sample_size'])

##temporarily make any single etiologies fixed = 1
data['fixed'] = np.where(data['case_name'].str.len() > 1, 0, 1)

#loop through each group, and create a list of unique etiologies in that group
for num in data['group_index'].unique():
	case_list = data.loc[data['group_index'] == num]['case_name']
	case_list_single = case_list[case_list.str.len() < 2]
    
    #for each unique etiology, see if there are rows that contain that etiology in multiple etiologies, within the group
	for etiology in case_list_single:
		subset = data.loc[(data['case_name'].str.contains("{}".format(etiology)) == True) & (data['group_index'] == num) & 
		(data['case_name'].str.len() > 1)] 
        
        #assign fixed to be 0 for these single etiologies that exist as multiple etiologies
		if len(subset) > 0:
			data.loc[(data['case_name'] == etiology) & (data['group_index']== num), 'fixed'] = 0

#group by each exclusive group and sum up the fixed means for single etiologies       
sums = data.groupby(['group_index', 'fixed', '%unknown'])['mean'].sum()
sums = sums.reset_index()

#drop fixed = 0, which we dont need to calculate fixed %
sums = sums.loc[sums['fixed'] == 1]

#if there are no fixed groups, calculate %redistribute with unknown only
#calculate the real %fixed by adding the summed means of fixed etiologies and %unknown
sums['%fixed'] = sums['mean'] + sums['%unknown']
sums['%redistribute'] = 1 - sums['%fixed']
sums = sums.rename(columns={'mean':'%etiology_fixed'})
sums = sums.drop(['%unknown', 'fixed'],axis=1)

##merge dataset back to original data, fill in NA %fixed and %etiology_fixed and %redistribute
data = data.merge(sums, how = 'left', on = ['group_index'])
data[['%fixed','%etiology_fixed']]=data[['%fixed','%etiology_fixed']].fillna(0) 
data['%redistribute'].fillna(1-data['%unknown'], inplace=True)
data = data.round(4)

##subset to locations that we need to split only, save an object to remerge nonsplit later
nonsplit_data = data.loc[(data['%redistribute'] == 0) | (data['fixed'] == 1)]
data = data.loc[(data['%redistribute'] != 0) & (data['fixed'] == 0) ]

##expand multiple etiologies data rows
data['etiologies_num'] = data['case_name'].str.len() 
data = pd.DataFrame(np.repeat(data.values, data['etiologies_num'].values, axis=0), columns=list(data))

for num in data['group_index'].unique():
    case_list = data.loc[data['group_index'] == num]['case_name']
    case_list_multiple = case_list[case_list.str.len() > 1]
    
    ##loop over rows that need to be split, subset
    if len(case_list_multiple) > 1:
        for mult_case in case_list_multiple.unique():
            subset = data.loc[(data['case_name'] == mult_case) & (data['group_index'] == num)]
            subset = subset.reset_index(drop=True)
            subset['index']=subset.index 
            
            ##index position of new case for each possible scenario
            if len(mult_case) == 2:
                subset.loc[(subset['index']==0) & (subset['etiologies_num']==2), 'case_name'] = str(mult_case)[0]
                subset.loc[(subset['index']==1) & (subset['etiologies_num']==2), 'case_name'] = str(mult_case)[1] 
            if len(mult_case) == 3:
                subset.loc[(subset['index']==0) & (subset['etiologies_num']==3), 'case_name'] = str(mult_case)[0]
                subset.loc[(subset['index']==1) & (subset['etiologies_num']==3), 'case_name'] = str(mult_case)[1]
                subset.loc[(subset['index']==2) & (subset['etiologies_num']==3), 'case_name'] = str(mult_case)[2]  
            
            ##append subsetted back to original df
            data = data.append(subset)

#subset to single etiology rows only
data = data.loc[data['case_name'].str.len() < 2]

collapse = data.groupby(['group_index', 'case_name'], as_index=False)['cases'].sum()
collapse = collapse.rename(columns={'cases':'adjusted_cases'})
collapse['split_samplesize'] = collapse['adjusted_cases'].groupby(collapse['group_index']).transform(sum)

#merge collapsed data back, calculate split_mean and adj_mean, multiply by %redistribute for final results
data["group_index"] = pd.to_numeric(data["group_index"])

data = data.merge(collapse, how = 'left', on = ['group_index', 'case_name'])
data = data.drop_duplicates(subset=['case_name', 'group_index'], keep="first")
data['split_mean'] = data['adjusted_cases']/data['split_samplesize']
data['adj_mean'] = data['split_mean'] * data['%redistribute']

#append back to nonsplit data, fill in means, drop old means for new
data = data.append(nonsplit_data)
data['adj_mean'] = data['adj_mean'].fillna(data['mean'])
data = data.drop(['mean'], axis =1)
data = data.rename(columns={'adj_mean':'mean'}) 

#save raw output before reformatting
data.to_excel("FILEPATH")

##get rid of columns for cleaning
data = data[['age_start', 'age_end', 'location_id', 'nid', 'sex', 'year_start', 'year_end', 'mean', 'case_name', 'id_var']]

#reformat with original data, drop duplicates to make 1:1 merge
raw_data = pd.read_excel(inpath)
raw_data = raw_data.drop(['mean', 'case_name', 'case_definition', 'age_start', 'age_end', 'year_start', 'year_end', 'sex'],axis=1)
raw_data= raw_data.drop_duplicates(subset=['nid', 'location_id'])

data["location_id"] = pd.to_numeric(data["location_id"])
data["nid"] = pd.to_numeric(data["nid"])

#merge datasets back
data_clean = data.merge(raw_data, how='left', on = ['age_start', 'age_end', 'location_id', 'nid', 'sex', 'year_start', 'year_end', 'mean', 'case_name'])

#save clean file for upload into parent bundle
data_clean.to_excel(FILEPATH, index = False, sheet_name='extraction')

##clean files for upload into etiology specific bundles
data_clean[['sex_issue', 'age_issue', 'measure_issue', 'year_issue', 'is_outlier']] = 0
data_clean['measure_adjustment'] = 0
data_clean[['bundle_id', 'ihme_loc_id', 'input_type', 'cases', 'uncertainty_type_value', 'uncertainty_type', 'note_sr', 'extractor']] = ""
data_clean['note_modeler'] = "custom etiology splits, see parent bundle for raw data"
data_clean['response_rate'] = ""
data_clean['unit_type'] = "Person"
data_clean['unit_value_as_published'] = 1

##split data into corresponding etiologies and save
for etiology in data_clean['case_name'].unique():
    data_etiology = data_clean.loc[data_clean['case_name'] == etiology]
    
    #fix columns for upload
    data_etiology['seq'] = ""
    
    ##create acause and bundle id for each etiology (from map or hardcode here)
    acause = cause_map.loc[cause_map['case_name_shortcut'] == etiology, 'acause'].values[0]
    bundle_id = cause_map.loc[cause_map['case_name_shortcut'] == etiology, 'bundle'].values[0].astype(int)
    data_etiology['bundle_id'] = bundle_id
    
    ##save in specific JWORK location
    file_path = "FILEPATH"
    data_etiology.to_excel('{}'.format("FILEPATH"), index=False, sheet_name="extraction")