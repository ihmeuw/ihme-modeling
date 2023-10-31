#from db_queries import get_model_results , get_population
import pandas as pd
import numpy as np
from db_queries import get_population, get_model_results
import os as os
import requests
from io import StringIO

import copy as copy
from multiprocessing import Pool

###########################################################################3
# prep upload
## Purpose: function to prep survey extracts after parallelization
def prep_upload(final,  recall_rules={'cv_2_week_recall':[0., 2.5],
                           'cv_1_month_recall':[2.5, 6.3],
                           'cv_3_month_recall':[6.3, 29.1],
                            'cv_12_month_recall' :[29.1, 999.]}):
    template = pd.read_excel('FILEPATH')
    assert final['mean'].min() > 0, 'zeros here'
    ############################

    for i in list(recall_rules.keys()):
        low_bound =recall_rules[i][0]
        high_bound =recall_rules[i][1]
        final[i] = np.where((final.recall_type_value > low_bound) & (final.recall_type_value <= high_bound), 1,0)
        
    ##set other cross walks
    final['cv_survey']= 1
    final['cv_med_contact'] = [ 'consult' in x for x in final.visit_type.tolist()]
    final['cv_med_contact'] *=1
    final['cases'] = final['sample_size'] * final['mean']
    
    cols_select = ['nid', 'location_name', 'location_id', 'sex', 'year_start', 'cv_sick', 'year_end', 'age_start', 'age_end',\
                  'mean',  'cases', 'sample_size', 'visit_type', 'recall_type_value','cv_med_contact' ]
    for i in list(recall_rules.keys()):
        cols_select.append(i)  
    
    temp = final[cols_select].reset_index(drop=True)
    
    ############################################
    ##Add additional names
    final['recall']=temp['recall_type_value'].values
    final['source_type'] = 'Survey - cross-sectional'
    final['unit_value_as_published']=1
    final['site_memo'] = 'UHC'
    final['sex_issue']= 0
    final['year_issue']=0
    final['age_issue']=0
    final['measure'] ='continuous'
    final['unit_type'] = 'Person'
       
    final['measure_issue'] =0
    final['measure_adjustment']=0
    final['representative_name']= 'Nationally and subnationally representative'
    final['urbanicity_type']= 'Mixed/both'
    final['recall_type'] = 'Period: weeks'
    final['extractor'] = 'USER'
    final['is_outlier']=0
    
    final['row_num'] = ""
  

    final['seq'] = '' 
    final['uncertainty_type'] = ''
    final['input_type'] =''
    
    final['design_effect']=''
    
    final.loc[(final.standard_error.isnull()) | (final.standard_error==0), 'uncertainty_type'] ='Sample size'
  
    
    add_cols = [x not in final.columns.tolist() for x in  template.columns]
    add_cols = template.columns[add_cols]              
                     
    add_cols_test = [x for x in add_cols if x not in final.columns.tolist()]
    for b in add_cols_test:
        final[b] = ''
    final = final.fillna('')
    return final



##############################################################
# Calculate standard error based off lower/upper or sample size
class calc_se(object):
    def __init__(self, df):
        self.df =df.copy()

    def __calc_se_lower_upper__(self):   

        x_ci = self.calc_se[~self.calc_se.lower.isnull()].copy()
        if len(x_ci)>0:
            assert x_ci.lower.isnull().sum()==0
            assert x_ci.upper.isnull().sum()==0

            x_ci['standard_error'] = (x_ci['mean'] - x_ci['lower'])/1.96
            x_ci['assign'] ='lower'
            self.ci= x_ci
        else:
            self.ci =[]


    def __calc_se_sample_size__(self):

       
        x_sample = self.calc_se.loc[(~self.calc_se.sample_size.isnull()) & (self.calc_se.standard_error.isnull())].copy()
        x_sample['cases'] = x_sample['mean']*x_sample['sample_size']


        x_sample = x_sample.loc[x_sample['cases']>0]
        
        

        x_sample_less_5 = x_sample.loc[x_sample.cases<=5].copy()
        x_sample_over_5 = x_sample.loc[x_sample.cases>5].copy()

        x_sample_less_5['standard_error'] = (5.-x_sample_less_5['mean'] * x_sample_less_5['sample_size'])/x_sample_less_5['sample_size']

        x_sample_less_5['standard_error'] += x_sample_less_5['mean']*x_sample_less_5['sample_size']*(5./x_sample_less_5['sample_size']**2)/5
        
        x_sample_over_5['standard_error'] = ((x_sample_over_5['cases'])**.5)/x_sample_over_5['sample_size']
        
        assert  x_sample_less_5['standard_error'].isnull().sum()==0, 'small_sample'
        assert  x_sample_over_5['standard_error'].isnull().sum()==0, 'small_sample'
       
        x_sample_less_5['assign']='less5'
        x_sample_over_5['assign']='over5'
        self.sample_size_calc_se = pd.concat((x_sample_less_5, x_sample_over_5))

    def calc_se(self):
        for i in['lower', 'upper', 'mean', 'standard_error', 'sample_size', 'age_start', 'age_end']:
            self.df[i].replace('', 'nan', inplace=True)
            self.df[i] = list(map(np.float, self.df[i]))

        assert len(self.df) == len(self.df.loc[(~self.df.standard_error.isnull()) | (~self.df.sample_size.isnull())])

        assert self.df['mean'].isnull().sum() ==0
        self.df.loc[self.df.standard_error==0, 'standard_error']=np.nan
        if 'level_0' in self.df.columns.tolist(): del self.df['level_0']
        self.hold = self.df.loc[~self.df.standard_error.isnull()].copy().reset_index()
        self.hold['assign'] ='reg'
        self.calc_se = self.df.loc[self.df.standard_error.isnull()].copy().reset_index()
        
        self.__calc_se_lower_upper__()
        self.__calc_se_sample_size__()

        if len(self.ci) > 0:
            self.hold = pd.concat((self.hold, self.ci))

        return pd.concat((self.hold, self.sample_size_calc_se ))

 
###############################################################################################
## compile survey_data
## Purpose: compiles parallelized survey extraction data
class compile_survey_data(object):
    def __init__(self):
        pass
    def compile_survey_data(self,path,
                             recall_rules,
                             df_reader, 
                            file_type='num_visit.csv'):

        ## helper function for mutliprocessing
        def read_in_data(input_file):
            """ used for multi processing"""

            temp = pd.read_csv(path+'/'+input_file)

            int_stuff = pd.read_pickle(path+'/'+input_file.replace(file_type, 'int_stuff.p'))
            col = list(int_stuff.keys())
            int_stuff = pd.DataFrame(list(zip(*[list(int_stuff.values())]*len(temp)))).T
            int_stuff.columns= col
            temp  = pd.concat((temp,int_stuff), axis=1)
            temp['filepath'] = path+'/'+input_file.replace(file_type, '')
            ## add on sample size
            ss = pd.read_pickle(path+'/'+input_file.replace(file_type, 'ss.p'))
            ss= ss.sort_values(['age_start', 'age_end', 'sex'])
            temp= temp.sort_values(['age_start', 'age_end', 'sex'])
            temp['sample_size']= ss['sample_size']
            temp['util_var_gated'] = ss['util_var_gated']
            ## for fraction
            if 'util_var.csv'==file_type:
                for i in list(recall_rules.keys()):
                    val= temp['recall_type_value'].unique()[0]
                    if (val>recall_rules[i][0]) & (val<=recall_rules[i][1]):
                        temp['recall_adj'] = val
            return temp

        ## num visit
        num_visit = pd.DataFrame()

        files = [ x for x in os.listdir(path) if 'num_visit.csv' in x]
        if len(files) > 0:
            files = [ x for x in os.listdir(path) if 'num_visit.csv' in x]
            file_type='num_visit.csv'
            print(files)

            num_visit= num_visit.append(list(map(read_in_data, files)))
            num_visit.rename(columns={'sick':'cv_sick',
                                      'se':'standard_error',
                                      'iso':'ihme_loc_id'}, inplace=True)
            num_visit['visit_type'] = num_visit['filepath'].apply(lambda x: x.split('_')[-2] +'_avg')
        
        ## fraction
        fraction =pd.DataFrame()
        files = sorted([ x for x in os.listdir(path) if 'util_var.csv' in x])
        file_type='util_var.csv'
        fraction= fraction.append(list(map(read_in_data, files)))
        fraction.rename(columns={'sick':'cv_sick',
                           'se':'standard_error',
                                'iso':'ihme_loc_id'}, inplace=True)
        fraction['visit_type'] = fraction['filepath'].apply(lambda x: x.split('_')[-2] +'_frac')

        ## add on location _id
        template = pd.read_excel('FILEPATH')
        convert = pd.read_csv("FILEPATH")
        convert = convert[['ihme_loc_id', "location_id", 'location_name']]

        ihme_loc_id = pd.read_csv('FILEPATH')
        ihme_loc_id['local_id']= ihme_loc_id.ihme_loc_id
        ihme_loc_id = ihme_loc_id.loc[[x not in convert.ihme_loc_id.tolist() for x in ihme_loc_id.local_id]]
        ihme_loc_id = ihme_loc_id[['ihme_loc_id', "location_id", 'location_name']]
        ihme_loc_id = pd.concat((convert, ihme_loc_id))

        fraction =fraction.merge(ihme_loc_id, on='ihme_loc_id', how='left')
        
        # Only do if it exists in the surveys you're extracting
        if len(num_visit) > 0:
            num_visit =num_visit.merge(ihme_loc_id, on='ihme_loc_id', how='left')
            
        ## calc coef of varaition    
        cv = fraction['standard_error']/fraction['mean']
        print('Here')
        ## assume exponential decline
        fraction = fraction.loc[fraction['mean'] != 1]
        rate = -np.log(1-fraction['mean'])/fraction['recall_type_value'].values
        print(rate)
        fraction['mean']= 1- np.array(list(map(np.exp,-rate*fraction['recall_adj'].values)))
        # recalc new standard error based off cv
        fraction['standard_error'] = fraction['mean']*cv
        del fraction['recall_adj']

        df_reader.loc[df_reader.util_recall_wks.isnull(), 'util_recall_wks'] = df_reader.loc[df_reader.util_recall_wks.isnull(), 'num_visits_recall_wks']
        df_reader.loc[df_reader.util_recall_wks.isnull(), 'util_recall_wks'] = df_reader.loc[df_reader.util_recall_wks.isnull(), 'num_bed_days_recall']
        df_reader['true_recall'] = df_reader['util_recall_wks']

        # Only if num_visit scripts were made
        if len(num_visit) > 0:
            num_visit = num_visit.merge(df_reader[['nid', 'true_recall']], on='nid', how='left', suffixes=('', '_y'))
        
        fraction= fraction.merge(df_reader[['nid', 'true_recall']], on='nid', how='left', suffixes=('', '_y'))

        return num_visit, fraction
########################################
## Purpose: look for age, sex, location and year overlap between two datasets

class overlap(object):
    def __init__(self):
        pass
    def gen_overlap(self,window, df1, df2):
        data ={'df1':df1,
            'df2':df2}
        for i in list(data.keys()):
            temp = data[i]
            temp['year_start'] = list(map(np.float, temp['year_start']))
            temp['year_round'] = temp['year_start'].values/window
            temp['year_round'] = np.round(temp['year_round'],0)
            temp['year_round'] *= window
            for j in ['age_start', 'age_end',  'location_id', 'year_round']:
                temp[j] = list(map(np.float, temp[j]))
            temp['sex'] = list(map(str, temp['sex']))
            data[i]=temp.copy()
        df1 = data['df1']
        df2 = data['df2']
        df2.rename(columns={'mean':'mean_ver2',
                'standard_error' : 'standard_error_ver2'}, inplace=True)
        
        final = df1.merge(df2[['age_start', 'age_end', 'sex', 'location_id', 'mean_ver2', 'standard_error_ver2', 'year_round']],
                          on =['age_start', 'age_end', 'sex', 'location_id', 'year_round'])
        del final['year_round']
        return final
    


####################################
## compile cw
## Purpose : compile for the number of visits given at least one visit
class compile_cw_fraction_to_avg(object):
    def __init__(self):
        pass
    
    def compile_data(self, path):
        """ read in num visits ever"""
    
        ## helper function for mutliprocessing
        def read_in_data(input_file, path=path):
            """ used for multi processing"""
           
            temp = pd.read_csv(path+'/'+input_file)
            int_stuff = pd.read_pickle(path+'/'+input_file.replace('num_visit_ever.csv', 'int_stuff.p'))
            col = list(int_stuff.keys())
            int_stuff = pd.DataFrame(list(zip(*[list(int_stuff.values())]*len(temp)))).T
            int_stuff.columns= col
            temp  = pd.concat((temp,int_stuff), axis=1)

            ## add on sample size
            ss = pd.read_pickle(path+'/'+input_file.replace('num_visit_ever.csv', 'ss.p'))
            ss= ss.sort_values(['age_start', 'age_end', 'sex'])
            temp= temp.sort_values(['age_start', 'age_end', 'sex'])
            temp['sample_size']= ss['sample_size']
            
            util_var = pd.read_csv(path+'/'+input_file.replace('num_visit_ever.csv', 'util_var.csv'))
            util_var.rename(columns={'mean':'mean_util'}, inplace=True)
            del util_var['se']
            temp = temp.merge(util_var, on=['age_start', 'age_end', 'sex'])
            
            
            
            return temp

        ## read in the data
        p= Pool(5)
        collect=pd.DataFrame()
        files = [ x for x in os.listdir(path) if 'num_visit_ever.csv' in x]

        collect= collect.append(list(map(read_in_data, files)))
        collect.rename(columns={'sick':'cv_sick',
                               'se':'standard_error'}, inplace=True)
        
              
        ##calc standard_error
        collect.loc[collect.standard_error==0, 'standard_error']=''
        collect['lower']=''
        collect['upper']=''

        collect= calc_se(collect.copy()).calc_se()
        del collect['lower']
        del collect['upper']
        
        return collect
		
		
