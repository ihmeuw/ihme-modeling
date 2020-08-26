
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
    #final = final.loc[(final['sex'] != 'Both') & (final['sample_size'] >30)]
    template = pd.read_excel(FILEPATH)
    assert final['mean'].min() > 0, 'why are there zeros here'
    ############################
    ##set cross walks
    
    for i in list(recall_rules.keys()):
        low_bound =recall_rules[i][0]
        high_bound =recall_rules[i][1]
        final[i] = np.where((final.recall_type_value > low_bound) & (final.recall_type_value <= high_bound), 1,0)
        
    ##set other cross walks
    final['cv_survey']= 1
    final['cv_med_contact'] = [ 'consult' in x for x in final.visit_type.tolist()]
    final['cv_med_contact'] *=1
    ## this is annoying
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
    final['is_outlier']=0
    
    final['row_num'] = ""
  
    ## fix these
    #temp.loc[temp.year_end =='n', 'year_end'] =  temp.loc[temp.year_end =='n', 'year_start']

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
# calc standard erros
class calc_se(object):
    ## calc standard erros
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
        print(x_sample['cases']<=0).sum()
        x_sample = x_sample.loc[x_sample['cases']>0]
        #assert (x_sample['cases']<=0).sum()==0,  'no_cases' 
        # check 
        
        
        # calc two differt ways of standard erorr
        
        x_sample_less_5 = x_sample.loc[x_sample.cases<=5].copy()
        x_sample_over_5 = x_sample.loc[x_sample.cases>5].copy()

        ## less than 5
        x_sample_less_5['standard_error'] = (5.-x_sample_less_5['mean'] * x_sample_less_5['sample_size'])/x_sample_less_5['sample_size']

        x_sample_less_5['standard_error'] += x_sample_less_5['mean']*x_sample_less_5['sample_size']*(5./x_sample_less_5['sample_size']**2)/5
        
        ## over 5
        x_sample_over_5['standard_error'] = ((x_sample_over_5['cases'])**.5)/x_sample_over_5['sample_size']
        
        assert  x_sample_less_5['standard_error'].isnull().sum()==0, 'small_sample'
        assert  x_sample_over_5['standard_error'].isnull().sum()==0, 'small_sample'
       
        x_sample_less_5['assign']='less5'
        x_sample_over_5['assign']='over5'
        self.sample_size_calc_se = pd.concat((x_sample_less_5, x_sample_over_5))

    def calc_se(self):
        ## do some checks
        for i in['lower', 'upper', 'mean', 'standard_error', 'sample_size', 'age_start', 'age_end']:
            self.df[i].replace('', 'nan', inplace=True)
            self.df[i] = list(map(np.float, self.df[i]))

        # check to make sure there is either a standard error or a sample size
        assert len(self.df) == len(self.df.loc[(~self.df.standard_error.isnull()) | (~self.df.sample_size.isnull())])

        assert self.df['mean'].isnull().sum() ==0
        self.df.loc[self.df.standard_error==0, 'standard_error']=np.nan
        #just in case
        if 'level_0' in self.df.columns.tolist(): del self.df['level_0']
        self.hold = self.df.loc[~self.df.standard_error.isnull()].copy().reset_index()
        self.hold['assign'] ='reg'
        self.calc_se = self.df.loc[self.df.standard_error.isnull()].copy().reset_index()
        
        self.__calc_se_lower_upper__()
        self.__calc_se_sample_size__()

        if len(self.ci) > 0:
            self.hold = pd.concat((self.hold, self.ci))

        return pd.concat((self.hold, self.sample_size_calc_se ))
################################################################################################
## age sex split
# Purpose: take dismod results and age sex split tabs
class sweet_age_split(object):
    
    def __init__(self, df, dismod_model_num):
        
        self.df=df.copy()
        self.dismod_model_num=dismod_model_num
        
        
    def prep_input_data(self):
        
        ## fix sex.. will fix later
        self.df['sex'] = self.df.sex.str.lower()
        self.df['sex'].replace("male", 1, inplace=True)
        self.df['sex'].replace("female", 2, inplace=True)
        self.df['sex'].replace("both", 3, inplace=True)
        self.df['sex'] = self.df.sex.astype(np.float)
        self.df.rename(columns={'sex':'sex_id'}, inplace=True)
        
        under_1=self.df[self.df.age_end<=1].copy()
        under_1.age_end.replace(1, .999, inplace=True)
        under_1.age_end.replace(.99, .999, inplace=True)
        under_1.age_end = under_1.age_end.astype(np.float)
        ## over 1
        over_1 = self.df[self.df.age_end>1].copy()  
        over_1.age_start = (over_1.age_start.values.astype(np.int)/5)*5
        over_1.age_start.replace(0, 1, inplace=True)
        over_1.age_end = (((over_1.age_end.values.astype(np.int)-1)/5)*5)+4 
        
        self.df = pd.concat((under_1, over_1))
        ## fix this
        self.df.age_end = self.df.age_end.astype(np.float)
        self.df.age_start = self.df.age_start.astype(np.float)
        
        
    def get_dismod_model(self):
        
        self.dismod_model = get_model_results('epi', model_version_id=self.dismod_model_num, 
                                              location_id=self.df.location_id.unique().tolist(),
                                               year_id=-1, sex_id=[1,2], age_group_id=-1)
    def prep_dismod_results(self):
        ## create all sex
        self.dismod_model = self.dismod_model.loc[~self.dismod_model.age_group_id.isin([1,33, 27, 164])]
        print(self.dismod_model)
        
        print('Getting population')
        locs = self.dismod_model.location_id.unique()
        years = self.dismod_model.year_id.unique().tolist()
        ages = self.dismod_model.age_group_id.unique().tolist()
        print(locs)
        print(years)
        print(ages)
        
        
        self.population = get_population(location_id=self.dismod_model.location_id.unique().tolist(),
                                        year_id = self.dismod_model.year_id.unique().tolist(),
                                        age_group_id=self.dismod_model.age_group_id.unique().tolist(), sex_id=[1,2])
        
        self.dismod_model = self.dismod_model.merge(self.population, 
                                                   on=['location_id', 'age_group_id', 'sex_id', 'year_id'],
                                                   how='left')
        
        ## aggregate to all sex
        self.all_sex = self.dismod_model.copy()
        self.all_sex['mean'] *= self.all_sex['population']
        self.all_sex['upper'] *= self.all_sex['population']
        self.all_sex = self.all_sex.groupby(['location_id',  'year_id', 'age_group_id']).sum().reset_index()
        self.all_sex['mean'] /= self.all_sex.population
        self.all_sex.upper /= self.all_sex.population
        
        ## add sex
        self.dismod_model = pd.concat((self.dismod_model[['location_id', 'age_group_id', 'sex_id', 'year_id', 'mean', 'population', 'upper']],
                                       self.all_sex[['location_id', 'age_group_id', 'sex_id', 'year_id', 'mean', 'population', 'upper']]))
        del self.all_sex  
        ## aggregate under 1
        
        under_5 = self.dismod_model.loc[self.dismod_model.age_group_id.isin([2,3,4])]
        self.dismod_model = self.dismod_model.loc[~self.dismod_model.age_group_id.isin([2,3,4])]
        under_5['mean'] *= under_5['population']
        under_5['upper'] *= under_5['population']
        under_5 = under_5.groupby(['location_id',  'year_id', 'sex_id']).sum().reset_index()
        under_5['mean'] /= under_5.population
        under_5.upper /= under_5.population
        under_5.age_group_id=28
        
        ## add age
        self.dismod_model = self.dismod_model.append(under_5)      
        self.dismod_model['se_age'] = (self.dismod_model.upper -self.dismod_model['mean'])/1.96
        
        
    def add_age_group_id(self):
        self.age_map  = pd.read_csv(FILEPATH)
        self.age_map.rename(columns={'age_upper':'age_end',
                               'age_lower':'age_start'}, inplace=True)
        self.age_map.age_start = self.age_map.age_start.astype(np.float)
        self.age_map.age_end = self.age_map.age_end.astype(np.float)
        
        if 'age_group_id' in self.df.columns.tolist(): del self.df['age_group_id']
        self.df = self.df.merge(self.age_map[['age_start', 'age_end', 'age_group_id']],
                                on=['age_start', 'age_end'], how='left')
        ## add under 1
        self.df.loc[(self.df.age_end<1), 'age_group_id']=28
        self.df.loc[(self.df.age_start<=1) & (self.df.age_end>=99), 'age_group_id']=22
        
    def add_year_id(self):
        self.df['year_id'] = np.round(self.df.year_start.values.astype(int)/5)*5
        self.df.loc[self.df.year_id<1990, 'year_id']=1990
        self.df.year_id.replace(2015, 2016, inplace=True)
        
    def adjust_all_age(self):      
                
        ## get factor
        self.all_age['id'] = np.arange(len(self.all_age)) ## later for merging.
        self.factor = self.all_age.merge(self.dismod_model[['age_group_id', 'sex_id', 'location_id', 'year_id', 'mean']],
                                      on=['age_group_id', 'sex_id', 'location_id', 'year_id'], suffixes=('', '_dismod'),
                                     how='left')
        ##
        self.factor['factor'] = self.factor['mean']/self.factor['mean_dismod']
        self.factor['cv_data'] = self.factor['standard_error']/self.factor['mean']
        self.factor = self.factor[['location_id',  'year_id', 'cv_data', 'factor', 'id']]
        self.factor = self.factor.merge(self.dismod_model.loc[(self.dismod_model.age_group_id !=22) &(self.dismod_model.sex_id!=3)],
                                   on=['year_id', 'location_id'])

        ## apply facotr to age pattern
        self.factor['se_new'] = self.factor['mean']*self.factor['cv_data']
        ## propgate error 
        self.factor['standard_error'] =(self.factor['se_new']**2)*(self.factor['se_age']**2) 
        self.factor.standard_error += (self.factor['se_age']**2)*(self.factor['mean']**2) 
        self.factor.standard_error += (self.factor['se_new']**2)*(self.factor['factor']**2)
        self.factor.standard_error **=.5
        self.factor['mean'] *=self.factor['factor']
        self.factor = self.factor.merge(self.age_map[['age_group_id', 'age_end', 'age_start']],
                                   on='age_group_id')

        #### drop these
        del self.all_age['mean']
        del self.all_age['standard_error']
        del self.all_age['age_end']
        del self.all_age['age_start']
        del self.all_age['age_group_id']
        del self.all_age['sex_id']
        del self.factor['upper']
        #3 merge on
        self.all_age = self.all_age.merge(self.factor[['mean', 'standard_error', 'age_group_id','id', 'sex_id',
                                                  'age_start', 'age_end']],on=['id'], how='right')
        del self.all_age['id']## delete the id
        
        
    def adjust_non_standard(self):
  
        collect=pd.DataFrame()
        if 'level_0' in self.non_standard.columns.tolist(): del self.non_standard['level_0']
        self.non_standard = self.non_standard.reset_index()
        if 'level_0' in self.non_standard.columns.tolist(): del self.non_standard['level_0']
            
            
        for i in np.arange(len(self.non_standard)):
            temp = self.non_standard.iloc[i]
            age_end = temp['age_end']
            age_start = temp['age_start']
            sex_id = temp['sex_id']
            location_id = temp['location_id']
            year_id = temp['year_id']
            age_group_id = self.age_map.loc[(self.age_map.age_start>=age_start)&(self.age_map.age_end<=age_end), 'age_group_id'].unique().tolist()

            ## get dismod model
            if sex_id ==3:
                factor = self.dismod_model.loc[(self.dismod_model.sex_id!=3)]
            else:
                factor = self.dismod_model.loc[(self.dismod_model.sex_id==sex_id)]

            factor = factor.loc[factor.location_id==location_id]
            factor = factor.loc[factor.age_group_id.isin(age_group_id)]
            factor = factor.loc[factor.year_id==year_id]

            ## aggregate 
            factor_save =factor.copy()
            factor['mean'] *= factor['population']
            factor = factor.groupby(['location_id','year_id']).sum().reset_index()
            factor['mean'] /= factor['population']
            ## calc new factor
            factor = temp['mean']/factor['mean']
            ## adjust age patter
            factor_save =  factor_save[['location_id', 'age_group_id', 'sex_id', 'year_id', 'mean', 'upper']]
            factor_save['se_age'] = (factor_save['upper'] - factor_save['mean'])/1.96
            factor_save['se_new'] =(temp['standard_error']/temp['mean'])* factor.values[0]
            factor_save['factor']= factor.values[0]

            ## calc se
            factor_save['standard_error'] =(factor_save['se_new']**2)*(factor_save['se_age']**2) 
            factor_save.standard_error += (factor_save['se_age']**2)*(factor_save['mean']**2) 
            factor_save.standard_error += (factor_save['se_new']**2)*(factor_save['factor']**2)
            factor_save.standard_error **=.5
            factor_save['mean'] *= factor_save['factor']
            ## add age end age start
            factor_save = factor_save.merge(self.age_map[['age_group_id', 'age_end', 'age_start']],
                                       on='age_group_id')

            #### exapnd
            del temp['mean']
            del temp['standard_error']
            del temp['age_end']
            del temp['age_group_id']
            del temp['age_start']
            del temp['sex_id']
            del factor_save['upper']

            temp =factor_save.merge(pd.DataFrame(temp).T,
                           on=['location_id', 'year_id'])
            collect = collect.append(temp.copy())

        self.non_standard =collect

    def run_everything(self):
        
        self.prep_input_data()
        self.get_dismod_model()
        self.prep_dismod_results()
        self.add_age_group_id()
        self.add_year_id()
                                                                     
        
        ## just in case
        if 95 in self.df.location_id.unique().astype(np.int):
            print('United kingdom' + str(len(self.df.loc[self.df.location_id==95])))                                                                                           
        
        self.all_age = self.df.loc[(self.df.age_group_id==22)]
        self.df =self.df[self.df.age_group_id!=22]
        self.non_standard = self.df.loc[((self.df.age_group_id.isnull())| (self.df.sex_id==3))]
        self.df = self.df.loc[~((self.df.age_group_id.isnull())| (self.df.sex_id==3))]
        ## don't need to adjust these
        self.adjust_all_age()
        self.adjust_non_standard()    
        self.all_age['tabs_split']=1
        self.non_standard['tabs_split']=1
        df = pd.concat((self.df, self.all_age, self.non_standard) )     
        
        return df
 
###############################################################################################
## compile survey_data
## Purpose: compiles parallelized survey extraction data
## TODO: Make it more extensible and use p.Map to increase speed...
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
            #try:
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
        num_visit =pd.DataFrame()
        #print(path)
        #print(os.listdir(path))
        #files = os.listdir(path)
        files = [ x for x in os.listdir(path) if 'num_visit.csv' in x]
        #print(files)
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
        template = pd.read_excel(FILEPATH)
        convert = pd.read_csv(FILEPATH)
        convert = convert[['ihme_loc_id', "location_id", 'location_name']]

        ihme_loc_id = pd.read_csv(FILEAPTH)
        ihme_loc_id['local_id']= ihme_loc_id.ihme_loc_id
        ihme_loc_id = ihme_loc_id.loc[[x not in convert.ihme_loc_id.tolist() for x in ihme_loc_id.local_id]]
        ihme_loc_id = ihme_loc_id[['ihme_loc_id', "location_id", 'location_name']]
        ihme_loc_id = pd.concat((convert, ihme_loc_id))
        #print(ihme_loc_id)

        fraction =fraction.merge(ihme_loc_id, on='ihme_loc_id', how='left')
        
        # Only do if it exists in the surveys you're extracting
        if len(num_visit) > 0:
            num_visit =num_visit.merge(ihme_loc_id, on='ihme_loc_id', how='left')
            
        ## calc coef of varaition    
        cv = fraction['standard_error']/fraction['mean']
        print('Here')
        fraction.to_csv(FILEPATH)
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
## gen overlap

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
    
############# my interp
## interpolate over dismod results... 

class interp(object):
    
    def __init__(self):
        pass
    
    def __get_model__(self, pass_id):
        self.results = get_model_results('epi', model_version_id=pass_id)
        self.results['se'] = (self.results['mean'] - self.results['lower'])/1.96
        # only vars I care aboult
        self.results = self.results[['location_id', 'age_group_id', 'sex_id', 'mean', 'se', 'year_id']]
        
    ## define interp
    def __my_interp__(self, x):

        new_mean = list(map(np.log, x['mean']))
        f_mean =interp1d(x['year_id'], new_mean)
        mean_val = list(map(np.exp, f_mean(np.arange(1990,2017))))

        new_se = list(map(np.log, x['se']))
        f_se =interp1d(x['year_id'], new_se)
        se_val = list(map(np.exp, f_se(np.arange(1990,2017))))

        return pd.DataFrame({'year_id': np.arange(1990, 2017),
                     'mean' : mean_val,
                     'se': se_val})
    
    def interp(self,  model_id):
        
        self.__get_model__(pass_id=model_id)
        print('got model results')
        self.cw_collect = pd.DataFrame()
        for name, group in self.results.groupby(['location_id', 'age_group_id', 'sex_id']):

            temp = self.__my_interp__(group)
            temp['location_id'] =name[0]
            temp['age_group_id']= name[1]
            temp['sex_id'] = name[2]
            self.cw_collect = self.cw_collect.append(temp.copy())
        both_sex = self.cw_collect.groupby(['age_group_id', 'location_id', 'year_id']).mean().reset_index()
        both_sex['sex_id']=3
        
        return pd.concat((self.cw_collect, both_sex))
 


#####################
### age weight
## Purpose: based off age end age start, year and location
## TODO imporve speed with subprocess
def create_age_weight(df):
    
    """Create mean age weights for wide age groups"""
    
    df['age_diff'] = df['age_end']-df['age_start']
    df['age_mean'] = df['age_end'] + df['age_start']
    df['age_mean'] /=2
    df['age_diff'] = df['age_end'] - df['age_start']
    df_sub = df.loc[df.age_diff>10]
    df_final = df.loc[df.age_diff<=10]
    pops = get_population(single_year_age=1, location_id=df_sub.location_id.unique().tolist(),
               year_id=df_sub.year_start.unique().tolist(), sex_id=[1,2],
                         age_group_id=np.arange(49, 148).tolist())
    pops.rename(columns={'age_group_id': 'age_start'}, inplace=True)
   
    pops['age_start'] -= 49 ## correct these age_groups
    df_sub = df_sub.reset_index()
    
    for i in np.arange(len(df_sub)):
        try:
            temp = df_sub.iloc[i].to_dict()
            pop_sub = pops.loc[(pops.location_id==temp['location_id']) & ( pops.year_id== temp['year_start'])]
            pop_sub = pop_sub.loc[(pop_sub.age_start>=temp['age_start']) & (pop_sub.age_start<= temp['age_end'])]
            ## get sex
            temp['sex'] = [ x.lower() for x in temp['sex']]
            if temp['sex']=='male': pop_sub  = pop_sub.loc[pop_sub.sex_id==1]
            if temp['sex']=='female': pop_sub  = pop_sub.loc[pop_sub.sex_id==2]
            if temp['sex']=='both': pop_sub  = pop_sub.loc[pop_sub.sex_id.isin([1,2])]
            df_sub.ix[i,'age_mean'] = (pop_sub['age_start'] * pop_sub['population']).sum()/pop_sub['population'].sum()

            #print 'worked! ' + str(temp['location_id'])
        except:
            if temp['location_id']==6:
                df_sub.ix[i,'age_mean']=37. ## china is wierd
            else:
                df_sub.ix[i,'age_mean']=41
            print(temp['location_id'])
            pass
    
    return pd.concat((df_final, df_sub))

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
        collect= calc_se().calc_se(collect.copy())
        del collect['lower']
        del collect['upper']
        
        return collect
		
		
		
######################################
###   