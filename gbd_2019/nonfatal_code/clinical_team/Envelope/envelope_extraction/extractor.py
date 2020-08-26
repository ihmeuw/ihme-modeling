# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 19:31:39 2016

"""
print('run')
import pandas as pd
import numpy as np
import os as os
import itertools
import time
import subprocess
import argparse
import platform
import shutil
import uuid
import heapq

import requests
#from StringIO import StringIO
from io import StringIO, BytesIO #In Python3, StringIO is in io
## some helper functions

def myround(x, base=5):
    return base *np.floor(x/5)


def flatten(l): return flatten(l[0]) + (flatten(l[1:]) if len(l) > 1 else []) if type(l) is list else [l]



def digit_test(x):
    if 'nan' != str(x):
        try:
            np.float(x)
            if np.float(x)>=0:
                return True
            else:
                return False
        except:
            return False
    else:
        return False

def digit_check_2(x):
    if 'nan' != str(x):
        try:
            np.float(x)
            return True
        except:
            return False
    else:
        return False

def is_nan(var):
    check =[x for x in var if str(x) =='nan']
    if (len(var)==1) & (len(check)==1):
        return True
    else:
        return False




class extractor(object):


    def __init__(self ):

        """ Create a reader from a line in the excel extraction form.
        This reader will be used to extract information of interest from the surveys
        """

        ## assign into reader and clean
        self.reader = {}
        self.survey ={}
        self.semicolon_present = {}
        self.collect =pd.DataFrame()

        self.DEBUG_RM=False
        self.DEBUG_sex=False
    def format_survey_info_1(self, read_line):
         """ create columns into dictionary and store everything as a list
         1) split columns with semi colon
         2) create list
         """

         # split and assing to list
         for i in list(read_line.keys()):

            self.reader[i] = str(read_line[i])
            if i not in ['filepaths', 'title']:
                # don't want to screw up filepaths
                self.reader[i] =self.reader[i].replace(' ', '')
                self.reader[i] = self.reader[i].lower()
            if ';' in self.reader[i]:
                self.semicolon_present[i] = 1
                self.reader[i] = [x.split(',') for x in self.reader[i].split(';')]
            else:
                self.semicolon_present[i] = 0
                self.reader[i] = [x for x in self.reader[i].split(',')]

         for i in ['util_var', 'num_visit', 'psu', 'strata', 'num_bed_days', 'util_var_gated' ]:
             self.reader[i] = flatten(self.reader[i])

    def check_number_of_responses_2(self):
        """ When multiple variables apply to one field, make sure there is a mathcing number of items"""

        len_filepath =len(self.reader['filepaths'])
        # handle filepaths

        if self.semicolon_present['merge_ids']!=self.semicolon_present['filepaths'] :
            self.reader['merge_ids'] = [self.reader['merge_ids']]*len_filepath
        assert (len(self.reader['merge_ids']) ==len_filepath ) | (len_filepath==1) , 'merge_ids and filepaths not same length'

        ## handle utilization
        dichot_vars=[ 'util_var_gated', 'util_var']
        opts = ['_yes', '_no', '_miss']
        for i,j in zip(dichot_vars*3, opts*2):
            if self.semicolon_present[i+j]!= self.semicolon_present[i] :
                self.reader[i+j] = [self.reader[i+j]]*len(self.reader[i])
            assert (len(self.reader[i+j]) == len(self.reader[i])) | (len(self.reader[i])==1), i+ j + ' not same length'

        cont =['num_visit', 'num_bed_days']
        for i in cont:
            if self.semicolon_present[i+'_miss']!= self.semicolon_present[i] :
                self.reader[i+'_miss'] = [self.reader[i+'_miss']]*len(self.reader[i])
            assert (len(self.reader[i+j]) == len(self.reader[i])) | (len(self.reader[i])==1) , i+ j + ' not same length'

        #### non formal providers
        cont =['dentist', 'formal_no']
        for i in cont:
            if self.semicolon_present['provider_'+i]!= self.semicolon_present['provider_var'] :
                self.reader['provider_'+i] = [self.reader['provider_'+i]]*len(self.reader['provider_var'])
            assert (len(self.reader['provider_'+i]) == len(self.reader['provider_var'])) | (len(self.reader['provider_var'])==1) , i+ j + ' not same length'


    def read_in_data_3(self):

        """ read in data"""
        self.reader['filepaths'] = flatten(self.reader['filepaths'])
        for i in np.arange(len(self.reader['filepaths'])):
            self.data_path =self.reader['filepaths'][i].strip()
            self.data_path = change_paths(self.data_path)



            ## pick a reader
            if (self.data_path.split('.')[-1].lower() == 'xls') | (self.data_path.split('.')[-1] == 'xlsx' ):
                self.survey.update({str(i): pd.read_excel(self.data_path)})

            elif (self.data_path.split('.')[-1].lower() == 'csv'):
                self.survey.update({str(i): pd.read_csv(self.data_path, low_memory=False)})
            elif (self.data_path.split('.')[-1].lower() == 'p') | (self.data_path.split('.')[-1] == 'pkl'):
                self.survey.update({str(i): pd.read_pickle(self.data_path)})
            elif (self.data_path.split('.')[-1].lower() == 'h5'):
                self.survey.update({str(i): pd.read_hdf(self.data_path, 'table')})

            elif (self.data_path.split('.')[-1].lower() == 'dta'):
                try:
                    self.survey.update({str(i): pd.read_stata(self.data_path, convert_categoricals=False)})
                except:
                    self.survey.update({str(i):  pd.read_stata(self.data_path, convert_categoricals=False, chunksize=1000).data()})
            else:
                raise ValueError('need to check data reader')

    def merge_data_4(self):

        """ merge all datasources"""
        self.all_data = self.survey['0']
        self.all_data.columns = list(map(str, self.all_data.columns))
        self.all_data.columns = [x.lower() for x in self.all_data.columns ]

        for i in np.arange(1, len(self.reader['filepaths'])):

            self.survey[str(i)].columns = [x.lower() for x in self.survey[str(i)].columns]
            self.survey[str(i)].columns = list(map(str, self.survey[str(i)].columns))
            self.all_data = self.all_data.merge(self.survey[str(i)], left_on =self.reader['merge_ids'][0],
                                            right_on =self.reader['merge_ids'][i], how='left', suffixes=('', '_y'))


    def __remove_na__(self,  varname, b,miss_opt=None, yes_opt=None, no_opt =None, ret=True):
        """ remove nas and assing data types"""
        if self.DEBUG_RM: print(('going into remove_na'), miss_opt, yes_opt, no_opt, self.all_data[varname].unique())

        self.all_data[varname] = list(map(str, self.all_data[varname].values))
        self.all_data[varname] = self.all_data[varname].str.lower()
        self.all_data[varname] = self.all_data[varname].str.replace(" ", "")

        if miss_opt is not None:
            miss_opt =list(map(str, miss_opt))
            miss_opt =[str(x).lower() for x in miss_opt if x != '']
            ## take care of missing
            self.all_data.loc[self.all_data[varname].isin(miss_opt),varname] = '-99567'
        # remove spaces
        if no_opt is not None: no_opt  = [x for x in no_opt if x != '']
        if yes_opt is not None: yes_opt = [x for x in yes_opt if x !='']


        self.all_data.loc[self.all_data[varname].isin(['.', "", 'no answer/refused', 'na', 'nan', 'nan']),varname] = '-99567'

        if self.DEBUG_RM: print(('after str convert'), miss_opt, yes_opt, no_opt, self.all_data[varname].unique())
        ## now try a float
        try:

            self.all_data[varname] = list(map(np.float, self.all_data[varname].values))

            if miss_opt is not None:
                float_miss_opt = [x for x in miss_opt if ('nan'==x) | (digit_check_2(x)) ]
                miss_opt =list(map(np.float, float_miss_opt))
                self.all_data.loc[self.all_data[varname].isin(miss_opt),varname] = -99567
            if yes_opt is not None:yes_opt = list(map(np.float, yes_opt ))
            if no_opt is not None: no_opt =list(map(np.float, no_opt))

            if self.DEBUG_RM: print(('float convert'), float_miss_opt, yes_opt, no_opt, self.all_data[varname].unique())

            if ret : return miss_opt, yes_opt, no_opt
        except:
            print('failed numeric remove')
            ## I only want numerics
            self.all_data[varname] = list(map(str, self.all_data[varname].values))
            self.all_data[varname] = self.all_data[varname].str.lower()
            self.all_data[varname] = self.all_data[varname].str.replace(" ", "")
            if miss_opt is not None:
                miss_opt =list(map(str, miss_opt))
                miss_opt = [str(x).lower() for x in miss_opt ]
            if yes_opt is not None:
                yes_opt = list(map(str, yes_opt))
                yes_opt = [str(x).lower() for x in yes_opt]
            if no_opt is not None:
                no_opt = list(map(str, no_opt))
                no_opt = [str(x).lower() for x in no_opt]

            if self.DEBUG_RM: print(('default to string'), miss_opt, yes_opt, no_opt,  self.all_data[varname].unique())
            ## check to see if there are a mixture of digitis

            if len([ x for x in self.all_data[varname].unique() if digit_test(x)])>0:
                if '99567' not in [ x for x in self.all_data[varname].unique() if digit_test(x)]:
                    print((self.all_data[varname].unique()))
                    print(b)
                    print(varname)
                    raise ValueError('there are a mixture of strings and numbers')


            if ret: return miss_opt, yes_opt, no_opt



    def __yes_no__(self, reader_var):
        """Take care of binary variables"""

        b= reader_var
        ### need to iterate through different question in case there are multiple
        for t in np.arange( len(self.reader[b])):
            # name these
            varname = self.reader[b][t]

            if len(self.reader[b])==1:

                ## Fill with option
                if b+'_no' in list(self.reader.keys()): no_opt = self.reader[b+'_no']
                else:no_opt =[np.nan]
                if b+'_miss' in list(self.reader.keys()): miss_opt = self.reader[b+'_miss']
                else:miss_opt =[np.nan]
                if b+'_yes' in list(self.reader.keys()): yes_opt = self.reader[b+'_yes']
                else: yes_opt =[np.nan]


            if len(self.reader[b])>1:
                ## Fill with option
                if b+'_no' in list(self.reader.keys()): no_opt = self.reader[b+'_no'][t]
                else:no_opt =[np.nan]
                if b+'_miss' in list(self.reader.keys()): miss_opt = self.reader[b+'_miss'][t]
                else:miss_opt =[np.nan]
                if b+'_yes' in list(self.reader.keys()): yes_opt = self.reader[b+'_yes'][t]
                else: yes_opt =[np.nan]



             ## incase nan's count as zeros
            if not is_nan(no_opt):
                temp = [str(x) for x in no_opt if 'nan' ==str(x)]
                other = [str(x) for x in no_opt if 'nan' != str(x)]
                if len(temp)>0:
                    print('nan as no?')
                    self.all_data[varname] = list(map(str, self.all_data[varname].values))
                    self.all_data.loc[self.all_data[varname].isin(['nan']), varname] =other[0]

            # get options
            if self.DEBUG_RM: print(varname, b)
            miss_opt, yes_opt, no_opt = self.__remove_na__(miss_opt=miss_opt, no_opt=no_opt,
                                                      yes_opt=yes_opt, varname=varname, b=b)



            ## get rid of varname
            self.all_data.loc[self.all_data[varname].isin(miss_opt),varname] = '-99567'
            try:
                self.all_data[varname] = list(map(np.float, self.all_data[varname].values))
            except:
                pass

            if self.DEBUG_RM: print(self.all_data[varname].unique())
            ## if yes and no resposnes are both present
            if not is_nan(yes_opt) and not is_nan(no_opt) :                ## do this to rpevent overwriting
                if self.DEBUG_RM: print(('first '), self.all_data[varname].unique(), no_opt, yes_opt)
                self.all_data.loc[self.all_data[varname].isin(yes_opt), varname] =1000
                self.all_data.loc[self.all_data[varname].isin(no_opt), varname] =0
                self.all_data.loc[self.all_data[varname]==1000, varname] =1

            ## if only yes responses are present
            elif not is_nan(yes_opt) and is_nan(no_opt):
                if self.DEBUG_RM: print(('second '), self.all_data[varname].unique())
                self.all_data.loc[self.all_data[varname].isin(yes_opt), varname]=1000.
                self.all_data.loc[(self.all_data[varname]!=1000.) & (self.all_data[varname].isnull()), varname]=0.
                self.all_data.loc[self.all_data[varname]==1000., varname] =1

            ## if only no responses are present
            elif is_nan(yes_opt) and not is_nan(no_opt):
                if self.DEBUG_RM: print(('third '), self.all_data[varname].unique())
                self.all_data.loc[self.all_data[varname].isin(no_opt), varname]=-1000.
                self.all_data.loc[(self.all_data[varname]!=-1000.) &(self.all_data[varname].isnull()) , varname]=1.
                self.all_data.loc[self.all_data[varname]==-1000., varname] =0.

            else:

                print(varname)
                raise ValueError('Error in extraction of binary responses in get_data')

            ## try converting to numeric
            try:
                self.all_data[varname] = list(map(np.float, self.all_data[varname].values))
                self.all_data[varname] = np.round(self.all_data[varname].values,2)
            except:
                print(varname)
                print(self.all_data[varname].unique())
                raise ValueError('cant convert')

            ## finally assign nans
            self.all_data.loc[self.all_data[varname]==-99567, varname] =np.nan
            if self.DEBUG_RM: print(('after number assignment'), self.all_data[varname].unique())
            ## look and see if there are other entries that we can remove
            self.all_data.loc[self.all_data[varname]>1, varname]=999.
            self.all_data.loc[self.all_data[varname]<0, varname]=999.
            ## count how many 999s
            number_of_999 =len(self.all_data.loc[self.all_data[varname]==999.])
            ## if large bring to my attention
            if number_of_999/len(self.all_data)>.05:
                raise ValueError('there are some other variables in here that we may want to look at')
            ## repalce 999s
            self.all_data.loc[self.all_data[varname]==999., varname] = np.nan
            if self.DEBUG_RM: print(('after number clean up'), self.all_data[varname].unique())
            ## internal check
            if len([ x for x in map(str, self.all_data[varname].unique()) if x not in ['0.0', '1.0', 'nan']])!=0.:
                print((self.all_data[varname].unique()))
                print(b)
                print(varname)
                raise ValueError('values are outside of [0, 1, np.nan]')

            ## really try to convet to floats... just in case anything strange happened
            try:
                self.all_data[varname] = list(map(np.float, self.all_data[varname].values))
            except:
                print(b)
                raise ValueError('util_var and num visits are not numbers')

            if self.DEBUG_RM: print(('end_loop'), self.all_data[varname].unique())
        ####################
        #sum across or max across
        ####################
        self.collect[b] =self.all_data[self.reader[b]].max(axis=1).values
        self.collect.loc[self.collect[b]>0., b]=1.

    def __cont__(self, reader_val):

        """ for continuous variables"""
        b = reader_val

        ## if multiple responses
        for t in np.arange(len(self.reader[b])):
            varname = self.reader[b][t]
            # convert to stirng
            if b+'_miss' in list(self.reader.keys()):
                if self.semicolon_present[b] ==1:
                    miss_opt = [ str(x) for x in self.reader[b+'_miss'][t]]
                else:
                    miss_opt = [ str(x) for x in self.reader[b+'_miss']]
            else:
                miss_opt = None
            ## remove NA values
            self.__remove_na__(miss_opt=miss_opt, ret=False,
                            varname=varname, b=b)

            ## convert to  floats
            try:
                self.all_data[varname] = list(map(np.float, self.all_data[varname].values))
            except:
                print(self.all_data[varname].unique())
                raise ValueError('unable to assign ' +b +' to float')

            # don't trust anything less than 1
            self.all_data.loc[self.all_data[varname] <0, varname] =np.nan


        ## summ across everything now
        self.collect[b] =self.all_data[self.reader[b]].sum(axis=1, skipna=True).values



    def get_gender_5(self):
        """ assign genders"""

        if 'nan' not in list(map(str,self.reader['sex'])):

            ## use function to assign male/female miss
            miss, male, female =self.__remove_na__(miss_opt =self.reader['sex_miss'], yes_opt =self.reader['male_vals'],
                              no_opt =self.reader['female_vals'], varname =self.reader['sex'][0],
                              b='sex')
            if self.DEBUG_sex:  print((miss, male, female, self.all_data[self.reader['sex'][0]].unique()))
            self.collect['sex'] = self.all_data[self.reader['sex'][0]].values
            ## replace somethings
            self.collect['sex'].replace(male, "Male", inplace=True)
            self.collect['sex'].replace(female, "Female", inplace=True)
            if self.DEBUG_sex: print((self.all_data[self.reader['sex'][0]].unique()))
            self.collect.loc[~self.collect.sex.isin(['Male', 'Female']), 'sex']= "Both"
            if self.DEBUG_sex: print((self.all_data[self.reader['sex'][0]].unique()))
            self.collect.loc[self.collect.sex.isnull(), 'sex'] ='Both'
            if (self.collect.sex=='Both').sum()/len(self.collect)> .4:
                print('No sex was assigned to over 40 precent of the respondents....')
                print((self.reader['nid']))
                print((self.reader['male_vals']))
                print((self.survey[self.reader['sex']][0]))

        else:
            print('no gender... really?')

            self.collect['sex'] ='Both'




    def get_age_6(self):

        # check to see if age is based off year
        if (str(self.reader['age_yr'][0]) == 'nan') & (str(self.reader['age_birth_yr'][0])!='nan') :

            miss = self.__remove_na__(miss_opt =self.reader['age_birth_yr_miss'], varname =self.reader['age_birth_yr'][0],
                              b='age_birth_yr', ret=True)

#            [self.all_data[self.reader['age_birth_yr'][0]].replace(x, np.nan, inplace=True) for x in miss]
#


            self.all_data.loc[self.all_data[self.reader['age_birth_yr'][0]].isin([1946]), self.reader['age_birth_yr'][0]] =np.nan
            self.all_data[self.reader['age_birth_yr'][0]] = list(map(np.float, self.all_data[self.reader['age_birth_yr'][0]]))
            self.all_data.loc[self.all_data[self.reader['age_birth_yr'][0]]<1890, self.reader['age_birth_yr'][0]] =np.nan
            self.reader['start_year']= [np.float(self.reader['start_year'][0])]
            self.all_data['age'] = self.reader['start_year'][0] -self.all_data[self.reader['age_birth_yr'][0]]
            self.reader['age_yr'] = ['age']
            self.reader['age_yr_miss'] = miss


        if 'nan' not in list(map(str, self.reader['age_yr'])):

            ### mannual
            self.all_data[self.reader['age_yr'][0]].replace(['90+', '+90'], 90, inplace=True)


            self.__cont__('age_yr')

            if 'age_yr' in self.collect.columns:
                self.collect['age_hold'] = self.collect['age_yr'].copy()
                self.collect['age_yr'] =myround(self.collect.age_yr)
                self.collect.loc[self.collect.age_yr>120, 'age_yr'] =999
                self.collect.loc[self.collect.age_yr<0, 'age_yr'] =999
                self.collect.loc[self.collect.age_yr.isnull(), 'age_yr'] =999
                self.collect.loc[(self.collect.age_yr>=100) & (self.collect.age_yr<=120), 'age_yr'] =95
                self.collect['age_end']  = self.collect['age_yr'].values+5
                self.collect.age_yr.replace(0, 1, inplace=True)
                self.collect.loc[(self.collect['age_hold']<=1) & (self.collect['age_hold']>=0), "age_yr"]=0
                self.collect.loc[(self.collect['age_hold']<=1) & (self.collect['age_hold']>=0), "age_end"]=1

                del self.collect['age_hold']



            if (self.collect.age_yr==999).sum()/len(self.collect)> .4:
                print('No age was assigned to over 40 precent of the respondents....')
                print((self.reader['nid']))
                print((self.reader['age_yr']))


    def get_utilization_7(self):
        """
        Assing utilization
        """

        ## take care of continuous
        if 'nan' not in list(map(str, self.reader['num_visit'])):
            self.__cont__('num_visit')

            self.collect.loc[self.collect.num_visit>40, 'num_visit'] =np.nan


        if 'nan' not in list(map(str, self.reader['num_bed_days'])):
            self.__cont__('num_bed_days')

        ## heck gated
        if 'nan' not in list(map(str, self.reader['util_var_gated'])):
            self.__yes_no__('util_var_gated')
            ## sick is gated
            self.collect['sick']=1
        else:
            self.collect['sick']=0

        ## check to see util var, but if not present, assing num_visit or num_bed_days
        if 'nan' not in  list(map(str, self.reader['util_var'])):
            self.__yes_no__('util_var')
        else:
            num_visit =0
            num_bed_day=0
            self.collect['util_var']=0

            if 'num_bed_days' in self.collect.columns:
                num_bed_day = len(self.collect[~self.collect['num_bed_days'].isnull()])
            if 'num_visit'  in self.collect.columns:
                num_visit = len(self.collect[~self.collect['num_visit'].isnull()])

            if num_visit>num_bed_day:
                self.collect.loc[self.collect['num_visit']>0, 'util_var']=1
            else:
                self.collect.loc[self.collect['num_bed_days']>0, 'util_var']=1
        if 'nan' not in list(map(str, self.reader['num_visit'])):
            ## check this out
            self.collect.loc[(self.collect.num_visit>0 )& (self.collect.util_var==0), 'util_var']=1
            self.collect.loc[(self.collect.num_visit.isnull() )& (self.collect.util_var==1), 'num_visit']=self.collect.num_visit.mean(skipna=True)
            self.collect.loc[(self.collect.num_visit==0 )& (self.collect.util_var==1), 'num_visit']=self.collect.num_visit.mean(skipna=True)


    def get_respond_8(self):
        """
        calculate sample sizes.
        based on responses to gates or based on response to util var.
        .sample size is a bit difficult for
        One based on age and sex missing and
        another on utilization
        """
        ## initial assume everyone is present
        self.collect['sample_size']=1

        if 'util_var_gated' in self.collect.columns:

            self.collect.loc[self.collect['util_var_gated'].isnull(), "sample_size"]= 0

        elif 'util_var' in self.collect.columns:

            self.collect.loc[self.collect['util_var'].isnull(), "sample_size"]= 0

        elif 'num_visit' in self.collect.columns:
            # if there are zeros, calc sample size by num visit. If not cacl based on age-sex
            self.collect.loc[(self.colect.age_yr==999) & (self.collect.sex=='Both'), "sample_size"]=0

        else:
            print('No gate, no util var, no num visit')
            print((self.reader['nid']))
            raise ValueError('Need sample size in '+ self.reader['title'][0])


    def get_surveyset_9(self):
        """
        Curate survey design. There are a total of three steps in this process

        1) get weights
            -if missigness is less than .1, use weights. If not, don't use weights. for missing
            weights assing mean value of weights
        2)  get psus
        3) get strata"""

        # step #1
        if 'nan' in list(map(str, self.reader['pweight'])):
            self.collect['pweight'] =1
        else:
            if len(self.collect) < 200000: 
                self.__cont__('pweight')
            else:
                self.collect['pweight'] = list(map(np.float,  self.all_data[self.reader['pweight'][0]].values))

            weight_check = np.sum(np.isnan(self.collect['pweight'])/np.float(len(self.collect)))
            if weight_check >=.4:
                self.collect['pweight'] =1
            else:
                self.collect.loc[self.collect.pweight.isnull(), "pweight"] = np.mean(self.collect.pweight)



        ##step 2 and3- combine psu and  strata
        for i in ['psu', 'strata']:
            if ('nan' not in list(map(str, self.reader[i]))) & (len(self.reader[i])>1):

                self.all_data[i+'_combine'] = self.all_data[self.reader[i][0]].astype('str')
                for j in np.arange(1, len(self.reader[i])):
                    self.all_data[i+'_combine']+='_'+self.all_data[self.reader[i][j]].astype('str')
                self.reader[i] = list(map(str ,[i+'_combine']))

        # step #2
        if 'nan' in list(map(str,self.reader['psu'])):
            self.collect['psu'] = np.arange(0, len(self.collect))
        else:
            self.collect['psu'] = self.all_data[self.reader['psu']]

            self.collect.loc[self.collect.psu.isnull(), 'psu'] = 'missing'

        # step #3
        if 'nan' in list(map(str, self.reader['strata'])):
            self.collect['strata'] =1
        else:
            self.collect['strata'] = self.all_data[self.reader['strata']]
            self.collect.loc[self.collect.strata.isnull(), 'strata'] = 'missing'


    def get_recall_10(self, recall_rules ={4.3:[0., 20.],
                                            52. :[20., 999.]}):
        if 'nan' != str(self.reader['util_recall_wks'][0]):
            self.collect['util_recall'] = np.float(self.reader['util_recall_wks'][0])
        elif 'nan' != str(self.reader['num_visits_recall_wks'][0]):
            self.collect['util_recall'] = np.float(self.reader['num_visits_recall_wks'][0])
        elif 'nan' != str(self.reader['num_bed_days_recall'][0]):
            self.collect['util_recall'] = np.float(self.reader['num_bed_days_recall'][0])
        else:
            print('No recall')
            print((self.reader['nid']))
            print((self.reader['title']))
            raise ValueError('no recall assinged')

        # coleect into recall bins
        survey_recall = np.float(self.collect.util_recall.unique())
        for i in list(recall_rules.keys()):
            if ((recall_rules[i][0] <=survey_recall) & (recall_rules[i][1]>survey_recall)):
                factor =i



        self.collect['true_recall'] = self.collect['util_recall'].copy()
        self.collect['util_recall']=factor
        if 'num_visit' in self.collect.columns :
            self.collect['num_visit'] *= factor/self.collect.true_recall.unique()



    def clean_names_11(self):
        self.collect['iso']=str(self.reader['ISO3'][0]).upper()
        self.collect['nid'] = self.reader['nid'][0]
        self.collect['year'] = self.reader['start_year'][0]
        self.collect['type']= self.reader['type'][0]

        self.collect.rename(columns= {'age_yr': 'age_start',
                         'util_recall': 'recall_type_value',
                             'year': 'year_start'}, inplace=True)

    def __group_bin__(self):
        """ all bins have to have at least thirty observations and 2 cases. probablya n easier way to do this but...hey"""
        age_group = self.collect[['age_start', 'age_end', 'sex', 'sample_size', 'util_var']].reset_index()

        age_group = age_group.loc[age_group.age_start<500]
        age_group = age_group.groupby(['age_start', 'age_end', 'sex']).sum().reset_index()

        age_group = age_group[['age_start', 'age_end', 'sex', 'sample_size', 'util_var']].drop_duplicates()
        age_group =age_group.sort_values('age_start').reset_index(drop=True)

        age_group['age_group']=np.nan

        for k in age_group.sex.unique():
            unique_vals = age_group.loc[age_group.sex==k, 'age_start'].tolist()

            for i in np.arange(len(unique_vals)):
                age_group.loc[(age_group.sex==k) & (age_group.age_start== unique_vals[i]), 'age_group']=i

        save_age_groups = age_group.copy()
        del save_age_groups['age_group']

        for i in age_group.sex.unique():
            unique_vals = age_group.loc[age_group.sex==i, 'age_group'].tolist()

            for j in np.sort(unique_vals):

                ss = age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'sample_size'].values.sum()
                util = age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'util_var'].values.sum()

                if (ss <5) | (util<1):

                    age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'age_group'] +=1

            ## take care of last grouping
            ss = age_group.loc[(age_group.sex==i) & (age_group.age_group==j+1), 'sample_size'].values.sum()
            util = age_group.loc[(age_group.sex==i) & (age_group.age_group==j+1), 'util_var'].values.sum()

            if (ss <5) | (util<1):

                if len(age_group.loc[(age_group.sex==i), 'age_group'].unique())>1:
                    second_to_last =sorted(set(age_group.loc[age_group.sex==i, 'age_group']))[-2]

                    age_group.loc[(age_group.sex==i) & (age_group.age_group==j+1), 'age_group'] =second_to_last

        ## check to see if there are any zeros
        male_group  = len(age_group.loc[age_group.sex=='Male', 'age_group'].unique())
        female_group = len(age_group.loc[age_group.sex=='Female', 'age_group'].unique())

        if (male_group==1) | (female_group==1):
            self.collect['sex'] = 'Both'
            self.collect['age_start']=0
            self.collect['age_end'] =100
        else:
            age_group['age_start_min'] = np.nan
            age_group['age_end_max'] =np.nan
            for i in age_group.sex.unique():
                for j in np.sort(age_group.age_group.unique()):
                    age_start_min =age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'age_start'].min()
                    age_start_max =age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'age_end'].max()
                    age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'age_start_min'] = age_start_min
                    age_group.loc[(age_group.sex==i) & (age_group.age_group==j), 'age_end_max'] = age_start_max



            age_group = age_group[['age_start', 'age_end', 'sex', 'age_start_min', 'age_end_max']]

            self.collect = self.collect.merge(age_group , on=['age_start', 'age_end', 'sex'], how='left')
            del self.collect['age_start']
            del self.collect['age_end']

            self.collect.rename(columns={'age_start_min': 'age_start',
                                        'age_end_max': 'age_end'}, inplace=True)
            self.collect.loc[self.collect.age_start.isnull(), 'age_start']=999
            self.collect.loc[self.collect.age_end.isnull(), 'age_end']= 1004



    def get_numbers_12(self, miss_labs=[np.nan, '', '.', ],
                       path =FILEPATH ):
        """ calc weighted values.... this did not work in rpy2"""


        # make everything consistent
        col_labs  = ['pweight', 'strata', 'psu',  'sex', 'util_var_gated', 'util_var',  'num_visit']
        col_labs = [x for x in col_labs if x not in self.collect.columns]
        # couldn't do this in a list comp
        for i in col_labs:
            self.collect[i] =np.nan


        self.collect.loc[self.collect.sex.isnull(), 'sex'] ='Both'

        ## get min and max with no missing in age and sex
        try:
            min_age_survey =self.collect.loc[~((self.collect.sex=='Both') |((self.collect.age_start>120) & (self.collect.age_end>120))) & (self.collect.util_var==1), 'age_start'].min()
            max_age_survey =self.collect.loc[~((self.collect.sex=='Both') |((self.collect.age_start>120) & (self.collect.age_end>120)))& (self.collect.util_var==1), 'age_end'].max()
        except:
            min_age_survey=0
            max_age_survey=100

        print(min_age_survey)
        print(max_age_survey)

        mask =self.collect.age_start>120
        self.collect.loc[mask, 'age_start'] = min_age_survey
        self.collect.loc[mask, 'age_end']=max_age_survey


        ## sample check
        sample_check = len(self.collect)
        zero_check = np.float(len(self.collect.loc[self.collect.sample_size==0]))
        ## chekc sample size
        if zero_check/sample_check>.2:
            print((zero_check/sample_check))
            if self.reader['nid'][0] not in ['138595', '66763','60405', '31770', '111486', '111487', '238523', '111488', '95306']: ## odd exceptions
                raise ValueError('dropped a lot of obs')

        ## drop missing samples
        self.collect = self.collect.loc[self.collect.sample_size!=0]

        ## correct for any missing vals that should be zero
        if self.collect.num_visit.isnull().sum() != len(self.collect):
            self.collect.loc[(self.collect.num_visit.isnull()) & (self.collect.sample_size==1), 'num_visit']=0

        self.collect.loc[(self.collect.util_var.isnull()) & (self.collect.sample_size==1), 'util_var']=0

        ## this is for the WHS because it's doe not sample under 18's well
        if self.reader['nid'] ==542:
            self.collect = self.collect.loc[self.collect.age_end>18]

        self.__group_bin__()


        ## save sample size


        ss =self.collect[['age_start', 'age_end', 'sex', 'sample_size', 'num_visit', 'util_var_gated', 'util_var']].groupby(['age_start', 'age_end', 'sex']).sum().reset_index()
        ss =self.collect[['age_start', 'age_end', 'sex', 'sample_size', 'num_visit', 'util_var_gated', 'util_var']].groupby(['age_start', 'age_end', 'sex']).sum().reset_index()

        ## write sample size to file
        ss.to_pickle(path+self.reader['unique_id'][0] + "_" + self.reader['type'][0]+'_ss.p')


        ## store some relevant info
        interesting_stuff  = {'iso':str(self.reader['ISO3'][0]).upper(),
                                'nid' : self.reader['nid'][0],
                                'year_start': self.reader['start_year'][0],
                                  'year_end': self.reader['end_year'][0],
                                'visit_type': self.reader['type'][0],
                                'sick' : self.collect.sick.unique()[0],
                                'recall_type_value': self.collect.recall_type_value.unique()[0],
                                'title' : self.reader['title'][0],
                                'true_recall': self.collect.true_recall.unique()[0] }
        ## write data and other itneresting stuff
        pd.to_pickle(interesting_stuff, path+self.reader['unique_id'][0] + "_" + self.reader['type'][0]+'_int_stuff.p')


        ## write data for R
        self.collect.to_csv(path+self.reader['unique_id'][0]+ "_" + self.reader['type'][0]+'_survey_in.csv')


        ##see if you need to look into num visit
        self.avg_visit = self.collect.loc[~self.collect.num_visit.isnull()]
        self.avg_visit_ever = self.avg_visit.loc[self.avg_visit.num_visit>0]
        print('test')
        if len(self.avg_visit_ever)>0:
            ss_ever =self.avg_visit_ever[['age_start', 'age_end', 'sex', 'sample_size', 'num_visit', 'util_var']].groupby(['age_start', 'age_end', 'sex']).sum().reset_index()
            ss_ever.to_pickle(path+self.reader['unique_id'][0] +"_" + self.reader['type'][0]+ '_ss_ever.p')




#########################################################################
## gdoc


def gdoc_query(url):
    ## read in data and drop dp
    ## read in dat... move up
    response = requests.get(url)
    #df_reader =pd.read_excel(StringIO(response.content)).T
    df_reader =pd.read_excel(BytesIO(response.content)).T #python3 bytes vs str issue. need get bytes into string first

    ## fix indexing

    df_reader.columns = df_reader.iloc[0]

    df_reader =df_reader.reset_index()
    df_reader.drop(0, inplace=True)
    del df_reader['index']

    df_reader =df_reader.reset_index()#... yeah whatever

    ## rename some things
    df_reader.rename(columns={ 'ids (order ids according to filepaths above, seperate ids by by comma and id"groups" by semicolon (e.g. household_id, pers_id ; HH_id, pid))' : 'merge_ids',
                    'pweight_national' : 'pweight',
                    'visit_type (OP, IP, any medical contact)': 'type',
                    'num_bed_days (# of days admitted to hospital)' : 'num_bed_days'}, inplace=True)
    ## get rid of nans
    df_reader = df_reader.loc[:,[not pd.isnull(x) for x in df_reader.columns]]
    df_reader['type'] = df_reader['type'].str.lower()

    ## create unique id
    df_reader['unique_id'] = df_reader['ISO3'].astype(str)+'_'+ df_reader['nid'].astype(str) +'_'+ df_reader['title'].astype(str) +'_'+ df_reader['start_year'].astype(str)
    ## get rid of nasty format
    df_reader['unique_id'] =df_reader['unique_id'].apply(lambda x:x.replace(" ", "_"))
    df_reader['unique_id']=df_reader['unique_id'].apply(lambda x:x.replace("'", ""))
    df_reader['unique_id']=df_reader['unique_id'].apply(lambda x:x.replace(",", ""))
    df_reader['unique_id']=df_reader['unique_id'].apply(lambda x:x.replace('"', ""))
    df_reader['unique_id']=df_reader['unique_id'].apply(lambda x:x.replace('(', ""))
    df_reader['unique_id']=df_reader['unique_id'].apply(lambda x:x.replace(')', ""))
    return df_reader
