"""
The Params class provides the parameters useful for general GBD modeling
"""

from .filepaths import Filepaths
import csv
from os.path import join
import pandas as pd
    
def get_expanded_ages(as_tuples=False):
    """
    Get dict of expanded age groups (5-year ages up to 95-100) to their age range.
    
    Arguments:
        bool include_80:
            If True, includes the 80+ age group in this dict. If False, omits it
    Outputs:
        dict where keys are the names of all age groups used in the analysis (with
            or without the 80+ age group) and the values are 2-element lists 
            containing the start and end points of the corresponding age-group.
    """
    result = {}
    result[0] = [0,7/365.25]
    result[.01] = [7/365.25,28/365.25]
    result[.1] = [28/365.25,1]
    result[1] = [1,5]
    for i in range(5,91,5):
        result[i] = [i,i+5]
    result[95] = [95,100]
    return result
    
def get_ages_no80_noUnder1():
    """Get dict of ages used in EN matrix to their age range."""
    result = get_expanded_ages()
    del result[.01]
    del result[.1]
    result[0][1] = 1
    return result
    
def get_ages_local():
    """This function to be used when developing code without access to IHME's network."""
    result = {0:[0,7/365.25],
            .01:[7/365.25,28/365.25],
            .1:[28/365.25,1],
            1:[1,5]}
    for i in range(5,91,5):
        result[i] = [i,i+5]
    result[95] = [95,100]
    return result
    
def age_csv_to_dict(path):
    with open(path,'rb') as csvfile:
        reader = csv.reader(csvfile)
        # skip header row
        reader.next()
        result = {float(row[0]):[float(row[1]),float(row[2])] for row in reader}
    return result

def all_age_csvs_to_dict(parent_dir):
    result = []
    for f in ['gbd_ages','full_ages','full_ages_no80','full_ages_no80_noUnder1']:
        result.append(age_csv_to_dict(join(parent_dir,f+'.csv')))
    return result  
      
class Params:

    def __init__(self,age_files_dir=None,incl_ages=True):
        
        self.paths = Filepaths()
        if incl_ages:
            if age_files_dir:
                self.gbd_ages_dict,self.full_ages_dict,self.full_ages_dict_no80,self.full_ages_dict_no80_noUnder1 = all_age_csvs_to_dict(age_files_dir)
            else:
                from .sql import get_ages
                self.gbd_ages_dict = get_ages()
                self.full_ages_dict = get_ages(split_terminal=True)
                self.full_ages_dict_no80 = get_ages(incl_80plus=False,split_terminal=True)
                self.full_ages_dict_no80_noUnder1 = get_ages(split_terminal=True,incl_80plus=False,incl_under1=False)
    
            self.gbd_ages_list = sorted(self.gbd_ages_dict.keys())
            self.full_ages_list = sorted(self.full_ages_dict.keys())
            self.full_ages_list_no80 = sorted(self.full_ages_dict_no80.keys())
            self.full_ages_list_no80_noUnder1 = sorted(self.full_ages_dict_no80_noUnder1.keys())
        
        self.drawnum = 1000
        self.reporting_years = [1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016]
        