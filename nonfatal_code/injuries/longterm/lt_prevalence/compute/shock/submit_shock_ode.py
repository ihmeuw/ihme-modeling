"""
PURPOSE: This script runs the inc-to-prev ODE solver for both shock e-codes and 
for all n-codes in series. The calling of this script from the parent Stata 
script should be parallelized across country-sex-platform (roughly 1000 jobs).



DATE: DATE

Arguments:
    Arg 1: string iso3
        Country on which you are running ODE solver.
    Arg 2: string sex
        female or male
    Arg 3: string ages_dir
        Path to directory that contains csv's of the lower and upper bounds for GBD age-groups.
        This is needed so that these values aren't pulled from the SQL server thousands
        of times when this code is run in parallel.
        For the time being, you may use this directory:
    Arg 4: string pyhme_parent
        Path to the parent directory for the pyHME package. Currently located at:
    Arg 5: string inj_code_dir
        Path to folder containing injury custom model code.
    Arg 6: string pop_s_path
        filepath to csv that contains cleaned single-year-age-group 
        populations (to create this file use 
    Arg 7: string pop_grp_path
        filepath to csv that contains GBD-age-group populations. This must be a 
        csv with the following columns: iso3, year, sex, age, pop.
        (to create this file use 
    Arg 8: string inc_dir
        Folder that contains incidence inputs for this particular platform. This 
        parent folder will contain folders for N-codes, each of which will contain 
        folders for E-codes, each of which will contain incidence csv files. 
        Note that these csvs must be named ANYTHING_[iso3]_[year]_[sex].csv. 
        Each file must have the following columns: age, draw0-draw999.
    Arg 9: string out_dir
        folder where you wish to output prevalences for this particular platform. 
        In this folder, this code will generate N-code folders, and then E-code 
        folders within the N-code folders. The resulting prevalence csvs will 
        thus be nested by platform/N-code/E-code.
    Arg 10: string time_path
        location where you want to save a 1-value csv containing the elapsed time
        (in hours) of this script.
    Arg 11: string check_path
        path to where you want check-file to be written (so that parent script 
        in Stata knows when it can stop blocking)
    Optional Arg: string mort_dir (only include if this platform has excess mortality)
        directory containing excess mortality inputs for 
        this particular platform. This parent folder will contain folders for 
        certain N-codes, which will each contain excess mortality csvs. Note 
        that these csvs must be named ANYTHING_[iso3]_[year]_[sex].csv. Each 
        file must have the following columns: age, draw0-draw999.

        
OUTPUTS:
    csv's of prevalence draws for each location-year within the years that we 
        are saving (1990, 1995, 2000, 2005, 2010, 2013)
"""

from sys import argv
from sys import path as sys_path
from os import path
from glob import glob

def main(ages_dir,pyhme_parent,inj_code_dir,iso3,sex,pop_s_path,pop_grp_path,inc_dir,mort_dir,out_dir,check_path):
    
    # Imports
    sys_path.append(inj_code_dir)
    sys_path.append(pyhme_parent)
    from py_modules import params,age_files_dir
    from pyHME.epi.inc_to_prev_annual import main as inc_to_prev
    
    ncodes = params.InjParams(age_files_dir).ncodes
    
    for n in ncodes:
        print n
        if mort_dir:
            mort_parent = path.join(mort_dir,n)
            has_mort = path.exists(mort_parent)
        else:
            has_mort = False
        if not has_mort: mort_parent = None
        
        for e in ['inj_war_execution','inj_war_warterror','inj_disaster']:
            print e
            inc_parent = path.join(inc_dir,n,e)
            out_parent = path.join(out_dir,e,n)
            
            # only run if shock incidence exists for this E-N-country-sex (else do nothing)
            print "Checking if inc_ exists"
            inc_exists = glob(path.join(inc_parent,"incidence_%s_*_%s*" %(iso3,sex)))
            if inc_exists:
                print "Trying to run inc to prev"
                inc_to_prev(ages_dir,pyhme_parent,iso3,sex,pop_s_path,
                        pop_grp_path,out_parent,inc_parent,
                        mort_dir=mort_parent)
                        
    # save checkfile when all done
    tmp = open(check_path,'wb')
    tmp.close()
    
if __name__ == '__main__':
    iso3 =          argv[1]
    sex =           argv[2]
    ages_dir =      argv[3]
    pyhme_parent =  argv[4]
    inj_code_dir =  argv[5]
    pop_s_path =    argv[6]
    pop_grp_path =  argv[7]
    inc_dir =       argv[8]
    out_dir =       argv[9]
    time_file =     argv[10]
    check_path =    argv[11]
    if len(argv) == 13:
        mort_dir =  argv[12]
    else:
        mort_dir =  None
    
    
    sys_path.append(pyhme_parent)
    from pyHME.functions import start_timer,end_timer
    start_time = start_timer()
    
    main(ages_dir,pyhme_parent,inj_code_dir,iso3,sex,pop_s_path,pop_grp_path,inc_dir,mort_dir,out_dir,check_path)

    end_timer(start_time,time_file)

    