'''
Description: Wrapper for the R processes that estimate incidence, used as a 
    placeholder until these processes can be rewritten in python
How To Use:
Contributors: INDIVIDUAL_NAME
'''

import cancer_estimation.py_utils.common_utils as utils
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd
from sys import argv
from time import sleep
import os
import subprocess



def generate_estimates(acause, location_id, cnf_model_version_id, is_resubmission=0, 
                       faux_correct=1, is_estimation_yrs=1):
    ''' Runs a subprocess that passes all arguments to the R script that 
            calculates incidence draws
    '''
    faux_correct = int(faux_correct)
    is_resubmission = int(is_resubmission) 
    is_estimation_yrs = int(is_estimation_yrs)
    print('Beginning incidence estimation...')
    r_script = utils.get_path("calculate_incidence", process="nonfatal_model")
    cmd = "bash {shl} {scr} {ac} {l} {id} {fc} {rs} {ey}".format(
                    shl=utils.get_path("r_shell"), scr=r_script, ac=acause,
                    l=location_id, id=cnf_model_version_id, 
                    fc=faux_correct, rs=is_resubmission, ey=is_estimation_yrs)
    print(f"The command I'm about to run is\n{cmd}")
    subprocess.run(cmd, shell=True, check=True)
    return(True)
    

if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_version_id = int(argv[3])
    faux_correct = int(argv[4])
    is_estimation_yrs = int(argv[5])
    generate_estimates(acause, location_id, cnf_model_version_id, faux_correct, is_estimation_yrs)

