'''
Description: Wrapper for the R processes that estimate incidence, used as a 
    placeholder until these processes can be rewritten in python
How To Use:
'''

import cancer_estimation.py_utils.common_utils as utils
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd
from sys import argv
from time import sleep
import os
import subprocess



def generate_estimates(acause, location_id, cnf_model_version_id, faux_correct, is_resubmission=False):
    ''' Runs a subprocess that passes all arguments to the R script that 
            calculates incidence draws
    '''
    print('Beginning incidence estimation...')
    r_script = utils.get_path("calculate_incidence", process="nonfatal_model")
    cmd = "bash {shl} {scr} {ac} {l} {id} {fc} {rs}".format(
                    shl=utils.get_path("r_shell"), scr=r_script, ac=acause,
                    l=location_id, id=cnf_model_version_id, fc=faux_correct, rs=is_resubmission)
    subprocess.call(cmd, shell=True)
    return(True)
    

if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_version_id = int(argv[3])
    faux_correct = argv[4]
    generate_estimates(acause, location_id, cnf_model_version_id, faux_correct)

