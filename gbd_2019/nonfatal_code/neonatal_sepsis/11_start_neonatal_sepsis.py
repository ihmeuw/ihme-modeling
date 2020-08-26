from __future__ import print_function
from builtins import zip
from builtins import str
import subprocess
import os

# updated to python 3

if __name__ == '__main__':
    # Run the neonatal severity split process with the following me_ids
    print(os.getcwd())
    birth_prev_ids = [1594]
    cfr_ids = ['cfr']
    mild_prop_ids = ['long_mild']
    modsev_prop_ids = ['long_modsev']
    acauses = ['neonatal_sepsis']
    zipped = list(zip(birth_prev_ids, cfr_ids, mild_prop_ids, modsev_prop_ids, acauses))
    # Submit the neonatal severity split jobs, completed by neonatal_work.py
    for birth_prev, cfr, mild_prop, modsev_prop, acause in zipped:
        submission_params = ["qsub", "-P", "proj_custom_models",
                             "-e", "FILEPATH", "-o", "FILEPATH",
                             "-l", "fthread=10", "-l", "m_mem_free=10G", "-l", "h_rt=00:30:00", "-l", "archive", "-q",
                             "all.q",
                             "-N", "%s%s" % (acause, birth_prev), "-cwd", "FILEPATH/python_shell.sh",
                             "FILEPATH/neonatal_work.py",
                             str(birth_prev), str(cfr), str(mild_prop), str(modsev_prop), str(acause)]
        print(submission_params)
        subprocess.check_output(submission_params)
