""" Launch/qsub nine_fit.py for each cause"""


# set up environment
import sys
sys.path += ['.', '..', '/homes/strUser/india_state_splittling'] # FIXME: install iss as a module so this is not necessary

import subprocess

import iss

for c in iss.data.causes('MCCD_ICD9'):
    log = '/clustertmp/strUser/iss/nine_log_%s.txt'%c
    name_str = 'iss_9_%s'%c
    call_str = 'qsub -cwd -o %s -e %s ' % (log, log) \
                    + '-N %s ' % name_str \
                    + 'run_on_cluster.sh nine_fit.py %s' % c
            
    subprocess.call(call_str, shell=True)
