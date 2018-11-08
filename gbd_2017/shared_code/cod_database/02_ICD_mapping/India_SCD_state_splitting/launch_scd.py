""" qsub scd_fit.py for each cause"""


# set up environment
import sys
user = 'USERNAME'
sys.path += ['.', '..', 'FILEPATH'.format(user=user)] 

import subprocess

import iss

import pandas as pd

acauses = iss.data.causes('SCD', acause=True)

acauses.remove('_gc')

scd_map = pd.read_stata('FILEPATH')
map_gc_causes = set(scd_map[scd_map.yll_cause=='_gc'].cause_code.unique())
scd_causes = set(iss.data.causes('SCD', acause=False))
gc_causes = scd_causes.intersection(map_gc_causes)

# append gc codes to acauses to get causes that will be modeled
causes = acauses + list(gc_causes) + ['all']

for c in causes:
    log = 'FILEPATH'.format(user=user, cause=c)
    name_str = 'iss_scd_{cause}'.format(cause=c)
    call_str = 'ADDRESS'
    
    subprocess.call(call_str, shell=True)
