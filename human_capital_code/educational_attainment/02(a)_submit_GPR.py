
# coding: utf-8

# In[1]:

import os
os.chdir(".../code/model")
import sys
import pandas as pd
import matplotlib.pyplot as plt
import spacetime.spacetime as st
reload(st)
import gpr.gpr as gpr
reload(gpr)
import numpy as np
def logit(p):
    return np.log(p) - np.log(1 - p)

def inv_logit(p):
    return np.exp(p) / (1 + np.exp(p))
dir = '...'
draws = 0
draws = int(draws)
model_version = " "


# In[2]:


locs = pd.read_csv(' ')
locs = locs[locs['level'] >= 3]


print('locs loaded')

# In[4]:

#make directory for GPR outputs
directory = '{dir}/data/output_data/{model_version}/gpr/'.format(dir=dir,model_version=model_version)
if not os.path.exists(directory):
    os.makedirs(directory)
directory = '{dir}/data/output_data/{model_version}/gpr_draws/'.format(dir=dir,model_version=model_version)
if draws > 0:
    if not os.path.exists(directory):
        os.makedirs(directory)


# In[ ]:




# In[5]:

# ANY REASON THIS IS GPR 2?

# for c_iso in data.ihme_loc_id.unique():
    # os.popen('qsub -P proj_covariates -N EduGPR_{c_iso} -pe multi_slot 5 {dir}/code/shells/python_shell.sh {dir}/code/model/02_gpr2.py {c_iso} {model_version} {draws} '.format(c_iso=c_iso,dir=dir,model_version=model_version,draws=draws))

for c_iso in locs.ihme_loc_id.unique():
  if not os.path.isfile('{dir}/data/output_data/{model_version}/gpr_draws/{c_iso}.csv'.format(dir = dir, model_version = model_version, c_iso = c_iso)):
    print(c_iso)
    os.popen('qsub -N EduGPR_{c_iso} -P proj_human_capital -pe multi_slot 10 {dir}/code/shells/pyenv_shell.sh {dir}/code/model/02(b)_gpr.py {c_iso} {model_version} {draws} '.format(c_iso=c_iso,dir=dir,model_version=model_version,draws=draws))

#-P proj_covariates
# In[ ]:
