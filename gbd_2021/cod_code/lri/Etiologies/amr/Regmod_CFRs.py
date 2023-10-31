##################################################
## Project: Antimicrobial Resistance - Generalizable RegMod CFR Models
## Script purpose: Calculate case fatality rates for
##                 infectious syndromes by pathogen
## Date: 12/6/22
## Author: Lucien Swetschinski, Daniel Araki
##################################################

import pandas as pd
import numpy as np
import sys
import os
import getpass
user = getpass.getuser()
regmodpath = '/ihme/homes/{}/regmod/src'.format(user)
if not regmodpath in sys.path:
    sys.path.append('/ihme/homes/{}/regmod/src'.format(user))

from regmod.data import Data
from regmod.variable import Variable
from regmod.models import PoissonModel
from regmod.prior import GaussianPrior, UniformPrior

data = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/regmod_test/data_for_resp_comm_cfr_regmod.csv')

data['age_group_name'] = np.select(
    [data['age_group_id'] == 42,
    data['age_group_id'] == 179,
    data['age_group_id'] == 239,
    data['age_group_id'] == 25,
    data['age_group_id'] == 26],
    ['Neonatal', 'PN-5', '5-50', '50-70', '70plus'])

data['logn'] = np.log(data['cases'])

# subset to non-hospital-acquired data only (community and unknown)
data = data.loc[data['hosp'] != 'hospital',]
# limit to data with 5 or more cases
data = data.loc[data['cases'] >= 5,]

data['spec_source'] = data['source']
data.loc[data['source'].isin(['AUT_HDD', 'CAN_DAD', 'NZL_NMDS', 'SGUL', 'inicc', 'varna_bulgaria', 'vietnam']),
         'spec_source'] = 'other'

# update PCV and hib for respiratory and zero out PCV3 and hib coverage for non pneumococcus/hib
data = data.drop(['PCV3_coverage_prop', 'Hib3_coverage_prop'], axis = 1)

cumulo_pcv = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/hib_pcv_vaccine_coverage/cumulative_indirect_pcv_coverage.csv')
cumulo_hib = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03a_pathogen/hib_pcv_vaccine_coverage/cumulative_indirect_hib_coverage.csv')

# reset iord loc id to merge nicely
data.loc[data['location_id'] == 44767, 'location_id'] = 95

data = data.merge(cumulo_pcv, on = ['year_id', 'location_id'], validate = 'many_to_one')
data = data.merge(cumulo_hib, on = ['year_id', 'location_id'], validate = 'many_to_one')

data = data.rename(columns ={'cumulative_PCV':'PCV3_coverage_prop', 'cumulative_hib':'Hib3_coverage_prop'})

data.loc[data['pathogen'] != 'streptococcus_pneumoniae', 'PCV3_coverage_prop'] = 0
data.loc[data['pathogen'] != 'haemophilus_influenzae', 'Hib3_coverage_prop'] = 0

# generate dummies
dummydata = pd.get_dummies(data, columns = ['hosp', 'ICU', 'age_group_name', 'spec_source', 'pathogen'])

rgmd_data = Data(
    col_obs='deaths',
    # ref levels: 'hosp_community', 'ICU_mixed', 'age_group_name_5-50', 'spec_source_other', 'pathogen_staphylococcus_aureus'
    # to enforce the reference groups, omit them from the provided covariates 
    col_covs=['haqi', 'PCV3_coverage_prop', 'Hib3_coverage_prop', 'hosp_unknown', 'ICU_ICU_only', 
       'age_group_name_50-70',
       'age_group_name_70plus', 'age_group_name_Neonatal',
       'age_group_name_PN-5', 'spec_source_BRA_SIH', 'spec_source_ITA_HID',
       'spec_source_MEX_SAEH', 'spec_source_USA_NHDS', 'spec_source_USA_SID',
       'spec_source_US_MedMined_BD', 'spec_source_iord', 
       'pathogen_acinetobacter_baumanii', 'pathogen_chlamydia_spp',
       'pathogen_enterobacter_spp', 'pathogen_escherichia_coli',
       'pathogen_flu', 'pathogen_fungus', 'pathogen_group_b_strep',
       'pathogen_haemophilus_influenzae', 'pathogen_klebsiella_pneumoniae',
       'pathogen_legionella_spp', 'pathogen_mycoplasma', 
       'pathogen_poly_other', 'pathogen_pseudomonas_aeruginosa',
       'pathogen_rsv', 'pathogen_other',
       'pathogen_streptococcus_pneumoniae', 'pathogen_virus'],  # intercept is added by default
    col_offset='logn',
    df=dummydata
)

priorvars = [
    # monotonicity priors for vaccine coverage, effect has to be negative or 0
    Variable('PCV3_coverage_prop', priors=[UniformPrior(lb=-np.inf, ub=0.0)]),
    Variable('Hib3_coverage_prop', priors=[UniformPrior(lb=-np.inf, ub=0.0)]),
    # regularizing priors for source, want effect of HAQI to predominate in explaining differences 
    # between sources so it is easier to extrapolate the data to other countries
    Variable("spec_source_BRA_SIH", priors=[GaussianPrior(mean=0.0, sd=0.1)]),
    Variable("spec_source_ITA_HID", priors=[GaussianPrior(mean=0.0, sd=0.1)]),
    Variable("spec_source_MEX_SAEH", priors=[GaussianPrior(mean=0.0, sd=0.1)]),
    Variable("spec_source_USA_NHDS", priors=[GaussianPrior(mean=0.0, sd=0.1)]),
    Variable("spec_source_USA_SID", priors=[GaussianPrior(mean=0.0, sd=0.1)]),
    Variable("spec_source_US_MedMined_BD", priors=[GaussianPrior(mean=0.0, sd=0.1)]),
    Variable("spec_source_iord", priors=[GaussianPrior(mean=0.0, sd=0.1)])
]

# instantiate the variables object
nonpriorvars = [var for var in rgmd_data.col_covs if var not in ['PCV3_coverage_prop', 'Hib3_coverage_prop', 
                                                                 'spec_source_BRA_SIH', 'spec_source_ITA_HID',
                                                                 'spec_source_MEX_SAEH', 'spec_source_USA_NHDS',
                                                                 'spec_source_USA_SID', 'spec_source_US_MedMined_BD',
                                                                 'spec_source_iord']]

variables = priorvars + [Variable(xi) for xi in nonpriorvars]

poissmod = PoissonModel(
    data=rgmd_data,
    param_specs={
        'lam': {
            'variables': variables,
            'use_offset': True
        }
    }
)

# fit model
poissmod.fit(options = {'maxiter':10000})

# model coefficients
result = {[x.name for x in variables][i]: poissmod.opt_coefs[i] for i in range(len(variables))}
for key, value in sorted(result.items()):
    print(key, ':', value)

# read in prediction template
predtemp = pd.read_csv('/mnt/team/amr/priv/intermediate_files/03b_cfr/respiratory_infectious/respiratory_infectious_intercept_comm_vax/prediction_template.csv')

# zero out vaccine covariates for non-vaccine pathogens
predtemp.loc[predtemp['pathogen'] != 'streptococcus_pneumoniae', 'PCV3_coverage_prop'] = 0
predtemp.loc[predtemp['pathogen'] != 'haemophilus_influenzae', 'Hib3_coverage_prop'] = 0

# limit to community-acquired
predtemp = predtemp.loc[predtemp['hosp'] == 'community',]
# predict out for the reference source category
predtemp['spec_source'] = 'other'

# rename age groups
predtemp['age_group_name'] = np.select(
    [predtemp['age_group_id'] == 42,
    predtemp['age_group_id'] == 179,
    predtemp['age_group_id'] == 239,
    predtemp['age_group_id'] == 25,
    predtemp['age_group_id'] == 26],
    ['Neonatal', 'PN-5', '5-50', '50-70', '70plus'])

# generate dummies
predtemp = pd.concat([predtemp, pd.get_dummies(predtemp.loc[:, ['hosp', 'ICU', 'age_group_name', 'spec_source', 'pathogen']],
                                              columns = ['hosp', 'ICU', 'age_group_name', 'spec_source', 'pathogen'])], axis = 1)

# 0 out all modelled variables that are not in the prediction template, things like ICU status
# that we bias correct for
for i in list(set(['haqi', 'PCV3_coverage_prop', 'Hib3_coverage_prop', 'hosp_unknown', 'ICU_ICU_only', 
       'age_group_name_50-70',
       'age_group_name_70plus', 'age_group_name_Neonatal',
       'age_group_name_PN-5', 'spec_source_BRA_SIH', 'spec_source_ITA_HID',
       'spec_source_MEX_SAEH', 'spec_source_USA_NHDS', 'spec_source_USA_SID',
       'spec_source_US_MedMined_BD', 'spec_source_iord', 'spec_source_NZL_NMDS',
       'spec_source_AUT_HDD', 'pathogen_acinetobacter_baumanii', 'pathogen_chlamydia_spp',
       'pathogen_enterobacter_spp', 'pathogen_escherichia_coli',
       'pathogen_flu', 'pathogen_fungus', 'pathogen_group_b_strep',
       'pathogen_haemophilus_influenzae', 'pathogen_klebsiella_pneumoniae',
       'pathogen_legionella_spp', 'pathogen_mycoplasma', 
       'pathogen_poly_other', 'pathogen_pseudomonas_aeruginosa',
       'pathogen_rsv', 'pathogen_other', 
       'pathogen_streptococcus_pneumoniae', 'pathogen_virus']) - set(predtemp.columns)):
    predtemp[i] = 0

# instantiate data object
datapred = Data(
    col_covs=['haqi', 'PCV3_coverage_prop', 'Hib3_coverage_prop', 'hosp_unknown', 'ICU_ICU_only', 
       'age_group_name_50-70',
       'age_group_name_70plus', 'age_group_name_Neonatal',
       'age_group_name_PN-5', 'spec_source_BRA_SIH', 'spec_source_ITA_HID',
       'spec_source_MEX_SAEH', 'spec_source_USA_NHDS', 'spec_source_USA_SID',
       'spec_source_US_MedMined_BD', 'spec_source_iord', 
       'pathogen_acinetobacter_baumanii', 'pathogen_chlamydia_spp',
       'pathogen_enterobacter_spp', 'pathogen_escherichia_coli',
       'pathogen_flu', 'pathogen_fungus', 'pathogen_group_b_strep',
       'pathogen_haemophilus_influenzae', 'pathogen_klebsiella_pneumoniae',
       'pathogen_legionella_spp', 'pathogen_mycoplasma', 
       'pathogen_poly_other', 'pathogen_pseudomonas_aeruginosa',
       'pathogen_rsv', 'pathogen_other',
       'pathogen_streptococcus_pneumoniae', 'pathogen_virus'],  # intercept is added by default
    df=predtemp
)

# generate predictions
predtemp['predict'] = poissmod.params[0].get_param(coefs=beta_pred, data=datapred)

poisspreds_out = predtemp.loc[:, [i for i in oldpreds.columns]]

poisspreds_out.to_csv('/mnt/team/amr/priv/model_results/03b_cfr/respiratory_infectious/point_predictions/poisson_comminterceptvax_point_predictions2.csv', index = False)