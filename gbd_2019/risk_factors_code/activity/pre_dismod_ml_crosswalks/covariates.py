import pandas as pd
import numpy as np
import re 

""" 
for now, reads from flat files generated after using 
`gce` gbd central function. script is in FILEPATH
 
only a few covariates are supported

must name covariate CSVs per their cov_name_short 
"""


def merge_covariates(dat, covariates, cov_dir):
    """ """

    if cov_dir is None:
        raise ValueError('You need a covariate directory.')
    common_cov_cols = ['age_group_id', 'sex_id', 'year_id', 'location_id', 'mean_value']

    for c in covariates:
        cov_name = re.sub('_mean', '', c)
        cov_df = _get_covariates(cov_dir, cov_name, common_cov_cols)

        if 'sex_id' in cov_df.columns:
            common_cols = common_cov_cols[:-1]
        else: 
            common_cols = ['location_id', 'year_id']

        dat = dat.merge(cov_df, how='left', on=common_cols)
   
    return dat 


def _get_covariates(cov_dir, cov_name, common_cov_cols):
    """ """

    if cov_name == 'obesity':
        ob_raw = pd.read_csv(cov_dir+"prev_obesity_age_spec.csv")
        ob = ob_raw.copy(deep=True)
        ob = ob[common_cov_cols]
        ob = ob.rename(columns={'mean_value': 'obesity_mean'})
        return ob

    elif cov_name == 'ldi':
        ldi_raw = pd.read_csv(cov_dir + "ldi_pc.csv")
        ldi = ldi_raw.copy(deep=True)
        ldi = ldi[['location_id', 'year_id', 'mean_value']]
        ldi = ldi.rename(columns={'mean_value': 'ldi_mean'})
        ldi['ldi_mean'] = np.log(ldi['ldi_mean'])
        return ldi

    elif cov_name == 'educ':
        educ_raw = pd.read_csv(cov_dir+"educ.csv")
        educ = educ_raw.copy(deep=True)
        educ = educ[common_cov_cols]
        educ = educ.rename(columns={'mean_value': 'educ_mean'})
        return educ

    elif cov_name == 'urban':
        urban_raw = pd.read_csv(cov_dir + "urbanicity.csv")
        urban = urban_raw.copy(deep=True)
        urban = urban[['location_id', 'year_id', 'mean_value']]
        urban = urban.rename(columns={'mean_value': 'urban_mean'})
        return urban

    elif cov_name == 'sdi':
        sdi_raw = pd.read_csv(cov_dir+"sdi.csv")
        sdi = sdi_raw.copy(deep=True)
        sdi = sdi[['location_id', 'year_id', 'mean_value']]
        sdi = sdi.rename(columns={'mean_value': 'sdi_mean'})
        return sdi 

    else: 
        raise ValueError('Covariate {} is not yet supported.'.format(cov_name))