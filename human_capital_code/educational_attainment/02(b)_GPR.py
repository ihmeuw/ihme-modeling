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


c_iso = str(sys.argv[1])
model_version = str(sys.argv[2])
draws = int(sys.argv[3])

low_memory = False

data = pd.read_csv(
    '.../data/output_data/{}/linear_prior.csv'.format(
        model_version))
data = data.loc[data.ihme_loc_id == '{c_iso}'.format(c_iso=c_iso)]
data['year'] = data['year_id']

df_list = []
gpr_data = pd.DataFrame()
for iso in pd.unique(data['ihme_loc_id']):
    print(iso)
    for sex in [1, 2]:
        for age in pd.unique(data['age_group_id']):
            # Run one country-year-age-sex group at a time
            iso_sex_age_data = data.loc[(
            (data.ihme_loc_id == '{iso}'.format(iso=iso)) & (data.sex_id == sex) & (data.age_group_id == age)), :]
            amp = iso_sex_age_data['mad'].values[0] * 1.4826
            gpr_out = gpr.fit_gpr(iso_sex_age_data, year_variable='year_id', amp=amp, scale=40,
                                  obs_variable='logit_mean', obs_var_variable='logit_mean_variance',
                                  mean_variable='logit_prior_mean', diff_degree=6)
            df_list.append(gpr_out)

gpr_data = pd.concat(df_list)

##Convert GPR Results to Normal Space
# for var in ['gpr_mean','gpr_upper','gpr_lower','st_prediction'] + ['draw{}'.format(i) for i in range(100)]:
for var in ['gpr_mean', 'gpr_upper', 'gpr_lower']:
    gpr_data['{var}'.format(var=var)] = np.where(gpr_data['age_group_id'] == 6,
                 inv_logit(gpr_data['{var}'.format(var=var)]) * 3,
                 np.where(gpr_data['age_group_id'] == 7,
                          inv_logit(gpr_data['{var}'.format(var=var)]) * 8,
                          np.where(gpr_data['age_group_id'] == 8,
                                   inv_logit(gpr_data['{var}'.format(var=var)]) * 13,
                                   inv_logit(gpr_data['{var}'.format(var=var)]) * 18)))

#    gpr_data['{var}'.format(var=var)] = inv_logit(gpr_data['{var}'.format(var=var)]) * 18

gpr_data.to_csv(
    '...data/output_data/{model_version}/gpr/{c_iso}.csv'.format(
        model_version=model_version, c_iso=c_iso))

if draws > 0:
    df_list = []
    gpr_data = pd.DataFrame()
    for iso in pd.unique(data['ihme_loc_id']):
        print iso
        for sex in [1,2]:
            for age in pd.unique(data['age_group_id']):
                # Run one country-year-age-sex group at a time
                iso_sex_age_data = data.loc[((data.ihme_loc_id == '{iso}'.format(iso=iso)) & (data.sex_id == sex ) & (data.age_group_id == age)),:]
                amp = iso_sex_age_data['mad'].values[0] * 1.4826
                gpr_out = gpr.fit_gpr(iso_sex_age_data,year_variable='year',amp=amp,scale=40,obs_variable='logit_mean', obs_var_variable='logit_mean_variance', mean_variable='logit_prior_mean',diff_degree=6,draws=draws)
                df_list.append(gpr_out)

    gpr_data = pd.concat(df_list)

    ##Convert GPR Results to Normal Space
    # for var in ['gpr_mean','gpr_upper','gpr_lower','st_prediction'] + ['draw{}'.format(i) for i in range(100)]:
    for var in ['gpr_mean', 'gpr_upper', 'gpr_lower'] + ['draw{}'.format(i) for i in range(draws)]:
        gpr_data['{var}'.format(var=var)] = np.where(gpr_data['age_group_id'] == 6,
                 inv_logit(gpr_data['{var}'.format(var=var)]) * 3,
                 np.where(gpr_data['age_group_id'] == 7,
                          inv_logit(gpr_data['{var}'.format(var=var)]) * 8,
                          np.where(gpr_data['age_group_id'] == 8,
                                   inv_logit(gpr_data['{var}'.format(var=var)]) * 13,
                                   inv_logit(gpr_data['{var}'.format(var=var)]) * 18)))
    # gpr_data['{var}'.format(var=var)] = inv_logit(gpr_data['{var}'.format(var=var)]) * 18

    gpr_data.to_csv(
        '...data/output_data/{model_version}/gpr_draws/{c_iso}.csv'.format(
            model_version=model_version, c_iso=c_iso))
