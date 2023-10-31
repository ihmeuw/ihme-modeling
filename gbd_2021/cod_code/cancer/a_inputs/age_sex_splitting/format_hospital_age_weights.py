'''
Description: Creates incidence age weights to be used in the prep pipeline using
                input from hospital team and updates the prep_incidence_rates table
                in the database
Contributors: USERNAME
'''

import pandas as pd
import numpy as np
import getpass
from datetime import datetime
from cancer_estimation.py_utils import data_format_tools as dft
from cancer_estimation._database import cdb_utils as cdb
import cancer_estimation.py_utils.common_utils as utils
db_link = cdb.db_api('cancer_db')

# import CI's weights
print("  Importing Clinical Weights")
input_file = "FILEPATH" # Update this path for each new iteration
input_df = pd.read_csv(input_file)
input_df = input_df[['age_group_id', 'sex_id', 'icg_id', 'counts', 'population']]

# recode sex_ids
input_df['sex'] = 1
input_df.loc[input_df['sex_id'] == "Female", 'sex'] = 2
input_df.drop(['sex_id'], axis=1, inplace = True)
input_df.rename(columns={'sex':'sex_id'}, inplace = True)


# import icg_id to icg_name map & subset to just cancers
print("  Mapping ICG to ICD and acause")
icg_map = pd.read_csv("FILEPATH") # update this version as needed
icg_acause_map = pd.read_csv(utils.get_path(key="mi_dataset_resources", process="mi_dataset") + "/age_sex_splitting/gbd_cancer_map.csv")
icg_map = icg_map.merge(icg_acause_map, how='right', on='icg_name')

# Merge input and icg_map
input_df = input_df.merge(icg_map, how='right', on='icg_id')

# correct both sex
print("  Creating both sex observations")
df_copy = input_df.copy()
df_copy['sex_id'] = 3

both_sexes_counts = dft.collapse(df_copy,
                                    by_cols=['icg_name', 'age_group_id'],
                                    func='sum', stub='counts')
both_sexes_pop = dft.collapse(df_copy,
                                    by_cols=['icg_name', 'age_group_id'],
                                    func='sum', stub='population')
both_sexes = both_sexes_counts.merge(both_sexes_pop,
                                       on=['icg_name', 'age_group_id'])
both_sexes['sex_id'] = 3

appended_df = input_df.append(both_sexes)

# correct age name 
appended_df.rename(columns={'age_group_id':'age'}, inplace=True)
appended_df.drop(['acause', 'cause_code', 'code_system_id', 'icg_id', 'icg_measure', 'map_version'], axis=1, inplace=True)

# Merge back full acauses
appended_df = appended_df.merge(icg_acause_map, on='icg_name', how='left')
appended_df.loc[appended_df['icg_name']=='average_cancer', 'acause'] = 'average_cancer'

# copy liver parent cases to hepatoblastoma since most liver cases in <5y.o. are hbl - decision made by USERNAME
hbl = appended_df.copy()
hbl = hbl.loc[hbl['acause'] == 'neo_liver',]
hbl['acause'] = 'neo_liver_hbl'
appended_df = appended_df.append(hbl)

# enforce age/sex restrictions
print("  Enforcing cause-age-sex restrictions")
appended_df.loc[appended_df['sex_id'] == 1 & ((appended_df['acause'] == 'neo_cervical') | (appended_df['acause'] == 'neo_ovarian') | \
    (appended_df['acause'] == 'neo_uterine')), 'counts'] = 0
appended_df.loc[appended_df['sex_id'] == 2 & ((appended_df['acause'] == 'neo_prostate') | (appended_df['acause'] == 'neo_testicular')), 'counts'] = 0
# > 10y.o - 1:6, 34, 238, 388, 389
appended_df.loc[((appended_df['acause'] == 'neo_liver_hbl') | (appended_df['acause'] == 'neo_eye_rb')) & ((appended_df['age'] > 6) & (appended_df['age'] != 34) & \
    (appended_df['age'] != 238) & (appended_df['age'] != 388) & (appended_df['age'] != 389)), 'counts'] = 0
# < 1.y.o - 388,389, 2:4
appended_df.loc[((appended_df['acause'] == 'neo_lymphoma') | (appended_df['acause'] == 'neo_lymphoma_burkitt') | (appended_df['acause'] == 'neo_lymphoma_other') | \
    (appended_df['acause'] == 'neo_bone')) & ((appended_df['age'] == 2) | (appended_df['age'] == 3) | (appended_df['age'] == 4) | (appended_df['age'] == 388) | \
        (appended_df['age'] == 389)), 'counts'] = 0
# < 2y.o - 238,388,389,2:4
appended_df.loc[(appended_df['acause'] == 'neo_hodgkins') & ((appended_df['age'] == 2) | (appended_df['age'] == 3) | (appended_df['age'] == 4) | (appended_df['age'] == 238) | \
    (appended_df['age'] == 388) | (appended_df['age'] == 389)), 'counts'] = 0
# < 5y.o - 34,238,388,389, 1:5
appended_df.loc[((appended_df['acause'] == 'neo_nasopharynx') | (appended_df['acause'] == 'neo_thyroid')) & ((appended_df['age'] == 1) | (appended_df['age'] == 2) | \
    (appended_df['age'] == 3) | (appended_df['age'] == 4) | (appended_df['age'] == 5) | (appended_df['age'] == 34) | (appended_df['age'] == 238) | \
        (appended_df['age'] == 388) | (appended_df['age'] == 389)), 'counts'] = 0
# < 10y.o. - 
appended_df.loc[(appended_df['acause'] == 'neo_eye_other') & ((appended_df['age'] == 1) | (appended_df['age'] == 2) | (appended_df['age'] == 3) | (appended_df['age'] == 4) | \
    (appended_df['age'] == 5) | (appended_df['age'] == 6) | (appended_df['age'] == 34) | (appended_df['age'] == 238) | (appended_df['age'] == 388) | \
        (appended_df['age'] == 389)), 'counts'] = 0
# < 15y.o - 34,238,388,389, 1:7
appended_df.loc[((appended_df['acause'] == 'neo_testicular') | (appended_df['acause'] == 'neo_cervical') | (appended_df['acause'] == 'neo_ovarian') | \
    (appended_df['acause'] == 'neo_bladder') | (appended_df['acause'] == 'neo_breast') | (appended_df['acause'] == 'neo_colorectal') | (appended_df['acause'] == 'neo_lung') | \
        (appended_df['acause'] == 'neo_melanoma') | (appended_df['acause'] == 'neo_mouth') | (appended_df['acause'] == 'neo_pancreas') | (appended_df['acause'] == 'neo_stomach')) \
            & ((appended_df['age'] == 1) | (appended_df['age'] == 2) | (appended_df['age'] == 3) | (appended_df['age'] == 4) | (appended_df['age'] == 5) | (appended_df['age'] == 6) | \
                (appended_df['age'] == 7) | (appended_df['age'] == 34) | (appended_df['age'] == 238) | (appended_df['age'] == 388) | (appended_df['age'] == 389)), 'counts'] = 0
# < 20y.o - 34,238,388,389, 1:8
appended_df.loc[((appended_df['acause'] == 'neo_esophageal') | (appended_df['acause'] == 'neo_gallbladder') | (appended_df['acause'] == 'neo_larynx') | \
    (appended_df['acause'] == 'neo_leukemia_ll_chronic') | (appended_df['acause'] == 'neo_meso') | (appended_df['acause'] == 'neo_myeloma') | (appended_df['acause'] == 'neo_nmsc') | \
        (appended_df['acause'] == 'neo_nmsc_scc') | (appended_df['acause'] == 'neo_otherpharynx') | (appended_df['acause'] == 'neo_prostate') | (appended_df['acause'] == 'neo_uterine')) & \
            ((appended_df['age'] == 1) | (appended_df['age'] == 2) | (appended_df['age'] == 3) | (appended_df['age'] == 4) | (appended_df['age'] == 5) | (appended_df['age'] == 6) | \
                (appended_df['age'] == 7) | (appended_df['age'] == 8) | (appended_df['age'] == 34) | (appended_df['age'] == 238) | (appended_df['age'] == 388) | (appended_df['age'] == 389)), \
                    'counts'] = 0

# Create Post Neonatal (4), 0-4 (1), and 1-4 (5) age groups
print("  Creating aggregate young age-groups")
zero_four = appended_df.copy()
zero_four = zero_four.loc[((zero_four['age'] == 2) | (zero_four['age'] == 3) | (zero_four['age'] == 34) | (zero_four['age'] == 238) | \
    (zero_four['age'] == 388) | (zero_four['age'] == 389)), ]
zero_four['age'] = 1
zero_four_pop = dft.collapse(zero_four, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='population')
zero_four_count = dft.collapse(zero_four, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='counts')
zero_four = zero_four_pop.merge(zero_four_count, on=['age', 'sex_id', 'acause'])

one_four = appended_df.copy()
one_four = one_four.loc[((one_four['age'] == 34) | (one_four['age'] == 238)),]
one_four['age'] = 5
one_four_pop = dft.collapse(one_four, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='population')
one_four_count = dft.collapse(one_four, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='counts')
one_four = one_four_pop.merge(one_four_count, on=['age', 'sex_id', 'acause'])

pn = appended_df.copy()
pn = pn.loc[((pn['age'] == 388) | (pn['age'] == 389)),]
pn['age'] = 4
pn_pop = dft.collapse(pn, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='population')
pn_count = dft.collapse(pn, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='counts')
pn = pn_pop.merge(pn_count, on=['age', 'sex_id', 'acause'])

appended_df = appended_df.append(zero_four)
appended_df = appended_df.append(one_four)
appended_df = appended_df.append(pn)

# Collapse all to create weights
print("  Collapsing rows")
ages_counts = dft.collapse(appended_df, by_cols=['acause', 'age', 'sex_id'], func='sum', stub='counts')
ages_pop = dft.collapse(appended_df, by_cols=['acause', 'age', 'sex_id'], func='sum', stub='population')

df = ages_counts.merge(ages_pop, on=['sex_id', 'acause', 'age'])

# convert to cancer age_ids
print("  Converting to cancer age groups")
df['cancer_age'] = df['age']
df.loc[df['age'] == 235, 'cancer_age'] = 25
df.loc[df['age'] == 32, 'cancer_age'] = 24
df.loc[df['age'] == 31, 'cancer_age'] = 23
df.loc[df['age'] == 30, 'cancer_age'] = 22
df.loc[df['age'] == 20, 'cancer_age'] = 21
df.loc[df['age'] == 19, 'cancer_age'] = 20
df.loc[df['age'] == 18, 'cancer_age'] = 19
df.loc[df['age'] == 17, 'cancer_age'] = 18
df.loc[df['age'] == 16, 'cancer_age'] = 17
df.loc[df['age'] == 15, 'cancer_age'] = 16
df.loc[df['age'] == 14, 'cancer_age'] = 15
df.loc[df['age'] == 13, 'cancer_age'] = 14
df.loc[df['age'] == 12, 'cancer_age'] = 13
df.loc[df['age'] == 11, 'cancer_age'] = 12
df.loc[df['age'] == 10, 'cancer_age'] = 11
df.loc[df['age'] == 9, 'cancer_age'] = 10
df.loc[df['age'] == 8, 'cancer_age'] = 9
df.loc[df['age'] == 7, 'cancer_age'] = 8
df.loc[df['age'] == 6, 'cancer_age'] = 7
df.loc[df['age'] == 5, 'cancer_age'] = 94
df.loc[df['age'] == 4, 'cancer_age'] = 93
df.loc[df['age'] == 3, 'cancer_age'] = 92
df.loc[df['age'] == 2, 'cancer_age'] = 91
df.loc[df['age'] == 1, 'cancer_age'] = 2

df.drop(['age'], axis=1, inplace=True)
df.rename(columns={'cancer_age':'age'}, inplace=True)


# generate incidence rate by cause, age, sex
print("  Calculating rates")
df['rate'] = df.counts / df.population

# Create NMSC and lymphoma parents
print("  Generating NMSC and Lymphoma parent weights")
nmsc = df.copy()
nmsc = nmsc.loc[(nmsc['acause'] == 'neo_nmsc_bcc') | (nmsc['acause'] == 'neo_nmsc_scc'),]
nmsc['acause'] = 'neo_nmsc'
nmsc_rate = dft.collapse(nmsc, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='rate')
nmsc_count = dft.collapse(nmsc, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='counts')
nmsc_population = dft.collapse(nmsc, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='population')
nmsc = nmsc_rate.merge(nmsc_count, on=['age', 'sex_id', 'acause'])
nmsc = nmsc.merge(nmsc_population, on=['age', 'sex_id', 'acause'])
df = df.append(nmsc)
lymph = df.copy()
lymph = lymph.loc[(lymph['acause'] == 'neo_lymphoma_burkitt') | (lymph['acause'] == 'neo_lymphoma_other'),]
lymph['acause'] = 'neo_lymphoma'
lymph_rate = dft.collapse(lymph, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='rate')
lymph_count = dft.collapse(lymph, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='counts')
lymph_pop = dft.collapse(lymph, by_cols=['age', 'sex_id', 'acause'], func='sum', stub='population')
lymph = lymph_rate.merge(lymph_count, on=['age', 'sex_id', 'acause'])
lymph = lymph.merge(lymph_pop, on=['age', 'sex_id', 'acause'])
df = df.append(lymph)

# Append in CLL weights from GBD2017 to fill in missingness
print("  Manual, temporary fix to CLL weights.")
inc_all = db_link.get_table("prep_incidence_rates")
cll = inc_all.loc[(inc_all['gbd_round_id'] == 7) & (inc_all['decomp_step'] == 1) & (inc_all['is_best'] == 1) & (inc_all['acause'] == 'neo_leukemia_ll_chronic'), ['age', 'sex_id', 'acause', 'rate']]
# One option where we get the CLL population by taking the mean of population from other leukemia causes
leuk_all = df.loc[(df['acause'] == 'neo_leukemia') | (df['acause'] == 'neo_leukemia_ll_acute') | (df['acause'] == 'neo_leukemia_ml_acute') | \
    (df['acause'] == 'neo_leukemia_ml_chronic') | (df['acause'] == 'neo_leukemia_other'), ]
leuk_all['acause'] = 'neo_leukemia_ll_chronic'
leuk_all = dft.collapse(leuk_all, by_cols=['age', 'sex_id', 'acause'], func='mean', stub='population')
cll = cll.merge(leuk_all, on=['age', 'sex_id', 'acause'])
cll['counts'] = cll['rate'] * cll['population']
cll.loc[(cll['age'] == 2) | (cll['age'] == 7) | (cll['age'] == 8) | (cll['age'] == 9) | (cll['age'] == 34) | (cll['age'] == 91) | (cll['age'] == 92) | \
    (cll['age'] == 93) | (cll['age'] == 94) | (cll['age'] == 238) | (cll['age'] == 388) | (cll['age'] == 389), ['counts', 'rate']] = 0
df = df.append(cll)


# add "average cancer" cause - previously this was taking the mean of all icg_ids, inflating the average_cancer weight
print("  Calculating average cancer weights")
avg_cancer = df.copy()
avg_cancer['acause'] = 'average_cancer'
avg_cancer_rate = dft.collapse(avg_cancer, by_cols=['age', 'sex_id', 'acause'], func='mean', stub='rate')
df = df.append(avg_cancer_rate)

# write intermediary file for CR weights-use
print("  Writing counts-included file")
df.to_csv('FILEPATH'), index=False)

df_final = df.copy()
df_final.drop(columns=['counts', 'population'], axis=1, inplace=True)
print("  Writing final file")
df_final.to_csv('FILEPATH'), index=False)
