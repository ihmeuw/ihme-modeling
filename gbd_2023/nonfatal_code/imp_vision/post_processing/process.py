"""
Project: GBD Vision Loss
Purpose: Execute the etiology specific adjustments for small causes and
causes not modeled by the vision loss modelers, severity splits, the etiology
squeeze and post etiology specific adjustments. 

The transformations that occur in this step of the pipeline include the
following:

1. IMPORTS
2. SET THE SEED AND GET INFORMATION PASSED FROM LAUNCH SCRIPT
3. SET FILE STRUCTURE AND MAKE FILES WHERE NECESSARY
4. READ INPUT FILES
5. PULL DRAWS FOR ALL CAUSES OF VISION LOSS
6. PULL CAUSES NOT MODELED FOR GBD
7. APPLY TRACHOMA GEOGRAPHIC RESTRICTIONS
8. TURN TRACHOMA PROPORTION INTO A PREVALENCE
9. MILD MODERATE CROSSWALK
10. SPECIAL PROCESSING FOR VITAMIN A
11. SPLIT VIT A, MENG ENCEPH INTO LOW VISION AND BLINDNESS
12. SPLIT OUT LOW VISION INTO MODERATE AND SEVERE VISION LOSS
13. CO AND MMD PROCESSING
14. ROP PROCESSING
15. SUBTRACT SMALLER CAUSES OF VISION LOSS FROM OTHER
16. MAKE COPY OF PRESQUEEZED ESTIMATES FOR VETTING PURPOSES
17. SQUEEZE 1
18. POST-HOC ADJUSTMENTS
19. SQUEEZE 2
20. MAKE SURE THERE ARE NO NEGATIVE DRAWS
21. TEST THAT THE SUM OF ALL OF THE ETIOLOGIES ADDS UP TO THE ENVELOPE
22. GET BEST CORRECTED BLINDNESS
23. MAKE NEAR VISION PREVALENCE ADJUSTMENT
24. OUTPUT FINAL ESTIMATES TO THE FINAL RESULTS DIRECTORY
"""

# ---IMPORTS-------------------------------------------------------------------

import argparse
import copy
import logging
import random
import sys
import time

import numpy as np
import pandas as pd

import gbd.constants as gbd
from get_draws.base.exceptions import InputsException

import config as cfg

from helpers.adjust_etiologies import (apply_grs,
                                       cap_vita,
                                       get_mod_plus_proportions)
from helpers.adjust_post_squeeze import (adjust_near_vision_prevalence,
                                        get_best_corrected_blindness)
from helpers.process_utils import (get_vision_draws,
                                   rename_draw_cols)
from helpers.squeeze_utils import (get_dict_totals,
                           add_subtract_envelope)

from core.core_python.general_utils import (impute_birth_estimates,
                                            keep_cols,
                                            sort_totals_and_envelope)
from core.core_python.io_utils import make_directory
from core.core_python.transform import transform_draws
from core.core_python.quality_checks import (check_df_for_nulls,
                                            check_for_nulls,
                                            check_df_for_negative_draws,
                                            check_for_negative_draws,
                                            get_draw_col_count,
                                            check_shape)
                            
sys.path.insert(0, cfg.core_path)

# ---SET THE SEED AND GET INFORMATION PASSED FROM LAUNCH SCRIPT----------------

logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s')

logging.info("The time at the beginning of the run is {}".format(time.time()))

random.seed(1430)

parser = argparse.ArgumentParser()
parser.add_argument("--location_id",
                    help="GBD location_id for one location",
                    type=int)
parser.add_argument("--mapping",
                    help="Serialized Metadata for cause, meid and severity "
                         "mapping",
                    type=str)
args = parser.parse_args()
location_id = args.location_id
serialized_mapping = args.mapping

logging.info("Running the pipeline for location_id {}".format(location_id))

# ---SET FILE STRUCTURE AND MAKE FILES WHERE NECESSARY-------------------------

# Make directories if they do not exist
for directory in [cfg.out_dir, cfg.intermediate_out_dir, cfg.final_out_dir]:
    make_directory(directory)

# ---READ INPUT FILES----------------------------------------------------------
# 1. Input meids: Presqueezed dismod and custom meids representing the input
# etiologies and envelopes.
# 2. Transformation meids: Meids used for transformation within the pipeline
# but are not outputted and do not go through either of the squeezes.
# 3. Output meids: Custom meids that are a result of all of the pipeline's
# retransformations and go into the central machinery.
# 4. Diagnostic meids: Custom meids that are produced after many of the
# transformations and are used for diagnostic purposes.
# -----------------------------------------------------------------------------

logging.info("Reading input files {}".format(time.time()))

# Un-serialize modeling codebook
cb = pd.read_pickle(serialized_mapping)
check_shape(cb)

if not cfg.pull_co:
    logging.info("Dropping meids for Corneal opacity")
    cb = cb[cb['label'] != 'co']

# Obtain metadata for input meids
input_vision_meid_codebook = cb.copy()
input_vision_meid_codebook = input_vision_meid_codebook[
    input_vision_meid_codebook['type'].isin(['input'])]
check_shape(input_vision_meid_codebook)

# Obtain metadata for envelope meids
vision_envelope_codebook = cb.copy()
vision_envelope_codebook = vision_envelope_codebook[
    vision_envelope_codebook['type'].isin(['envelope'])]
check_shape(vision_envelope_codebook)

# Obtain metadata for output meids
output_meids = cb.copy()
output_meids = output_meids[output_meids['type'].isin(['output', 'diagnostic'])]
output_meids = output_meids[['variable_name', 'modelable_entity_id']]
check_shape(output_meids)

# ---GET ENVELOPE LEVEL PREVALENCE AND SUM THE ENVELOPES TOGETHER TO GET TOTAL-

# Create a numpy array of presenting envelope modelable entity ids
presenting_meids = vision_envelope_codebook[
    'modelable_entity_id'].unique().tolist()
logging.info("the presenting vision loss envelope meids are " + str(
    presenting_meids))

# Use get draws to pull envelope meids for presenting moderate/severe/blindness
envelopes = {}
for meid in presenting_meids:
    sev = vision_envelope_codebook.query(
        "modelable_entity_id == @meid").sev.values[0].upper()
    logging.info("""
          Loading dismod draws for presenting {} vision loss envelope, meid
          {}""".format(sev, meid))
    envelopes[sev] = get_vision_draws(meid=meid,
                                      measure_id=ID,
                                      location_id=location_id,
                                      age_group_id=cfg.age_ids_list,
                                      release_id=cfg.release_id)
    if cfg.testing:
        envelopes[sev].to_csv("{}/{}_{}_envelope_get_draws_pull.csv".format(
            cfg.intermediate_out_dir, location_id, sev), index=False)

# Append the 3 envelopes together
total_vision_loss_envelope = envelopes[
    'LOW_MOD'].append([envelopes['LOW_SEV'], envelopes['BLIND']])

# Sum the prevalence of each envelope together to get total prevalence
total_vision_loss_envelope = total_vision_loss_envelope.groupby(
    ['location_id', 'age_group_id', 'year_id', 'sex_id'])
total_vision_loss_envelope = total_vision_loss_envelope.sum().reset_index()

# Quick sanity check to make sure the envelope summing occurred correctly
query = "age_group_id == IDand sex_id == ID and year_id == {}".format(cfg.current_year)

mod = envelopes['LOW_MOD'].query(query).draw_100.values[0]
sev = envelopes['LOW_SEV'].query(query).draw_100.values[0]
blind = envelopes['BLIND'].query(query).draw_100.values[0]
tot = total_vision_loss_envelope.query(query).draw_100.values[0]
test_sum = (mod + sev + blind)

if not np.isclose(tot, test_sum):
    raise ValueError("""
                     total envelope prevalence is not calculated correctly,
                     it is not equal to the sum of mod, sev, and blind""")

# ---PULL DRAWS FOR ALL CAUSES OF VISION LOSS----------------------------------

logging.info(
    "PULLING DRAWS FOR ALL CAUSES OF VISION LOSS {}".format(time.time()))

if cfg.pull_custom:
    custom_models = input_vision_meid_codebook[
        input_vision_meid_codebook['modeling_type'] != 'gbd']
    input_vision_meid_codebook = input_vision_meid_codebook[
        input_vision_meid_codebook['modeling_type'] != 'custom']


input_vision_meid_codebook = input_vision_meid_codebook.query(
    "modelable_entity_name != 'Presbyopia impairment envelope'")

meids = input_vision_meid_codebook.modelable_entity_id.values.tolist()

# Causes dictionary will hold information on each cause and severity level
causes = {}


for meid in meids:
    name = input_vision_meid_codebook.query(
        "modelable_entity_id == @meid").variable_name.values[0].upper()
    measure = input_vision_meid_codebook.query(
        "modelable_entity_id == @meid").measure.values[0].upper()
    measure_id = input_vision_meid_codebook.query(
        "modelable_entity_id == @meid").measure_id.values[0]

    logging.info(
        "loading {} draws for {} (meid {})".format(measure, meid, name))

    try:
        causes[name] = get_vision_draws(meid=meid,
                                        measure_id=measure_id,
                                        location_id=location_id,
                                        age_group_id=cfg.age_ids_list,
                                        release_id=cfg.release_id)

    except InputsException:
        causes[name] = get_vision_draws(meid=meid,
                                        measure_id=measure_id,
                                        location_id=location_id,
                                        age_group_id=cfg.no_birth_prev,
                                        release_id=cfg.release_id)
    causes[name] = causes[name][causes[name]['year_id'].isin(cfg.diagnostic_years)]
    causes[name] = causes[name].drop_duplicates()

    # Filter down to specific ages and make sure each cause has correct age
    # groups.
    causes[name] = causes[name].loc[causes[name].age_group_id.isin(
        cfg.age_ids_list)]

    if cfg.impute_birth_estimates:
        if 164 not in causes[name].age_group_id.unique():
            # Some dismod models do not output birth estimates so impute here
            causes[name] = impute_birth_estimates(causes[name])
            logging.info("""
                  no birth estimates. Imputed birth {} for {} using early
                  neontal values""".format(measure, name))
    if cfg.age_ids_list != list(np.sort(causes[name].age_group_id.unique())):
        logging.info(cfg.age_ids_list)
        logging.info(list(np.sort(causes[name].age_group_id.unique())))
        raise ValueError("""
                         The age_ids_list specified in the configuration file
                         does not match the age ids pulled from the database.
                         If you have to impute birth prev, you can set
                         impute_birth_prev equal to true and early neonatal
                         values will be used.""")

check_for_nulls(causes)


if cfg.pull_custom:
    # Load dismod results for all models
    for custom_meid in cfg.custom_meids:
        name = custom_models.query(
            "modelable_entity_id == @custom_meid"
        ).variable_name.values[0].upper()
        measure = custom_models.query(
            "modelable_entity_id == @custom_meid").measure.values[0].upper()
        measure_id = custom_models.query(
            "modelable_entity_id == @custom_meid").measure_id.values[0]

        logging.info(
            "loading {} draws for {} (meid {})".format(measure, custom_meid,
                                                       name))
        causes[name] = get_vision_draws(meid=custom_meid,
                                        measure_id=measure_id,
                                        location_id=location_id,
                                        age_group_id=cfg.age_ids_list,
                                        release_id=cfg.release_id)

check_for_nulls(causes)

# ---APPLY TRACHOMA GEOGRAPHIC RESTRICTIONS------------------------------------
# -----------------------------------------------------------------------------

logging.info(
    "BEGINNING TRACHOMA GEOGRAPHIC RESTRICTIONS {}".format(time.time()))
trach_cap = pd.read_csv(cfg.trach_grs)

causes['LOW_TRACH'] = apply_grs(cause_df=causes['LOW_TRACH'],
                                cap_df=trach_cap,
                                cap_col='value_endemicity',
                                age_restrictions=cfg.trach_age_restrictions,
                                location_id=location_id,
                                draw_col='draw_',
                                subset_by_year=True,
                                year_id=2023,
                                year_col='year_start',
                                draw_count=1000)
causes['BLIND_TRACH'] = apply_grs(cause_df=causes['BLIND_TRACH'],
                                  cap_df=trach_cap,
                                  cap_col='value_endemicity',
                                  age_restrictions=cfg.trach_age_restrictions,
                                  location_id=location_id,
                                  draw_col='draw_',
                                  subset_by_year=True,
                                  year_id=2023,
                                  year_col='year_start',
                                  draw_count=1000)
low_gr_trach = causes['LOW_TRACH'].copy()
blind_gr_trach = causes['BLIND_TRACH'].copy()

# ---TURN TRACHOMA PROPORTION INTO A PREVALENCE--------------------------------
# -----------------------------------------------------------------------------

# Convert proportion of low vision loss due to trachoma into a prevalence by
# multiplying prop * (moderate envelope prev + severe envelope prev)
logging.info(
    "TURNING TRACHOMA PROPORTION INTO A PREVALENCE {}".format(time.time()))
merge_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id']
drop_cols = ['measure_id', 'modelable_entity_id', 'model_version_id']

causes['LOW_TRACH'] = pd.merge(causes['LOW_TRACH'], envelopes['LOW_MOD'].drop(
    drop_cols, axis=1), on=merge_cols, suffixes=['_LOW_TRACH', '_MOD_ENV'])

causes['LOW_TRACH'] = pd.merge(causes['LOW_TRACH'], rename_draw_cols(
    envelopes['LOW_SEV'].drop(drop_cols, axis=1), "SEV_ENV"), on=merge_cols)

if cfg.testing:
    causes['LOW_TRACH'].to_csv("{}/{}_low_trach_raw_proportion.csv".format(
        cfg.intermediate_out_dir, location_id))

for i in range(1000):
    low_trach_prop = causes['LOW_TRACH']['draw_{}_LOW_TRACH'.format(i)]
    mod_env = causes['LOW_TRACH']['draw_{}_MOD_ENV'.format(i)]
    sev_env = causes['LOW_TRACH']['draw_{}_SEV_ENV'.format(i)]

    pres_vision_loss = (mod_env + sev_env)

    if np.any(pres_vision_loss < 0):
        pres_vision_loss *= np.where(pres_vision_loss < 0, 0, 1)

        f = open("{}/{}_low_ref_error_trach.txt".format(
            cfg.intermediate_out_dir, location_id), 'w')
        f.write("""
                Vision loss due to ref error larger than vision loss envelope
                in {} so trachoma vision loss draws capped at 0""".format(
                location_id))
        f.close()

    causes['LOW_TRACH'][
        'draw_{}'.format(i)] = low_trach_prop * pres_vision_loss

    if np.all(causes['LOW_TRACH']['draw_{}'.format(i)].values < 0):
        raise ValueError("Trachoma prevalence draws need to be >= 0")

keepcols = ['measure_id',
            'metric_id',
            'sex_id',
            'year_id',
            'location_id',
            'age_group_id']
keepcols.extend(("draw_{}".format(i) for i in range(1000)))

causes['LOW_TRACH'] = causes['LOW_TRACH'][keepcols]

if cfg.testing:
    causes['LOW_TRACH'].to_csv("{}/{}_low_trach_prevalence.csv".format(
        cfg.intermediate_out_dir, location_id))

causes['BLIND_TRACH'] = pd.merge(
    causes['BLIND_TRACH'], envelopes['BLIND'].drop(
        drop_cols, axis=1), on=merge_cols, suffixes=[
            '_BLIND_TRACH', '_BLIND_ENV'])

if cfg.testing:
    causes['BLIND_TRACH'].to_csv("{}/{}_blind_trach_raw_proportion.csv".format(
        cfg.intermediate_out_dir, location_id))

for i in range(1000):
    blind_trach_prop = causes['BLIND_TRACH']['draw_{}_BLIND_TRACH'.format(i)]
    blind_env = causes['BLIND_TRACH']['draw_{}_BLIND_ENV'.format(i)]

    if np.any(blind_env < 0):
        blind_env *= np.where(blind_env < 0, 0, 1)

        f = open(cfg.intermediate_out_dir + "/{"
                                          "}_blind_ref_error_trach.txt".format(
            location_id), 'w')
        f.write("""
                Blindness due to ref error larger than vision loss envelope in
                {} so trachoma vision loss draws capped at 0""".format(
                location_id))
        f.close()

    causes['BLIND_TRACH']['draw_{}'.format(
        i)] = blind_trach_prop * blind_env

    if np.all(causes['BLIND_TRACH']['draw_{}'.format(i)].values < 0):
        raise ValueError("Trachoma prevalence draws need to be >= 0")

causes['BLIND_TRACH'].drop(
    [c for c in causes["BLIND_TRACH"] if "TRACH" in c], axis=1, inplace=True)
causes['BLIND_TRACH'].drop(
    [c for c in causes["BLIND_TRACH"] if "ENV" in c], axis=1, inplace=True)

causes['LOW_TRACH'].drop(
    [c for c in causes["LOW_TRACH"] if "TRACH" in c], axis=1, inplace=True)
causes['LOW_TRACH'].drop(
    [c for c in causes["LOW_TRACH"] if "ENV" in c], axis=1, inplace=True)


for trach_cause in ['BLIND_TRACH', 'LOW_TRACH']:
    causes[trach_cause]['measure_id'] = 5

if cfg.testing:
    causes['BLIND_TRACH'].to_csv("{}/{}_blind_trach_prevalence.csv".format(
        cfg.intermediate_out_dir, location_id))

# ---MILD MODERATE CROSSWALK---------------------------------------------------
# -----------------------------------------------------------------------------

logging.info("BEGINNING MILD MODERATE CROSSWALK {}".format(time.time()))
mild_mod_x_walk = get_mod_plus_proportions(
    prop_path=cfg.meng_enceph_prop_path,
    age_group_set_id=cfg.age_group_set_id,
    release_id=cfg.release_id,
    age_ids_list=cfg.age_ids_list,
    under_5=cfg.under_5)

# ---SPECIAL PROCESSING FOR VITAMIN A------------------------------------------
# -----------------------------------------------------------------------------

logging.info(
    "BEGINNING SPECIAL PROCESSING FOR VITAMIN A {}".format(time.time()))
vita_cap = pd.read_csv(cfg.vita_grs)

# Assume that locations with a value of 0 have a prevalence of zero
logging.info(vita_cap.shape)
vita_prev = vita_cap.query("location_id == @location_id").vita_prev_0.values[0]
for i in range(0, 1000):
    causes['VITA']['draw_{}'.format(
        i)] = causes['VITA']['draw_{}'.format(i)] * vita_prev

# Set Vitamin A prevalence to be 0 for people under age .1
for i in range(1000):
    causes['VITA'].loc[causes['VITA'].age_group_id.isin(
        [ID]), 'draw_{}'.format(i)] = 0

# ---SPLIT VIT A, MENG ENCEPH INTO LOW VISION AND BLINDNESS--------------------
# -----------------------------------------------------------------------------

logging.info(
    "SPLIT VIT A, MENG ENCEPH INTO LOW VISION AND BLINDNESS {}".format(
        time.time()))
blindness_prop = pd.merge(envelopes['BLIND'],
                          total_vision_loss_envelope,
                          on=merge_cols,
                          suffixes=["_BLIND", "_TOTAL"])

for i in range(0, 1000):
    blindness_prop['draw_{}'.format(
        i)] = blindness_prop['draw_{}_BLIND'.format(
            i)] / blindness_prop['draw_{}_TOTAL'.format(i)]

keepcols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
keepcols.extend(("draw_{}".format(i) for i in range(1000)))

blindness_prop = blindness_prop[keepcols]

mod_sev_prop = pd.merge(envelopes['LOW_MOD'],
                        total_vision_loss_envelope,
                        on=merge_cols,
                        suffixes=["_MOD", "_TOTAL"])

mod_sev_prop = pd.merge(mod_sev_prop,
                        rename_draw_cols(envelopes['LOW_SEV'], "SEV"),
                        on=merge_cols)

for i in range(1000):
    severe = mod_sev_prop['draw_{}_SEV'.format(i)]
    moderate = mod_sev_prop['draw_{}_MOD'.format(i)]
    total = mod_sev_prop['draw_{}_TOTAL'.format(i)]
    mod_sev_prop['draw_{}'.format(i)] = (moderate + severe) / total

mod_sev_prop = mod_sev_prop[keepcols]

cause_list = [
    'ENCEPH', 'MENG']
for cause in cause_list:
    causes[cause] = pd.merge(causes[cause], mild_mod_x_walk,
                             on='age_group_id', suffixes=["_RAW", "_XWALK"])
    for i in range(1000):
        raw = causes[cause]['draw_{}_RAW'.format(i)]
        xwalk = causes[cause]['draw_{}_XWALK'.format(i)]
        causes[cause]['draw_{}'.format(i)] = raw * xwalk

    causes[cause].drop(
        [c for c in causes[cause].columns if "XWALK" in c],
        axis=1, inplace=True)
    causes[cause].drop(
        [c for c in causes[cause].columns if "RAW" in c], axis=1, inplace=True)

    # Create new dfs for blind and mod/sex
    causes['BLIND_{}'.format(cause)] = pd.merge(causes[cause],
                                                blindness_prop,
                                                on=merge_cols,
                                                suffixes=["_{}".format(
                                                    cause), "_PROP"])
    causes['LOW_{}'.format(cause)] = pd.merge(causes[cause],
                                              mod_sev_prop,
                                              on=merge_cols,
                                              suffixes=["_{}".format(
                                                  cause), "_PROP"])

    for i in range(1000):
        proportion_blind = causes['BLIND_{}'.format(
            cause)]["draw_{}_PROP".format(i)]
        proportion_mod_sev = causes['LOW_{}'.format(
            cause)]["draw_{}_PROP".format(i)]
        raw_blind = causes['BLIND_{}'.format(
            cause)]["draw_{}_{}".format(i, cause)]
        raw_mod_sev = causes['LOW_{}'.format(
            cause)]["draw_{}_{}".format(i, cause)]

        causes['BLIND_{}'.format(
            cause)]['draw_{}'.format(i)] = raw_blind * proportion_blind
        causes['LOW_{}'.format(
            cause)]['draw_{}'.format(i)] = raw_mod_sev * proportion_mod_sev

    causes['BLIND_{}'.format(
        cause)].drop([c for c in causes['BLIND_{}'.format(
            cause)].columns if cause in c], axis=1, inplace=True)
    causes['BLIND_{}'.format(
        cause)].drop([c for c in causes['BLIND_{}'.format(
            cause)].columns if "PROP" in c], axis=1, inplace=True)

    causes['LOW_{}'.format(cause)].drop([c for c in causes['LOW_{}'.format(
        cause)].columns if cause in c], axis=1, inplace=True)
    causes['LOW_{}'.format(cause)].drop([c for c in causes['LOW_{}'.format(
        cause)].columns if "PROP" in c], axis=1, inplace=True)

    # Delete the old dataframe
    del(causes[cause])

for cause in ['VITA']:
    # Create new dfs for blind and mod/sex
    causes['BLIND_{}'.format(cause)] = pd.merge(causes[cause],
                                                blindness_prop,
                                                on=merge_cols,
                                                suffixes=["_{}".format(
                                                    cause), "_PROP"])
    causes['LOW_{}'.format(cause)] = pd.merge(causes[cause],
                                              mod_sev_prop,
                                              on=merge_cols,
                                              suffixes=["_{}".format(
                                                  cause), "_PROP"])

    for i in range(1000):
        proportion_blind = causes['BLIND_{}'.format(
            cause)]["draw_{}_PROP".format(i)]
        proportion_mod_sev = causes['LOW_{}'.format(
            cause)]["draw_{}_PROP".format(i)]
        raw_blind = causes['BLIND_{}'.format(
            cause)]["draw_{}_{}".format(i, cause)]
        raw_mod_sev = causes['LOW_{}'.format(
            cause)]["draw_{}_{}".format(i, cause)]

        causes['BLIND_{}'.format(
            cause)]['draw_{}'.format(i)] = raw_blind * proportion_blind
        causes['LOW_{}'.format(
            cause)]['draw_{}'.format(i)] = raw_mod_sev * proportion_mod_sev

    causes['BLIND_{}'.format(
        cause)].drop([c for c in causes['BLIND_{}'.format(
            cause)].columns if cause in c], axis=1, inplace=True)
    causes['BLIND_{}'.format(
        cause)].drop([c for c in causes['BLIND_{}'.format(
            cause)].columns if "PROP" in c], axis=1, inplace=True)

    causes['LOW_{}'.format(cause)].drop([c for c in causes['LOW_{}'.format(
        cause)].columns if cause in c], axis=1, inplace=True)
    causes['LOW_{}'.format(cause)].drop([c for c in causes['LOW_{}'.format(
        cause)].columns if "PROP" in c], axis=1, inplace=True)

    # Delete the old dataframe
    del(causes[cause])

# ---SPLIT OUT LOW VISION INTO MODERATE AND SEVERE VISION LOSS-----------------
# -----------------------------------------------------------------------------

logging.info(
    "SPLIT OUT LOW VISION INTO MODERATE AND SEVERE VISION LOSS {}".format(
        time.time()))
check_for_nulls(causes)
mod_plus_sev = pd.merge(envelopes['LOW_MOD'],
                        envelopes['LOW_SEV'],
                        on=merge_cols,
                        suffixes=["_MOD_ENV", "_SEV_ENV"])

for i in range(1000):
    mod_prop = mod_plus_sev['draw_{}_MOD_ENV'.format(i)]
    sev_prop = mod_plus_sev['draw_{}_SEV_ENV'.format(i)]
    mod_plus_sev[
        'draw_{}_MOD_PROP'.format(i)] = mod_prop / (mod_prop + sev_prop)
    mod_plus_sev[
        'draw_{}_SEV_PROP'.format(i)] = sev_prop / (mod_prop + sev_prop)

mod_plus_sev.drop(
    [c for c in mod_plus_sev.columns if "ENV" in c], axis=1, inplace=True)

# Split out etiologies 
for cause in list(causes.keys()):
    # No need to split out refractive error since it is already split
    if "REF_ERROR" in cause:
        continue
    # No need to split out oncho since it is already split
    elif "ONCHO" in cause:
        continue
    # No need to split out ROP since it is already split
    elif "ROP" in cause:
        continue
    # No need to split out blind, just moderate + severe
    elif "BLIND" in cause:
        continue
    else:
        logging.info(cause)
        causes[cause] = pd.merge(causes[cause], mod_plus_sev, on=merge_cols)

        for sev in ["MOD", "SEV"]:
            # Create new dict items using the format low_mod_cause and low_sev_
            # cause for consistency with causes that do not get split (e.g.
            # LOW_MOD_REF_ERROR)
            new_name = "LOW_" + sev + cause[3:]
            logging.info(new_name)
            causes[new_name] = causes[cause].copy()
            for i in range(1000):
                pre_split = causes[new_name]['draw_{}'.format(i)]
                proportion = causes[new_name]['draw_{}_{}_PROP'.format(
                    i, sev)]
                causes[new_name]['post_split_{}'.format(
                    i)] = pre_split * proportion

                # Drop the old draw column and rename the new draw column
                causes[new_name].drop(
                    "draw_{}".format(i), axis=1, inplace=True)
                causes[new_name].rename(
                    columns={"post_split_{}".format(
                        i): 'draw_{}'.format(i)}, inplace=True)


            causes[new_name].drop(
                [c for c in causes[new_name].columns if "PROP" in c],
                axis=1,
                inplace=True)

        # Remove old cause from the dict
        del causes[cause]
check_for_nulls(causes)

# ---CO AND MMD PROCESSING-----------------------------------------------------

# -----------------------------------------------------------------------------

if cfg.subtract_co:
    oth_path = '{}/oth'.format(cfg.intermediate_out_dir)
    co_path = '{}/co'.format(cfg.intermediate_out_dir)
    post_sub_path = '{}/oth_post_sub'.format(cfg.intermediate_out_dir)

    make_directory(oth_path)
    make_directory(co_path)
    make_directory(post_sub_path)

    for sev in ['LOW_MOD', 'LOW_SEV', 'BLIND']:
        # Assign cause severity pair:
        # Subtract co from oth
        oth = causes['{}_OTHER'.format(sev)]
        oth.to_csv(
            "{}/oth_pre_subtraction_{}.csv".format(oth_path, location_id),
            index=False)

        co = causes['{}_CO'.format(sev)].copy()
        co.to_csv(
            "{}/co_pre_subtraction_{}.csv".format(co_path, location_id),
            index=False)

        oth = transform_draws(operator_name='sub',
                              df1=oth,
                              df2=co,
                              df1_draw_col='draw_',
                              df2_draw_col='draw_',
                              resultant_draw_col='draw_',
                              merge_cols=['location_id',
                                          'age_group_id',
                                          'year_id',
                                          'sex_id'],
                              draw_count=1000,
                              negative_draws=True)
        cause_sev = '{}_OTHER'.format(sev)
        # Check for negative draws
        check_df_for_negative_draws(oth, cause_sev)
        get_draw_col_count(oth, 'draw_', cause_sev)
        oth.to_csv("{}/oth_post_subtraction_{}.csv".format(post_sub_path,
                                                           location_id),
                   index=False)
        causes[cause_sev] = oth

if cfg.subtract_mmd:
    for sev in ['LOW_MOD', 'LOW_SEV', 'BLIND']:
        # Subtract mmd from oth
        oth = causes['{}_OTHER'.format(sev)]
        mmd = causes['{}_MMD'.format(sev)].copy()

        oth = transform_draws(operator_name='sub',
                              df1=oth,
                              df2=mmd,
                              df1_draw_col='draw_',
                              df2_draw_col='draw_',
                              resultant_draw_col='draw_',
                              merge_cols=['location_id',
                                          'age_group_id',
                                          'year_id',
                                          'sex_id'],
                              draw_count=1000,
                              negative_draws=True)
        cause_sev = '{}_OTHER'.format(sev)

        # Check for negative draws
        check_df_for_negative_draws(oth, cause_sev)
        get_draw_col_count(oth, 'draw_', cause_sev)
        causes[cause_sev] = oth

# ---ROP PROCESSING------------------------------------------------------------
# -----------------------------------------------------------------------------

logging.info("BEGINNING ROP PROCESSING {}".format(time.time()))
if cfg.testing:
    causes['LOW_MOD_ROP'].to_csv(
        "{}/pre_cap_mod_rop_{}.csv".format(
             cfg.intermediate_out_dir, location_id), index=False)
    causes['LOW_SEV_ROP'].to_csv("{}/pre_cap_sev_rop_{}.csv".format(
        cfg.intermediate_out_dir, location_id), index=False)
    causes['BLIND_ROP'].to_csv("{}/pre_cap_blind_rop_{}.csv".format(
        cfg.intermediate_out_dir, location_id), index=False)

draw_cols = ["draw_{}_ROP".format(i) for i in range(1000)]
env_cols = ["draw_{}_ENV".format(i) for i in range(1000)]

for sev in ['LOW_SEV', 'LOW_MOD', 'BLIND']:
    logging.info("capping {} ROP".format(sev))
    # Merge the rop prevalence and envelope prevalence dataframes
    df = causes["{}_ROP".format(sev)].merge(envelopes[sev],
                                            on=merge_cols,
                                            suffixes=['_ROP', '_ENV'])

    # Melt separately and then join (melting separately and then joining is
    # faster than melting altogether)
    rop_df = df.melt(id_vars=['year_id',
                              'sex_id',
                              'age_group_id',
                              'location_id'],
                     value_vars=draw_cols,
                     var_name='draw',
                     value_name='rop_prev')
    env_df = df.melt(id_vars=['year_id',
                              'sex_id',
                              'age_group_id',
                              'location_id'],
                     value_vars=env_cols,
                     var_name='draw',
                     value_name='env_prev')

    # Make the draw columns just a number so that we can merge on draw number
    rop_df['draw'] = rop_df.draw.str.split("_").str[1]
    env_df['draw'] = env_df.draw.str.split("_").str[1]

    df = rop_df.merge(env_df, on=['year_id',
                                  'sex_id',
                                  'age_group_id',
                                  'location_id',
                                  'draw'])

    df = df.set_index(['year_id', 'sex_id', 'location_id', 'draw'])

    rop_age3 = df.query("age_group_id == ID")
    rop_age3 = rop_age3.drop(['age_group_id', 'env_prev'], axis=1)

    birth_env = df.query("age_group_id == ID")
    birth_env = birth_env.drop(['age_group_id', 'rop_prev'], axis=1)

    new_df = rop_age3.join(birth_env)
    new_df['cap'] = new_df['env_prev'] * .95
    new_df['new_rop_prev'] = np.where(new_df['rop_prev'] > new_df['cap'],
                                      new_df['cap'],
                                      new_df['rop_prev'])
    new_df = new_df.drop(['env_prev', 'rop_prev', 'cap'], axis=1)

    # Now join new_df and df and do some processing so that the new rop
    # prevalence will apply to all ages within an age, sex, and year
    df = df.join(new_df)

    if cfg.testing:
        df.to_csv("{}/pre_cap_apply_{}_rop_{}.csv".format(
            cfg.intermediate_out_dir, sev, location_id), index=False)

    df = df.drop(['env_prev', 'rop_prev'], axis=1)
    df = pd.pivot_table(data=df,
                        columns='draw',
                        values='new_rop_prev',
                        index=['year_id',
                               'sex_id',
                               'location_id',
                               'age_group_id'])
    df.columns = ["draw_" + x for x in list(df.columns)]
    df = df.reset_index()

    # Reassign df to equal the sev ROP DataFrame
    del(causes["{}_ROP".format(sev)])
    causes["{}_ROP".format(sev)] = df
    del(df)

if cfg.testing:
    causes['LOW_MOD_ROP'].to_csv(
        "{}/mod_capped_rop.csv".format(cfg.intermediate_out_dir), index=False)
    causes['LOW_SEV_ROP'].to_csv(
        "{}/sev_capped_rop.csv".format(cfg.intermediate_out_dir), index=False)
    causes['BLIND_ROP'].to_csv(
        "{}/blind_capped_rop.csv".format(cfg.intermediate_out_dir))

# ---SUBTRACT SMALLER CAUSES OF VISION LOSS FROM OTHER-------------------------
# -----------------------------------------------------------------------------

if cfg.subtract_smaller_causes:

    logging.info("Subtracting ")
    for sev in ['LOW_MOD', 'LOW_SEV', 'BLIND']:
        # Assign cause severity pair:
        # Filter for other
        oth = causes['{}_OTHER'.format(sev)]

        # Filter for small causes
        var_list = ['{}_MENG'.format(sev), '{}_ENCEPH'.format(sev),
                    '{}_ROP'.format(sev), '{}_VITA'.format(sev)]
        small_causes = {k:v for k, v in causes.items() if k in var_list}

        # Sum the dictionary totals for the smaller causes
        sc_total = get_dict_totals(dictionary=small_causes,
                                   sev=sev,
                                   exclude_rop=False,
                                   exclude_vita=False)

        oth = transform_draws(operator_name='sub',
                              df1=oth,
                              df2=sc_total,
                              df1_draw_col='draw_',
                              df2_draw_col='draw_',
                              resultant_draw_col='draw_',
                              merge_cols=['location_id',
                                          'age_group_id',
                                          'year_id',
                                          'sex_id'],
                              draw_count=1000,
                              negative_draws=True)
        cause_sev = '{}_OTHER'.format(sev)

        # Check for negative draws
        check_df_for_negative_draws(oth, cause_sev)
        get_draw_col_count(oth, 'draw_', cause_sev)
        causes[cause_sev] = oth

# ---MAKE COPY OF PRESQUEEZED ESTIMATES FOR VETTING PURPOSES-------------------

logging.info("Copying meids for diagnostic purposes")
presqueezed = copy.deepcopy(causes)

# Remove geographic restricted Trachoma estimates since they do not go
# through the squeeze.
logging.info("Removing unneeded trachoma meids")
logging.info("Number of meids before removal: {}".format(
    len(list(presqueezed.keys()))))
logging.info("Number of meids after removal: {}".format(
    len(list(presqueezed.keys()))))

# Rename variable names to reflect the fact that these are for diagnostics only
for dict_key in list(presqueezed.keys()):
    new_key = dict_key + '_diag'
    presqueezed[new_key] = presqueezed.pop(dict_key)

# ---SQUEEZE 1-----------------------------------------------------------------
# Proportionally scale the prevalence of all vision loss etiologies, except
# ROP, to fit the envelope. 
# -----------------------------------------------------------------------------

logging.info("BEGINNING SQUEEZE 1 {}".format(time.time()))
# Get severity level totals. Make an empty dict and fill it with dataframes of
# all causes summed up. We need the sum of all causes for squeezing.
totals = {}

for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    totals[sev] = get_dict_totals(dictionary=causes,
                                  sev=sev,
                                  exclude_rop=True,
                                  exclude_vita=False)

# Now squeeze
squeezes = {}

for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    logging.info(sev)
    envelopes[sev] = pd.merge(envelopes[sev],
                              causes['{}_ROP'.format(sev)],
                              on=['location_id',
                                  'age_group_id',
                                  'year_id',
                                  'sex_id'],
                              suffixes=['_ENV', '_ROP'])

    for i in range(1000):
        envelope = envelopes[sev]['draw_{}_ENV'.format(i)]
        rop = envelopes[sev]['draw_{}_ROP'.format(i)]
        if not np.all(rop < envelope):
            # Where rop is greater than the envelope, cap it at 95% of the
            # envelope.
            rop = np.where(rop > envelope, envelope*.95, rop)

            f = open("{}/{}_{}_rop_greater_than_envelope.txt".format(
                 cfg.intermediate_out_dir, location_id, sev), 'w')
            f.write("""
                 Rop was greater than the envelope for at least one draw in {}
                 so we capped those values at .95 times the envelope""".format(
                     location_id))
            f.close()
        envelopes[sev]['draw_{}'.format(i)] = envelope - rop

    envelopes[sev].drop([c for c in envelopes[sev].columns if "ROP" in c],
                        axis=1,
                        inplace=True)
    envelopes[sev].drop([c for c in envelopes[sev].columns if "ENV" in c],
                        axis=1,
                        inplace=True)

    if np.all(envelopes[sev]['draw_{}'.format(i)].values < 0):
        raise ValueError("{} envelope prevalence draws need to be >= 0".format(
            sev))

for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    logging.info(sev)
    squeezes[sev] = pd.merge(totals[sev],
                             envelopes[sev],
                             on=merge_cols,
                             suffixes=['_TOT', '_ENV'])

    for i in range(1000):
        total = squeezes[sev]['draw_{}_TOT'.format(i)]
        envelope = squeezes[sev]['draw_{}_ENV'.format(i)]
        squeezes[sev]['draw_{}'.format(i)] = envelope / total

    squeezes[sev].drop([c for c in squeezes[sev].columns if "_TOT" in c],
                       axis=1,
                       inplace=True)
    squeezes[sev].drop([c for c in squeezes[sev].columns if "_ENV" in c],
                       axis=1,
                       inplace=True)

for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    logging.info(sev)
    sev_specific_causes = [
        cause for cause in list(
           causes.keys()) if sev in cause and "ROP" not in cause
        ]

    for cause in sev_specific_causes:
        logging.info(cause)
        get_draw_col_count(causes[cause], 'draw_', cause)
        logging.info(causes[cause].shape)
        causes[cause] = pd.merge(causes[cause],
                                 squeezes[sev],
                                 on=merge_cols,
                                 suffixes=['_pre_squeeze', '_squeeze'])
        get_draw_col_count(causes[cause], 'draw_', cause)
        logging.info(causes[cause].shape)

        for i in range(1000):
            pre_squeeze = causes[cause]['draw_{}_pre_squeeze'.format(i)]
            squeeze = causes[cause]['draw_{}_squeeze'.format(i)]
            causes[cause]['draw_{}'.format(i)] = pre_squeeze * squeeze

        causes[cause].drop(
            [c for c in causes[cause].columns if "_squeeze" in c],
            axis=1,
            inplace=True)
check_for_nulls(causes)

# ---POST-HOC ADJUSTMENTS------------------------------------------------------
# -----------------------------------------------------------------------------

logging.info("BEGINNING POST-HOC ADJUSTMENTS {}".format(time.time()))
# make a function to cap vision loss due to vitamin a deficiency
for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    logging.info("capping {} due to vitamin a deficiency".format(sev))

    # we want to see which and how many locations have vita capped, so the
    # logic below determines
    # which locations get capped and outputs a text file to denote that the
    # location was capped
    df = cap_vita(df=causes['{}_VITA'.format(sev)],
                  location_id=location_id,
                  age_ids_list=cfg.age_ids_list)
    if df.equals(causes['{}_VITA'.format(sev)]):
        continue
    elif not df.equals(causes['{}_VITA'.format(sev)]):

        f = open("{}/{}_capped_vita.txt".format(
             cfg.intermediate_out_dir, location_id), 'w')
        f.write("""
                capped vita vision loss over age group ID in
                location_id {}""".format(location_id))
        f.close()

        del causes['{}_VITA'.format(sev)]
        causes['{}_VITA'.format(sev)] = df

    del(df)
check_for_nulls(causes)

# ---SQUEEZE 2-----------------------------------------------------------------
# -----------------------------------------------------------------------------

logging.info("BEGINNING SQUEEZE 2 {}".format(time.time()))

# Get severity level totals
# Make an empty dict and fill it with dataframes of all causes summed up
# We need the sum of all causes for squeezing
totals = {}
squeezes2 = {}

# Get totals and envelope, both without ROP and the pre-capped vita estimates
for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    # Prep totals and envelope for second squeeze
    totals[sev] = get_dict_totals(dictionary=causes,
                                  sev=sev,
                                  exclude_rop=True,
                                  exclude_vita=True)
    check_df_for_nulls(totals[sev], sev)
    envelopes[sev] = add_subtract_envelope(envelopes=envelopes,
                                           causes=causes,
                                           sev=sev,
                                           cause="VITA",
                                           operation="SUBTRACT")
    check_df_for_nulls(envelopes[sev], sev)

    logging.info("generating squeeze 2 squeeze factors for " + sev)
    logging.info("Merging ENV and TOT columns")
    squeezes2[sev] = pd.merge(envelopes[sev],
                              totals[sev],
                              on=merge_cols,
                              suffixes=['_ENV', '_TOT'])
    check_df_for_nulls(squeezes2[sev], sev)

    logging.info("Dividing ENV by TOT")
    for i in range(1000):
        total = squeezes2[sev]["draw_{}_TOT".format(i)]
        env = squeezes2[sev]["draw_{}_ENV".format(i)]

        squeezes2[sev]["squeeze_{}".format(i)] = (env / total).fillna(1)
    check_df_for_nulls(squeezes2[sev], sev)

    logging.info("Dropping ENV columns")
    squeezes2[sev].drop([c for c in squeezes2[sev].columns if "_ENV" in c],
                        axis=1,
                        inplace=True)
    check_df_for_nulls(squeezes2[sev], sev)
    logging.info("Dropping TOT columns")
    squeezes2[sev].drop([c for c in squeezes2[sev].columns if "_TOT" in c],
                        axis=1,
                        inplace=True)
    check_df_for_nulls(squeezes2[sev], sev)

    if cfg.testing:
        squeezes2[
            sev].to_csv("{}/squeeze_factors_for_{}_second_squeeze.csv".format(
                cfg.intermediate_out_dir, sev))

# Squeeze 2
# Resqueeze to account for changes in vision loss due to vitamin a
for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    logging.info(sev + " squeeze 2")
    sev_specific_causes = [
        cause for cause in list(
            causes.keys()
            ) if sev in cause and "ROP" not in cause and "VITA" not in cause]

    for cause in sev_specific_causes:
        logging.info(cause + " squeeze 2")

        causes[cause] = pd.merge(causes[cause], squeezes2[sev], on=merge_cols)

        for i in range(1000):
            squeeze = causes[cause]['squeeze_{}'.format(i)]
            causes[cause]['draw_{}'.format(i)] *= squeeze

        causes[cause].drop(
            [c for c in causes[cause].columns if "_squeeze" in c],
            axis=1,
            inplace=True)
check_for_nulls(causes)

# ---MAKE SURE THERE ARE NO NEGATIVE DRAWS-------------------------------------

logging.info("MAKING SURE THERE ARE NO NEGATIVE DRAWS {}".format(time.time()))
check_for_negative_draws(causes)
check_for_nulls(causes)

# ---TEST THAT THE SUM OF ALL OF THE ETIOLOGIES ADDS UP TO THE ENVELOPE--------
# -----------------------------------------------------------------------------

logging.info(
    "TESTING THAT THE SUM ADDS UP TO THE ENVELOPE {}".format(time.time()))
totals = {}

for sev in ['BLIND', 'LOW_MOD', 'LOW_SEV']:
    totals[sev] = get_dict_totals(dictionary=causes,
                                  sev=sev,
                                  exclude_rop=False,
                                  exclude_vita=False)
    # Add back in ROP and VITA to the envelope
    envelopes[sev] = add_subtract_envelope(envelopes=envelopes,
                                           causes=causes,
                                           sev=sev,
                                           cause="ROP",
                                           operation="ADD")
    envelopes[sev] = add_subtract_envelope(envelopes=envelopes,
                                           causes=causes,
                                           sev=sev,
                                           cause="VITA",
                                           operation="ADD")
    df1, df2 = sort_totals_and_envelope(totals[sev], envelopes[sev])

    if np.allclose(df1, df2):
        logging.info("""
              The sum of all of the {} etiologies is equal to the envelope.
              Time to output final results.""".format(sev))
    else:
        raise Exception("""
                        The sum of all of the {} etiologies is NOT equal to the
                        envelope.""".format(sev))

# ---GET BEST CORRECTED BLINDNESS----------------------------------------------

logging.info("GETTING BEST CORRECTED BLINDNESS {}".format(time.time()))
squeezed_blind_re = causes['BLIND_REF_ERROR']
blindness_envelope = envelopes['BLIND']
causes['BC_BLIND'] = get_best_corrected_blindness(squeezed_blind_re,
                                                  blindness_envelope,
                                                  location_id)

# ---MAKE NEAR VISION PREVALENCE ADJUSTMENT------------------------------------

logging.info(
    "MAKING NEAR VISION PREVALENCE ADJUSTMENT {}".format(time.time()))
causes['NEAR_ADJ'] = adjust_near_vision_prevalence(
    location_id=location_id,
    age_ids_list=cfg.age_ids_list,
    release_id=cfg.release_id)

# ---OUTPUT FINAL ESTIMATES TO THE FINAL RESULTS DIRECTORY---------------------

logging.info(
    "OUTPUT FINAL ESTIMATES TO THE FINAL RESULTS DIR {}".format(time.time()))

# Add geographically restricted trachoma estimates to the other causes
causes['LOW_GR_TRACH'] = low_gr_trach
causes['BLIND_GR_TRACH'] = blind_gr_trach

# Add presqueezed estimates to the other causes
causes.update(presqueezed)

for cause in list(causes.keys()):
    logging.info(cause)
    meid = output_meids.query("variable_name == '{}'".format(
        cause.lower())).modelable_entity_id.values[0]
    logging.info(meid)

    logging.info("""
          Final draws are prepped for {}. The corresponding meid
          is {}""".format(cause, meid))

    causes[cause]['modelable_entity_id'] = meid

    # make directories if they do not exists
    directory = cfg.final_out_dir + "/{}/".format(meid)
    make_directory(directory)
    save_cols = causes[cause].columns
    causes[cause].to_hdf(path_or_buf=directory + "{}.h5".format(location_id),
                         key='draws',
                         mode='w',
                         data_columns=save_cols)

logging.info("CODE HAS FINISHED RUNNING {}".format(time.time()))
