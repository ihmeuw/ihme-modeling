"""
Project: GBD Hearing Loss
Purpose: Execute the hearing loss parent squeeze, hearing aid adjustments,
etiology specific adjustments, etiology squeeze and tinnitus adjustments for a
given location. 

The transformations that occur in this step of the pipeline include the
following:

1. GET INPUT ARGUMENTS
2. PARSE MAPPING FILE
3. PARSE PROPORTION AND SPLIT DATA
4. LAY FOUNDATION FOR CREATING DIAGNOSTIC ESTIMATES
5. GET DRAWS
6. PREP NORMAL ENVELOPE
7. APPLY PARENT SQUEEZE
8. CALCULATE HEARING AIDS PROPORTION
9. APPLY HEARING AIDS PROPORTION
10. PREP FOR ETIOLOGY SPECIFIC ADJUSTMENTS
11. MEASURE CONVERSION FOR AGE RELATED/OTHER HEARING LOSS
12. SPLIT SEVERITIES FOR OTITIS
13. CROSSWALK AND SPLIT MENINGITIS
14. SQUEEZE ETIOLOGIES
15. APPLY TINNITUS ADJUSTMENT
16. OUTPUT FINAL ESTIMATES
"""

import argparse
import logging
import sys
import time

import numpy as np
import pandas as pd
from scipy.stats import beta

from get_draws.api import get_draws

import config as cfg
from transformations.adjust import (apply_meningitis_prop, split_otitis)
from transformations.squeeze import (HearingLossEtiologySqueeze, HearingLossParentSqueeze)

from core.aggregation import aggregate_causes
from core.general_utils import impute_birth_estimates
from core.io_utils import make_directory
from core.transform import (cap_draws,
                       merge_draws,
                       fill_draws,
                       rename_draws,
                       transform_draws)
from core.quality_checks import (check_df_for_nulls,
                            check_shape,
                            check_occurances_for_df,
                            merge_check)


if __name__ == '__main__':

    logging.basicConfig(
            level=logging.INFO,
            format=' %(asctime)s - %(levelname)s - %(message)s')

    # ---GET INPUT ARGUMENTS---------------------------------------------------
    start = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--location_id",
                        help="GBD location_id for one location",
                        type=int)
    parser.add_argument("--mapping",
                        help="Serialized Metadata for cause, meid and "
                             "severity mapping",
                        type=str)
    parser.add_argument("--serialized_ha",
                        help="Serialized custom bundle data required for the "
                             "hearing aids adjustment",
                        type=str)
    parser.add_argument("--ha_coverage_locs",
                        help="List of Location_ids where hearing aids "
                             "proportion data exists",
                        type=int)
    parser.add_argument("--serialized_tin",
                        help="Serialized custom bundle data required for the "
                             "tinnitius split",
                        type=str)
    parser.add_argument("--serialized_regression",
                        help="Serialized draws representing the hearing aids "
                             "regression",
                        type=str)
    parser.add_argument("--serialized_meng",
                        help="Serialized proportion data for the meningitis "
                             "mild to moderate crosswalk",
                        type=str)

    args = parser.parse_args()
    location_id = args.location_id
    serialized_mapping = args.mapping
    serialized_ha = args.serialized_ha
    ha_coverage_locs = args.ha_coverage_locs
    serialized_tin = args.serialized_tin
    serialized_regression = args.serialized_regression
    serialized_meng = args.serialized_meng

    logging.info("Running pipeline for location_id {}".format(location_id))

    # ---PARSE MAPPING FILE----------------------------------------------------
    # Split the hearing modeling codebook that was passed as an input argument
    # into four separate dataframe that will contain the metadata for all of
    # the meids that are used in the pipeline. The four dataframes are as
    # follows:
    #
    # 1. Input meids: Presqueezed dismod and custom meids
    # representing the input etiologies and envelopes.
    # 2. Transformation meids: Meids used for transformation within the
    # pipeline but are not outputted and do not go through either of the
    # squeezes.
    # 3. Output meids: Custom meids that are a result of all of the pipeline's
    # retransformations and go into the central machinery.
    # 4. Diagnostic meids: Custom meids that are produced after many of the
    # transformations and are used for diagnostic purposes.
    # -------------------------------------------------------------------------

    # Un-serialize modeling codebook
    cb = pd.read_pickle(serialized_mapping)
    check_shape(cb)
    cb = cb[['variable_name_new',
             'entity',
             'severity',
             'modelable_entity_id',
             'type',
             'impute_birth_prev']]

    # Obtain metadata for input meids
    input_metadata = cb.copy()
    input_metadata = input_metadata[
        input_metadata['type'].isin(['input', 'envelope'])]
    check_shape(input_metadata)

    # Obtain metadata for meids used in transformations.
    transformation_metadata = cb.copy()
    transformation_metadata = transformation_metadata[
        transformation_metadata['type'] == 'transformations']
    check_shape(transformation_metadata)

    # Obtain metadata for output meids
    output_metadata = cb.copy()
    output_metadata = output_metadata[output_metadata['type'] == 'output']
    output_metadata = output_metadata[['variable_name_new',
                                       'modelable_entity_id']]
    check_shape(output_metadata)

    # Obtain metadata for diagnostic meids
    diagnostic_metadata = cb.copy()
    diagnostic_metadata = diagnostic_metadata[
        diagnostic_metadata['type'] == 'diagnostic']
    diagnostic_metadata = diagnostic_metadata[['variable_name_new',
                                               'modelable_entity_id']]
    check_shape(diagnostic_metadata)


    input_meids = input_metadata['modelable_entity_id'].unique().tolist()
    transformation_meids = transformation_metadata[
        'modelable_entity_id'].unique().tolist()

    # Un-serialize data:
    tinnitus_data = pd.read_pickle(cfg.serialized_tin_path)
    check_shape(tinnitus_data)
   
    hearing_aid_coverage = pd.read_pickle(cfg.serialized_ha_path)
    check_shape(hearing_aid_coverage)
    regression_draws = pd.read_pickle(cfg.serialized_regression_path)
    check_shape(regression_draws)
    meningitis_xwalk_prop = pd.read_pickle(cfg.serialized_meng_path)
    check_shape(meningitis_xwalk_prop)



    all_estimates = []

   
    logging.info("Pulling draws for envelopes and etiologies")
    draws = pd.DataFrame()
    for i in input_meids:
        temp_df = get_draws(gbd_id_type=cfg.id_type,
                            gbd_id=i,
                            source=cfg.source,
                            measure_id=cfg.input_measure_ids,
                            location_id=location_id,
                            age_group_id=cfg.age_ids_list,
                            status=cfg.current_status,
                            release_id=cfg.release_id
                           )
        draws = pd.concat([draws, temp_df], ignore_index=True)

    draws = draws.drop(labels=['model_version_id', 'metric_id'], axis=1)
    draws = check_df_for_nulls(draws, 'input draws')

    logging.info("Pulling draws for hearing aids coverage")
    hearing_aid_locs = [location_id, ha_coverage_locs]
    hearing_aid_draws = get_draws(gbd_id_type=cfg.id_type,
                                  gbd_id=transformation_meids,
                                  source=cfg.source,
                                  measure_id=cfg.save_measure_id,
                                  location_id=hearing_aid_locs,
                                  age_group_id=cfg.age_ids_list,
                                  status=cfg.current_status,
                                  release_id=cfg.release_id)
    hearing_aid_draws = hearing_aid_draws.drop(labels=['modelable_entity_id',
                                                       'measure_id',
                                                       'model_version_id',
                                                       'metric_id'],
                                               axis=1)

    # Add birth prevalence estimates. Estimates should be equal to zero.
    # Add zero draws 
    ha_birth_prev = hearing_aid_draws.copy()
    ha_birth_prev = ha_birth_prev[ha_birth_prev['age_group_id'] == ID]
    ha_birth_prev = ha_birth_prev[
        ha_birth_prev['location_id'] == cfg.nt_ha_coverage]
    # Change age_group_id to ID
    ha_birth_prev['age_group_id'] = cfg.birth_prev
    ha_birth_prev = fill_draws(df=ha_birth_prev, fill_val=0)
    hearing_aid_draws = pd.concat([hearing_aid_draws, ha_birth_prev])

    # Initiate quality checks
    hearing_aid_draws = check_df_for_nulls(df=hearing_aid_draws,
                                           entity_name='hearing aid draws')  
    check_occurances_for_df(df=hearing_aid_draws, cols=cfg.dd_cols)
    

    # Merge on code books
    before = draws.shape[0]
    draws = draws.merge(cb, on='modelable_entity_id', how='left')
    after = draws.shape[0]
    merge_check(before, after)

    # For input models that do not have birth prevalence, fill in birth
    # prevalence estimates with estimates from early neonatal (age_group_id ID).
    no_bp_input_draws = draws.copy()
    no_bp_input_draws = no_bp_input_draws[
        no_bp_input_draws['impute_birth_prev'] == 'yes']
    no_bp_input_draws = impute_birth_estimates(df=no_bp_input_draws)
    no_bp_input_draws = no_bp_input_draws[
        no_bp_input_draws['age_group_id'] == cfg.birth_prev]
    draws = pd.concat([draws, no_bp_input_draws])
    check_occurances_for_df(df=draws, cols=cfg.dd_cols)

    # Split out cause specific and envelope draws
    envs = draws[draws['type'] == 'envelope']
    envs = envs.drop(labels=['variable_name_new',
                             'modelable_entity_id',
                             'entity',
                             'measure_id',
                             'type',
                             'impute_birth_prev'],
                     axis=1)
    envs = check_df_for_nulls(df=envs, entity_name='envelope draws')
    check_occurances_for_df(df=envs, cols=cfg.dds_cols)

    causes = draws[draws['type'] == 'input']
    causes = causes.drop(labels=['variable_name_new',
                                 'modelable_entity_id',
                                 'severity',
                                 'measure_id',
                                 'type',
                                 'impute_birth_prev'],
                         axis=1)
    causes = check_df_for_nulls(df=causes, entity_name='input causes')
    check_occurances_for_df(df=causes, cols=cfg.dde_cols)

    norm_env = envs.copy()
    norm_env = norm_env[norm_env['severity'] == 'norm']
    transformation_variable = fill_draws(df=norm_env, fill_val=1)
    adj_norm = transform_draws(operator_name='sub',
                               df1=transformation_variable,
                               df2=norm_env,
                               merge_cols=cfg.dds_cols)
    envs = envs[envs['severity'] != 'norm']
    envs = pd.concat([adj_norm, envs])

    # ---APPLY PARENT SQUEEZE--------------------------------------------------
    # Squeeze the severity specific envelopes to ensure that they sum up to 1.
    # -------------------------------------------------------------------------

    ps_val_cols = ['draw_{}'.format(x) for x in range(cfg.draw_count)]
    hlps = HearingLossParentSqueeze()
    envs = hlps.run(envelopes=envs,
                    id_cols=cfg.dd_cols,
                    val_cols=ps_val_cols,
                    merge_cols=cfg.dd_cols,
                    draw_count=cfg.draw_count)
    check_occurances_for_df(df=envs, cols=cfg.dds_cols)
    envs = check_df_for_nulls(df=envs, entity_name='squeezed envelopes')

    # Assign post parent squeeze estimates to a seperate dataframe for
    # diagnostic purposes.
    post_ps = envs.copy()
    post_ps['entity'] = 'env'
    post_ps['diagnostic_type'] = 'post_ps'
    post_ps['ringing'] = 'nr'
    all_estimates.append(post_ps)

    # ---CALCULATE HEARING AIDS PROPORTION-------------------------------------
    # Country X severity specific hearing aid coverage = (country X hearing
    # aid coverage / Norway hearing aid coverage) * Norway severity
    # specific hearing aid coverage.
    #
    # -------------------------------------------------------------------------

    hearing_aid_cov_target_loc = hearing_aid_draws.copy()
    hearing_aid_cov_adjustment_locs = hearing_aid_draws.copy()

    # Hearing aid coverage for location being paralellized
    hearing_aid_cov_target_loc = hearing_aid_cov_target_loc[
        hearing_aid_cov_target_loc['location_id'] == location_id]
    # Hearing aid coverage for locations being used for the regression
    hearing_aid_cov_adjustment_locs = hearing_aid_cov_adjustment_locs[
        hearing_aid_cov_adjustment_locs[
            'location_id'] == cfg.nt_ha_coverage]

    # Calculate ratio
    coverage_ratio = transform_draws(operator_name='truediv',
                                     df1=hearing_aid_cov_target_loc,
                                     df2=hearing_aid_cov_adjustment_locs,
                                     merge_cols=['year_id',
                                                 'age_group_id',
                                                 'sex_id'])
    coverage_ratio = coverage_ratio.replace(np.inf, 0)

    # Add birth prevalence estimates to the regression draws. 
    # Estimates should be equal to zero.
    reg_bp = regression_draws.copy()
    reg_bp = reg_bp[reg_bp['age_group_id'] == ID]
    reg_bp['age_group_id'] = cfg.birth_prev
    reg_bp = fill_draws(df=reg_bp, fill_val=0)
    updated_reg_draws = pd.concat([regression_draws, reg_bp])

    # Country X severity specific hearing aid coverage = (country
    # X hearing aid coverage / Norway hearing aid coverage) * Norway
    # severity specific hearing aid coverage
    coverage_prop = transform_draws(operator_name='mul',
                                    df1=coverage_ratio,
                                    df2=updated_reg_draws,
                                    merge_cols=['age_group_id',
                                                'sex_id'])

    # Assume there is no correction for complete hearing loss (deafness)
    # and assign the coverage proportion to 0 for all estimates for
    # complete hearing loss.
    coverage_prop_com = coverage_prop.copy()
    coverage_prop_com = coverage_prop_com[
        coverage_prop_com['severity'] == 'pro']
    coverage_prop_com['severity'] = 'com'

    coverage_prop_com = fill_draws(df=coverage_prop_com, fill_val=0)

    coverage_prop = pd.concat([coverage_prop_com, coverage_prop])

    # ---APPLY HEARING AIDS PROPORTION-----------------------------------------
    # The hearing aids adjustment essentially calculates the prevalence of
    # people with hearing aids for each severity and then adjusts the
    # envelopes to account for those with hearing aids by moving them down a
    # severity Essentially two major transformations occur to complete this adjustment:
    #
    # 1). People with hearing aids in their current severity get removed from
    # that severity and added to a lower severity (similar to how best
    # corrected vision loss works although this is not part of the vision loss
    # post processing pipeline).
    #
    # 2). People with hearing aids from a higher severity get added to that
    # severity.
    # -------------------------------------------------------------------------

    # Category by category apply the adjustment. Convert hearing aid
    # coverage to prevalence(severity prevalence * hearing aid coverage)
    coverage_prev = transform_draws(operator_name='mul',
                                    df1=envs,
                                    df2=coverage_prop,
                                    merge_cols=['age_group_id',
                                                'sex_id',
                                                'severity',
                                                'year_id'])
    check_occurances_for_df(df=coverage_prev, cols=cfg.dds_cols)

    # Make sure hearing aid coverage is never over 95 % for a given
    # severity and 0 if missing (birth prevalence)
    coverage_prev = cap_draws(draws=coverage_prev,
                              draws_to_cap=envs,
                              cap_pct=0.95,
                              merge_cols=cfg.dds_cols)

    less_severe_envs = envs.copy()
    check_occurances_for_df(df=envs, cols=cfg.dds_cols)
    less_severe_envs = less_severe_envs[less_severe_envs['severity'].isin(
        ['mld', 'mod', 'mod_sev', 'sev'])]

    less_severe_envs = transform_draws(operator_name='sub',
                                       df1=less_severe_envs,
                                       df2=coverage_prev,
                                       merge_cols=cfg.dds_cols)

    adj_envs = []

    # Split up envelopes by severity
    env_mld = less_severe_envs.copy()
    env_mod, coverage_mod = less_severe_envs.copy(), coverage_prev.copy()
    env_mod_sev, coverage_mod_sev = less_severe_envs.copy(), coverage_prev.copy()
    env_sev, coverage_sev = less_severe_envs.copy(), coverage_prev.copy()
    env_pro, coverage_pro = envs.copy(), coverage_prev.copy()
    env_com, coverage_com = envs.copy(), coverage_prev.copy()

    env_mld = env_mld[env_mld['severity'] == 'mld']
    env_mod = env_mod[env_mod['severity'] == 'mod']
    env_mod_sev = env_mod_sev[env_mod_sev['severity'] == 'mod_sev']
    env_sev = env_sev[env_sev['severity'] == 'sev']
    # The hearing aids adjustment is not applied to the severe, profound and
    # complete envelopes.
    adj_envs.append(env_sev)
    env_pro = env_pro[env_pro['severity'] == 'pro']
    adj_envs.append(env_pro)
    env_com = env_com[env_com['severity'] == 'com']
    adj_envs.append(env_com)

    coverage_mod = coverage_mod[coverage_mod['severity'] == 'mod']
    coverage_mod_sev = coverage_mod_sev[
        coverage_mod_sev['severity'] == 'mod_sev']
    coverage_sev = coverage_sev[coverage_sev['severity'] == 'sev']
    coverage_pro = coverage_pro[coverage_pro['severity'] == 'pro']
    coverage_com = coverage_com[coverage_com['severity'] == 'com']

    # Apply coverage for people with moderate hearing loss and hearing aids
    # to the mild envelope.
    env_mld = transform_draws(operator_name='add',
                              df1=env_mld,
                              df2=coverage_mod,
                              merge_cols=cfg.dd_cols)
    env_mld['severity'] = 'mld'
    adj_envs.append(env_mld)

    # Apply coverage for people with moderately severe hearing loss and hearing
    # aids to the moderate envelope.
    env_mod = transform_draws(operator_name='add',
                              df1=env_mod,
                              df2=coverage_mod_sev,
                              merge_cols=cfg.dd_cols)
    env_mod['severity'] = 'mod'
    adj_envs.append(env_mod)

    # Apply coverage for people with severe hearing loss and hearing
    # aids to the moderately severe envelope.
    env_mod_sev = transform_draws(operator_name='add',
                                  df1=env_mod_sev,
                                  df2=coverage_sev,
                                  merge_cols=cfg.dd_cols)
    env_mod_sev['severity'] = 'mod_sev'
    adj_envs.append(env_mod_sev)

    # Combine adjusted data
    envs = pd.concat(adj_envs)
    envs = check_df_for_nulls(df=envs,
                              entity_name='Hearing aids proportion '
                                          'envelopes')
    check_occurances_for_df(df=envs, cols=cfg.dds_cols)

    # Assign post hearing aid adjustment estimates to a separate dataframe for
    # diagnostic purposes.
    post_ha = envs.copy()
    post_ha['entity'] = 'env'
    post_ha['diagnostic_type'] = 'post_ha'
    post_ha['ringing'] = 'nr'
    all_estimates.append(post_ha)

    # ---PREP FOR ETIOLOGY SPECIFIC ADJUSTMENTS--------------------------------
    # We need to ensure that the sum of the etiologies does not exceed values
    # for the envelopes. Before doing so, we need to apply adjustments to
    # Age related and other hearing loss, Meningitis, and Otitis (estimates
    # for congenital hearing loss are calculated during the etiology
    # squeeze). 
    # -------------------------------------------------------------------------

    # Split out etiologies:
    adj_etiologies = []

    other_hearing = causes.copy()
    other_hearing = other_hearing[other_hearing['entity'] == 'oth']

    otitis = causes.copy()
    otitis = otitis[otitis['entity'] == 'otitis']

    meningitis = causes.copy()
    meningitis = meningitis[meningitis['entity'] == 'meng']

    # ---CONVERT AGE-RELATED OTHER HEARING LOSS TO A PREVALENCE----------------
    # Turn other hearing loss from proportion into prevalence by severity
    # Prevalence of other hearing loss at given severity = proportion of
    # other hearing loss * envelope prevalence (hearing aid adjusted) at given
    # severity.
    # -------------------------------------------------------------------------

    # Create a copy of the envelopes specifically for the pre-etiology
    # squeeze etiology adjustments
    et_adj_envs = envs.copy()

    # This should split out age related and other hearing loss by severity.
    other_hearing = transform_draws(operator_name='mul',
                                    df1=other_hearing,
                                    df2=et_adj_envs,
                                    merge_cols=cfg.dd_cols)
    adj_etiologies.append(other_hearing)

    # ---SPLIT SEVERITIES FOR OTITIS-------------------------------------------
    # Chronic Otitis media only leads to mild and moderate hearing loss with
    # zero prevalence for categories above moderate. Mild and moderate are
    # split proportionally as per Figure 2
    # (FILEPATH).
    # -------------------------------------------------------------------------

    # Calculate proportion using a beta distribution by creating 1000
    # observations 
    # 
    otitis_beta = beta.rvs(cfg.ot_alpha,
                           cfg.ot_beta,
                           size=cfg.draw_count,
                           random_state=1)
    draw_cols = ["draw_{}".format(x) for x in range(cfg.draw_count)]
    # Get the list of betas into a format where they can be turned into a
    # dataframe by reshaping the list to a wide format.
    otitis_beta = otitis_beta.reshape((1,) + otitis_beta.shape)
    ot_mld_prop = pd.DataFrame(otitis_beta, columns=[draw_cols])
    ot_mld_prop['entity'] = 'otitis'
    # Replace otitis values with proportion draws to ensure both dataframes
    # have the same shape.
    ot_mld_prop = fill_draws(df=otitis, fill_val=ot_mld_prop)

    # Apply beta distribution to Otitis to split out mild and moderate
    # Otitis from the Otitis etiology.
    otitis_sevs = []

    otitis_mld_mod = split_otitis(ot_df=otitis,
                                  ot_prop=ot_mld_prop,
                                  mld_ot_merge_cols=cfg.dde_cols,
                                  mod_ot_merge_cols=cfg.dd_cols)

    otitis_sevs.append(otitis_mld_mod)

    # Assign estimates for moderately severe, severe, profound and complete
    # Otitis to zero.
    ot_fill_template = otitis_mld_mod.copy()
    ot_fill_template = ot_fill_template[
        ot_fill_template['severity'] == 'mld']
    filled_ot = fill_draws(df=ot_fill_template, fill_val=0)

    for ot_fill_sev in cfg.ot_fill_sevs:
        higher_sevs = filled_ot.copy()
        higher_sevs['severity'] = ot_fill_sev
        otitis_sevs.append(higher_sevs)

    adj_otitis = pd.concat(otitis_sevs)

    adj_otitis = check_df_for_nulls(df=adj_otitis,
                                    entity_name='split otitis')
    check_occurances_for_df(df=adj_otitis, cols=cfg.dds_cols)

    adj_etiologies.append(adj_otitis)

    # ---CROSSWALK AND SPLIT MENINGITIS----------------------------------------
    # Apply crosswalk proportions to split into mild and moderate and then
    # apply a proportion of the envelopes to split moderate or worse into
    # moderate, moderately severe, severe, profound and complete.
    # -------------------------------------------------------------------------

    # Prepare meningitis crosswalk proportion for transformations.
    meningitis_xwalk_prop = rename_draws(df=meningitis_xwalk_prop,
                                         old_name='crosswalk',
                                         new_name='draw_')
    meningitis_xwalk_prop = fill_draws(df=meningitis,
                                       fill_val=meningitis_xwalk_prop)

    # Apply the crosswalk to the input Meningitis model.
    xwalked_meng = apply_meningitis_prop(meningitis_df=meningitis,
                                         prop=meningitis_xwalk_prop,
                                         merge_cols=cfg.dde_cols)

    xwalked_meng = check_df_for_nulls(df=xwalked_meng,
                                      entity_name='crosswalked meningitis')
    check_occurances_for_df(df=xwalked_meng, cols=cfg.dd_cols)
    mld_meng = xwalked_meng.copy()
    mld_meng = mld_meng[mld_meng['severity'] == 'mld']
    adj_etiologies.append(mld_meng)

    # Proportionally split meningitis prevalence by the envelope (not
    # squeezed yet).
    tot_val_cols = ['draw_{}'.format(x) for x in range(cfg.draw_count)]
    total_df = aggregate_causes(df=et_adj_envs,
                                id_cols=cfg.dd_cols,
                                val_cols=tot_val_cols)

    total_df = check_df_for_nulls(df=total_df, entity_name='total')
    check_occurances_for_df(df=total_df, cols=cfg.dd_cols)

    # Divide numerator by the total
    split_prop = transform_draws(operator_name='truediv',
                                 df1=et_adj_envs,
                                 df2=total_df,
                                 merge_cols=cfg.dd_cols)

    split_prop = check_df_for_nulls(df=split_prop,
                                    entity_name='split prop')
    check_occurances_for_df(df=split_prop, cols=cfg.dds_cols)

    split_meningitis = transform_draws(operator_name='mul',
                                       df1=split_prop,
                                       df2=xwalked_meng,
                                       merge_cols=cfg.dds_cols)

    split_meningitis = check_df_for_nulls(df=split_meningitis,
                                          entity_name='split meningitis')

    # Removed mild estimates since they are being assigned to the result of the
    # meningitis crosswalk.
    split_meningitis = split_meningitis[split_meningitis['severity'] != 'mld']
    check_occurances_for_df(df=split_meningitis, cols=cfg.dd_cols)

    adj_etiologies.append(split_meningitis)

    adj_etiologies = pd.concat(adj_etiologies, axis=0)

    adj_etiologies = check_df_for_nulls(df=adj_etiologies,
                                        entity_name='all adjusted etiologies')
    check_occurances_for_df(df=adj_etiologies, cols=cfg.ddes_cols)

    # ---SQUEEZE ETIOLOGIES----------------------------------------------------
    # Squeeze the severity specific etiologies to ensure that they sum up to
    # the value of the severity specific envelopes.
    # -------------------------------------------------------------------------

    # Assign pre etiology squeeze estimates to a separate dataframe for
    # diagnostic purposes.
    pre_es = adj_etiologies.copy()
    # Add presqueezed congenital estimates
    congenital = HearingLossEtiologySqueeze()
    pre_es_cong = congenital.make_congenital(envelopes=et_adj_envs)
    pre_es = pd.concat([pre_es, pre_es_cong])
    pre_es['diagnostic_type'] = 'pre_es'
    pre_es['ringing'] = 'nr'
    all_estimates.append(pre_es)

    # Begin the etiology squeeze
    es_val_cols = ['draw_{}'.format(x) for x in range(cfg.draw_count)]
    hles = HearingLossEtiologySqueeze()
    squeezed_etiologies = hles.run(etiologies=adj_etiologies,
                                   envelopes=et_adj_envs,
                                   squeeze_ages=cfg.es_age_groups,
                                   id_cols=cfg.dds_cols,
                                   val_cols=es_val_cols,
                                   merge_cols=cfg.dds_cols,
                                   draw_count=cfg.draw_count)

    # Assign post hearing aid adjustment estimates to a separate dataframe for
    # diagnostic purposes.
    post_es = squeezed_etiologies.copy()
    post_es['diagnostic_type'] = 'post_es'
    post_es['ringing'] = 'nr'
    all_estimates.append(post_es)

    # ---APPLY TINNITUS ADJUSTMENT---------------------------------------------
    # Split up severity/etiology-specific estimates with proportion that has
    # ringing (tinnitus).
    # -------------------------------------------------------------------------

    # To propogate uncertainty, use a beta distribution for proportion due to
    # Tinnitus.
    tin_beta_dfs = []
    tin_sevs = tinnitus_data[cfg.tinnitus_severity_col].unique().tolist()

    # Generate beta distributions for each severity seperately to ensure that
    # the alpha and beta values are being applied to the correct severity .
    for tin_sev in tin_sevs:
        tin_data_by_sev = tinnitus_data.copy()
        tin_data_by_sev = tin_data_by_sev[
            tin_data_by_sev['severity_name'] == tin_sev]
        tin_data_by_sev = tin_data_by_sev.reset_index()
        tin_alpha = tin_data_by_sev['alpha'][0]
        tin_beta = tin_data_by_sev['beta'][0]
        tin_beta_dist = beta.rvs(tin_alpha,
                                 tin_beta,
                                 size=(cfg.draw_count, 1),
                                 random_state=1)

        tin_beta_df_by_sev = pd.DataFrame(tin_beta_dist, columns=[tin_sev])

        # Transpose dataframe so severities are in a single column and draws
        # are in a wide format.
        tin_beta_df_by_sev = tin_beta_df_by_sev.transpose()
        tin_beta_df_by_sev = tin_beta_df_by_sev.reset_index()
        tin_val_cols = ['severity'] + [
            'draw_{}'.format(x) for x in range(cfg.draw_count)]
        tin_beta_df_by_sev.columns = tin_val_cols
        tin_beta_dfs.append(tin_beta_df_by_sev)

    tin_beta_df = pd.concat(tin_beta_dfs)

    # Execute the Tinnitus split
    # Split Etiologies with ringing (etiologies * ringing proportion)
    causes_ringing = transform_draws(operator_name='mul',
                                     df1=squeezed_etiologies,
                                     df2=tin_beta_df,
                                     merge_cols=['severity'])
    causes_ringing['ringing'] = 'r'

    # Split Etiologies without ringing (etiologies * (1 - ringing proportion)
    transformation_variable = fill_draws(df=tin_beta_df, fill_val=1)
    prop_no_ringing = transform_draws(operator_name='sub',
                                      df1=transformation_variable,
                                      df2=tin_beta_df,
                                      merge_cols=['severity'])
    causes_no_ringing = transform_draws(operator_name='mul',
                                        df1=squeezed_etiologies,
                                        df2=prop_no_ringing,
                                        merge_cols=['severity'])
    causes_no_ringing['ringing'] = 'nr'

    # Combine ringing and no ringing data sets
    final_et = pd.concat([causes_ringing, causes_no_ringing])

    final_et = check_df_for_nulls(df=final_et,
                                  entity_name='Tinnitus splits')
    check_occurances_for_df(df=final_et, cols=cfg.ddesr_cols)

    # Check for negative values
    draws_to_check = final_et._get_numeric_data()
    draws_to_check[draws_to_check < 0] = 0

    # Check to ensure tinnitus split occurred correctly
    logging.info("Confirming the tinnitus split was done correctly")
    splits_aggregated = final_et.copy()
    tin_val_cols = ['draw_{}'.format(x) for x in range(cfg.draw_count)]
    splits_aggregated = aggregate_causes(df=splits_aggregated,
                                         id_cols=cfg.ddes_cols,
                                         val_cols=tin_val_cols)

    tin_check_df = merge_draws(left_df=splits_aggregated,
                               right_df=squeezed_etiologies,
                               merge_cols=cfg.ddes_cols,
                               suffixes=['_sa', '_se'])
    for i in range(cfg.draw_count):
        if not np.allclose(tin_check_df['draw_{}_sa'.format(i)],
                           tin_check_df['draw_{}_se'.format(i)]):
            raise ValueError("""The sum of all of the split etiologies is not
                                equal to the pre-split etiologies in 
                                draw {}.
                             """.format(i))

    

    # ---OUTPUT FINAL ESTIMATES------------------------------------------------
    # Once the Tinnitus split has been executed, output results for each meid.
    # -------------------------------------------------------------------------

    logging.info("Merging on meid columns for output estimates")
    format_cols = ['entity', 'severity', 'ringing']
    final_et['variable_name_new'] = final_et[format_cols].apply(
        lambda row: '-'.join(row.values.astype(str)), axis=1)
    # Make sure all values of this column are ints
    output_metadata['modelable_entity_id'] = output_metadata[
        'modelable_entity_id'].astype(int)

    final_et = final_et.merge(output_metadata,
                              on='variable_name_new',
                              how='left')

    # Make sure all values of this column are ints
    final_et['modelable_entity_id'] = final_et[
            'modelable_entity_id'].astype(int)

    final_et = check_df_for_nulls(df=final_et,
                                  entity_name='Final estimates')

    final_et = final_et.drop(labels=['entity',
                                     'severity',
                                     'variable_name_new',
                                     'ringing'],
                             axis=1)

    # Format diagnostic meids for merging:
    final_diag = pd.concat(all_estimates)

    logging.info("Merging on meid columns for diagnostic estimates")
    format_cols = ['entity', 'severity', 'ringing', 'diagnostic_type']
    final_diag['variable_name_new'] = final_diag[format_cols].apply(
        lambda row: '-'.join(row.values.astype(str)), axis=1)
    # Make sure all values of this column are ints
    diagnostic_metadata['modelable_entity_id'] = diagnostic_metadata[
        'modelable_entity_id'].astype(int)

    final_diag = final_diag.merge(diagnostic_metadata,
                                  on='variable_name_new',
                                  how='left')

    # Make sure all values of this column are ints
    final_diag['modelable_entity_id'] = final_diag[
        'modelable_entity_id'].astype(int)

    final_diag = check_df_for_nulls(df=final_diag,
                                    entity_name='Final estimates')

    final_diag = final_diag.drop(labels=['entity',
                                         'severity',
                                         'variable_name_new',
                                         'ringing',
                                         'diagnostic_type'],
                                 axis=1)
    final_et = pd.concat([final_et, final_diag])

    logging.info("Outputting final draws")
    output_meids = final_et['modelable_entity_id'].unique().tolist()

    for meid in output_meids:
        output_df = final_et.copy()
        output_df = output_df[output_df['modelable_entity_id'] == meid]
        output_df = output_df.reset_index(drop=True)
        output_path = "{}/{}".format(cfg.final_out_dir, int(meid))
        make_directory(output_path)
        save_cols = output_df.columns
        output_file = "{}/{}.h5".format(output_path, location_id)
        output_df.to_hdf(path_or_buf=output_file,
                         key='draws',
                         mode='w',
                         data_columns=save_cols)
                         
    logging.info(f"TIME: {time.time() - start: 0.2f} seconds")