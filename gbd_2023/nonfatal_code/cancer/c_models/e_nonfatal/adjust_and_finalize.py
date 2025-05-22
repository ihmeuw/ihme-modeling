
# -*- coding: utf-8 -*-
'''
Description: Prepares data for upload by applying proportions to prevent
    double-counting of sequelae
Notes: Special adjustments are made to prevent ddouble-counting of cancer sequelae.
            A select number of tumorectomy procedures, those managed by this script,
            are specific to a cancer (are only used to address cancer) AND are
            estimated elsewhere. To prevent double-counting the incidence and prevalence
            of these
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME 
'''
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd
import cancer_estimation.py_utils.data_format_tools as dft
import cancer_estimation.py_utils.common_utils as utils
from cancer_estimation.c_models import epi_upload
from cancer_estimation._database import cdb_utils as cdb
from sys import argv
import pandas as pd
from get_draws.api import get_draws
from ihme_dimensions import dfutils as rs
from chronos.interpolate import interpolate 
from getpass import getuser
import numpy as np

from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def decomp_prefix_cols(faux_correct): 
    ''' This function will take in faux_correct boolean, and return a string that 
        specifies whether 'decomp' should be added as a prefix to column names
    '''
    if faux_correct == True: 
        decomp_string = 'decomp_'
    else: 
        decomp_string = ''
    return(decomp_string) 


def procedure_me_id(acause):
    me_table = pd.read_csv('{}/cnf_model_entity.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    #(cdb.db_api('cancer_db')).get_table('cnf_model_entity')
    me_id = me_table.loc[me_table['is_active'].eq(1) & 
                         me_table['acause'].eq(acause) &
                         me_table['me_tag'].eq('procedure_proportion'),
                         'modelable_entity_id']
    if len(me_id) == 0:
        me_id = None
    else:
        me_id = me_id.item()
    return(me_id)


def sequelae_fractions(acause):
    ''' Defines fractions from lit review to be used when splitting sequela
    '''
    # Set fractions of population recieving treatment according to  
    #   what is basically their primary disabilty (sequela)
    pros_incont_frac = 0.18 # pct. who primarily develop incontinence
    pros_impot_frac =0.55 # pct. who primarily develop impotence
    # Define dict
    fractions = {
        'neo_prostate': {
            # Fractions used to calculate the controlled phase
            18781: {'fraction': pros_impot_frac}, # with impotence
            18782: {'fraction': pros_incont_frac},  # with incontinence
            # Fractions used to calculate the metrics of sequela beyond ten years
            18784 : {'fraction': pros_impot_frac}, 
            18785 : {'fraction': pros_incont_frac}
        }
    }
    # Add me_tags to dict (enables later linking of data to modelable_entity_id)
    me_tbl = pd.read_csv('{}/cnf_model_entity.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    meids = list(fractions['neo_prostate'].keys())
    for me in meids:
        if me_tbl.loc[me_tbl['modelable_entity_id'].eq(me), 'is_active'].item() == 0:
            del fractions['neo_prostate'][me]
        else:
            tag = me_tbl.loc[me_tbl['modelable_entity_id'].eq(me), 'me_tag'].item()
            fractions['neo_prostate'][me]['me_tag'] = tag
    if acause in fractions.keys():
        return(fractions[acause])
    else:
        return(False)


def load_procedure_proportions(procedure_me_id, location_id, is_estimation_yrs = True):
    ''' Downloads estimates for the proportion of the cancer population that
            receives a given procedure
    '''
    print("    loading procedure proportions...")
    release_id = utils.get_gbd_parameter('current_release_id')
    max_year = utils.get_gbd_parameter('max_year')
    min_year = utils.get_gbd_parameter('min_year_epi')

    if is_estimation_yrs:
        max_year = max(utils.get_gbd_parameter('estimation_years'))
    prop_df = interpolate(gbd_id_type='modelable_entity_id', 
                gbd_id= procedure_me_id,
                source = 'epi',
                measure_id=18,
                location_id=location_id, 
                reporting_year_start = min_year,
                reporting_year_end=max_year,
                sex_id=[1,2], 
                release_id=release_id
                )
    return(prop_df)


def load_estimates(metric_name, acause, location_id, faux_correct):
    ''' Loads previously-generated estimates per the metric_name
    '''
    decomp_str = decomp_prefix_cols(faux_correct)
    this_step = nd.nonfatalDataset(metric_name, acause)
    uid_cols = this_step.uid_cols
    if metric_name == "survival":
        type_cols = nd.get_columns('{}absolute_survival'.format(decomp_str))
    else:
        type_cols = nd.get_columns('{}{}'.format(decomp_str,metric_name))
    #
    input_file = this_step.get_output_file(location_id)
    input_data = pd.read_csv(input_file)
    return(input_data[uid_cols+type_cols])


def add_decade_to_age(x):
    ''' Updates an age_group_id to the id of the age group that is ten years older.
        Does this to reflect that the cohort has aged
    '''
    # This is also not a problem currently since causes modeled with procedures
    # have yld_age_start of at least 5 years. 
    age_dict = {1: 6, 19: 30, 20: 31, 30: 32, 31: 33, 32: 44, 33: 45, 235: 301}
    if x >= 5 and x < 19:
        return(x + 2)
    elif x in age_dict.keys():
        return(age_dict[x])
    else:
        raise AssertionError(
            "age group id, {}, is not in acceptable range".format(x))


def calc_total_prevalence(df, uid_cols):
    ''' Calculates a prevalence "total" value to be uploaded for troubleshooting
    '''
    sum_df = df.loc[df['me_tag'].isin(['primary_phase', 'controlled_phase',
                                        'metastatic_phase', 'terminal_phase'])]
    sum_df.loc[:, 'me_tag'] = "computational_total"
    sum_df = dft.collapse(sum_df, by_cols=uid_cols, stub='prev')
    # return(df.append(sum_df))
    return pd.concat([df, sum_df])


def apply_procdedure_proportions(df, proportions, acause, metric_name, faux_correct):
    ''' Multiplies estimates by procedure proportions, adding to the dataframe
            a set of estimates for the number of cancer events that do not recieve
            the given procedure
        -- Note:
            As of 2018-07-10, incidence data are adjusted after modeling
            and are not processed through this function, although the ability 
            to do so remains 

    '''
    decomp_str = decomp_prefix_cols(faux_correct)
    print("    adjusting to avoid double-counting procedures for {}...".format(metric_name))
    # Return if adjustment is unnecessary (if there is no rate id for the cause)
    uid_cols = nd.nonfatalDataset(metric_name, acause).uid_cols
    draw_cols = nd.get_columns("{}draw_cols".format(decomp_str))
    type_cols = nd.get_columns('{}{}'.format(decomp_str,metric_name))
    mrg_cols = [c for c in uid_cols if c != 'me_tag']
    # Subset estimates to the phase wherein procedures occur
    if metric_name == 'prevalence':
        mrg_df = df.loc[df['me_tag'] == "controlled_phase", :].copy()
        del mrg_df['me_tag']
    elif metric_name == 'incidence':
        mrg_df = df.copy()
    # For data where sequela are a fraction of the number of procedures, multiply
    #       the procedure proportion by those fractions
    if metric_name == 'prevalence' and bool(sequelae_fractions(acause)):
        # Generate dataframe to containing the fractions
        fracs = pd.DataFrame().from_dict(sequelae_fractions(acause), 
                                        orient='index')
        fracs['acause']= acause
        fracs = fracs[~fracs['me_tag'].eq("procedure_sequelae")] 
        # Merge dataframe with proportions to expand
        proportions['acause'] = acause
        props = proportions.merge(fracs)
        # Adjust proportions by me
        props[draw_cols] = props[draw_cols
                                ].multiply(props['fraction'], axis='index')
        del props['acause']
    else:
        # Determine fraction of population that does not recieve the procedure
        props = proportions.copy()
        props['me_tag'] = "adjusted_controlled_phase_a"
    # Apply proportions to estimates
    #   Note: may drop some data if proportions are only for estimation years
    mrg_df = mrg_df.merge(props, on=mrg_cols, how='inner')
    adj_df = mrg_df[uid_cols]
    evnt_wo_proc = pd.DataFrame(mrg_df[type_cols].values *
                                            mrg_df[draw_cols].values).fillna(0)
    evnt_wo_proc.columns = type_cols
    adj_df[type_cols] = evnt_wo_proc
    assert not adj_df.isnull().any().any(), "Error calculating procedure proportions"
    # For prevalence, append the adjusted data to the rest of the estimates
    if metric_name == 'prevalence':
        sq_df = dft.collapse(adj_df, mrg_cols, combine_cols=type_cols
                    ).sort_values(mrg_cols)
        cntrl_df = df.loc[df['me_tag'].eq("controlled_phase"), :
                    ].merge(mrg_df[mrg_cols].drop_duplicates(), 
                            on=mrg_cols, how='inner'
                    ).sort_values(mrg_cols)
        nosq_df = cntrl_df[mrg_cols]
        no_proc = pd.DataFrame(cntrl_df[type_cols].values -
                                                    sq_df[type_cols].values)
        no_proc.columns = type_cols
        nosq_df[type_cols] = no_proc
        nosq_df['me_tag'] = "adjusted_controlled_phase"
        adj_df = adj_df.append(nosq_df)
        output_data = df.append(adj_df)
    # Incidence of cancers with the procedure is estimated elsewhere, so there
    #      is no need to preserve the unadjusted data
    else:
        output_data = adj_df
    return(output_data[uid_cols+type_cols])


def calc_procedure_tenplus(inc_df, proportions, acause, location_id, faux_correct):
    ''' Multiplies incidence draws by the procedure proportion and the absolute
            survival proportion at 10 years to estimate the number of cases
            surviving for at least 10 years
    '''
    # Load known values
    print("    calculating the incidence of procedures with surv > ten years...")
    decomp_str = decomp_prefix_cols(faux_correct)
    uid_cols = nd.nonfatalDataset().uid_cols
    type_cols = nd.get_columns('{}incidence'.format(decomp_str))
    draw_cols = nd.get_columns("{}draw_cols".format(decomp_str))
    abs_surv_draw_cols = nd.get_columns('{}absolute_survival'.format(decomp_str))
    max_estimation_year = utils.get_gbd_parameter('max_year')
    max_survival_months = nd.nonfatalDataset().max_survival_months
    # Estimate incidence of procedure
    mrg_df = inc_df.merge(proportions)
    adj_df = mrg_df[uid_cols]
    num_procedures = (mrg_df[type_cols].values * mrg_df[draw_cols].values)
    adj_df[type_cols] = pd.DataFrame(num_procedures).fillna(0)
    # Estimate number of procedures resulting in survival beyond ten years
    surv_df = load_estimates('survival', acause, location_id, faux_correct)
    surv_df = surv_df.loc[surv_df['survival_month'].eq(max_survival_months), 
                        uid_cols + abs_surv_draw_cols]
    adj_df = adj_df.merge(surv_df)
    pbt_df = adj_df[uid_cols]
    num_procedures_10ys = adj_df[type_cols].values * \
                                        adj_df[abs_surv_draw_cols].values
    
    from warnings import simplefilter
    simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

    pbt_df[draw_cols] = pd.DataFrame(num_procedures_10ys).fillna(0)
    # Update years and age categories
    pbt_df.loc[:, 'age_group_id'] = pbt_df['age_group_id'].apply(
        add_decade_to_age)
    pbt_df.loc[:, 'year_id'] += 10
    # drop data that are now out of scope
    pbt_df = pbt_df.loc[pbt_df['year_id'] <= max_estimation_year, :]
    # For procedures whose sequelae are fractional,
    if sequelae_fractions(acause):
        pbt_df = split_sequelae(pbt_df, acause, location_id)
    else:
        pbt_df.loc[:, 'modelable_entity_id'] = \
            nd.get_modelable_entity_id(acause, 'procedure_sequelae')
    return(pbt_df)


def split_sequelae(df, acause, location_id):
    ''' Splits estimates into sequela based on proportions from literature
    '''
    print("    splitting sequelae...")
    uid_cols = nd.nonfatalDataset().uid_cols +['modelable_entity_id']
    draw_cols = nd.get_columns("draw_cols")
    # Generate dataframe containing the procedure_sequelae fractions
    fracs = pd.DataFrame().from_dict(sequelae_fractions(acause), orient='index'
            ).reset_index().rename(columns={'index':'modelable_entity_id'})
    fracs = fracs[fracs['me_tag'].eq("procedure_sequelae")] 
    fracs['acause']= acause
    # Merge dataframe with data
    df['acause'] = acause
    split_df = df.merge(fracs)
    split_df[draw_cols] = split_df[draw_cols].multiply(split_df['fraction'], axis='index')
    assert split_df[draw_cols].notnull().all().all(), "Nulls in split sequelae"
    return(split_df)


def save_procedure_inputs(df, acause, location_id, is_estimation_yrs):
    '''' Formats and saves procedure data for upload into the epi database
    '''
    uid_cols = nd.nonfatalDataset().uid_cols +['modelable_entity_id']
    draw_cols = nd.get_columns("draw_cols")
    epi_estimate_cols = ['mean', 'lower', 'upper']
    data = df.loc[:, uid_cols + draw_cols].copy()
    # apply formatting
    data.loc[df['age_group_id'].isin([33, 44, 301]), 'age_group_id'] = 235
    data = dft.collapse(data, by_cols=uid_cols, stub='draw')
    epi_df = epi_upload.format_draws_data(data)
    epi_df = epi_upload.convert_to_rate(epi_df, epi_estimate_cols, location_id, acause)
    
    # Add metadata
    epi_df['measure'] = 'incidence'
    epi_df['unit_type'] = "Person*year"
    epi_df['extractor'] = getuser()
    epi_df['location_id'] = location_id
    # Finalize and export
    for me_id in epi_df['modelable_entity_id'].unique():
        print("me_id " + str(me_id) + " sequela split")
        me_table = nd.load_me_table()
        bundle_id = int(me_table.loc[me_table['modelable_entity_id'].eq(me_id),
                                                             'bundle_id'].item())
        this_output = epi_df.loc[epi_df['modelable_entity_id'].eq(me_id), :]
        this_output = epi_upload.EpiUploadDataframe(this_output).data
        # Save output without testing (epi formatter has already tested data per
        #   epi specs)
        # add location_id to enable save_outputs
        this_output['location_id'] = location_id
        nd.save_outputs("dismod_inputs", this_output, acause,
                        bundle_id, skip_testing=True, is_estimation_yrs=is_estimation_yrs)


def save_model_results(df, metric_name, acause, faux_correct, location_id, is_estimation_yrs):
    ''' Saves a separate output file for each me_tag in the dataframe
    '''
    decomp_str = decomp_prefix_cols(faux_correct)
    uid_cols = nd.nonfatalDataset(metric_name, acause).uid_cols
    data_cols = nd.get_columns('{}{}'.format(decomp_str,metric_name))
    draw_cols = nd.get_columns("{}draw_cols".format(decomp_str))
    d_step = utils.get_gbd_parameter('current_decomp_step')
    if metric_name == "incidence":
        measure_id = utils.get_gbd_parameter('incidence_measure_id')
        df.loc[:, 'me_tag'] = 'primary_phase'
    elif metric_name == "prevalence":
        measure_id = utils.get_gbd_parameter('prevalence_measure_id')
    for this_tag in df['me_tag'].unique():
        me_id = nd.get_modelable_entity_id(acause, this_tag)
        if me_id is None:
            continue
        print("me_id " + str(me_id) + " " + this_tag)
        output_data = df.loc[df['me_tag'].eq(this_tag), uid_cols + data_cols]
        output_data.columns = uid_cols + draw_cols
        output_data['modelable_entity_id'] = me_id   
        output_data['is_outlier']  = 0 
        output_data['{}_location_year'.format(d_step)] = '{} updated estimates'.format(d_step)
        output_data = epi_upload.format_draws_data(output_data)
        # convert counts to rate space and drop draw_count columns 
        output_data = epi_upload.convert_to_rate(output_data, draw_cols, location_id, acause)
        cols = output_data.columns
        keep_cols = [i for i in cols if '_count' not in i]
        output_data = output_data[keep_cols]
        nd.save_outputs("final_results", output_data,
                        acause, me_id, measure_id,is_estimation_yrs=is_estimation_yrs)


def generate_estimates(acause, location_id, faux_correct=True, is_estimation_yrs=True):
    ''' Applies procedure adjustments where necessary, then saves separate outputs
            by measure and cancer phase
    '''
    print("Begin final adjustments...")

    inc_df = load_estimates('incidence', acause, location_id, faux_correct)
    prev_input = load_estimates('prevalence', acause, location_id, faux_correct)
    prev_df = calc_total_prevalence(prev_input, 
                    uid_cols=nd.nonfatalDataset('prevalence', acause).uid_cols)
    pr_id = procedure_me_id(acause)
    if pr_id is not None:
        prop_df = load_procedure_proportions(pr_id, location_id, is_estimation_yrs)
        prev_df = apply_procdedure_proportions(
            prev_df, prop_df, acause, 'prevalence', faux_correct)
        proc_data = calc_procedure_tenplus(
                    inc_df, prop_df, acause, location_id, faux_correct)
        save_procedure_inputs(proc_data, acause, location_id, is_estimation_yrs)
        
        save_model_results(inc_df, 'incidence', acause, faux_correct, location_id, is_estimation_yrs)
        save_model_results(prev_df, 'prevalence', acause, faux_correct, location_id, is_estimation_yrs)
    else:
        save_model_results(inc_df, 'incidence', acause, faux_correct, location_id, is_estimation_yrs)
        save_model_results(prev_df, 'prevalence', acause, faux_correct, location_id, is_estimation_yrs)
    success_file = nd.nonfatalDataset(
        'final_results', acause).get_output_file("finalized_"+str(location_id))
    open(success_file, 'a').close()
    print(str(success_file) + " saved.")
    return(True)


if __name__ == "__main__":
    print('adjust and finalize called!')
    acause = argv[1]
    location_id = int(argv[2])
    faux_correct = bool(int(argv[3]))
    is_estimation_yrs = bool(int(argv[4]))
    generate_estimates(acause, location_id, faux_correct, is_estimation_yrs)