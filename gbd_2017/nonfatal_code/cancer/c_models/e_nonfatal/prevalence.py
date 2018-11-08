

# -*- coding: utf-8 -*-
'''
Description: Calculates prevalence for the cancer nonfatl pipeline
'''
import utils.common_utils as utils
import c_models.e_nonfatal.nonfatal_dataset as nd
from c_models.e_nonfatal.tests import testing_nonfatal as test_nd
import utils.data_format_tools as dft
import _database.cdb_utils as cdb
from sys import argv
import pandas as pd
import numpy as np


def load_survival(acause, location_id):
    ''' Returns survival estimation subset required for prevalence estimation
    '''
    uid_cols = nd.nonfatalDataset("survival", acause).uid_cols
    abs_surv_col = nd.get_columns("absolute_survival")
    this_dataset = nd.nonfatalDataset("survival", acause)
    input_file = this_dataset.get_output_file(location_id)
    surv_data = pd.read_csv(input_file)
    return(surv_data[uid_cols + [abs_surv_col]])


def load_incidence(acause, location_id):
    ''' Returns incidence estimation subset required for prevalence estimation
    '''
    uid_cols = nd.nonfatalDataset().uid_cols
    inc_cols = nd.get_columns("incidence")
    input_file = nd.nonfatalDataset(
        "incidence", acause).get_output_file(location_id)
    inc_data = pd.read_csv(input_file)[uid_cols+inc_cols]
    return(inc_data[uid_cols + inc_cols])


def load_durations(acause):
    '''
    '''
    db_link = cdb.db_api('cancer_db')
    if acause[:8] == "neo_liver_":
        sequelae_cause = "neo_liver"
    elif acause == "neo_leukemia_other":
        sequelae_cause = "neo_leukemia_ll_chronic"
    elif acause == "neo_nmsc":
        sequelae_cause = "neo_nmsc_scc"
    elif acause == "neo_other_cancer":
        sequelae_cause = "neo_other"
    else:
        sequelae_cause = acause
    sq_df = db_link.get_table('sequela_durations')
    this_sq = sq_df.loc[sq_df['acause'] == sequelae_cause, :]
    this_sq.loc[:, 'acause'] = acause
    assert this_sq['sequela_duration'].notnull().all(), "error loading sequela durations"
    assert len(this_sq) > 0, "Error loading sequela durations"
    return(this_sq[['acause', 'me_tag', 'sequela_duration']])


def load_sequela_framework(surv_df, acause):
    ''' Adjust sequela duration based on survival
            First adjust incremental sequela duration (including controlled) to 
            equal total months from diagnosis (at midyear) to death (at end 
            year). This is the total amount of time someone may experience any 
            of the sequela (from diagnosis to death, separated out by the amount
            of time they are living with the cancer)

        Then iteratively adjust duration of each sequela so time lived with 
            cancer is equal to the sum of all sequela durations
            First zero-out metastatic_phase and terminal_phase for events that 
            occur at the maximum survival duration.
            - The terminal phase is set and not adjusted
            - The most flexible phase is controlled: Adjust controlled time to 
            equal the difference between incremental_duration sequela duration 
            and the duration of each of the other sequela
            - The next most flexible time is primary diagnosis and treatment
            - Finally we can adjust the metastatic time if the totals still do 
            not add up
    '''
    def adjust_duration(df, stage, uid_cols):
        '''
        '''
        sd_col = 'sequela_duration'
        input_cols = df.columns.tolist()
        this_phase = (df['me_tag'] == stage)
        df = df.merge(df[~this_phase].groupby(uid_cols, as_index=False)[
            sd_col].sum().rename(columns={sd_col: 'tot_dur'}))
        df.loc[this_phase, sd_col] = df['incremental_duration'] - df['tot_dur']
        df.loc[this_phase & (df[sd_col] <= 0), sd_col] = 0
        assert not df.duplicated(uid_cols+['me_tag']).any(), \
            "ERROR: error when calculating sequela_durations for {} stage".format(
                stage)
        return(df[input_cols])
    #
    print("    creating sequela framework...")
    nf_ds = nd.nonfatalDataset("survival", acause)
    uid_cols = nf_ds.uid_cols
    max_survival_months = nf_ds.max_survival_months
    seq_dur = load_durations(acause)
    # Add sequela durations
    surv_df.loc[:, 'acause'] = acause
    df = pd.merge(surv_df[uid_cols + ['acause']], seq_dur, on='acause')
    df.loc[:, 'raw_sequela_duration'] = df['sequela_duration']
    df.loc[:, 'incremental_duration'] = df['survival_month'] + 6
    # Set the 'beyond maximum' survival years to the duration of survival
    #   for the final period
    end_of_period = (df['survival_month'].eq(max_survival_months))
    df.loc[end_of_period, 'incremental_duration'] = max_survival_months - 6
    # Set late-phase duration to 0 at the end of the survival period 
    #   (if someone survives beyond the maximum duration, they are treated
    #   as 'survivors', so there are no terminal or metastatic phases)
    late_phase = (df['me_tag'].isin(["terminal_phase", "metastatic_phase"]))
    end_of_period = df['survival_month'].eq(max_survival_months)
    df.loc[late_phase & end_of_period, 'sequela_duration'] = 0
    # Iteratively adjust sequela duration (see docstring for explanation)
    for stage in ['controlled_phase', "primary_phase", "metastatic_phase"]:
        df = adjust_duration(df, stage, uid_cols)
    assert df['incremental_duration'].notnull().all(), \
        "error calculating sequela durations"
    return(df)


def calc_mortality(surv_df, acause, location_id):
    ''' Calculate mortality, the number of people who die of the
        cause during the interval (year), where
        mort= incremental_mortality_proportion*incidence.
        Returns a datafrane of mortality by uid
    '''
    print("    estimating absolute mortality...")
    uid_cols = nd.nonfatalDataset("survival", acause).uid_cols
    inc_cols = nd.get_columns("incidence")
    incr_mort_cols = [nd.get_columns('incremental_mortality')]
    mort_cols = nd.get_columns('mortality')
    incr_mort_df = calc_increm_mort(surv_df, acause, location_id)
    inc_df = load_incidence(acause, location_id)
    mrg_df = incr_mort_df.merge(inc_df)
    df = mrg_df[uid_cols]
    df[mort_cols] = \
        pd.DataFrame(mrg_df[inc_cols].values * mrg_df[incr_mort_cols].values)
    df = df.merge(incr_mort_df)
    return(df)


def calc_increm_mort(surv_df, acause, location_id):
    ''' Returns a dataframe of incremental survival estimates by uid
    '''
    def im_draw(df, draw_num, surv_uids):
        ''' Returns the dataframe with estimate of absolute survival for the 
                requested draw_num
        '''
        # Subset to only the necessary data
        max_surv = nd.nonfatalDataset().max_survival_months
        draw_uids = nd.nonfatalDataset().uid_cols
        abs_surv_col = nd.get_columns("absolute_survival") 
        increm_mort_col = nd.get_columns("incremental_mortality")
        # Calculate incremental mortality, the number of people who have lived
        #   with the disease for each period (those who die in year one
        #   had the disease for only a year)
        df[increm_mort_col] = df.sort_values(surv_uids).groupby(
            draw_uids)[abs_surv_col].diff(-1).fillna(0).clip(lower=0)
        # Calculate the number of people surviving with the disease at and
        #   beyond the maximum year
        at_max_surv_months = (df['survival_month'] == max_surv)
        mort_total = df[~at_max_surv_months
                       ].groupby(draw_uids, as_index=False
                       )[increm_mort_col].agg(np.sum
                       ).rename(columns={increm_mort_col: 'total_mort'})
        df = df.merge(mort_total)
        df.loc[at_max_surv_months, increm_mort_col] = 1 - df['total_mort']
        # test and return
        assert not df.isnull().any().any(), "Error in im_draw {}".format(i)
        return(df.loc[:, surv_uids+[increm_mort_col]])

    # Generate incremental mortality draws
    output_uids = nd.nonfatalDataset("survival", acause).uid_cols
    abs_surv_cols = [nd.get_columns("absolute_survival")]
    incr_mort_cols = [nd.get_columns("incremental_mortality")]
    output_df = surv_df.loc[:, output_uids]
    print("    estimating incremental mortality proportion...")
    # Note: this section remains written with a loop to facilitate future
    #   processing of absolute survival draws
    for i, as_col in enumerate(abs_surv_cols):
        this_draw = im_draw(df=surv_df.loc[:, output_uids + [as_col]],
                            draw_num=i,
                            surv_uids=output_uids)
        output_df = output_df.merge(this_draw, on=output_uids)
    return(output_df[output_uids + incr_mort_cols])


def calc_prevalence(sequela_framework, mort_df, acause):
    '''
    '''
    print("    calculating prevalence...")
    prev_cols = nd.get_columns('prevalence')
    mort_cols = nd.get_columns('mortality')
    surv_uids = nd.nonfatalDataset("survival", acause).uid_cols
    prev_uids = nd.nonfatalDataset("prevalence", acause).uid_cols
    # Create the prevalence estimation frame from the survival and mortality 
    #       frames
    mrg_df = pd.merge(sequela_framework, mort_df)
    df = mrg_df[surv_uids + ['me_tag']]
    # Calculate prevalence of each sequela by multiplying sequela duration
    #     by the number of people surviving for only that duration
    df[prev_cols] = mrg_df[mort_cols].mul(mrg_df['sequela_duration'], axis=0)
    df = dft.collapse(df, combine_cols=prev_cols,
                      by_cols=prev_uids, func='sum')
    df.loc[:, prev_cols] = df[prev_cols] / 12  # convert to years
    assert not df.isnull().any().any(), "Error in im_draw {}".format(i)
    return(df)


def generate_estimates(acause, location_id):
    ''' Runs the prevalence estimation pipeline
    '''
    output_file = nd.nonfatalDataset(
        "prevalence", acause).get_output_file(location_id)
    print("Begin prevalence estimation...")
    surv_df = load_survival(acause, location_id)
    mort_df = calc_mortality(surv_df, acause, location_id)
    sequela_framework = load_sequela_framework(surv_df, acause)
    prev_df = calc_prevalence(sequela_framework, mort_df, acause)
    nd.save_outputs("prevalence", prev_df, acause)


if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    generate_estimates(acause, location_id)
