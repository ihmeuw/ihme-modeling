# -*- coding: utf-8 -*-
"""
Created on Fri Jan 1 13:41:38 2017

Last edited on Thu Jun 28 

This function generates risk-weighted latent TB prevalence based on TST
induration distributions.

"""
###############################################################################
# Imports and default file definitions
###############################################################################

import numpy as np
import pandas as pd
from platform import system
# The following need to be imported in the cluster environment
from db_queries import get_population, get_model_results, get_location_metadata
from db_tools.ezfuncs import query

# Setting program global variables
if system() == "Windows":
    ADDRESS = "ADDRESS"
    ADDRESS = "ADDRESS"
    ADDRESS = "ADDRESS"
else:
    ADDRESS = "ADDRESS"
    ADDRESS = "ADDRESS"
    ADDRESS = "ADDRESS"

# Update decomp step (added for GBD 2019)
decomp = 'iterative'

# Maximum induration considered (all higher indurations adjusted down to max)
upper_induration_mm = 20.0
# Relative risk for HIV+ individuals at 0mm induration (used for HIV adjustment)
hiv_adjustment_rr = 0.0978

tst_dose_adjustment = round(1.070113, 0) #mm, must be nearest quarter-mm to match properly
tu_0mm_adjustment = 0.819809313 #fraction of population which would remain in the 0mm category


# Basic math and helper functions

# This function fills blank indurations for the upper induration category
def get_upper_induration(ind_high_raw, max_induration_mm=upper_induration_mm):
    if np.isnan(ind_high_raw):
        return max_induration_mm
    elif type(ind_high_raw) in [int, float] and ind_high_raw < 1.1:
        return 0
    else:
        return ind_high_raw

#This function helps to standardize all indurations
def ind_standardize(induration, max_induration_mm=upper_induration_mm):
    if induration <max_induration_mm:
        return induration
    else:
        return max_induration_mm

def get_rand_normal_from_proportion(proportion, sample_size):
    # Generate standard deviation for the proportion
    # Zero if the proportion is 0
    std_dev = 0
    if proportion > 0:
        std_dev = np.sqrt(proportion * (1 - proportion) / sample_size)
    # Generate 1 random-normal draw for proportion of cases in this induration size
    rand_normal = np.random.normal() * std_dev + proportion
    if rand_normal <= 0:
        return 0
    else:
        return rand_normal

def get_percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_

def zeromm_dose_adjustment(in_df):
    # Create dummy variable to indicate rows that need adjustment
    in_df['adjust_dummy'] = 0
    in_df.loc[(in_df['dosage_tu'] < 5), 'adjust_dummy'] = 1
    in_df.loc[(in_df['dosage_tu'] == 2) & (in_df['ppd_type'] == 'RT23'), 'adjust_dummy'] = 0

    # Adjust less-that-5TU studies up to 5TU studies
    in_df['ind_bin_low'] = in_df.apply(lambda x: (x['ind_bin_low'] + tst_dose_adjustment) 
                                    if ((x['adjust_dummy']==1) & (x['ind_bin_low']>0)) 
                                    else x['ind_bin_low'], axis = 1)
    in_df['ind_bin_high'] = in_df.apply(lambda x: (x['ind_bin_high'] + tst_dose_adjustment) 
                                    if ((x['adjust_dummy']==1) & (x['ind_bin_high']>0)) 
                                    else x['ind_bin_high'], axis = 1)

    tu5_df = in_df.loc[in_df['adjust_dummy']==0].copy()
    nonzero_df = in_df.loc[(in_df['ind_bin_high'] != 0) & (in_df['adjust_dummy']==1)].copy() 

    zero_mm_row = in_df.loc[
                            #(in_df['ind_bin_low'] == 0) & 
                            (in_df['ind_bin_high'] == 0) &
                            (in_df['adjust_dummy']==1)].copy()
    zero_mm_row['adjust_dummy'] = zero_mm_row['adjust_dummy'].apply(lambda x: int(float(x)))

    zero_0mm_row = zero_mm_row.copy()
    zero_0mm_row['cases'] = zero_0mm_row['cases'] * tu_0mm_adjustment
    
    #Under 5TU more-than-zero-millimeters
    zero_adj_row = zero_mm_row.copy()
    zero_adj_row['cases'] = zero_adj_row['cases'] * (1 - tu_0mm_adjustment)
    zero_adj_row['ind_bin_low'] = tst_dose_adjustment
    zero_adj_row['ind_bin_high'] = tst_dose_adjustment

    to_append = tu5_df.append(nonzero_df, ignore_index=True)
    to_append = to_append.append(zero_0mm_row, ignore_index=True)
    all_df = to_append.append(zero_adj_row, ignore_index=True)
    return all_df


# Standardize induration bins for a dataframe containing TST data
# Takes one dataframe (TST) and returns a dataframe
def standardize_data(tst, tu_adj=True):
    # Set any lower values of less than ~1 millimeter to 0
    tst['ind_bin_low'] = tst['ind_bin_low'].apply(lambda x: 0 if x < 1.1 else x)
    # If the upper induration category is NAN, set it to the induration upper bound
    tst['ind_bin_high'] = tst['ind_bin_high'].apply(get_upper_induration)
    # Adjust any induration bins where ind_bin_high <= .5 down to 0
    tst['ind_bin_low'] = tst.apply(lambda x: 0 if x['ind_bin_high'] < .5 
                                               else x['ind_bin_low'], axis = 1)
    if tu_adj:
        tst = zeromm_dose_adjustment(tst)
    # Create midpoints for the induration bins
    tst['ind_midpoint'] = (tst.ind_bin_low.add(tst.ind_bin_high))/2
    # Correct so that all groups with induration size > 20 mm are equal to 20 mm (maximum induration size)
    tst['ind_midpoint'] = tst['ind_midpoint'].apply(ind_standardize)
    # Make a new text field to join on for later
    tst['midpoint_indexing'] = tst.apply(lambda x: "{:.2f}mm".format(x['ind_midpoint']), axis=1)
    
    # Ensure that the "age" bins are in a format that can be used for comparison
    tst['age_start'] = tst.apply(lambda x: np.round(x['age_start'], 3), axis=1)
    tst['age_end'] = tst.apply(lambda x: np.round(x['age_end'], 3), axis=1)

    # The only relevant fields are those that have at least one case
    tst = tst[tst['cases'] > 0]

    return tst


# Group a DF by identifiers and calculate the sample size for each group
# Takes: DF, list of ID vars, list of measurements, list of vars of interest
# Returns the formatted DF
def calculate_sample_size(in_df, identifiers, measurements, vars_of_interest):
    # First, combine all identical combinations of ID vars + measurement vars
    subset_grouped = identifiers + measurements    
    
    # Join the induration sizes into groups based on identifiers and TST induration
    in_df = in_df[subset_grouped + vars_of_interest]
    in_df.loc[:, 'cases'] = in_df['cases'].apply(np.float)
    gb1 = in_df.groupby(subset_grouped).sum()
    gb1 = gb1.reset_index()
    
    # Now, group by all identifiers WITHOUT induration
    # Generate a sample size, relative proportions, and 
    gb1 = gb1.rename(columns = {0: 'cases'})
    gb1.loc[:, 'group_id'] = np.nan
    gbd_groupings = gb1.groupby(identifiers)
    
    group_num = 0
    each_group = []
    
    # Iterate through each of the groups
    print("Calculating total sample size for each group...")
    for name, group in gbd_groupings:
        # Calculate the total sample size
        group_sample_size = np.round(group.cases.sum(), decimals = 3)
        group.loc[:, 'sample_size_new'] = group_sample_size
        # For each row, calculate the number of cases as a proportion of the sample size
        group.loc[:, 'cases_proportional'] = group.loc[:,'cases'] / group_sample_size
        # Add a group ID to that row
        group.loc[:, 'group_id'] = group_num
        group_num += 1
        group = group.reset_index()
        each_group.append(group)
    
    # Finally, reset the index to return a single, ungrouped DF
    # This is the df that will eventually be matched to the collapsed results
    formatted = pd.concat(each_group, axis = 0, sort = True)
    return formatted


# Subset; join with relative risk data; melt
def rr_join_melt(formatted_tst_df, rr_df):
    print("Merging with draws and melting...")
    
    merge_these = formatted_tst_df[['group_id', 
                                    'cases_proportional', 
                                    'midpoint_indexing', 
                                    'sample_size_new']]
    
    # Merge with relative risk draws
    rr_draws_joined = pd.merge(merge_these, rr_df, 
                               how = "left", 
                               left_on = "midpoint_indexing", 
                               right_on = "induration_size")
    
    # Create a list of variable names that need to be melted
    draw_fields = ['rr_draw_' + str(i) for i in range(0, 1000)]
    
    # Subset to only columns that will be melted
    rr_draws_joined = rr_draws_joined[['group_id', 'cases_proportional', 'sample_size_new'] + draw_fields]
    # Melt based on the draw fields
    rr_melted = pd.melt(rr_draws_joined, 
                        id_vars = ['group_id', 'cases_proportional', 'sample_size_new'], 
                        value_vars = draw_fields, 
                        var_name = 'draw', 
                        value_name = 'rr_value')
    # Return the melted df
    return rr_melted
    

# Split into groups; calculate relative risk for each group; export to new df
def melted_calc(rr_melted, id_vars):
    # Create a new field for the draw value of cases_proportional
    print("Generating draws for proportion of each induration bin...")
    rr_melted['case_prop_draw'] = rr_melted.apply(lambda x: get_rand_normal_from_proportion(x['cases_proportional'], x['sample_size_new']), axis = 1)
    
    # Now, for each row, generate this draw for the risk-weighted proportion
    print("Generating risk-weighted prevalence for each induration category...")
    rr_melted['this_draw_rr_contribution'] = rr_melted['case_prop_draw'] * rr_melted['rr_value']
    
    print("Collapsing across indurations...")
    # Subset to the only two necessary columns, the group_id and the rr_contribution
    rr_melted = rr_melted[['group_id', 'draw', 'this_draw_rr_contribution']]
    # Across each group and draw, sum all of the rrs into a single number
    rr_by_draw = rr_melted.groupby(by = ['group_id', 'draw'], axis = 0).sum().reset_index()
    #Subset to ignore draws last column to relative_risk
    rr_by_draw = rr_by_draw[['group_id', 'this_draw_rr_contribution']]
    #Rename columns
    rr_by_draw.columns = ['group_id', 'relative_risk']
    
    #Collapse across draws, aggregating by mean, 5th percentile, and 95th percentile
    print("Collapsing across draws...")
    rr_grouped = rr_by_draw.groupby(by = ['group_id'], axis = 0).agg([np.mean, 
                                                                 get_percentile(2.5), 
                                                                 get_percentile(97.5)]).reset_index()
    #Rename columns
    rr_grouped.columns = ['group_id', 'mean', 'lower', 'upper']
    
    return rr_grouped


# Merge with the original formatted TST dataframe
def merge_using_identifiers(results_df, template_df, identifiers):
    print("Merge with identifiers...")
    # Finally, merge back onto old identifiers in the "formatted" df
    template_df = template_df[identifiers + ['group_id']]
    
    final_values = template_df.merge(results_df, 
                                     how = 'inner', 
                                     on = 'group_id').drop_duplicates()
    return final_values

    

# The following function takes a data frame with all the data, corrected but in
# its original induration categories. It returns a dataframe where the 0mm induration
# category is separate for each input source. The splitting happens based on a 
# meta-analysis of 0mm prevalence by super-region.
def zeromm_split(df_mixed, identifiers):
    # Add "super_region_name" and "super_region_id" to the input dataframe
    q_1 = get_location_metadata(location_set_id = 35)
    all_locs = q_1[['location_id', 'super_region_id', 'super_region_name']]

    # Merge onto the input dataframe. We need a super region to split, so it's an inner join
    # First, make sure that both location_id columns are ints
    all_locs.loc[:, 'location_id'] = all_locs.loc[:, 'location_id'].apply(lambda x: int(float(x)))
    df_mixed.loc[:, 'location_id'] = df_mixed.loc[:, 'location_id'].apply(lambda x: int(float(x)))
    data_by_sr = pd.merge(left=df_mixed, right=all_locs, on=['location_id'], how='inner')
    # Check how many have been dropped
    num_dropped = df_mixed.shape[0] - data_by_sr.shape[0]
    print("Merged onto super-region: {} out of {} rows were dropped (invalid location_id)".format(num_dropped,
                                                                                           df_mixed.shape[0]))
    # Check all NIDs that have had all their rows dropped
    missing_nids = [i for i in df_mixed['nid'].unique().tolist() if i not in data_by_sr['nid'].unique().tolist()]
    print("All rows dropped from the following NIDs: {}".format(missing_nids))
    # Convert ind_bin_low and ind_bin_high back into floats so they can be used for 
    #  upcoming calculations
    data_by_sr.loc[:, 'ind_bin_low'] = data_by_sr.loc[:, 'ind_bin_low'].apply(float)
    data_by_sr.loc[:, 'ind_bin_high'] = data_by_sr.loc[:, 'ind_bin_high'].apply(float)

    # Separate into studies/groups that need to be split and 
    # studies/groups that can be used for splitting
    grouped = data_by_sr.groupby(by=identifiers)
    needs_splitting = []
    already_split_list = []


    for name, g in grouped:
        if ((g['ind_bin_low'] < 0.5) & (g['ind_bin_high'] < 0.5)).any():
            already_split_list.append(g)
        else:
            needs_splitting.append(g)

    use_to_split = pd.concat(already_split_list)
    # Subset down to exclude groups with group_review = 0 and excluded = 1
    if 'group_review' in use_to_split.columns:
        use_to_split = use_to_split.loc[use_to_split['group_review'] != 0]
    if 'excluded' in use_to_split.columns:
        use_to_split = use_to_split.loc[use_to_split['excluded'] != 1]

    # Use this function to exclude any studies with poorly matching bins and to subset down
    # to the matching bin size
    def match_upper_ind(in_df, max_ind):
        any_within_1mm = in_df['ind_bin_high'].apply(lambda x: abs(max_ind - x) <= .5)
        if not(any_within_1mm.any()):
            # Return an empty dataframe
            return pd.DataFrame({'nid':[], 'cases_zeromm':[], 'cases_pos':[]})
        else:
            # Subset down to the matching induration categories
            in_df = in_df.loc[in_df['ind_bin_high'] <= (max_ind + .5)]
            # Get the nid
            this_nid = in_df['nid'].iloc[0]
            # Get the cases at (approx) 0 mm
            c_zeromm = in_df.loc[in_df['ind_bin_high'] < 0.5].cases.sum()
            # Get the cases that ARE NOT at 0 mm
            c_pos = in_df.cases.sum() - c_zeromm
            # Return a df with the nid, cases at 0mm, and cases greater than 0mm
            return pd.DataFrame({'nid': [this_nid],
                                 'cases_zeromm': [c_zeromm],
                                 'cases_pos': [c_pos]})

    # Iterate through the dataframes that need splitting and split each one
    print("  Running {} meta-analyses on pre-split studies...".format(len(needs_splitting)))
    for df in needs_splitting:
        split_on = use_to_split.copy()
        # Subset use_to_split only to the correct super-region
        # EXCEPT FOR the Latin America and Caribbean super-region
        this_super_region = df['super_region_name'].iloc[0]
        if this_super_region != 'Latin America and Caribbean':
            split_on = split_on.loc[split_on['super_region_name'] == this_super_region]
        # Exclude use_to_split studies where the bins don't nicely match up
        # Subset down to match the 0-X mm lowest bin
        # (This is all done in the function match_upper_ind, defined above)
        if df.loc[df['ind_bin_low'] < 0.5, 'ind_bin_high'].size == 0:
            ####ALREADY SPLIT 0MM DATA IS NOT BEING ADJUSTED UPWARD
            pass
        else:
            zero_bin_upper = df.loc[df['ind_bin_low'] < 0.5, 'ind_bin_high'].iloc[0]
            meta = (split_on.groupby(by = identifiers)
                                    .apply(lambda x: match_upper_ind(in_df = x,
                                                    max_ind = zero_bin_upper))
                                    .reset_index(drop = True))
            # Do a meta-analysis to get a single proportion for 0mm in the lowest bin
            if meta.shape[0] > 0:
                meta['n_i'] = meta['cases_zeromm'] + meta['cases_pos']
                meta['p_i'] = meta['cases_zeromm'] / meta['n_i']
                meta['var_i'] = meta['p_i'] * (1 - meta['p_i']) / meta['n_i']
                meta['numerator_i'] = meta['p_i'] / meta['var_i']
                meta['denominator_i'] = 1 / meta['var_i']
                # This is the prevalence of 0mm cases within the group 
                # See Theo's paper for the formula: http://bit.ly/2pMzA75
                meta_prevalence = meta.numerator_i.sum() / meta.denominator_i.sum()
            else:
                # Backup in case no matching studies were returned
                # This should happen very infrequently
                meta_prevalence = .5
            # Split the 0-Xmm bin to 0-0mm and 1-X mm using the ratio
            zero_mm_row = df.loc[df['ind_bin_low'] < 0.1]
            # The df for appending
            to_append = df.loc[df['ind_bin_low'] >= 0.1]
            pos_row = zero_mm_row.copy()
            zero_mm_row['ind_bin_low'] = 0
            zero_mm_row['ind_bin_high'] = 0
            zero_mm_row['cases'] = zero_mm_row['cases'] * meta_prevalence
            zero_mm_row['cases_proportional'] = (zero_mm_row['cases'] / 
                                                 zero_mm_row['sample_size_new'])
            zero_mm_row['dosage_tu'] = zero_mm_row['dosage_tu'].apply(lambda x: int(float(x)))

            pos_row['ind_bin_low'] = 1
            pos_row['cases'] = pos_row['cases'] * (1 - meta_prevalence)
            pos_row['cases_proportional'] = pos_row['cases'] / pos_row['sample_size_new']
            to_append = to_append.append(zero_mm_row, ignore_index = True)
            to_append = to_append.append(pos_row, ignore_index = True)

            # Append the split study to already_split_list
            already_split_list.append(to_append)

    # All of the dataframes should now be split and 
    all_split = pd.concat(already_split_list, ignore_index = True)
    return all_split


# Adjust the final results based on estimated HIV prevalence within the sample
#  group
def hiv_adjust(unadjusted_df, ind_grouping, identifiers_list, 
               hiv_rr=hiv_adjustment_rr):
    # Get all unique locations that we have data for
    unique_locs = ind_grouping.location_id.unique().tolist()
    age_groups = [1,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235]
    
    # This program only considers individuals with a CD4 count < 200
    # 9321 = HIV/AIDS Prevalence CD4 <200
    hiv_meid = 9321
    # Get relevant HIV prevalence for all age groups    
    hiv_prev = get_model_results('epi', 
                                 gbd_id = hiv_meid, 
                                 measure_id = 5,
                                 location_id = unique_locs, 
                                 age_group_id = age_groups, 
                                 sex_id = [1, 2], 
                                 status = 'best', 
                                 year_id = -1, 
                                 decomp_step = decomp)
    # Subset to useful columns only
    hiv_prev = hiv_prev[['year_id', 'age_group_id', 
                         'location_id', 'sex_id', 'mean']].copy()
    hiv_prev = hiv_prev.rename(columns = {'mean': 'hiv_prev'})
    
    # Get populations for all listed locations
    gbd_years = hiv_prev.year_id.unique().tolist()
    pops = get_population(age_group_id = age_groups, 
                          location_id = unique_locs,
                          year_id = gbd_years, 
                          sex_id = [1, 2], 
                          decomp_step = decomp)
    # Subset to useful columns only
    pops = pops[['year_id', 'age_group_id', 'location_id', 'sex_id', 'population']]
    
    # Join the two datasets
    join_on = ['year_id', 'age_group_id', 'location_id', 'sex_id']
    hiv_prev_pops = pd.merge(left = hiv_prev, right = pops, 
                             how = 'inner', on = join_on)
    # Add a sex_id = 3 column by merging
    male_only = hiv_prev_pops[hiv_prev_pops['sex_id'] == 1].copy()
    female_only = hiv_prev_pops[hiv_prev_pops['sex_id'] == 2].copy()
    both_sexes = pd.merge(left = male_only, right = female_only,
                          on = ['age_group_id', 'location_id', 'year_id'],
                          suffixes = ('_male', '_female'), how = 'inner')
    both_sexes['population'] = (both_sexes['population_male']
                                + both_sexes['population_female'])
    both_sexes['hiv_prev'] = ((both_sexes['hiv_prev_male'] * both_sexes['population_male'] 
                           + both_sexes['hiv_prev_female'] * both_sexes['population_female'])
                           / both_sexes['population'])
    both_sexes.drop(labels = ['hiv_prev_male', 'hiv_prev_female',
                            'population_male', 'population_female',
                            'sex_id_male', 'sex_id_female'],
                    axis = 1, inplace = True)
    both_sexes['sex_id'] = 3
    hiv_prev_pops = pd.concat([hiv_prev_pops, both_sexes], sort=True)
    hiv_prev_pops['sex_id'] = hiv_prev_pops['sex_id'].apply(float)
    # Get the upper and lower age groups from the GBD database

    # For the next line, you'll need an 'epi' definition in your .ODBC file
    age_groups_df = query(q, conn_def = 'ADDRESS')
    # Merge back onto the HIV/population df
    hiv_prev_pops = pd.merge(left = hiv_prev_pops, right = age_groups_df, 
                             on = 'age_group_id')
    
    # Get the middle year to join on
    ind_grouping['year_start'] = ind_grouping['year_start'].apply(float)
    ind_grouping['year_end'] = ind_grouping['year_end'].apply(float)

    ind_grouping['year_id'] = np.round(((ind_grouping['year_start'] 
                                        + ind_grouping['year_end'])
                                        / 2), 0)
    ind_grouping['year_id'] = (ind_grouping['year_id'].apply(int)
                                    .apply(lambda x: 1980 if x < 1980 else x))


    # The HIV adjustment calculation should be done only for people with 0mm indurations
    ind_grouping = ind_grouping.loc[ind_grouping['ind_bin_high'] < .1]

    # Get the sex_id from the sex
    sex_id_dict = {'Male': 1, 'male': 1, 'Female': 2, 'female': 2, 'Both': 3, 'both': 3}
    ind_grouping['sex_id'] = ind_grouping['sex'].apply(lambda x: sex_id_dict[x])
    
    # Now, merge with the HIV population data on sex, location, and year (NOT age)

    joined = pd.merge(left = ind_grouping, right = hiv_prev_pops, 
                      on = ['sex_id', 'location_id', 'year_id'], 
                      how ='inner')
    
    # Select only columns where the GBD age group range and the data age group
    #  range intersect
    # First, set age_start and age_end back to floats
    joined['age_start'] = joined['age_start'].apply(float)
    joined['age_end'] = joined['age_end'].apply(float)

    joined = joined[(joined['age_group_years_start'] <= joined['age_end']) &
                    (joined['age_group_years_end'] > joined['age_start'])]
    # Create updated age group categories to fit the actual age-start and age-end
    joined['age_group_start_adj'] = joined.apply(lambda x: np.max([x['age_group_years_start'],
                                                               x['age_start']]),
                                                               axis = 1)
    # Subtract 1 from age_group_years_end to reflect our use of demographer notation
    #  (that is, using age 4 to represent 4 years, 0 days to 4 years, 364.99.. days)
    joined['age_group_end_adj'] = joined.apply(lambda x: min([x['age_group_years_end'] - 1,
                                                              x['age_end']]),
                                                              axis = 1)
    # Create updated population count reflecting the fraction of the age group
    #  actually contained within the range
    # Again, the +1 reflects differences with the GBD age range due to demographer notation
    joined['pop_adj'] = (joined['population']
                         * (joined['age_group_end_adj'] + 1 - joined['age_group_start_adj'])
                         / (joined['age_group_years_end'] - joined['age_group_years_start']))
    joined['hiv_prev_count'] = joined['hiv_prev'] * joined['pop_adj']
    # Subset only to identifiers + pop_adj and hiv_prev_count
    group_identifiers = ['group_id','cases_proportional']
    to_group = joined[group_identifiers + ['hiv_prev_count', 'pop_adj']].copy()
    # Now, group by identifiers and sum hiv_prev_count (num.) and pop_adj (denom.)
    summed = to_group.groupby(by = group_identifiers).sum().reset_index(drop = False)
    # Divide combined numerator by combined denominator to get total prevalence
    summed['hiv_prev_weighted_avg'] = summed['hiv_prev_count'] / summed['pop_adj']
    summed = summed.drop(labels = ['hiv_prev_count', 'pop_adj'], axis = 1)
    
    # Create the adjustement:
    # Adjustment = HIV prevalence in this population * proportion of 0mm in study * proportion of HIV patients
    #  who return 0mm results when they actually have latent TB  
    summed['hiv_adjustment'] = (summed['cases_proportional'] 
                               * summed['hiv_prev_weighted_avg'] 
                               * hiv_rr)
    summed = summed[['group_id', 'hiv_adjustment']].copy()
    summed['group_id'] = summed['group_id'].apply(lambda x: int(float(x)))
    unadjusted_df['group_id'] = unadjusted_df['group_id'].apply(lambda x: int(float(x)))
    # Merge onto the results df, using group_id as the unique identifier
    adjusted_df = pd.merge(left = unadjusted_df,
                           right = summed,
                           on = ['group_id'],
                           how = 'left')
    # Fill any NaNs
    adjusted_df['hiv_adjustment'] = adjusted_df['hiv_adjustment'].fillna(0)
    # Add the adjustment to the mean, lower, and upper
    for i in ['mean', 'lower', 'upper']:
        adjusted_df[i] = adjusted_df[i] + adjusted_df['hiv_adjustment']

    # Drop the adjustment column and return
    adjusted_df = adjusted_df.drop(labels = ['hiv_adjustment'], axis = 1)
    return adjusted_df


###############################################################################
# Run the entire program
###############################################################################
def risk_weighted_tst(infile, riskfile, outfile = None, 
                      max_tst_induration = 20, return_df = False):
    # Imports the TST data and the relative risk draws
    print("Reading input data...")
    tst_raw = pd.read_excel(io = infile, skiprows = [1])
    rr_draws = pd.read_csv(riskfile)

    # Standardize input induration data
    print("Standardizing input data...")
    tst_standardized = standardize_data(tst_raw, tu_adj=True)
    
    # Set all of the variables that will be used to split data into specific age-sex-year-location-bcg groups
    print("Grouping data by study...")
    identifiers = ['nid', 'field_citation_value', 'file_path', 'author_last',
                   'pub_year', 'source_type', 'location_name', 'location_id',
                   'smaller_site_unit', 'site_memo', 'sex', 'sex_issue',
                   'year_start', 'year_end', 'year_issue', 'age_start',
                   'age_end', 'age_issue', 'age_demographer', 'bcg_scar',
                   'ppd_type', 'dosage_tu', 'group', 'specificity',
                   'representative_name', 'urbanicity_type', 'extractor']
    measurements = ['midpoint_indexing', 'ind_bin_low', 'ind_bin_high']
    var_of_interest = ['cases']

    # Fill NA values of identifiers so that things don't get dropped in the groupings
    tst_standardized.loc[:, identifiers] = tst_standardized.loc[:, identifiers].fillna(value=9999999)
    # Turn into string to facilitate matching
    for i in identifiers + measurements:
        tst_standardized.loc[:, i] = tst_standardized.loc[:, i].apply(str).copy()
    # Set unique groups and sample sizes for the data
    print("Grouping data and calculating sample size...")
    tst_grouped = calculate_sample_size(tst_standardized, identifiers, 
                                        measurements, var_of_interest)
    # Join TST values to RR values and melt
    joined_melted_df = rr_join_melt(tst_grouped, rr_draws)
    
    # Perform calculations on the melted data
    rr_weighted = melted_calc(joined_melted_df, id_vars = identifiers)
    # Merge back to identifiers
    with_ids = merge_using_identifiers(results_df = rr_weighted,
                                       template_df = tst_grouped,
                                       identifiers = identifiers)
    # Using the original grouped data and the current risk-weighted data, adjust
    #  for estimated HIV prevalence in the population
    print("Splitting 0 mm from >0 mm...")
    all_split = zeromm_split(df_mixed = tst_grouped, identifiers = identifiers)

    print("Running HIV correction...")
    final_df = hiv_adjust(unadjusted_df = with_ids,
                          ind_grouping = all_split,
                          identifiers_list = identifiers)
    ###UNCLEAR HOW HIV_ADJUSTED DATA INCORPORATED INTO RISK_WEIGHTED VALUES???
    #final_df=with_ids
    # Replace any of the NaNs that were originally in the identifiers
    final_df[identifiers] = final_df[identifiers].replace(to_replace = {9999999 : np.nan})

    # Save to excel
    if outfile is not None:
        final_df.sort_values(by = 'group_id', axis = 0).to_excel(outfile, index = False)
    print("Finished!")
    if return_df:
        return final_df


#############################################################################################
# RUN THIS SECTION
#############################################################################################

# Run the TST program using filepaths and settings specified here
if __name__ == '__main__':
    # Ensure that the code is drawing from the right extraction file and the MOST
    #   UPDATED relative risk file!
    #infile = j_header + r"Project/TB/GBD 2017/LTBI/Extractions/tst_lit_extract_age-sex-split_ermadd_plusKOR_20171017.xlsm"
    infile = ADDRESS + "FILEPATH/njh_erm_combined_2017_01_07.xlsm"
    #riskfile = j_header + r"temp/dismod_ode/TST_5/results/rr_draws_updated.csv"
    riskfile = "FILEPATH//risk_curve_split.csv"
    #outfile = j_header + r"Project/TB/TST/Risk_weighted_calc/outputs/2019_07_01/GBD2017_KOR_tstmoved_2019_07_01.xlsx"
    outfile = ADDRESS+ "FILEPATH//GBD2016_pt2.xlsx"

    risk_weighted_tst(infile = infile,
                      riskfile = riskfile,
                      outfile = outfile,
                      max_tst_induration = 20,
                      return_df = False)
