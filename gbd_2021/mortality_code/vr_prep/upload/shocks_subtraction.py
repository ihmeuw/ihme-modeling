import pandas as pd
import sys
import time
import numpy as np
import warnings 
warnings.simplefilter('error')

from hierarchies.dbtrees import agetree

def read_format_input_vr(fpath):

    """
    This function reads in input empirical deaths, combines all unknown deaths together,
    and does a quick duplicates check.
    """

    vr = pd.read_csv(fpath)

    # Fill NA undelrying NIDS with placeholder for grouping
    vr = vr.fillna("--filledNA--")

    # Sum together unknown deaths. Comes in 2 age group ids, -1 and 283
    unknown = vr.loc[vr.age_group_id.isin([-1, 283])]
    unknown = unknown.groupby(list(set(unknown.columns) - {'age_group_id', 'deaths'}))
    unknown = unknown.deaths.sum().reset_index()
    unknown['age_group_id'] = 283


    # Combine back with main df
    vr = pd.concat(objs = [vr[~(vr.age_group_id.isin([-1, 283]))], unknown], sort=True)

    # Check for duplicates
    dups = vr.groupby(['age_group_id', 'location_id', 'sex_id', \
                       'year_id', 'source', 'source_type_id']).deaths.count().reset_index()

    assert not any(dups.deaths > 1)


    return(vr)


def get_age_split_mapping(age_group_list, gbd_round_id):
    """
    Helper function to collapse_small_age_groups()
    Look up each age group id to determine the underlying most-detailed ages

    """
    mapping = []
    for age_group_id in age_group_list:
        for l in agetree(age_group_id, gbd_round_id=gbd_round_id).leaves():
            mapping.append({
                'age_group_id_aggregate': age_group_id,
                'age_group_id': l.id})
    return pd.DataFrame(mapping)


def expand_merge_shock_ages(data, shocks, gbd_round_id):

    """
    This function will map the standard age groups in the shocks dataset to all the age groups in our main dataset.

    Steps:
    1. Map all of the detailed age groups in the shocks dataset to whatever aggregate age groups are present in the VR dataset
        - Since there are no "unknown" age/sex shocks, we do not subtract shocks from unknown sex or unknown age groups.
        - Special case: When "unknown" is the only age group, we will set it to 22 so all shocks are aggregated to unknown.
        - When unknown is the only sex available, sum up male/female shocks to subtract from Unknown
    2. Aggregate shock deaths up to the aggregate age groups present in VR
    3. Combine aggregated shock deaths with the main dataframe

    """

    # Rename columns for merging purposes
    shocks = shocks.rename(columns={'year' : 'year_id', 'sex' : 'sex_id', 'numkilled' : 'shock_deaths'})

    # Make sure we're not dropping any rows of data
    starting_rows = data.shape[0]

    ##### 1. Create the mapping ----------------------------------------------------------
    # For each of the aggregate age groups in "data", find the mapping down to the standard age group IDs in "shocks"
    age_map = get_age_split_mapping(data.age_group_id.unique(), gbd_round_id)

    ## Special case: When unknown is the only age group, use 22 instead as the all age sum.
    id_cols = ['location_id','nid', 'source', 'year_id']
    age_counts = data.groupby(id_cols).age_group_id.nunique().reset_index()
    age_counts = age_counts.rename(columns={"age_group_id" : "age_group_count"})

    data = pd.merge(data, age_counts, on=id_cols)

    data.loc[(data.age_group_id==283) & (data.age_group_count==1), 'age_group_id'] = 22

    data.loc[(data.age_group_id==22) & (data.age_group_count>1), 'age_group_id'] = 283

    # Identify groups in the input data where unknown sex is the only sex grouping
    sex_counts = data.groupby(id_cols).sex_id.nunique().reset_index()
    sex_counts = sex_counts.rename(columns={"sex_id" : "sex_id_count"})

    data = pd.merge(data, sex_counts, on=id_cols)

    # Split off unknown sexes separately
    unknown_sex = data.loc[(data.sex_id==9) & (data.sex_id_count==1)]
    data = data.loc[~((data.sex_id==9) & (data.sex_id_count==1))]

    ##### 2. Aggregate up shocks to match age groups in input dataset ------------------------------------------------------

    shocks_mapped = pd.merge(shocks, age_map, how='left', on='age_group_id')

    # Aggregate subshocks to the aggregate age group ID
    shocks_mapped = shocks_mapped.groupby(['location_id', 'year_id', 'sex_id', 'age_group_id_aggregate']).shock_deaths.sum().reset_index()

    # Aggregate age ID is now just plain ol' age group ID
    shocks_mapped = shocks_mapped.rename(columns={"age_group_id_aggregate" : "age_group_id"})

    # Create both sex shocks to subtract from unknown_only
    both_sex_shocks = shocks_mapped.groupby(['location_id', 'year_id', 'age_group_id']).shock_deaths.sum().reset_index()
    both_sex_shocks['sex_id'] = 9

    #### 3. Merge on aggregates to shocks ----------------------------------------------------------
    output = pd.merge(data, shocks_mapped, how='left', on=['location_id', 'year_id', 'sex_id', 'age_group_id'])
    unknown_sex = pd.merge(unknown_sex, both_sex_shocks, how='left', on=['location_id', 'year_id', 'sex_id', 'age_group_id'])

    # Combine
    output = pd.concat([output, unknown_sex], sort=True)

    # Fill missing shock deaths with NAs
    output.shock_deaths = output.shock_deaths.fillna(0)

    # Assert row equality
    assert output.shape[0] == starting_rows

    # refresh the index
    output = output.reset_index(drop=True)

    return(output)

def subtract_shocks(df, diagnostic_path=None, diagnostic_path2=None):

    """
    Conditions:

    1. If VR > shocks, simply subtract shocks and we're done.
    2. If -1 < VR-shocks < 0, set VR_noshock to 0
    3. If 0.6*shocks < VR < shocks, scale shocks down by 40% and subtract
    4. If 0.6*shocks > VR, do not subtract shocks. The input VR source in this case
    is likely not complete enough to be adjusted.

    """


    ## Initial adjustment: Simple subtraction
    output = df.copy()

    # Save diagnostics of negative deaths. could be useful to review
    neg_deaths = output[output.deaths < output.shock_deaths]

    if neg_deaths.shape[0] > 0:
      neg_deaths.to_csv(diagnostic_path, index=False)
      
    # Save diagnostic of large negative shocks
    neg_shocks = output[(output.deaths - output.shock_deaths) > output.deaths*2]
    
    if neg_shocks.shape[0] > 0:
      neg_shocks.to_csv(diagnostic_path2, index=False)

    ## Conditional handling of negative deaths
    # 1. VR > shocks
    output.loc[output.deaths >= output.shock_deaths, 'noshock_deaths'] = output.deaths - output.shock_deaths

    # 2. shocks - 1 < VR < shocks
    output.loc[((output.shock_deaths - 1) <= output.deaths) & (output.deaths < output.shock_deaths), 'noshock_deaths'] = 0

    # 3. 0.6*shocks < VR < shocks
    output.loc[(0.6*output.shock_deaths <= output.deaths) & (output.deaths < (output.shock_deaths-1)), 'noshock_deaths'] = output.deaths - 0.6*output.shock_deaths

    # 4. VR < 0.6*shocks
    output.loc[output.deaths < 0.6*output.shock_deaths, 'noshock_deaths'] = output.deaths

    # Check for missingness and negative deaths
    assert not any(output.noshock_deaths < 0)
    assert not any(output.noshock_deaths.isna())

    # Only need shock subtracted deaths at this point.
    output = output.drop(columns = ['deaths', 'shock_deaths'])
    output = output.rename(columns={'noshock_deaths' : 'deaths'})

    return(output)


if __name__ == "__main__":

    NEW_RUN_ID = sys.argv[1]
    IS_COD_VR = sys.argv[2].upper()
    GBD_ROUND_ID = int(sys.argv[3])
    WITH_SHOCK = sys.argv[4].upper()

    # Set up directories, input/output files
    data_dir = "".format(NEW_RUN_ID)
    shock_file = "".format()

    if IS_COD_VR == "TRUE":
        input_file = "".format()
        if WITH_SHOCK == "TRUE":
            output_file = "".format()
        else:
            output_file = "".format()
        diagnostic_file = "".format()
        diagnostic_file2 = "".format()
    else:
        input_file = "".format()
        if WITH_SHOCK == "TRUE":
            output_file = "".format()
        else:
            output_file = "".format()
        diagnostic_file = "".format()
        diagnostic_file2 = "".format()

    shocks = pd.read_stata(shock_file)
    vr = read_format_input_vr(input_file)
    
    if WITH_SHOCK=="TRUE":
        shocks.loc[:,'numkilled'] = 0

    source_type_ids = pd.read_csv("".format())

    vr = pd.merge(vr, source_type_ids, on='source_type_id', validate="m:1")

    # Use regex to parse out VR/nonVR data. Should make this more robust
    vr_data = vr[(vr.source_type.str.contains("[VR|vr]")) & ~(vr.source.str.contains("SRS|srs|DSP|dsp|MCCD|mccd|CRS"))]
    nonvr_data = vr[~(vr.source_type.str.contains("[VR|vr]")) | (vr.source.str.contains("SRS|srs|DSP|dsp|MCCD|mccd|CRS"))]
    
    # Map/aggregate/merge on the shocks dataset to the main dataset
    print("Adding on shocks")
    vr_with_shocks = expand_merge_shock_ages(vr_data, shocks, GBD_ROUND_ID)

    # Subtract shocks
    print("Subtracting shocks")
    vr_noshock = subtract_shocks(vr_with_shocks, diagnostic_file, diagnostic_file2)

    # Reset all-age only to unknown only
    vr_noshock.loc[(vr_noshock.age_group_count==1) & (vr_noshock.age_group_id==22), 'age_group_id'] = 283

    # Drop unnecessary columns
    vr_noshock = vr_noshock.drop(columns=['age_group_count', 'sex_id_count'])

   	# ensure no column mismatches before binding
    assert set(vr_noshock.columns) == set(nonvr_data.columns), f"""

        The no shock dataset is missing columns {set(nonvr_data.columns) - set(vr_noshock.columns)}
        the original dataset is missing columns {set(vr_noshock.columns) - set(nonvr_data.columns)}
        """

     # Append VR and non-VR sources together
    final_output = pd.concat([vr_noshock, nonvr_data], sort=True)

    # Drop source type column
    final_output = final_output.drop(columns=['source_type'])

    # Reset the placeholder NAs
    final_output.loc[final_output.underlying_nid == "--filledNA--", 'underlying_nid'] = np.nan

    # Output
    print(f"Writing output to {output_file}")
    final_output.to_csv(output_file, index=False)
