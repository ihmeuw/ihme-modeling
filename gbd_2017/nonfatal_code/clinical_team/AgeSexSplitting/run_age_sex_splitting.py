import getpass
import sys
import warnings
import numpy as np
import pandas as pd
from db_queries import get_population, get_cause_metadata, get_rei_metadata
from db_tools.ezfuncs import query

from age_sex_split import split_age_sex

# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH/Functions"
sys.path.append(prep_path)
repo = r"FILEPATH/hospital/"
import hosp_prep
import gbd_hosp_prep

def run_age_sex_splitting(df, round_id=0, verbose=False, write_viz_data=True):
    """
    Takes in dataframe of data that you want to split, and a list of sources
    that need splitting, and then uses age_sex_splitting.py to split.  Returns
    The split data.  Along the way saves pre split and post split data in a
    format that's good for plotting. Runs hosp_prep.drop_data() and
    hosp_prep.apply_restrictions().  Meant to be ran on ALL hospital data!

    This function assumes
    that the weights you want to use are already present. 

    Parameters
        df: Pandas DataFrame
            The data to be split. can contain data that doesn't need to be split
            The age sex splitting code checks what parts of the data need
            splitting.
        round_id: int
            Specifies what round of splitting to run. used for file names.  If
            it's set to 1 or 2, it'll save a file that can be used for
            visualization.  If it's anything else nothing happens.
        verbose: Boolean
            Specifies if information about age groups before and after, and case
            counts before and after, should be printed to the screen.
    """

    if write_viz_data:
        # write data used for visualization
        if round_id == 1 or round_id == 2:
            # save pre_split data for viz, attach location name and bundle name and save
            pre_split = df.copy()
            pre_split = gbd_hosp_prep.all_group_id_start_end_switcher(pre_split)
            pre_split = pre_split.merge(query(QUERY),
                                     )
            pre_split.drop(['year_end', 'age_group_unit', 'age_end', 'nid',
                            'facility_id', 'representative_id', 'diagnosis_id',
                            'outcome_id', 'metric_id'], axis=1, inplace=True)
            pre_split.to_csv(r"FILEPATH/"
                             r"pre_split_round_{}.csv".format(round_id), index=False,
                             encoding='utf-8')

    # drop data and apply age/sex restrictions
    df = hosp_prep.drop_data(df, verbose=False)
    df = hosp_prep.apply_restrictions(df, col_to_restrict='val')

    # list of col names to compare for later use
    pre_cols = df.columns

    df_list = []  

    # columns that uniquely identify rows of data
    id_cols = ['source', 'nid', 'nonfatal_cause_name',
               'year_id', 'age_group_id', 'sex_id', 'location_id']
    perfect_ages = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                    18, 19, 20, 28, 30, 31, 32, 235]
    perfect_sexes = [1, 2]
    rows = df.shape[0]
    numer = 0

    for source in df.source.unique():
        # make df of one source that needs to be split
        splitting_df = df[df.source == source].copy()
        numer += splitting_df.shape[0]
        print(source)
        print("working: {}% done".format(float(numer)/rows * 100))
        if verbose:
            print "{}'s age groups before:".format(source)
            print splitting_df[['age_group_id']].\
                drop_duplicates().sort_values(by='age_group_id')

        # create year_id
        splitting_df['year_id'] = splitting_df['year_start']
        splitting_df.drop(['year_start', 'year_end'], axis=1, inplace=True)
        if verbose:
            print "now splitting {}".format(source)
        # check if data needs to be split, if not append to dflist and move on
        if set(splitting_df.age_group_id.unique()).\
            symmetric_difference(set(perfect_ages)) == set() and\
            set(splitting_df.sex_id.unique()).symmetric_difference(set()) == set():
            df_list.append(splitting_df)
            continue
        # the function from CoD team that does the splitting
        split_df = split_age_sex(splitting_df,
                                 id_cols,
                                 value_column='val',
                                 level_of_analysis='nonfatal_cause_name',
                                 fix_gbd2016_mistake=False)
        if verbose:
            print(
                "Orig value sum {} - New value sum {} = {} \n".format(
                    splitting_df.val.sum(),
                    split_df.val.sum(),
                    splitting_df.val.sum() - split_df.val.sum()
                )
            )
        pre_val = splitting_df.val.sum()
        post_val = split_df.val.sum()
        if pre_val - post_val > (pre_val * .005):
            warnings.warn("Too many cases were lost, a {} percent change (1 - post/pre)".format((1 - (float(post_val) / float(pre_val))) * 100))

        if verbose:
            print "{}'s ages after:".format(source)
            print split_df[['age_group_id']].\
                drop_duplicates().sort_values(by='age_group_id')
        # append split data to our list of dataframes
        df_list.append(split_df)

    # bring the list of split DFs back together
    df = pd.concat(df_list).reset_index(drop=True)
    # check that we have the right number of age groups
    assert df[['age_group_id']].drop_duplicates()\
        .shape[0]==21

    # Compare data after splitting to before splitting
    df['year_start'] = df['year_id']
    df['year_end'] = df['year_id']
    
    df.drop(['year_id'], axis=1, inplace=True)
    assert set(df.columns).symmetric_difference(set(pre_cols)) == set()

    if write_viz_data:
        # data used for visualization
        if round_id == 1 or round_id == 2:
            viz = df.merge(query(QUERY)

            viz = gbd_hosp_prep.all_group_id_start_end_switcher(viz)

            viz.drop(['year_end', 'age_group_unit', 'age_end', 'nid',
                     'facility_id', 'representative_id', 'diagnosis_id',
                     'outcome_id', 'metric_id'], axis=1).to_csv(
                         r"FILEPATH/"
                         r"post_split_round_{}.csv".format(round_id), index=False,
                         encoding='utf-8')

    return(df)
