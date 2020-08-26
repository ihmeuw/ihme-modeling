"""
A)	Estimate_id 1 (Mean0) non-UTLA- this will use draws and apply CFs at the draw level
B)	Estimate_id 1(Mean0) UTLA- this uses only mean and sample size where sample size is gbd population. Only the CF point estimates will be applied.
C)	Estimate_id 6 (maternal mean0)this uses a point estimate from the envelope and sample size where sample size is the special maternal sample size. Only the CF point estimates will be applied.

1)	The data is too large to process at once so Ive split it up by age, sex and bins of 5 years worth of data to aggregate (eg 1997-2002, 2003-2007, etc)
2)	Draws have already been creating using HUE

3)	Then it maps all 3 data types to bundle and aggregates draws together for A) and mean estimates together for B) and C)
4)	Next we need to make sure the data is square, so that we're adding years together properly. We will only include a bundle in the square data for a given location and year if it has been diagnosed in that location in a previous year. After making square we re-apply age/sex restrictions.
5)	Then we merge on gbd population. This is the denominator for data types A) and B)
6)	Next, merge on maternal sample size. This is the denominator for data type C) 
7)	Convert from rate to count space by multiplying mean/draws by the population or maternal sample size. For A) this happens to each draw. For B) and C) only to the mean estimate
8)	Next the code attaches the inpatient admissions. I converted this over from the existing code but again Im kind of fuzzy on why we need these rather than population?
9)	We sum the numerators and denominators together to get 5 year values. An example here is probably helpful. If we have only 3 years of data available from 1994, 1995, 1996  then wed sum all the numerator cases, say 10, 0 and 20 and all the denominator populations say 1000, 1020 and 1070 to get 5 year values of 30 in the numerator and 3090 in the denominator and year start/end values of 1993-1997. For B) and C) this is done only once for the mean. For A) its repeated for each draw, although the denominator is the same for each draw. Only the numerator changes.
10)	To return to rate space we divide the summed numerators by the summed denominators. To re-iterate, the denominators for A) and B) are population and for C) theyre the adjusted maternal sample size.

"""
import sys
import time
import itertools
import functools
import warnings
import os
import pandas as pd
import numpy as np
from db_queries import get_population

from clinical_info.Mapping import clinical_mapping
from clinical_info.Functions import hosp_prep, data_structure_utils as dsu
from clinical_info.Envelope import apply_bundle_cfs as abc


def create_draws(df, draws, draw_name="draw_"):
    """
    This creates the actual draws using standard dev and the random normal func from numpy
    """
    zeros = df[df['mean'] == 0].copy()
    df = df[df['mean'] != 0]
    # generate se to use as std for rnorm
    df = get_est_se(df)

    # create the draws themselves
    np.random.seed(10)  # important to keep outside of for loop
    # I think if data is added or lost it will affect this
    for draw in range(0, draws, 1):
        df["{}{}".format(draw_name, draw)] =\
                                             np.exp(np.random.normal(loc=np.log(df['mean']),
                                             scale=df['est_se'],
                                             size=df.shape[0]))
        zeros["{}{}".format(draw_name, draw)] = 0
    df = pd.concat([df, zeros], sort=False, ignore_index=True)
    return df

def get_est_se(df):
    """
    calculate the log standard error using UIs
    """
    df['est_se'] = (((np.log(df['mean']) - np.log(df['lower'])) / 1.96) + ((np.log(df['upper']) - np.log(df['mean'])) / 1.96)) / 2
    return df

def expand_bundles(df, drop_null_bundles=True):
    """
    This Function maps baby sequelae to Bundles.
    When our data is at the baby seq level there are no duplicates, one icd code
    goes to one baby sequela. At the bundle level there are multiple bundles
    going to the same ICD code so we need to duplicate rows to process every
    bundle. This function maps bundle ID onto baby sequela and duplicates rows.
    Then it can drop rows without bundle ID.  It does this by default.

    Parameters:
        df: Pandas DataFrame
            Must have nonfatal_cause_name column
        drop_null_bundles: Boolean
            If true, will drop the rows of data with baby sequelae that do not
            map to a bundle.
    """

    assert "bundle_id" not in df.columns, "bundle_id has already been attached"
    assert "icg_id" in df.columns,\
        "'icg_id' must be a column"

    df = clinical_mapping.map_to_gbd_cause(df,
                                           input_type='icg',
                                           output_type='bundle',
                                           write_unmapped=False,
                                           truncate_cause_codes=False,
                                           extract_pri_dx=False,
                                           prod=True,
                                           groupby_output=False)

    if drop_null_bundles:
        # drop rows without bundle id
        df = df[df.bundle_id.notnull()]
    assert df.shape[0] > 0, "All the data has been lost in this function"
    return df

def drop_cols(df, use_draws):
    """
    icg id and name are dropped because the data has been mapped to bundle
    sample size and cases are dropped because they will be re-made when
    aggregating to 5 years. They also contain Null values which lose rows
    in a groupby
    """
    if use_draws:
        master_drop = ['icg_id', 'icg_name', 'sample_size', 'cases',
                       'mean'] #, 'lower', 'upper', 'est_se']
    else:
        master_drop = ['icg_id', 'icg_name', 'cases', 'sample_size'] # , 'lower', 'upper']

        # upper/lower no longer present
        # assert df['lower'].isnull().sum() == df.shape[0], "not all lowers are null. why?"
        # assert df['upper'].isnull().sum() == df.shape[0], "not all uppers are null. why?"

    to_drop = [c for c in master_drop if c in df.columns]
    df.drop(to_drop, axis=1, inplace=True)
    assert df.shape[0] > 0, "All the data has been lost in this function"
    return df

def agg_to_bundle(df, use_draws):
    """
    maps to bundle id
    drops columns that will cause trouble in the merge
    sums mean values together to get from ICG to bundle ratesgit
    """

    df = expand_bundles(df)

    df = drop_cols(df, use_draws=use_draws)

    if use_draws:
        sum_cols = df.filter(regex='draw_').columns.tolist()
    else:
        sum_cols = ['mean']

    # use all columns except those which we sum to aggregate to bundle
    grouper = df.columns.drop(sum_cols).tolist()

    sum_dict = dict(list(zip(sum_cols, ['sum'] * len(sum_cols))))

    df = df.groupby(grouper).agg(sum_dict).reset_index()
    assert df.shape[0] > 0, "All the data has been lost in this function"
    return df

def make_square(df, use_draws, run_id, try_to_break=False):
    """
    Function that inserts zeros for demographic and etiolgy groups that were not
    present in the source.  A zero is inserted for every age/sex/etiology
    combination for each location and for only the years available for that
    location. If a location has data from 2004-2005 we will create explicit
    zeroes for those years but not for any others.

    Age and sex restrictions are applied after the data is made square.

    Parameters:
        df: Pandas DataFrame
            Must be aggregated and collapsed to the bundle level.
    """
    start_square = time.time()
    assert "bundle_id" in df.columns, "'bundle_id' must exist"
    assert "icg_id" not in df.columns, ("Data must not be at the",
                                        " icg_id level")
    if use_draws:
        chk_col = 'draw_0'
    else:
        chk_col = 'mean'
    # create a series of sorted, non-zero  values to make sure
    # the func doesn't alter anything
    check_mean = df.loc[df[chk_col] > 0, chk_col].sort_values().\
        reset_index(drop=True)
    if try_to_break:
        # confirmed this breaks the assert below.. which means the assert is
        # working as we'd expect. A better assert fail output would be good
        check_mean.iloc[3] = .030214

    # square the dataset
    # do we need to make it all square? or just 1 col? Try to do it a faster way below
    df = make_zeros(df, run_id=run_id, etiology='bundle_id', cols_to_square=[chk_col])

    # assert the sorted means are identical
    assert (check_mean == df.loc[df[chk_col] > 0, chk_col].sort_values().\
        reset_index(drop=True)).all()
    print("Data squared in {} min".format((time.time() - start_square)/60))

    if use_draws:
        # populate the newly created draw0 rows with all zero draws
        draw_cols = df.filter(regex="draw_").columns.tolist()
        df.loc[df['draw_0'] == 0, draw_cols] = 0

    # re-apply age sex restrictions after making data square
    df = clinical_mapping.apply_restrictions(df, age_set='age_group_id', cause_type='bundle')

    print("Data made square and age/sex restrictions applied in {} min".\
        format((time.time() - start_square)/60))

    return df

def merge_population(df, gbd_round_id, decomp_step, use_draws):
    """
    Function that attaches population info to the DataFrame.  Checks that there
    are no nulls in the population columns.  This has to be ran after the data
    has been made square!

    Parameters:
        df: Pandas DataFrame
    """
    if use_draws:
        chkcol = 'draw_3'  # not draw0 to make sure 0's are getting propogated
    else:
        chkcol = 'mean'

    zero_msg = """There are no rows with zeros, implying
        that the data has not been made square.  This function should be ran
        after the data is square"""
    # assert (df[chkcol] == 0).any(), zero_msg
    if not (df[chkcol] == 0).any():
        warnings.warn(zero_msg)

    # create age/year/location lists to use for pulling population
    age_list = list(df.age_group_id.unique())
    loc_list = list(df.location_id.unique())
    year_list = list(df.year_id.unique())

    # pull population
    pop = get_population(age_group_id=age_list, location_id=loc_list,
                         sex_id=[1, 2], year_id=year_list,
                         gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    demography = ['location_id', 'year_id', 'sex_id',
                  'age_group_id']
    # keep only merge cols and pop
    pop = pop[demography + ['population']]

    pre_shape = df.shape[0]  # store for before comparison
    # then merge population onto the hospital data
    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert pre_shape == df.shape[0], "number of rows don't match after merge"

    # assert that there are no nulls in population column:
    hosp_prep.report_if_merge_fail(df, check_col="population",
                                   id_cols=demography, store=True,
                                   filename="population_merge_failure")

    return df

def rate_to_count(df, use_draws, maternal_ests=[6, 7, 8, 9], drop_rate=True):
    """
    Converts information in ratespace to countspace by mutliplying rates by
    population.

    Parameters:
        df: Pandas DataFrame
            Must already have population information.
    """

    assert "population" in df.columns, "'population' has not been attached yet."
    if use_draws:
        assert 'draw_0' in df.columns, "missing draws"
        # we want to take all draws
        cols_to_count = df.filter(regex='draw_').columns.tolist()
    else:
        assert {'mean'}.issubset(df.columns)
        # we want to take every single rate column into count space
        cols_to_count = ["mean"]

    # do the actual conversion
    print("going from rate to count space. With Draws == {}".format(use_draws))
    start = time.time()
    for col in cols_to_count:
        if use_draws:
            df[col + "_count"] = df[col] * df['population']

        else:
            df[col + "_count"] = np.nan
            # use population for UTLA data
            df.loc[~df['estimate_id'].isin(maternal_ests), col + "_count"] =\
                df.loc[~df['estimate_id'].isin(maternal_ests), col] *\
                    df.loc[~df['estimate_id'].isin(maternal_ests),'population']

            # use maternal sample size for maternal data, but only when we're not using draws
            assert df.loc[df['estimate_id'].isin(maternal_ests), 'mean_count'].\
                   notnull().sum() == 0, "We expect all maternal counts to be null here"

            df.loc[df['estimate_id'].isin(maternal_ests), col + "_count"] =\
                df.loc[df['estimate_id'].isin(maternal_ests), col] *\
                    df.loc[df['estimate_id'].isin(maternal_ests), 'maternal_sample_size']

            assert df.loc[df['estimate_id'].isin(maternal_ests), 'mean_count'].\
                       isnull().sum() == 0, "We expect Zero maternal counts to be null here"

    if drop_rate:
        # Drop the rate columns
        df.drop(cols_to_count, axis=1, inplace=True)
    run_m = (time.time()-start)/60
    print("it took {} min to go from rate to count space".format(round(run_m, 3)))
    return df

def expandgrid(*itrs):
    """create a template df with every possible combination of passed in
    lists."""
    product = list(itertools.product(*itrs))
    return({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})

def pooled_square_builder(loc, est_types, ages, sexes, loc_bundle, loc_years, eti_est_df, etiology):

    # list of values in the etiology column for the given location
    cause_type = loc_bundle.loc[loc_bundle.location_id == loc, etiology].unique().tolist()

    dat = pd.DataFrame(expandgrid(ages, sexes, [loc],
                                  loc_years[loc_years.location_id == loc].
                                  year_id.unique(),
                                  est_types,
                                  cause_type))
    dat.columns = ['age_group_id', 'sex_id', 'location_id', 'year_id', 'estimate_id', etiology]
    dat = dat.merge(eti_est_df, how='left', on=[etiology, 'estimate_id'])
    dat = dat[dat['keep'] == 1]
    dat.drop('keep', axis=1, inplace=True)

    return dat

def make_zeros(df, cols_to_square, run_id, etiology='bundle_id'):
    """
    takes a dataframe and returns the square of every
    age/sex/cause type(bundle or baby seq) which
    exists in the given dataframe but only the years
    available for each location id
    """
    if type(cols_to_square) == str:
        cols_to_square = [cols_to_square]

    ages = df.age_group_id.unique()
    sexes = df.sex_id.unique()
    est_types = df['estimate_id'].unique().tolist()

    # new method which pulls in the loc bundle data from drive
    loc_bundle_path = FILEPATH.format(run_id)
    loc_bundle = pd.read_csv(loc_bundle_path)

    # check that this file has all the bundles we need it to
    bundle_diff = set(df.bundle_id.unique()) - set(loc_bundle.bundle_id.unique())
    assert bundle_diff == set(),\
        ("DataFrame loc_bundle is missing bundles! DataFrame df has bundles "
         "that are not in loc_bundle: {}".format(bundle_diff))

    loc_years = df[['year_id', 'location_id']].drop_duplicates()

    eti_est_df = pd.read_csv(FILEPATH.format(run_id))

    sqr_df_list = []
    locs = df.location_id.unique()
    for loc in locs:
        temp_sqr_df = pooled_square_builder(loc=loc, ages=ages,
                                       sexes=sexes, loc_years=loc_years,
                                       loc_bundle=loc_bundle,
                                       eti_est_df=eti_est_df, etiology=etiology,
                                       est_types=est_types)
        sqr_df_list.append(temp_sqr_df)
    sqr_df = pd.concat(sqr_df_list, ignore_index=True, sort=False)

    # get a key to fill in missing values for newly created rows
    missing_col_key = df[['location_id', 'year_id', 'source_type_id', 'nid', 'representative_id', 'source', 'run_id', 'diagnosis_id']].copy()
    missing_col_key.drop_duplicates(inplace=True)

    # inner merge sqr and missing col key to get all the column info we lost
    pre = sqr_df.shape[0]
    sqr_df = sqr_df.merge(missing_col_key, how='inner', on=['location_id', 'year_id'])
    assert pre == sqr_df.shape[0], "Should cases have increased from {} to {}".format(pre, sqr_df.shape[0])

    # left merge on our built out template df to create zero rows
    sqr_df = sqr_df.merge(df, how='left', on=['age_group_id', 'sex_id', 'location_id', 'year_id',
                                                  'estimate_id', etiology, 'source_type_id', 'nid',
                                                  'representative_id', 'source', 'run_id', 'diagnosis_id'])
    assert pre == sqr_df.shape[0], "Rows should not have changed from {} to {}".format(pre, sqr_df.shape[0])

    # fill missing values of the col of interest with zero
    for col in cols_to_square:
        sqr_df[col] = sqr_df[col].fillna(0)

    return sqr_df

def merge_denominator(df, run_id, use_draws, full_coverage_sources):
    """
    Merges denominator stored from when cause fractions were run.  Returns df
    with a column named "denominator".  Meant to be run after data has been
    made square.  There WILL be null values of denominator after the merge.
    This would happen whenever there was a demographic that literally had no
    admissions whatsoever in a data source.  At the moment it is an open
    question whether or not we should drop these null rows, or give them a
    'denominator' value of zero.

    This is a function susceptible to problems. If create_cause_fractions was
    ran a while ago, the information may not match or be up to date anymore.
    Making the denominator should happen right before this function is used.

    The purpose of this is so that the rows that are inserted as zero to make
    the data square can have information for uncertainty for the inserted rows.
    Later, 'denominator' will be renamed to sample_size.

    Parameters:
        df: Pandas DataFrame
    """
    if use_draws:
        chk1 = 'draw_0_count'
    else:
        chk1 = 'mean_count'
    denom_path = FILEPATH.format(run_id)
    warnings.warn("""

                  ENSURE THAT THE DENOMINATORS FILE IS UP TO DATE.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(denom_path
                  )))))
    denoms = pd.read_csv(denom_path)
    denoms = dsu.year_id_switcher(denoms)

    pre = df.shape[0]
    df = df.merge(denoms,
                  how='left', on=["age_group_id", "sex_id",
                                  "year_id", "location_id"])

    assert pre == df.shape[0], "number of rows changed after merge"
    
    # denom for full coverage sources is pop
    df.loc[df.source.isin(full_coverage_sources), 'denominator'] =\
        df.loc[df.source.isin(full_coverage_sources), 'population']

    # check that data is zero when denominator is null
    assert (df.loc[df.denominator.isnull(), chk1] == 0).all(), (chk1,
        " should be 0")

    # This removes rows that were inserted for demographic groups that never
    # existed in hospital data sources.
    print("pre null denom drop shape", df.shape)
    df = df[df.denominator.notnull()]
    print("post null denom", df.shape)

    return df

def merge_maternal_denominator(df, run_id, maternal_ests):
    """
    Attach the maternal denominators where we've made the data square, but
    also where we haven't. This basically replace the 'sample_size' denoms
    that are present in the data but should be identical
    """
    denom_path = FILEPATH.format(run_id)
    warnings.warn("""

                  ENSURE THAT THE *MATERNAL* DENOMINATORS FILE IS UP TO DATE.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(denom_path
                  )))))
    denoms = pd.read_hdf(denom_path)
    denoms = dsu.year_id_switcher(denoms)
    denoms = denoms[['age_group_id', 'sex_id', 'year_id', 'location_id', 'sample_size']]
    denoms.rename(columns={'sample_size': 'maternal_sample_size'}, inplace=True)

    pre = df.shape[0]
    df = df.merge(denoms,
                  how='left', on=["age_group_id", "sex_id",
                                  "year_id", "location_id"])
    assert pre == df.shape[0], "number of rows changed after merge"
    # nully maternal ss for non maternal estimates
    df.loc[~df['estimate_id'].isin(maternal_ests), 'maternal_sample_size'] = np.nan
    return df

def aggregate_to_dismod_years(df, use_draws):
    """
    Function to collapse data into 5-year bands.  This is done in order to
    reduce the strain on on dismod by reducing the number of rows of data. Data
    must already be square! Data must be in count space!

    Before we have years like 1990, 1991, ..., 2014, 2015.  After we will have:
        1988-1992
        1993-1997
        1998-2002
        2003-2007
        2008-2012
        2013-2017

    Parameters:
        df: Pandas DataFrame
            Contains data at the bundle level that has been made square.
    """
    if use_draws:
        chk= 'draw_0_count'
        drawcols = df.filter(regex='draw_').columns.tolist()
    else:
        chk = 'mean_count'

    zero_msg = """There are no rows with zeros, implying
        that the data has not been made square.  This function should be ran
        after the data is square"""
    # assert (df[chk] == 0).any(), zero_msg
    if not (df[chk] == 0).any():
        warnings.warn(zero_msg)

    if df[chk].max() > df['population'].max():
        warnings.warn("Some {} are insanely large so we're matching them to 5 times population".format(chk))
        assert False, "why are values so large??"
        # df.loc[df['mean_count'] > df['population'] * 5, 'mean_count'] =\
        #     df.loc[df['mean_count'] > df['population'] * 5, 'population'] * 5

    # we need to switch back to year start end
    df.rename(columns={'year_id': 'year_start'}, inplace=True)
    df['year_end'] = df['year_start']

    # first change years to 5-year bands
    df = hosp_prep.year_binner(df)

    # we are mixing data from different NIDs together.  There are special NIDs
    # for aggregated data that the Data Indexers 
    # makes for us.
    warnings.warn("update this to use the new source table in the database")
    df = hosp_prep.apply_merged_nids(df, assert_no_nulls=False, fillna=True)

    # rename "denominator" to "sample_size" in df (not covered_df)
    df.rename(columns={"denominator": "sample_size"}, inplace=True)

    cols_to_sum = df.filter(regex="count$").columns.tolist()
    cols_to_sum = cols_to_sum + ['population', 'sample_size', 'maternal_sample_size']
    pre_cases = 0
    for col in cols_to_sum:
        pre_cases += df[col].sum()

    groups = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id',
              'nid', 'representative_id', 'bundle_id', 'estimate_id']

    sum_dict = dict(list(zip(cols_to_sum, ["sum"] * len(cols_to_sum))))
    print("performing the groupby and sum across dismod years")
    years = df.groupby(groups).size().reset_index(drop=True)
    df = df.groupby(groups).agg(sum_dict).reset_index()
    df['years'] = years

    post_cases = 0
    for col in cols_to_sum:
        post_cases += df[col].sum()

    accepted_loss = 300
    lost_cases = abs(pre_cases - post_cases)
    print("The acceptable loss is {} cases. {} cases were lost to floating point error".format(accepted_loss, round(lost_cases,2)))
    assert lost_cases < accepted_loss,\
        ("some cases were lost. "
         "From {} to {}. A difference of {}".format(pre_cases, post_cases, abs(pre_cases - post_cases)))

    return df

def count_to_rate(df, review, use_draws, maternal_ests=[]):
    """
    Function that transforms from count space to rate space by dividing by
    dividing by the (aggregated) population.  Returns two dataframes.  This
    performs the transformation for each dataframe separatly.
    Pass in both dataframes!  The order of the returne dataframes is important!
    The main part of hosital data, i.e., sources without full coverage, is
    returned first.  Second item returned is the fully covered sources.

    Example call:
        not_fully_covered, full_covered = count_to_rate(df, covered_df)

    Parameters:
        df: Pandas DataFrame
            df containing most hospital data, the sources that are not fully
            covered.
        covered_df: Pandas DataFrame
            df containing the covered sources.
    """

    # get a list of the count columns
    cnt_cols = df.filter(regex="count$").columns

    print("going from count to rate space. With Draws == {}".format(use_draws))
    for col in cnt_cols:
        new_col_name = col[:-6]
        df[new_col_name] = df[col] / df['population']

    if maternal_ests:
        # create maternal adj mean using maternal sample size
        df.loc[df['estimate_id'].isin(maternal_ests), 'mean'] =\
            df.loc[df['estimate_id'].isin(maternal_ests), 'mean_count'] / \
            df.loc[df['estimate_id'].isin(maternal_ests), 'maternal_sample_size']
    
    if not review:
        # drop the count columns
        df.drop(cnt_cols, axis=1, inplace=True)

    return df

def create_source_map(df):
    # create a map from location and year to source
    source_map = df[['location_id', 'year_id', 'source']].drop_duplicates()
    source_map.rename(columns={'year_id': 'year_start'}, inplace=True)
    source_map['year_end'] = source_map['year_start']
    # we have two sources for the same 5 year bin, this creates some problems of duping data
    source_replace = {'CHN_SHD': 'CHN_NHSIRS',
                      'GEO_Discharge_16_17': 'GEO_COL_14',
                      'ECU_INEC_15_17': 'ECU_INEC_97_14',
                      'NZL_NMDS_16_17': 'NZL_NMDS'}

    print("""Adding years to a source's name was dumb, replacing them now, some years in name 
             might be inconsistent with underlying data""")

    # for now just overwrite key with value, wont' affect underlying data
    for key, value in source_replace.items():
        source_map.loc[source_map['source'] == key, 'source'] = value
    # bin to 5 year groups so it matches with the data after agg to dismod
    source_map = hosp_prep.year_binner(source_map).drop_duplicates()
    assert len(source_map) == len(source_map[['location_id', 'year_start']].drop_duplicates()),\
        "These duplicates will break the merge"
    return source_map

def write_draws(df, run_id, age_group, sex, year_bin_start):
    # convert mixed type cols to numeric
    df = hosp_prep.fix_col_dtypes(df, errors='raise')

    print("Writing the df file...")
    file_path = FILEPATH.\
                format(r=run_id, a=age_group, s=sex, y=year_bin_start)
    hosp_prep.write_hosp_file(df, file_path, backup=False)
    return df

def run_helper(df, use_draws, run_id, gbd_round_id, decomp_step, maternal_ests, full_coverage_sources):
    """
    just wrap up some other funcs cause it was too much typing
    """
    df = agg_to_bundle(df, use_draws=use_draws)

    if not use_draws:
        no_draw_est1_locs = df.query("estimate_id == 1").location_id.unique().tolist()
    df = make_square(df, run_id=run_id, use_draws=use_draws)
    if not use_draws:
          df = df[(df['estimate_id'] != 1) | df['location_id'].isin(no_draw_est1_locs)]

    df = merge_population(df, use_draws=use_draws, gbd_round_id=gbd_round_id, decomp_step=decomp_step)
    df = merge_maternal_denominator(df, run_id, maternal_ests=[6, 7, 8, 9])

    df = rate_to_count(df, use_draws=use_draws, drop_rate=True)

    # now that data has been made square attach sample size for those rows
    df = merge_denominator(df, run_id, use_draws=use_draws,
                           full_coverage_sources=full_coverage_sources)

    df = aggregate_to_dismod_years(df, use_draws=use_draws)
    df = count_to_rate(df, use_draws=use_draws, review=False, maternal_ests=maternal_ests)

    # this will be useful to identify which columns should and SHOULDN"T have null draws
    if use_draws:
        df['use_draws'] = 1
    else:
        df['use_draws'] = 0
        assert df.query("location_id == 4765 & bundle_id == 75 & estimate_id==1").shape[0] == 0

    return df

def read_split_icg(read_path, age_group, sex, year_bin_start):
    """Read in the two draw files and then keep only the year we want"""
    year_bin_end = year_bin_start + 4
    df = []
    for ftype in ['_df_env.H5', '_df_fc.H5']:
        tmp = pd.read_hdf(f'{read_path}{ftype}', key="df")
        tmp = tmp.query("age_group_id == @age_group & sex_id == @sex & "\
                      "year_id >= @year_bin_start & year_id <= @year_bin_end")
        df.append(tmp)
    df = pd.concat(df, sort=False, ignore_index=True)

    assert df.year_id.unique().size <= 5, "too many years"
    df = df[df.estimate_id.isin([1, 6])]  # should only contain these but just in case

    return df

if __name__ == '__main__':
    start = time.time()
    age_group = int(sys.argv[1])
    sex = int(sys.argv[2])
    year_bin_start = int(sys.argv[3])
    decomp_step = sys.argv[4]
    gbd_round_id = int(sys.argv[5])
    run_id = int(sys.argv[6])
    draws = int(sys.argv[7])

    # should this be moved up to a job arg?
    # maybe pull it in as in inpatient property
    full_coverage_sources = ['UK_HOSPITAL_STATISTICS']

    # read and subset input for bundle data
    read_path = FILEPATH.\
                format(r=run_id, a=age_group, s=sex)

    df = read_split_icg(read_path=read_path,
                        age_group=age_group, sex=sex,
                        year_bin_start=year_bin_start)

    # source map needs ALL data
    source_map = create_source_map(df)

    # data which has full coverage or is maternal adjusted (both use sample size not draws)
    no_draw_df = df[((df['source'].isin(full_coverage_sources)) & (df['estimate_id'] == 1)) | (df['estimate_id'] == 6)].copy()
    drops = no_draw_df.filter(regex='draw_').columns.tolist()
    no_draw_df.drop(drops, axis=1, inplace=True)

    # keep only mean0 non uk
    draw_df = df[(df['estimate_id'] == 1) & ~(df['source'].isin(full_coverage_sources))].copy()

    assert df.shape[0] == (draw_df.shape[0] + no_draw_df.shape[0]), "something went wrong in the data split"
    del df

    # draws already present in new file gen env draws
    # draw_df = create_draws(df=draw_df, draws=draws, draw_name="draw_")

    if no_draw_df.shape[0] == 0:
        warnings.warn("There is no data for this age/sex/year group. Mostly likely because "\
                      "it's for males in an early gbd year")
    else:
        no_draw_df = run_helper(df=no_draw_df, run_id=run_id, use_draws=False, gbd_round_id=gbd_round_id,
                                decomp_step=decomp_step, maternal_ests=[6, 7, 8, 9],
                                full_coverage_sources=full_coverage_sources)

    draw_df = run_helper(df=draw_df, run_id=run_id, use_draws=True, gbd_round_id=gbd_round_id,
                         decomp_step=decomp_step, maternal_ests=[],
                         full_coverage_sources=full_coverage_sources)


    if no_draw_df.shape[0] > 0:
        # concat draw and no draw data if no draw data exists
        df = pd.concat([draw_df, no_draw_df], sort=False, ignore_index=True)
        del draw_df
        del no_draw_df
    else:
        df = draw_df.copy()
        del draw_df

    # merge on source
    pre = df.shape
    df = df.merge(source_map, how='left', on=['location_id', 'year_start', 'year_end'])
    assert pre[0] == df.shape[0], "row counts changed"
    assert pre[1] == df.shape[1] - 1, "col counts changed"

    df = write_draws(df, run_id=run_id, age_group=age_group, sex=sex, year_bin_start=year_bin_start)
    runm = (time.time()-start)/60
    print("prepping to apply bundle data w/env uncertainty ran in {} minutes".format(runm))

    # pull in the apply_env_only main functions here
    abc.apply_bundle_cfs_main(df=df,
                              age_group_id=age_group,
                              sex_id=sex,
                              year=year_bin_start,
                              run_id=run_id,
                              gbd_round_id=gbd_round_id,
                              decomp_step=decomp_step,
                              full_coverage_sources=full_coverage_sources)
    # done!
