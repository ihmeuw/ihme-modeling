
import sys
import warnings
import pandas as pd
from db_tools.ezfuncs import query
from db_queries import get_location_metadata, get_population

from clinical_info.Functions import hosp_prep, gbd_hosp_prep
from clinical_info.Mapping import clinical_mapping
from clinical_info.Mapping import bundle_swaps
from clinical_info.AgeSexSplitting import compute_weights
from clinical_info.AgeSexSplitting import run_age_sex_splitting as rass


def get_russia_map():
    """Load the Russian map from scratch, rename cols and keep only the
    cols of interest removing any duplicate values cause by the
    inc/prev variable"""

    rus = pd.read_excel(FILEPATH)
    cols = {'Level1-Bundel ID': 'bundle_id',
            'cause_Russian': 'cause'}
    rus.rename(columns=cols, inplace=True)

    rus = rus[['cause', 'cause_english', 'bundle_id']].drop_duplicates()

    rus = fix_russia_map(rus)
    return rus


def fix_russia_map(rus):
    rus.loc[rus['bundle_id'] == 116, 'bundle_id'] = 0
    rus.loc[rus['bundle_id'] == 548, 'bundle_id'] = 0

    print("swap 196 to 3038 and 213 to 3033 to update the map")
    rus.loc[rus['bundle_id'] == 196, 'bundle_id'] = 3038
    rus.loc[rus['bundle_id'] == 213, 'bundle_id'] = 3033

    print("Diabetes and Psoriasis have the same bundle_id mapping to multiple "
          "portions of the cause hierarchy. Remove the child mappings\n")
    child_causes = ['diabetes mellitus type i', 'diabetes mellitus type ii',
                    'psoriasis arthropathic']
    rus.loc[rus['cause_english'].isin(child_causes), 'bundle_id'] = 0
    # hard code some tests
    for b in [173, 232]:
        assert len(rus.query("bundle_id == @b")
                   ) == 1, "Removing child causes didn't work"

    print("Bundle_id 0 is a placeholder for causes that don't map to bundle, removing it\n")
    rus = rus[rus.bundle_id != 0]

    return rus


def get_bundle_measure(rus, map_version):
    """Pull data from the mapping tables to get measures for all
    bundles present in Russia"""

    # we need measure to identify which estimate file to pull from
    bundle_measure = clinical_mapping.get_bundle_measure(
        map_version=map_version)
    bundle_measure = bundle_measure.rename(
        columns={"bundle_measure": "measure"})

    measure = query("select measure_id, measure from shared.measure;",
                    conn_def='epi')
    measure.loc[measure.measure == "prevalence", "measure"] = "prev"
    measure.loc[measure.measure == "incidence", "measure"] = "inc"

    bundle_measure = bundle_measure.merge(measure, how='left', on='measure')

    assert bundle_measure.measure_id.notnull().all(),\
        "mapping to measure_id failed"

    bundle_measure = bundle_measure.loc[:, ["bundle_id", "measure_id"]]

    diff = set(rus.bundle_id.unique()) - set(bundle_measure.bundle_id.unique())

    assert not diff, diff

    return bundle_measure


def get_rus_data():
    """read in the prepped russia data, fix a cause and return it"""

    backprev = pd.read_csv(FILEPATH)
    backinc = pd.read_csv(FILEPATH)
    df = pd.concat([backprev, backinc], sort=False, ignore_index=True)

    # manually fix a typo in russian cause name
    df.loc[df['cause'] == 'нарушения обмена глюкозаминогликанов (мукополисахаридозы)',
           'cause'] = 'нарушения обмена гликозаминогликанов (мукополисахаридозы)'
    to_int = ['location_id', 'metric_id']
    for col in to_int:
        df[col] = df[col].astype(int)
    return df


def get_age_group_id(df, remove_cols):
    """data contains age start and end values, convert these to age_group_id"""

    # switch to age end exclusive
    df['age_end'] = df['age_end'] + 1
    df = gbd_hosp_prep.all_group_id_start_end_switcher(
        df=df, clinical_age_group_set_id=1, remove_cols=remove_cols,
        ignore_nulls=False)

    return df


def left_merge_map(back, rus):
    """Merge the map onto the data, a left merge with data on left means
    we will retain causes that don't map to bundle. This can be used for
    future review"""

    pre = len(back)
    df = back.merge(rus, how='left', on=['cause', 'cause_english'])
    assert pre == len(df)
    return df


def check_merge(df, rus):
    """Test the merge to ensure that there aren't any cases where one of the
    two cause columns is preventing bundle from being mapped on"""

    # we merge on 2 vars, so make sure there aren't mapping diffs
    cause_missing = set(df.loc[df['bundle_id'].isnull(), 'cause']).intersection(
        set(rus['cause']))
    cause_english_missing = set(df.loc[df['bundle_id'].isnull(
    ), 'cause_english']).intersection(set(rus['cause_english']))

    assert not cause_missing, "These causes are missing {}".format(
        cause_missing)
    assert not cause_english_missing, "These causes are missing {}".format(
        cause_english_missing)
    print("The merge matched on russian/english language cause names")
    return


def split_rates_counts(df, rus, map_version="current"):
    """The extracted data contains both rates and counts. Split these metrics
    into separate DFs and do an inner merge to retain just the bundle/measure
    combos we want"""

    # data in rate space
    df_r = df[df['metric_id'] == 3].copy()
    # data in count space
    df_c = df[df['metric_id'] == 1].copy()

    # inner join to keep only bundle/measure combos we use
    bun_meas = get_bundle_measure(rus, map_version=map_version)
    df_r = df_r.merge(bun_meas, how='inner', on=['bundle_id', 'measure_id'])
    df_c = df_c.merge(bun_meas, how='inner', on=['bundle_id', 'measure_id'])

    return df_r, df_c


def agg_to_bundle(df):
    """Some bundles have multiple causes. We need to aggregate the causes
    together to get counts of bundle cases"""

    assert 'bundle_id' in df.columns, "This isn't gonna work without bundle"
    cases = df.val.sum()
    group_cols = df.drop('val', axis=1).columns.tolist()

    df = df.groupby(group_cols).agg({'val': 'sum'}).reset_index()
    assert df.val.sum() == cases
    return df


def test_mapped_data(df, locs):
    """Test a bunch of columns in the prepped russia data."""

    failures = []
    # everything mapping to bundle has sex_id 3, confirm this
    if df['sex_id'].unique().size != 1:
        failures.append(
            "We expect to only have sex_id of 3. Need more review for other values")

    # test there are no age group overlaps
    for b in df['bundle_id'].unique():
        tmp = df[df['bundle_id'] == b].copy()
        ages = tmp.query("year_start > 2011")[['age_start', 'age_end']].drop_duplicates(
        ).sort_values('age_start').reset_index(drop=True)
        if len(ages) == 1:
            continue
        for row in ages.index[:-1]:
            if ages.loc[row, 'age_end'] != ages.loc[row + 1, 'age_start']:
                failures.append("review the age group values for overlap")

    extra_locs = set(df.location_id) - set(locs.location_id)
    if extra_locs:
        failures.append(
            "There seem to be some locations outside of our hierarchy: {}".format(extra_locs))
    # now find missing locs
    miss_locs = set(locs.location_id) - set(df.location_id)
    if miss_locs:
        failures.append(
            "There seem to be some locations outside of our hierarchy: {}".format(miss_locs))
    # test there are no year overlaps...
    if (df['year_end'] != df['year_start']).any():
        failures.append("There are observations that span more than 1 year")

    if df['nid'].isnull().any():
        failures.append("There are missing NIDs present")

    if len(df[['measure_id', 'bundle_id']].drop_duplicates()) != df['bundle_id'].unique().size:
        failures.append("There are multiple measures for 1 bundle")

    id_cols = ['location_id', 'year_start',
               'sex_id', 'age_group_id', 'bundle_id']
    if len(df[id_cols].drop_duplicates()) != len(df):
        failures.append(
            "The id_cols {} don't uniquely identify observations".format(id_cols))

    if df.isnull().sum().sum() != 0:
        failures.append("There are null values, can't have that")

    assert not failures, "\n\n".join(failures)
    print("No failures detected by test_mapped_data()")
    return


def prep_cols_for_up(df):
    """The data base only takes certain columns. Drop other stuff from df
    before upload"""

    to_drop = ['location_name', 'ihme_loc_id',
               'metric_id', 'cause', 'cause_english']
    df.drop(to_drop, axis=1, inplace=True)

    nids = {
        2009: 408789,
        2010: 408789,
        2011: 409153,
        2012: 409154,
        2013: 409155,
        2014: 409156,
        2015: 409157,
        2016: 409158,
        2017: 409159
    }
    df = hosp_prep.fill_nid(df, nids)

    return df


def identify_incomplete_bundle_mappings(df, rus):
    """Some causes don't exist in the reports for certain years
    ie bundle 182 is not a cause they report from 2014 to 2017
    This is causing flucuations over time in our estimates
    So this script identifies and returns a list of years and bundles
    which should be removed after the data is squared"""

    # a list of tuples (year, bundle)
    year_bundle_remove = []

    years = df.year_start.sort_values().unique().tolist()
    print("Reviewing the bundle to cause mapping for years {}".format(years))
    for b in rus.bundle_id.unique():
        causes = rus.loc[rus['bundle_id'] == b, 'cause_english'].unique()
        for y in years:
            tmp = df.query(
                "year_start == @y and cause_english in {}".format(tuple(causes))).copy()
            assert tmp.cause.unique().size == \
                tmp.cause_english.unique().size == \
                len(tmp[['cause', 'cause_english']].drop_duplicates()),\
                "english and russian causes don't match"
            diff_causes = set(causes).symmetric_difference(
                set(tmp['cause_english'].unique()))
            if diff_causes:
                year_bundle_remove.append((y, b))

    return year_bundle_remove


def prep_russian_count_data(use_all_age_before_2012, create_to_remove_mappings,
                            map_version="current"):
    """run through most of the functions defined above to get map, get data,
    map data and subset just the count data for """

    rus = get_russia_map()
    back = get_rus_data()

    map_back = left_merge_map(back, rus)
    check_merge(map_back, rus)

    map_back = bundle_swaps.apply_bundle_swapping(df=map_back,
                                                  map_version_older=24,
                                                  map_version_newer=28,
                                                  drop_data=True)
    rus = bundle_swaps.apply_bundle_swapping(df=rus, map_version_older=24,
                                             map_version_newer=28,
                                             drop_data=True)

    df_r, df_c = split_rates_counts(map_back, rus, map_version=map_version)
    # go from per 100k to per cap
    df_r['val'] = df_r['val'] / 1e5

    df_r = get_age_group_id(df_r, remove_cols=False)
    df_c = get_age_group_id(df_c, remove_cols=False)

    if use_all_age_before_2012:
        # data before 2011 is missing an age group, so use all age data
        d1 = df_c[(df_c['year_start'] < 2012) & (df_c['age_group_id'] == 22)]
        # use age detail except for bundles 131 and 114, b/c they don't exist in certain detailed data
        d2 = df_c[(df_c['year_start'] > 2011) & (df_c['age_group_id']
                                                 != 22) & ~(df_c['bundle_id'].isin([114, 131]))]
        # create df just for those bundles
        d3 = df_c[(df_c['bundle_id'].isin([114, 131])) & (
            df_c['age_group_id'] == 22) & (df_c['year_start'] > 2011)]

        assert set(d1.age_group_id) == set(
            [22]), "We should only use all age data before 2012"
        assert set(d1.year_start) == set(
            [2009, 2010, 2011]), "All age data shouldn't be used for other years"
        assert 22 not in d2.age_group_id.unique().tolist(
        ), "newer data shouldn't use all age data"
        assert len(d1) + len(d2) + len(d3) < len(df_c)
        df_c = pd.concat([d1, d2, d3], sort=False, ignore_index=True)

    else:
        # remove the all-ages tabulations
        df_c = df_c.query("age_group_id != 22")

    df = df_c.copy()

    if create_to_remove_mappings:
        to_remove = identify_incomplete_bundle_mappings(df, rus)

    locs = get_location_metadata(location_set_id=35)
    locs = locs[(locs['path_to_top_parent'].str.contains(",62,"))
                & (locs['level'] == 4)]

    locs_to_remove = set(df.ihme_loc_id) - set(locs.ihme_loc_id)

    df = df[~df['ihme_loc_id'].isin(locs_to_remove)]

    possibly_missing = set(locs.ihme_loc_id) - set(df.ihme_loc_id)
    assert not possibly_missing, "Why isn't {} present in the data?".format(
        possibly_missing)

    df = prep_cols_for_up(df)
    df = agg_to_bundle(df)
    test_mapped_data(df, locs)

    # add and remove cols needed for various parts of the process
    df['source'] = "RUS_MOH"
    df['facility_id'] = 'unknown'
    df['representative_id'] = 1
    df.drop(['age_start', 'age_end'], axis=1, inplace=True)

    if create_to_remove_mappings:
        return df, to_remove
    else:
        return df


def create_polish_weights(run_id, gbd_round_id, decomp_step, weight_path,
    map_version="current"):
    """Create weights using the inp+otp data from Poland

    Params:
        weight_path: (str) where the finished weights should be written to on disk
    Returns:
        Nothing, writes the weights to a csv in the run_id
    """

    pol = pd.read_csv(FILEPATH.format(run_id))

    pol.drop(['age_start', 'age_end'], axis=1, inplace=True)
    pol['product'] = pol['mean']
    pol['source'] = 'POL_claims'
    pol['facility_id'] = 'unknown'
    # make the product column square
    pol = hosp_prep.make_zeros(
        df=pol, cols_to_square='product', etiology='bundle_id')
    # compute age/sex weights for splitting
    pol_weights = compute_weights.compute_weights(df=pol, round_id=1,
                                                  squaring_method='bundle_source_specific',
                                                  run_id=run_id,
                                                  gbd_round_id=gbd_round_id,
                                                  decomp_step=decomp_step,
                                                  clinical_age_group_set_id=1,
                                                  level='bundle_id',
                                                  overwrite_weights=False,
                                                  inp_pipeline=False,
                                                  map_version=map_version)
    pol_weights = pol_weights[['age_group_id',
                               'sex_id', 'bundle_id', 'weight', 'to_keep']]
    pol_weights.to_csv(weight_path, index=False)

    return


def re_apply_measure_id(df):
    """measure_id is being lost for the squared data, so re-add it"""
    assert 'bundle_id' in df.columns and 'measure_id' in df.columns,\
        "come on how do you expect this to work?"
    if df['measure_id'].isnull().sum() == 0:
        print("No missing measures, returning the data")
        return df

    mmap = df[['bundle_id', 'measure_id']].drop_duplicates()
    mmap = mmap[mmap['measure_id'].notnull()]
    mmap.columns = ['bundle_id', 'map_measure_id']
    pre = len(df)
    df = df.merge(mmap, how='left', on='bundle_id')
    assert pre == len(df), "Rows changed, that's bad"
    df.loc[df['measure_id'].isnull(
    ), 'measure_id'] = df.loc[df['measure_id'].isnull(), 'map_measure_id']
    assert df['measure_id'].notnull().all(), "There are still null measures"
    df.drop('map_measure_id', axis=1, inplace=True)

    return df


def add_final_cols(df, run_id):

    exp_cols = pd.read_csv(FILEPATH, nrows=1).columns
    exp_cols = set(exp_cols)
    df['diagnosis_id'] = 3
    df['lower'] = None
    df['upper'] = None
    df['run_id'] = run_id
    df['source_type_id'] = 17
    df.rename(columns={'population': 'sample_size',
                       'year_start': 'year_start',
                       'year_end': 'year_end',
                       'val': 'cases'}, inplace=True)
    df.drop(['measure_id', 'facility_id', 'source',
             'cases'], axis=1, inplace=True)
    diff_cols = set(exp_cols).symmetric_difference(set(df.columns))
    assert not diff_cols, diff_cols

    return df


def remove_excessive_zeros(df, cause_type, check_col, drop_incomplete_mappings,
    to_remove):
    """Some causes don't exist in a given report, and then get filled with
    zeros in our squaring process. This function removes those year/bundle
    combinations because they're not true zeros. The causes simply aren't in
    the report(s)

    Params:
        df: (pd.DataFrame) Russian clinical data
        cause_type: (str) Either bundle_id or cause_english, depending on the
            df passed
        check_col: (str) Column to use to check for zeros
    """

    assert cause_type in [
        'bundle_id', 'cause_english'], "Don't understand {}".format(cause_type)
    assert check_col in df.columns, "Can't check against a col that doesn't exist in data"
    assert df.index.unique().size == df.index.size, "index isn't unique"

    for y in df['year_start'].unique():
        for ct in df[cause_type].unique():
            tmp = df.query("year_start == @y and bundle_id == @ct")
            if len(tmp) > 0:
                if (tmp[check_col] == 0).all():
                    print(
                        "Removing rows for year {}, {} {} b/c they're ALL zero".format(y, cause_type, ct))
                    df = df[~df.index.isin(tmp.index)]

    # check missing data after remove excessive zeros func
    res = [yb for yb in to_remove if len(
        df.query("year_start == @yb[0] and bundle_id == @yb[1]")) > 0]
    if drop_incomplete_mappings:
        for yb in res:
            tmp = df.query("year_start == @yb[0] and bundle_id == @yb[1]")
            print("Removing rows for year {}, {} {} b/c the data is incompletely mapped".format(
                yb[0], cause_type, yb[1]))
            df = df[~df.index.isin(tmp.index)]
        assert not [yb for yb in to_remove if len(
            df.query("year_start == @yb[0] and bundle_id == @yb[1]")) > 0]

    return df


if __name__ == '__main__':
    run_id = int(sys.argv[1])  # 5
    gbd_round_id = int(sys.argv[2])  # 6
    decomp_step = sys.argv[3]  # 'step2'
    map_version = sys.argv[4]  # 28

    # force correct datatypes
    run_id = int(run_id)
    gbd_round_id = int(gbd_round_id)
    decomp_step = str(decomp_step)
    map_version = int(map_version)

    print(f"Command-line arguments passed into this script: {sys.argv[1:]}")

    create_new_weights = True

    df, to_remove = prep_russian_count_data(use_all_age_before_2012=True,
                                            create_to_remove_mappings=True,
                                            map_version=map_version)

    # combine the tiny age group to make 15+
    warnings.warn("we're combining age groups b/c age-sex splitting can't "
                  "handle a smaller input group than output group")
    pre_val = df.val.sum()
    df.loc[df['age_group_id'].isin([254, 364]), 'age_group_id'] = 29
    group_cols = df.drop('val', 1).columns.tolist()
    df = df.groupby(group_cols).agg({'val': 'sum'}).reset_index()
    assert df.val.sum() == pre_val, "something went wrong when grouping ages"

    pre_drop = len(df)
 
    # need to apply a/s restricts before splitting data
    df = clinical_mapping.apply_restrictions(
        df, age_set='age_group_id', cause_type='bundle', map_version=map_version)

    weight_path = FILEPATH.format(
        run_id)

    if create_new_weights:
        print("Creating weights from Polish data")
        create_polish_weights(run_id=run_id, gbd_round_id=gbd_round_id,
                              decomp_step=decomp_step, weight_path=weight_path, map_version=map_version)
    else:
        print("We're NOT creating new weights from Polish data")

    df = rass.run_age_sex_splitting(df, run_id=run_id,
                                    gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                                    round_id=0, verbose=True, write_viz_data=False,
                                    level='bundle_id', weight_path=weight_path,
                                    inp_pipeline=False, clinical_age_group_set_id=1)

    # make the data square and apply age-sex restricts
    df = hosp_prep.make_zeros(df, cols_to_square='val', etiology='bundle_id')
    df = clinical_mapping.apply_restrictions(
        df, age_set='age_group_id', cause_type='bundle', map_version=map_version)
    df = re_apply_measure_id(df)

    # We can't add sample size(population) until after we age-sex split the data
    df = gbd_hosp_prep.get_sample_size(df.copy(),
                                       gbd_round_id=gbd_round_id,
                                       decomp_step=decomp_step,
                                       fix_group237=False,
                                       clinical_age_group_set_id=1)
    df['estimate_id'] = 21
    df['mean'] = df['val'] / df['population']

    df = add_final_cols(df, run_id)

    assert df.drop(['upper', 'lower'], 1).notnull().all().all(
    ), "Null values found which sucks {}".format(df[df.isnull().any(axis=1)])

    df = remove_excessive_zeros(df, cause_type='bundle_id', check_col='mean',
                                drop_incomplete_mappings=True,
                                to_remove=to_remove)

    df.to_csv(FILEPATH.format(run_id), index=False)
