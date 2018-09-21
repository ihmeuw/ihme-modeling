from __future__ import print_function
import pandas as pd
import os
import sys
import shutil
import argparse
from threading import Thread
from transmogrifier.draw_ops import get_draws
from transmogrifier.draw_ops import interpolate
from transmogrifier.maths import interpolate as interpolate_manual

sys.path.append('FILEPATH')
import cluster_helpers as ch

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry
import sdg_utils.tests as sdg_test


def submit_job_location_draws(indicator_type, location_id):
    """Submit a cluser job to process a location year."""
    log_dir = "FILEPATH"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
        os.mkdir(os.path.join(log_dir, 'errors'))
        os.mkdir(os.path.join(log_dir, 'output'))
    jobname = "sdg_loc_{it}_{loc}".format(it=indicator_type, loc=location_id)
    worker = "{}/data_prep/prep_draw_files_parallel.py".format(SDG_REPO)
    shell = "{}/sdg_utils/run_on_cluster.sh".format(SDG_REPO)
    args = {
        '--process': "run_location",
        '--indicator_type': indicator_type,
        '--location_id': location_id
    }
    if indicator_type in ['codcorrect', 'risk_exposure']:
        jobslots = 3
    else:
        jobslots = 18
    job_id = ch.qsub(worker, shell, 'proj_sdg', custom_args=args,
                     name=jobname, log_dir=log_dir, slots=jobslots, verbose=True)
    return job_id


def submit_job_collect(job_ids, indicator_type):
    """Submit a cluster job to collect processed draws."""
    log_dir = "FILEPATH"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
        os.mkdir(os.path.join(log_dir, 'errors'))
        os.mkdir(os.path.join(log_dir, 'output'))
    jobname = "sdg_collect_{it}".format(it=indicator_type)
    worker = "{}/data_prep/prep_draw_files_parallel.py".format(SDG_REPO)
    shell = "{}/sdg_utils/run_on_cluster.sh".format(SDG_REPO)
    args = {
        '--process': "collect",
        '--indicator_type': indicator_type,
    }
    holds = job_ids
    job_id = ch.qsub(worker, shell, 'proj_sdg', custom_args=args,
                     name=jobname, log_dir=log_dir, slots=20,
                     holds=holds, verbose=True)
    return job_id


def custom_age_weights(age_group_years_start, age_group_years_end):
    """Get age weights scaled to age group start and end"""
    t = qry.get_age_weights(4)

    t = t.query(
        'age_group_years_start >= {start} & age_group_years_end <= {end}'.format(
            start=age_group_years_start, end=age_group_years_end)
    )
    # scale weights to 1
    t['age_group_weight_value'] =  \
        t['age_group_weight_value'] / \
        t['age_group_weight_value'].sum()

    return t[['age_group_id', 'age_group_weight_value']]


def age_standardize(df, indicator_type):
    """Make each draw in the dataframe a rate, then age standardize.
    """

    if indicator_type == 'como':
        group_cols = dw.COMO_GROUP_COLS
    elif indicator_type == 'codcorrect':
        group_cols = dw.CC_GROUP_COLS
    else:
        raise ValueError("bad type: {}".format(indicator_type))

    assert set(df.sex_id.unique()) == {3}, \
        'falsely assuming only both sexes included'
    assert set(df.metric_id.unique()) == {1}, \
        'falsely assuming df is all numbers'
    db_pops = qry.get_pops(both_sexes=True)
    db_pops = db_pops[['location_id', 'year_id',
                       'sex_id', 'age_group_id', 'population']]

    # do special things for the 30-70 causes
    # merge special age weights on these cause ids using is_30_70 indicator
    df['is_30_70'] = df.cause_id.apply(
        lambda x: 1 if x in dw.CC_THIRTY_SEVENTY_CAUSE_IDS else 0)

    # get age weights with is_30_70 special weights
    age_weights = custom_age_weights(0, 125)
    age_weights['is_30_70'] = 0
    age_weights_30_70 = custom_age_weights(30, 70)
    age_weights_30_70['is_30_70'] = 1
    age_weights = age_weights.append(age_weights_30_70, ignore_index=True)
    all_age = pd.DataFrame({'age_group_id': 22, 'age_group_weight_value': 1, 'is_30_70': 0},
                            index=[0])
    age_weights = age_weights.append(all_age, ignore_index=True)

    df = df.merge(db_pops, how='left')
    assert df.population.notnull().values.all(), 'merge with pops failed'
    df = df[df.population.notnull()]
    df = df.merge(age_weights, on=['age_group_id', 'is_30_70'], how='left')
    assert df.age_group_weight_value.notnull().values.all(), 'age weights merg'

    # concatenate the metadata with the draw cols times the pop
    # this multiplies each draw column by the population column
    df = pd.concat(
        [
            df[group_cols],
            df[dw.DRAW_COLS].apply(
                lambda x: (x / df['population']) *
                df['age_group_weight_value']
            )
        ],
        axis=1
    )

    # now a rate, age standardized
    df['metric_id'] = 3
    df.loc[df.age_group_id != 22, 'age_group_id'] = 27

    df = df.groupby(group_cols, as_index=False)[dw.DRAW_COLS].sum()
    return df


def custom_interpolate(df):
    """Interpolate attributable burden draws"""
    draw_cols = ['draw_{}'.format(i) for i in xrange(1000)]
    id_cols = list(set(df.columns) - (set(draw_cols + ['year_id'])))
    dfs = []
    for year_range in [[1990, 1995], [1995, 2000], [2000, 2005], [2005, 2010], [2010, 2016]]:
        start_df = (df.ix[df.year_id == year_range[0]].sort_values(id_cols)
                    .reset_index(drop=True))
        end_df = (df.ix[df.year_id == year_range[1]].sort_values(id_cols)
                  .reset_index(drop=True))
        ydf = interpolate_manual(start_df, end_df, id_cols, 'year_id', draw_cols,
                                 year_range[0], year_range[1])
        dfs.append(ydf.query('year_id < {} or year_id == 2016'.format(year_range[1])))
    df = pd.concat(dfs)
    return df


def write_output(df, indicator_type, location_id):
    """Write output in way that is conducive to parallelization.

    1. Splits dataframe into each gbd_id, which is final form for each type.
    2. Copies the location data for each location in the location path
    3. Writes the file to gbd_id / location_id named with the original
        location_id
    """
    if indicator_type == 'codcorrect':
        out_dir = dw.CC_TEMP_OUT_DIR
        gbd_id_col = 'cause_id'
    elif indicator_type == 'como':
        out_dir = dw.COMO_TEMP_OUT_DIR
        gbd_id_col = 'cause_id'
    elif indicator_type == 'risk_exposure':
        out_dir = dw.RISK_EXPOSURE_TEMP_OUT_DIR
        gbd_id_col = 'rei_id'
    elif indicator_type == 'risk_burden':
        out_dir = dw.RISK_BURDEN_TEMP_OUT_DIR
        gbd_id_col = 'rei_id'
    else:
        raise ValueError("bad type: {}".format(indicator_type))
    # write to each path location so that aggregates can be made later
    for gbd_id in set(df[gbd_id_col]):
        gbd_id_dir = os.path.join(out_dir, str(gbd_id))
        # make sure the directory exists (probably create it)
        try:
            if not os.path.exists(gbd_id_dir):
                os.mkdir(gbd_id_dir)
        except OSError:
            pass
        t = df.ix[df[gbd_id_col] == gbd_id]
        # write location
        out_path = '{d}/{location_id}.h5'.format(
            d=gbd_id_dir,
            location_id=location_id
        )
        t.to_hdf(out_path, key="data", format="table",
                  data_columns=['location_id', 'year_id', 'age_group_id'])


def process_location_risk_burden_draws(location_id, test=False):
    ''' Given a list of rei_ids, use gopher to get attributable burden draws
    and save to out directory.

    '''

    dfs=[]
    for rei_id in dw.RISK_BURDEN_REI_IDS + dw.RISK_BURDEN_DALY_REI_IDS:
        print(rei_id)
        if rei_id in dw.RISK_BURDEN_REI_IDS:
            measure_id = 1
        elif rei_id in dw.RISK_BURDEN_DALY_REI_IDS:
            measure_id = 2
        else:
            raise ValueError("no measure found")
        print('Getting draws')
        df = get_draws(gbd_id_field=['cause_id', 'rei_id'], gbd_id=[294, rei_id],
                       source='burdenator', version=dw.BURDENATOR_VERS,
                       location_ids=location_id, year_ids=[], age_group_ids=[], sex_ids=[],
                       num_workers=3,
                       n_draws=1000, resample=True)

        # keep years we want
        df = df.query('measure_id == {}'.format(measure_id))
        df = df.query('metric_id == 1')
        df = df.query('age_group_id in {} and sex_id in [1, 2]'.format(range(2, 21) + range(30, 33) + [235]))
        df = df.query('year_id in {}'.format(range(1990, 2011, 5) + [2016]))

        # aggregate to both sexes
        df['sex_id'] = 3
        df = df.groupby(dw.RISK_BURDEN_GROUP_COLS, as_index=False)[dw.DRAW_COLS].sum()
        pops = qry.get_pops(both_sexes=True)
        df = df.merge(pops, how = 'left', on = ['location_id','age_group_id','sex_id','year_id'])
        df = pd.concat([
            df[dw.RISK_BURDEN_GROUP_COLS],
            df[dw.DRAW_COLS].apply(lambda x: x / df['population'])
        ], axis=1
        )
        df['metric_id'] = 3

        # keep the right columns
        df = df[
            dw.RISK_BURDEN_GROUP_COLS +
            dw.DRAW_COLS
        ]

        # interpolate years
        print('Interpolating')
        df = custom_interpolate(df)

        # age-standardize
        age_weights = qry.get_age_weights(4)
        df = df.merge(age_weights)
        df = pd.concat([
            df[dw.RISK_BURDEN_GROUP_COLS],
            df[dw.DRAW_COLS].apply(lambda x: x * df['age_group_weight_value'])
        ], axis=1
        )
        df['age_group_id'] = 27
        df = df.groupby(dw.RISK_BURDEN_GROUP_COLS, as_index=False)[dw.DRAW_COLS].sum()
        dfs.append(df)

    df = pd.concat(dfs)
    write_output(df, 'risk_burden', location_id)
    return df


def process_location_risk_exposure_draws(location_id, test=False):
    """Return yearly age standardized estimates of each rei_id.

    Arguments:
        location_id: the location_id to process

    Returns:
        pandas dataframe like so:
        [ID_COLS] : [dw.DRAW_COLS]
    """
    dfs = []

    # version_df = pd.DataFrame()
    risks = set(dw.RISK_EXPOSURE_REI_IDS).union(
        set(dw.RISK_EXPOSURE_REI_IDS_MALN))
    if test:
        years = [2016]
    else:
        years = []
    for rei_id in risks:
        print("pulling {r}".format(r=rei_id))
        if test or rei_id == 166:
            df = get_draws(gbd_id_field='rei_id', gbd_id=rei_id,
                           source='risk',
                           location_ids=[location_id], year_ids=years, age_group_ids=[], sex_ids=[],
                           draw_type='exposure')
        elif not test and rei_id == 86:
            df = interpolate(gbd_id_field='rei_id', gbd_id=rei_id,
                             source='risk',
                             reporting_year_start=1990, reporting_year_end=2016,
                             location_ids=[location_id], age_group_ids=[], sex_ids=[],
                             measure_ids=19, draw_type='exposure')
        else:
            df = interpolate(gbd_id_field='rei_id', gbd_id=rei_id,
                             source='risk',
                             reporting_year_start=1990, reporting_year_end=2016,
                             location_ids=[location_id], age_group_ids=[], sex_ids=[],
                             draw_type='exposure')

        # remove any other ages besides main gbd ages
        df = df.query('(age_group_id >= 2 & age_group_id <= 20) or age_group_id in [30, 31, 32, 235] and sex_id in [1, 2]')
        df = df.query('year_id >= 1990')

        if rei_id == 166:
            # only keep 10+ for smoking
            df = df.query('age_group_id >= 7')
            df = df.query('parameter=="cat1"')

        # set the rei_id because it isnt in the get_draws pull
        df['rei_id'] = rei_id

        # these are prevalence rates
        df['metric_id'] = 3
        if rei_id == 86:
            df['measure_id'] = 19
        else:
            df['measure_id'] = 5

        dfs.append(df[dw.RISK_EXPOSURE_GROUP_COLS + dw.DRAW_COLS])

    df = pd.concat(dfs, ignore_index=True)

    # COLLAPSE SEX
    print("collapsing sex")
    df = df.merge(qry.get_pops(), how='left')
    assert df.population.notnull().values.all(), 'merge with pops fail'
    # overriding the sex variable for collapsing
    df['sex_id'] = df.rei_id.apply(lambda x: 2 if x == 167 else 3)

    # for stunting and wasting (where we only have under-5), keep only under-5 and aggregate ages
    df.ix[df['rei_id'].isin(dw.RISK_EXPOSURE_REI_IDS_MALN), 'age_group_id'] = 1

    # make all ages for PM 2.5
    df.ix[df['rei_id'] == 86, 'age_group_id'] = 22

    df = pd.concat([df[dw.RISK_EXPOSURE_GROUP_COLS],
                    df[dw.DRAW_COLS].apply(lambda x: x * df['population'])],
                   axis=1
                   )
    # so unnecessary programmatically but good for documentation -
    #  these are now prev cases
    df['metric_id'] = 1
    # now that its in cases it is possible to collapse sex
    df = df.groupby(dw.RISK_EXPOSURE_GROUP_COLS, as_index=False).sum()

    # RETURN TO RATES
    print("returning to rates")
    df = df.merge(qry.get_pops(), how='left')
    assert df.population.notnull().values.all(), 'merge with pops fail'
    df = pd.concat([df[dw.RISK_EXPOSURE_GROUP_COLS],
                    df[dw.DRAW_COLS].apply(lambda x: x / df['population'])],
                   axis=1
                   )
    df['metric_id'] = 3

    # AGE STANDARDIZE
    print("age standardizing")
    wgts = custom_age_weights(10, 125) # FOR SMOKING ONLY
    df = df.merge(wgts, on=['age_group_id'], how='left')
    assert df.age_group_weight_value.notnull().values.all(), \
        'merge w wgts failed'
    df = pd.concat([df[dw.RISK_EXPOSURE_GROUP_COLS],
                    df[dw.DRAW_COLS].apply(
        lambda x: x * df['age_group_weight_value'])],
        axis=1
    )
    df['age_group_id'] = 27
    df = df.groupby(
        dw.RISK_EXPOSURE_GROUP_COLS, as_index=False
    )[dw.DRAW_COLS].sum()

    df = df[dw.RISK_EXPOSURE_GROUP_COLS + dw.DRAW_COLS]
    write_output(df, 'risk_exposure', location_id)
    return df


def process_location_cc_draws(location_id, test=False):
    """Pull mortality numbers, limiting to desired ages by cause

    Gets all years >1990 and ages for the location id as mortality numbers
    from get_draws
    """
    dfs = []
    cause_age_sets = [
        [dw.CC_ALL_AGE_CAUSE_IDS, range(2, 21) + range(30, 33) + [235]],
        [dw.CC_THIRTY_SEVENTY_CAUSE_IDS, range(11, 19)],
        [dw.PRE_1990_CAUSES, [22]]
    ]
    if test:
        years = [2016]
    else:
        years = []
    for causes, ages in cause_age_sets:
        gbd_ids = {'cause_ids': causes}
        df = get_draws(gbd_id_field=['cause_id'] * len(causes), gbd_id=causes,
                           source='codcorrect', version=dw.CC_VERS,
                           location_ids=[location_id], year_ids=years, age_group_ids=ages, sex_ids=[3],
                           measure_ids=1)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)

    # keep relevant years
    df = df.ix[(df['year_id'] >= 1990) |
               ((df['cause_id'].isin(dw.PRE_1990_CAUSES)) &
                (df['year_id'] >= 1980)
                )
               ]

    # make sure index variables are ints
    for idvar in dw.CC_GROUP_COLS:
        df[idvar] = df[idvar].astype(int)

    # make sure it looks like we expect
    assert set(df.ix[df['cause_id'].isin(dw.PRE_1990_CAUSES)].age_group_id) == set([22]), \
        'unexpected age group ids found'
    assert set(df.ix[~df['cause_id'].isin(dw.PRE_1990_CAUSES)].age_group_id) == \
        set(range(2, 21) + range(30, 33) + [235]), \
        'unexpected age group ids found'
    assert set(df.sex_id) == set([3]), \
        'unexpected sex ids found'
    if not test:
        assert set(df.ix[df['cause_id'].isin(dw.PRE_1990_CAUSES)].year_id) == \
            set(range(1980, 2017)), \
            'unexpected year ids found'
        assert set(df.ix[
            ~df['cause_id'].isin(dw.PRE_1990_CAUSES)
        ].year_id) == \
            set(range(1990, 2017)), \
            'unexpected year ids found'
    assert set(df.location_id) == set([location_id]), \
        'unexpected location ids found'

    # age standardize
    df = age_standardize(df, 'codcorrect')

    # write the output
    df = df[dw.CC_GROUP_COLS + dw.DRAW_COLS]
    write_output(df, 'codcorrect', location_id)

    return df


def process_location_como_draws(location_id, measure_id, test=False):
    """Pull indidence rates, merging with population to make cases

    Gets all years, ages, and sexes for the location id as incidence rates
    from get_draws, and combines into all ages, both
    sexes cases.
    """
    db_pops = qry.get_pops()
    if measure_id == 6:
        causes = dw.COMO_INC_CAUSE_IDS
    elif measure_id == 5:
        causes = dw.COMO_PREV_CAUSE_IDS
    else:
        raise ValueError("bad measure_id: {}".format(measure_id))

    dfs = []
    if test:
        years = [2016]
    else:
        years = []
    for cause_id in causes:
        print("pulling {c}".format(c=cause_id))
        if test:
            df = get_draws(gbd_id_field='cause_id', gbd_id=cause_id,
                           source='como', version=dw.COMO_VERS,
                           location_ids=[location_id], year_ids=years, age_group_ids=[], sex_ids=[1, 2],
                           measure_ids=[measure_id])
        else:
            df = interpolate(gbd_id_field='cause_id', gbd_id=cause_id,
                             source='como', version=dw.COMO_VERS,
                             reporting_year_start=1990, reporting_year_end=2016,
                             location_ids=[location_id], age_group_ids=[], sex_ids=[1, 2],
                             measure_ids=[measure_id])

        # these pull in as rates
        df['metric_id'] = 3

        # make sure it looks like we expect
        assert set(df.age_group_id) == set(range(2, 21) + range(30, 33) + [235]), \
            'unexpected age group ids found'
        assert set(df.sex_id) == set([1, 2]), \
            'unexpected sex ids found'
        if not test:
            assert set(df.year_id) == set(range(1990, 2017)), \
                'unexpected year ids found'
        assert set(df.location_id) == set([location_id]), \
            'unexpected location ids found'

        # compile
        dfs.append(df[dw.COMO_GROUP_COLS + dw.DRAW_COLS])

    df = pd.concat(dfs, ignore_index=True)

    # merge with pops to transform to cases
    df = df.merge(db_pops, how='left')
    assert df.population.notnull().values.all(), 'merge with populations failed'

    # concatenate the metadata with the draw cols times the pop
    # this multiplies each draw column by the population column
    df = pd.concat(
        [
            df[dw.COMO_GROUP_COLS],
            df[dw.DRAW_COLS].apply(lambda x: x * df['population']),
            df['population']
        ],
        axis=1
    )

    # now its numbers (this line is for readability)
    df['metric_id'] = 1

    # aggregate sexes
    df['sex_id'] = 3

    # collapse sexes together
    df = df.groupby(dw.COMO_GROUP_COLS,
                    as_index=False)[dw.DRAW_COLS + ['population']].sum()
    df = pd.concat(
        [
            df[dw.COMO_GROUP_COLS],
            df[dw.DRAW_COLS].apply(lambda x: x / df['population'])
        ],
        axis=1
    )
    df['metric_id'] = 3

    # AGE STANDARDIZE
    print("age standardizing")
    wgts = custom_age_weights(0, 125)
    df = df.merge(wgts, on=['age_group_id'], how='left')
    assert df.age_group_weight_value.notnull().values.all(), \
        'merge w wgts failed'
    df = pd.concat([df[dw.COMO_GROUP_COLS],
                    df[dw.DRAW_COLS].apply(
        lambda x: x * df['age_group_weight_value'])],
        axis=1
    )
    df['age_group_id'] = 27
    df = df.groupby(
        dw.COMO_GROUP_COLS, as_index=False
    )[dw.DRAW_COLS].sum()

    df = df[dw.COMO_GROUP_COLS + dw.DRAW_COLS]
    write_output(df, 'como', location_id)
    return df


def collect_all_processed_draws(indicator_type):
    """Append together all the processed draws and write output per cause id"""
    if indicator_type == 'codcorrect':
        gbd_ids = set(dw.CC_ALL_AGE_CAUSE_IDS).union(
            set(dw.CC_THIRTY_SEVENTY_CAUSE_IDS))
        gbd_ids = gbd_ids.union(set(dw.PRE_1990_CAUSES))
        group_cols = dw.CC_GROUP_COLS
        temp_dir = dw.CC_TEMP_OUT_DIR
        version_id = dw.CC_VERS
    elif indicator_type in ['como_prev', 'como_inc']:
        if indicator_type == 'como_inc':
            gbd_ids = set(dw.COMO_INC_CAUSE_IDS)
        else:
            gbd_ids = set(dw.COMO_PREV_CAUSE_IDS)
        group_cols = dw.COMO_GROUP_COLS
        temp_dir = dw.COMO_TEMP_OUT_DIR
        version_id = dw.COMO_VERS
    elif indicator_type == 'risk_exposure':
        gbd_ids = set(dw.RISK_EXPOSURE_REI_IDS).union(
            set(dw.RISK_EXPOSURE_REI_IDS_MALN))
        group_cols = dw.RISK_EXPOSURE_GROUP_COLS
        temp_dir = dw.RISK_EXPOSURE_TEMP_OUT_DIR
        version_id = dw.RISK_EXPOSURE_VERS
    elif indicator_type == 'risk_burden':
        gbd_ids = set(dw.RISK_BURDEN_REI_IDS).union(
            set(dw.RISK_BURDEN_DALY_REI_IDS))
        group_cols = dw.RISK_BURDEN_GROUP_COLS
        temp_dir = dw.RISK_BURDEN_TEMP_OUT_DIR
        version_id = dw.BURDENATOR_VERS
    else:
        raise ValueError("bad indicator type: {}".format(indicator_type))

    out_dir = '{d}/{it}/{v}'.format(d=dw.INPUT_DATA_DIR,
                                    it=indicator_type, v=version_id)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    err_list = []
    for gbd_id in gbd_ids:
        gbd_id_dir = os.path.join(temp_dir, str(gbd_id))
        processed_draws = os.listdir(gbd_id_dir)
        gbd_id_dfs = []
        for f in processed_draws:
            path = os.path.join(gbd_id_dir, f)
            gbd_id_df = pd.read_hdf(path)
            gbd_id_dfs.append(gbd_id_df)

        gbd_id_df = pd.concat(gbd_id_dfs, ignore_index=True)
        assert not gbd_id_df[group_cols].duplicated().any(), 'duplicates'
        # some locations are strings, make all ints
        gbd_id_df['location_id'] = gbd_id_df.location_id.astype(int)

        try:
            # test that all level three locations are present, but don't break
            #   all the writing if just one is wrong
            sdg_test.all_sdg_locations(gbd_id_df)
            gbd_id_df.to_hdf('{d}/{gbd_id}.h5'.format(
                d=out_dir,
                gbd_id=gbd_id), key="data", format="table",
                data_columns=['location_id', 'year_id']
            )
            print("{g} finished".format(g=gbd_id))
        except ValueError, e:
            err_list.append(e)
            print("Failed: {g}".format(g=gbd_id), file=sys.stderr)
            continue
    for e in err_list:
        # will raise the first error
        raise(e)


def run_all(indicator_type, test=False):
    """Run each cod correct location-year draw job"""

    # set some locals based on indicator type
    if indicator_type == 'codcorrect':
        temp_dir = dw.CC_TEMP_OUT_DIR
        delete_dir = dw.CC_TEMP_OUT_DIR_DELETE
    elif indicator_type in ['como_prev', 'como_inc']:
        temp_dir = dw.COMO_TEMP_OUT_DIR
        delete_dir = dw.COMO_TEMP_OUT_DIR_DELETE
    elif indicator_type == 'risk_exposure':
        temp_dir = dw.RISK_EXPOSURE_TEMP_OUT_DIR
        delete_dir = dw.RISK_EXPOSURE_TEMP_OUT_DIR_DELETE
    elif indicator_type == 'risk_burden':
        temp_dir = dw.RISK_BURDEN_TEMP_OUT_DIR
        delete_dir = dw.RISK_BURDEN_TEMP_OUT_DIR_DELETE
    else:
        raise ValueError("{it} not supported".format(it=indicator_type))
    # set location ids to run
    locations = qry.get_sdg_reporting_locations()
    location_ids = list(
        locations.location_id.unique()
    )

    # change those if it is a test
    if test:
        location_ids = [68, 165]
    # location_ids = [133, 6, 71, 39, 76, 173, 142, 208, 81, 116, 117, 217, 25, 93, 95]

    # make a new directory to use and delete the other one in a new thread
    print("making a new temp directory and starting a thread to delete old...")
    assert os.path.exists(temp_dir), '{} doesnt exist'.format(temp_dir)
    shutil.move(temp_dir, delete_dir)
    # start a thread that deletes the delete dir
    thread_deleting_old = Thread(target=shutil.rmtree, args=(delete_dir, ))
    thread_deleting_old.start()
    os.mkdir(temp_dir)

    print("processing draws...")
    # initialize list of job ids to hold on
    job_ids = []
    for location_id in location_ids:

        job_ids.append(submit_job_location_draws(
            indicator_type,
            location_id)
        )

    print('collecting all output')
    if not test:
        submit_job_collect(job_ids, indicator_type)

    print('waiting for temp dir deletion to finish...')
    thread_deleting_old.join()
    print('... done deleting and all jobs submitted.')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        "Process draws for SDG analysis."
    )
    parser.add_argument("--process", required=True,
                        choices=["launch", "run_location", "collect"],
                        help="The process to run. "
                        "'launch' to do everything.")
    parser.add_argument("--indicator_type", required=True,
                        choices=["codcorrect",
                                 "como_inc", "como_prev",
                                 "risk_exposure", "risk_burden"],
                        help="The indicator type to run.")
    parser.add_argument("--location_id", type=int,
                        help="When processing a draw, "
                        "the location_id to process")
    args = parser.parse_args()
    test = False
    if args.process == "launch":
        run_all(args.indicator_type, test=test)
    elif args.process == "run_location":
        location_id = args.location_id
        if args.indicator_type == 'codcorrect':
            process_location_cc_draws(location_id, test=test)
        elif args.indicator_type == 'como_inc':
            process_location_como_draws(location_id, 6, test=test)
        elif args.indicator_type == 'como_prev':
            process_location_como_draws(location_id, 5, test=test)
        elif args.indicator_type == 'risk_exposure':
            process_location_risk_exposure_draws(location_id, test=test)
        elif args.indicator_type == 'risk_burden':
            process_location_risk_burden_draws(location_id, test=test)
        else:
            raise ValueError("bad type: {}".format(args.indicator_type))
    elif args.process == "collect":
        collect_all_processed_draws(args.indicator_type)
