import os
from datetime import datetime

import functools
from multiprocessing import Pool
import numpy as np
import pandas as pd
import random
import getpass

from get_draws.api import get_draws

from uhc_estimation import specs, misc, risk_adjuster

ART_DIR = FILEPATH
GBD_ROUND = 6 # which gbd_round_id to use
ASFR_DIR = FILEPATH
DECOMP_STEP = 'step4'
# Determine which central processes to pull data from
CODCORRECT_VERSION_ID = 116
BURDENATOR_VERSION_ID = 128
COMO_VERSION_ID = 461
DALYNATOR_VERSION_ID = 45

def get_draw_parameters(service_proxy, service_population, uhc_version_dir, value_type, test=False):
    '''
    Define draw parameters for a service population.
    '''
    loc_df = pd.read_hdf(FILEPATH)
    loc_df = loc_df.loc[loc_df.level.isin([3, 4])]
    loc_df['subnat_scale'] = False
    subnat_locs = [135, 6, 163]
    for subnat_loc in subnat_locs:
        loc_df.loc[
            loc_df.path_to_top_parent.str.contains('[^0-9]{}[^0-9]'.format(subnat_loc)),
            'subnat_scale'
        ] = True
    location_id = loc_df.loc[(loc_df.level == 3), 'location_id'].tolist()
    scale_location_id = loc_df.loc[loc_df.level == 3, 'location_id'].tolist()
    if test:
        location_id = random.sample(location_id, 10)
        year_id = [specs.YEAR_ID[0], specs.YEAR_ID[-1]]
    else:
        year_id = specs.YEAR_ID
    age_group_id = specs.POPULATION_AGE[service_population]

    if value_type == 'coverage':
        rates = True
    elif value_type == 'observed_burden':
        rates = False

    # Some causes need to be subset to sex_id 2 only, do that subsetting here
    if service_proxy in ['EC treatment of breast cancer', 'EC treatment of cervical cancer', 'EC treatment of uterine cancer']:
        sex_id = [2]
    else:
        sex_id = [3]

    draw_parameters = {
        'location_id':location_id, 'scale_location_id':scale_location_id,
        'year_id':year_id, 'age_group_id':age_group_id, 'sex_id':sex_id, 'rates':rates,
        'to_check':['location_id', 'year_id', 'age_group_id']
    }

    return draw_parameters


def param_check(df, draw_parameters):
    '''
    Make sure dataframe contains all values that would be expected, based on
    parameters.
    '''
    failure = False
    error_messages = []
    for param in draw_parameters['to_check']:
        param_missing = list(set(draw_parameters[param]) - set(df[param].tolist()))
        if len(param_missing) > 0:
            error_messages.append(
                'Missing values for {}: {}'.format(
                    param, misc.connect_list(param_missing, ', ')
                )
            )
            failure = True
        param_excess = list(set(df[param].tolist()) - set(draw_parameters[param]))
        if len(param_excess) > 0:
            error_messages.append(
                'Excess values for {}: {}'.format(
                    param, misc.connect_list(param_excess, ', ')
                )
            )
            failure = True
    if failure: raise ValueError('\n' + '\n'.join(error_messages))


def fetch_outputs_draws(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    '''
    required kwargs:
        gbd_id ([int]) = ids for `get_draws` call
        gbd_id_type ([str]) = types associated w/ each id.
        measure_id (int) = What is the measure of the indicator.
    '''
    if 'split_maternal' in kwargs.keys():
        split_maternal = kwargs['split_maternal']
    else:
        split_maternal = False

    # keep relevant kwargs
    kwargs = {fkey: kwargs[fkey] for fkey in ['gbd_id', 'gbd_id_type', 'measure_id']}

    # load pops
    pop_df = pd.read_hdf(FILEPATH)
    pop_df = pop_df.loc[
        (pop_df.location_id.isin(draw_parameters['location_id'])) &
        (pop_df.year_id.isin(draw_parameters['year_id'])) &
        (pop_df.age_group_id.isin(draw_parameters['age_group_id'])) &
        (pop_df.sex_id.isin(draw_parameters['sex_id']))
    ]

    # load draws
    if kwargs['measure_id'] in [1, 4]:
        source = 'codcorrect'
        metric_id = 1
    elif kwargs['measure_id'] in [3, 5, 6]:
        source = 'como'
        metric_id = 3
    elif kwargs['measure_id'] == 2:
        source = 'dalynator'
        metric_id = 1
    if 'rei_id' in kwargs['gbd_id_type']:  # override if risks involved
        source = 'burdenator'
        metric_id = 1
    data_dir = f'FILEPATH'
    data_file = 'd{}_gbd{}_meas{}.h5'.format(
        uhc_id, '_'.join([str(i) for i in kwargs['gbd_id']]), kwargs['measure_id']
    )
    if os.path.exists(FILEPATH):
        print("using previously stored data")
        print(FILEPATH)
        # if we've stored the data, use that
        df = pd.read_hdf(FILEPATH)
        df = df.loc[
            (df.location_id.isin(draw_parameters['location_id'])) &
            (df.year_id.isin(draw_parameters['year_id'])) &
            (df.age_group_id.isin(draw_parameters['age_group_id'])) &
            (df.sex_id.isin(draw_parameters['sex_id']))
        ]
        param_check(df, draw_parameters)
    else:
        print('Start draw retrieval: {}'.format(datetime.now().strftime('%H:%M:%S')))

        if source == 'codcorrect':
            print("pulling new draws")
            print("""            df = get_draws(
                source=source, gbd_round_id=GBD_ROUND, version_id={id}, 
                num_workers=30,
                metric_id=metric_id,
                location_id=draw_parameters['location_id'],
                year_id=draw_parameters['year_id'],
                age_group_id=draw_parameters['age_group_id'],
                sex_id=draw_parameters['sex_id'],
                decomp_step=DECOMP_STEP,
                **kwargs {kwargs}
            )
            	""".format(id=CODCORRECT_VERSION_ID, kwargs=kwargs))

            print("draw parameters (not including loc, which would cause too much printing)")
            print('year ids {}'.format(draw_parameters['year_id']))
            print('age group ids {}'.format(draw_parameters['age_group_id']))
            print('sex id(s) {}'.format(draw_parameters['sex_id']))
            print('rates {}'.format(draw_parameters['rates']))

            df = get_draws(
                source=source, gbd_round_id=GBD_ROUND, version_id=CODCORRECT_VERSION_ID, 
                num_workers=30,
                metric_id=metric_id,
                location_id=draw_parameters['location_id'],
                year_id=draw_parameters['year_id'],
                age_group_id=draw_parameters['age_group_id'],
                sex_id=draw_parameters['sex_id'],
                decomp_step=DECOMP_STEP,
                **kwargs
            )


        elif source == 'como':
            print("pulling new draws")
            print("""            df = get_draws(
                source={source}, gbd_round_id=GBD_ROUND, version_id={id}, num_workers=30,
                metric_id={metric_id},
                location_id=draw_parameters['location_id'],
                year_id=draw_parameters['year_id'],
                age_group_id=draw_parameters['age_group_id'],
                sex_id=draw_parameters['sex_id'],
                decomp_step={decomp_step},
                **kwargs {kwargs}
            )
                """.format(source=source, id=COMO_VERSION_ID, metric_id=metric_id, decomp_step=DECOMP_STEP, kwargs=kwargs))

            print("draw parameters (not including loc, which would cause too much printing)")
            print('year ids {}'.format(draw_parameters['year_id']))
            print('age group ids {}'.format(draw_parameters['age_group_id']))
            print('sex id(s) {}'.format(draw_parameters['sex_id']))
            print('rates {}'.format(draw_parameters['rates']))

            df = get_draws(
                source=source, gbd_round_id=GBD_ROUND, version_id=COMO_VERSION_ID, num_workers=30,
                metric_id=metric_id,
                location_id=draw_parameters['location_id'],
                year_id=draw_parameters['year_id'],
                age_group_id=draw_parameters['age_group_id'],
                sex_id=draw_parameters['sex_id'],
                decomp_step=DECOMP_STEP,
                **kwargs
            )

        elif source=='dalynator':
            print("pulling new draws")
            print("""            df = get_draws(
                source={source}, gbd_round_id=GBD_ROUND, version_id={vid}, num_workers=30,
                metric_id={metric_id},
                location_id=draw_parameters['location_id'],
                year_id=draw_parameters['year_id'],
                age_group_id=draw_parameters['age_group_id'],
                sex_id=draw_parameters['sex_id'],
                decomp_step={decomp_step},
                **kwargs {kwargs}
            )
                """.format(vid=DALYNATOR_VERSION_ID, source=source, metric_id=metric_id, decomp_step=DECOMP_STEP, kwargs=kwargs))

            print("draw parameters (not including loc, which would cause too much printing)")
            print('year ids {}'.format(draw_parameters['year_id']))
            print('age group ids {}'.format(draw_parameters['age_group_id']))
            print('sex id(s) {}'.format(draw_parameters['sex_id']))
            print('rates {}'.format(draw_parameters['rates']))

            df = get_draws(
                source=source, gbd_round_id=GBD_ROUND, version_id=DALYNATOR_VERSION_ID, num_workers=30,
                metric_id=metric_id,
                location_id=draw_parameters['location_id'],
                year_id=draw_parameters['year_id'],
                age_group_id=draw_parameters['age_group_id'],
                sex_id=draw_parameters['sex_id'],
                decomp_step=DECOMP_STEP,
                **kwargs
            )

            if split_maternal == True:
                print("splitting maternal disease burden in half between anc for the mother and contraception")
                df.loc[df.cause_id == 366, specs.DRAW_COLS] *= .5

        else:
            assert False, ("source should be one of codcorrect, como, or dalynator")

        # print('End draw retrieval: {}'.format(datetime.now().strftime('%H:%M:%S')))
        # print(f'Size of object returned: {round(df.memory_usage().sum() / 1e9, 3)} GB')
        df = df.loc[
            (df.location_id.isin(draw_parameters['location_id'])) &
            (df.year_id.isin(draw_parameters['year_id'])) &
            (df.age_group_id.isin(draw_parameters['age_group_id'])) &
            (df.sex_id.isin(draw_parameters['sex_id']))
        ]
        param_check(df, draw_parameters)
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        df.to_hdf(FILEPATH)

    if metric_id == 3:
        df = df.merge(pop_df)
        df[specs.DRAW_COLS] = (
            df[specs.DRAW_COLS].values.transpose() * df['population'].values
        ).transpose()
    df = df.groupby(specs.ID_COLS + ['age_group_id'], as_index=False)[specs.DRAW_COLS].sum()

    # convert to rate if specified, otherwise just add
    if draw_parameters['rates']:
        df = misc.age_standardize(df, specs.ID_COLS, specs.DRAW_COLS, draw_parameters, uhc_version_dir)
    else:
        df = df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].sum()

    return df[specs.ID_COLS + specs.DRAW_COLS]


def fetch_ratio_draws(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    '''
    required kwargs:
        gbd_id ([int]) = ids for `get_draws` call
        gbd_id_type ([str]) = types associated w/ each id.
        measure_id (int) = What is the measure of the indicator.
    '''
    nf_df = fetch_outputs_draws(
        draw_parameters, uhc_version_dir, uhc_id, **kwargs
    )
    kwargs['measure_id'] = 1
    death_df = fetch_outputs_draws(
        draw_parameters, uhc_version_dir, uhc_id, **kwargs
    )

    # get ratio
    df = misc.draw_math(
        [death_df, nf_df], specs.ID_COLS, specs.DRAW_COLS, '/'
    )

    return df[specs.ID_COLS + specs.DRAW_COLS]


def fetch_art_draws_loc(location_id, age_group_id):
    df = pd.read_csv(FILEPATH)
    df = df.loc[
        (df.year_id.isin(specs.YEAR_ID)) &
        (df.age_group_id.isin(age_group_id)) &
        (df.sex_id.isin([1, 2])),
        ['location_id', 'year_id', 'age_group_id', 'sex_id'] + specs.DRAW_COLS
    ]

    return df


def fetch_art_draws(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    # load age- and sex-specific data
    _fetch_art_draws_loc = functools.partial(
        fetch_art_draws_loc,
        age_group_id=draw_parameters['age_group_id']
    )
    pool = Pool(30)
    dfs = pool.map(_fetch_art_draws_loc, draw_parameters['location_id'])
    pool.close()
    pool.join()
    df = pd.concat(dfs)

    # coverage is by sex, age, loc, year when we read it in. we need to take a
    #   few steps to get rid of that level of specificity, we need coverage by
    #   location and year, NOT by loc, year, age, and sex.
    # load prevalence and aggregate
    print("""
        prev_df = get_draws(
        source='como', gbd_round_id=GBD_ROUND, version_id={id}, num_workers=30,
        metric_id=3,
        location_id=draw_parameters['location_id'],
        year_id=draw_parameters['year_id'],
        age_group_id=draw_parameters['age_group_id'],
        sex_id=[1, 2],
        decomp_step=DECOMP_STEP,
        **kwargs {kwargs}
    )
    	""".format(id=COMO_VERSION_ID, kwargs=kwargs))
    prev_df = get_draws(
        source='como', gbd_round_id=GBD_ROUND, version_id=COMO_VERSION_ID, num_workers=30,
        metric_id=3,
        location_id=draw_parameters['location_id'],
        year_id=draw_parameters['year_id'],
        age_group_id=draw_parameters['age_group_id'],
        sex_id=[1, 2],
        decomp_step=DECOMP_STEP,
        **kwargs
    )
    prev_df = prev_df[['location_id', 'year_id', 'age_group_id', 'sex_id'] + specs.DRAW_COLS]

    pop_df = pd.read_hdf(FILEPATH)
    prev_df = prev_df.merge(
        pop_df[['location_id', 'year_id', 'age_group_id', 'sex_id', 'population']]
    )

    # multiply prevalence (proportion) by population to get number of people with
    #   HIV/AIDS
    prev_df[specs.DRAW_COLS] = (
        prev_df[specs.DRAW_COLS].values.transpose() * prev_df['population'].values
    ).transpose()

    # multiply number of people with HIV/AIDS by coverage to get number of people
    #   covered
    df = misc.draw_math(
        [df, prev_df], ['location_id', 'year_id', 'age_group_id', 'sex_id'],
        specs.DRAW_COLS, '*'
    )

    # get number of people covered and number of people with HIV/AIDS for each
    #   year and loc (sum up by sex and by age)
    df = df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].sum()
    prev_df = prev_df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].sum()
    
    # now divide number of people covered by number of people with HIV/AIDs to get
    #   back into coverage space
    df = misc.draw_math(
        [df, prev_df], specs.ID_COLS, specs.DRAW_COLS, '/'
    )
    
    draw_parameters['to_check'] = ['location_id', 'year_id']
    param_check(df, draw_parameters)

    return df[specs.ID_COLS + specs.DRAW_COLS]


def fetch_asfr_draws_age(age_group_years_start, ihme_loc_ids):
    '''
    To be run in parallel, grab all locs w/in an age group.
    '''
    dfs = []
    for ihme_loc_id in ihme_loc_ids:
        df = pd.read_csv(FILEPATH)
        df['year_id'] = np.floor(df['year']).astype(int)
        df = df.loc[df.year_id.isin(specs.YEAR_ID)]
        df['sim'] = 'draw_' + df['sim'].astype(str)
        df = pd.pivot_table(
            df, index='year_id', columns='sim', values='val'
        ).reset_index()
        df['ihme_loc_id'] = ihme_loc_id
        dfs.append(df)
    df = pd.concat(dfs)
    df['age_group_years_start'] = age_group_years_start

    return df


def fetch_asfr_draws(draw_parameters, uhc_version_dir):
    '''
    Iterate over age and location, reading in ASFR draws.
    '''
    loc_df = pd.read_hdf(FILEPATH)
    age_df = pd.read_hdf(FILEPATH)

    # by age 10-54, grab all the data (looping over iso3)
    ihme_loc_ids = loc_df.loc[
        loc_df.location_id.isin(draw_parameters['location_id']),
        'ihme_loc_id'
    ].tolist()
    _fetch_asfr_draws_age = functools.partial(
        fetch_asfr_draws_age, ihme_loc_ids=ihme_loc_ids
    )
    pool = Pool(9)
    dfs = pool.map(_fetch_asfr_draws_age, range(10, 55, 5))
    pool.close()
    pool.join()
    df = pd.concat(dfs)

    # clean up location and age
    df = df.merge(loc_df[['ihme_loc_id', 'location_id']])
    df = df.merge(age_df[['age_group_years_start', 'age_group_id']])
    df = df.drop(['ihme_loc_id', 'age_group_years_start'], axis=1)
    param_check(df, draw_parameters)

    # age-standardize (NOTE: already rates, so counts=False)
    df = misc.age_standardize(
        df, specs.ID_COLS, specs.DRAW_COLS, draw_parameters, uhc_version_dir, False
    )

    return df[specs.ID_COLS + specs.DRAW_COLS]


def fetch_mmr_draws(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    '''
    required kwargs:
        gbd_id ([int]) = ids for `get_draws` call
        gbd_id_type ([str]) = types associated w/ each id.
        measure_id (int) = What is the measure of the indicator.
    '''
    # NOTE: need to break age/sex params, set up for all-ages (for because of met need)
    draw_parameters['age_group_id'] = range(7, 16)
    draw_parameters['sex_id'] = [2]

    # load age-standardized draws for maternal deaths and births
    death_df = fetch_outputs_draws(
        draw_parameters, uhc_version_dir, uhc_id, **kwargs
    )
    births_df = fetch_asfr_draws(draw_parameters, uhc_version_dir)

    # calc age-standardized MMR
    df = misc.draw_math(
        [death_df, births_df], specs.ID_COLS, specs.DRAW_COLS, '/'
    )

    return df[specs.ID_COLS + specs.DRAW_COLS]


def fetch_burden_draws(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    '''
    Replace measure_id w/ DALYs.

    required kwargs:
        gbd_id ([int]) = ids for `get_draws` call
        gbd_id_type ([str]) = types associated w/ each id.
    '''
    if 'measure_id' in kwargs.keys():
        del kwargs['measure_id']
    kwargs.update({'measure_id':2})

    # load draws
    df = fetch_outputs_draws(
        draw_parameters, uhc_version_dir, uhc_id, **kwargs
    )

    return df[specs.ID_COLS + specs.DRAW_COLS]


def location_path_reader(location_id, filepath):
    '''
    Helper for location-specific files.
    '''
    filename = filepath.format(location_id)
    if filename[-3:] == 'csv':
        df = pd.read_csv(filename)
    elif filename[-2:] == 'h5':
        df = pd.read_hdf(filename)
    else:
        raise ValueError('Only prepared for `.csv` and `.h5` extensions.')

    return df


def fetch_misc_from_disk(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    '''
    Find things that are stored in files somewhere.

    required kwargs:
        path (str) = Path storing files (e.g., ST-GPR)
        OR
        file (str) = File storing data
    '''
    print(uhc_id)
    print(kwargs)
    if 'path' in kwargs.keys():
        _location_path_reader = functools.partial(
            location_path_reader, filepath=kwargs['path'] + '/{}.csv'
        )
        pool = Pool(10)
        dfs = pool.map(_location_path_reader, draw_parameters['location_id'])
        pool.close()
        pool.join()
        df = pd.concat(dfs)
    elif 'file' in kwargs.keys():
        if kwargs['file'][-3:] == 'csv':
            df = pd.read_csv(kwargs['file'])
        elif kwargs['file'][-2:] == 'h5':
            df = pd.read_hdf(kwargs['file'])
    else:
        raise ValueError('Requires either `path` or `file` in spec_args.')

    df = df.loc[
        (df.location_id.isin(draw_parameters['location_id'])) &
        (df.year_id.isin(draw_parameters['year_id']))
    ].sort_values(['location_id', 'year_id']).reset_index(drop=True)
    draw_parameters['to_check'] = ['location_id', 'year_id']
    param_check(df, draw_parameters)

    return df[specs.ID_COLS + specs.DRAW_COLS]


def compile_dfs(intermediate_dir, uhc_ids, uhc_version_dir):
    '''
    Read the file for each uhc_id.
    '''
    def read_uhc_df(uhc_id):
        df = pd.read_hdf(FILEPATH)
        df['uhc_id'] = uhc_id

        return df

    dfs = [read_uhc_df(uhc_id) for uhc_id in uhc_ids]

    return dfs


def fetch_risk_standardized_mortality(draw_parameters, uhc_version_dir, uhc_id, **kwargs):
    '''
    args:
        gbd_id ([int]) = ids for `get_draws` call
        gbd_id_type ([str]) = types associated w/ each id.
        measure_id (int) = What is the measure of the indicator.
    '''
    # parse args

    # get amenable PAF and apply ceiling
    print(draw_parameters['sex_id'])
    print(uhc_id)
    print(kwargs)
    amen_paf_df = get_draws(
        source='burdenator', gbd_round_id=GBD_ROUND, num_workers=20,
        gbd_id_type=['rei_id'] + kwargs['gbd_id_type'],
        gbd_id=[403] + kwargs['gbd_id'], measure_id=1, metric_id=2,
        location_id=draw_parameters['location_id'] + [1],
        year_id=draw_parameters['year_id'],
        age_group_id=draw_parameters['age_group_id'],
        sex_id=[1,2],
        # Use new formula for amenable PAF so that air pollution is not risk-standardized
        version_id=129,
        decomp_step=DECOMP_STEP
    )

    amen_paf_df = amen_paf_df.loc[
        (amen_paf_df.age_group_id.isin(draw_parameters['age_group_id'])) &
        (amen_paf_df.year_id.isin(draw_parameters['year_id']))
    ]

    amen_paf_df = risk_adjuster.scale_amen_paf(amen_paf_df)

    # get all risk PAF and deaths
    true_paf_df = get_draws(
        source='burdenator', gbd_round_id=GBD_ROUND, num_workers=20,
        gbd_id_type=['rei_id'] + kwargs['gbd_id_type'],
        gbd_id=[169] + kwargs['gbd_id'], measure_id=1, metric_id=2,
        location_id=draw_parameters['location_id'] + [1],
        year_id=draw_parameters['year_id'],
        age_group_id=draw_parameters['age_group_id'],
        sex_id=[1,2],
        version_id=BURDENATOR_VERSION_ID,
        decomp_step=DECOMP_STEP
    )
    death_df = get_draws(
        source='codcorrect', gbd_round_id=GBD_ROUND, num_workers=20,
        gbd_id_type=kwargs['gbd_id_type'],
        gbd_id=kwargs['gbd_id'], measure_id=1, metric_id=1,
        location_id=draw_parameters['location_id'] + [1],
        year_id=draw_parameters['year_id'],
        age_group_id=draw_parameters['age_group_id'],
        sex_id=[1,2],
        version_id=CODCORRECT_VERSION_ID,
        decomp_step=DECOMP_STEP
    )

    # agg over cause, age, sex
    death_df, true_paf_df, amen_paf_df = risk_adjuster.cause_age_sex_agg(
        death_df, true_paf_df, amen_paf_df, draw_parameters, uhc_version_dir
    )

    # risk-standardize
    death_df = risk_adjuster.delete_risk(
        death_df.loc[death_df.location_id != 1],
        amen_paf_df.loc[amen_paf_df.location_id != 1],
        specs.ID_COLS, specs.DRAW_COLS
    )
    death_df = risk_adjuster.add_global_risk(
        death_df, amen_paf_df.loc[amen_paf_df.location_id == 1],
        specs.ID_COLS, specs.DRAW_COLS
    )

    pop_df = pd.read_hdf(FILEPATH)
    pop_df = pop_df.loc[
        (pop_df.location_id.isin(draw_parameters['location_id'])) &
        (pop_df.year_id.isin(draw_parameters['year_id'])) &
        (pop_df.age_group_id.isin(draw_parameters['age_group_id'])) &
        (pop_df.sex_id.isin(draw_parameters['sex_id']))
    ]
    pop_df = pop_df.groupby(
        specs.ID_COLS, as_index=False
    ).population.sum()

    # convert to rate
    if 'sex_id' in list(death_df):
        assert len(death_df.sex_id.unique()) == 1, 'Assumes one sex present (if both, must be aggregated).'
    death_df = death_df.merge(pop_df, on=specs.ID_COLS)
    death_df[specs.DRAW_COLS] = (
        death_df[specs.DRAW_COLS].values.transpose() / death_df['population'].values
    ).transpose()

    return death_df[specs.ID_COLS + specs.DRAW_COLS]


################################################################################
COVERAGE_READER = {
    'cfr':fetch_ratio_draws,
    'log_cfr':fetch_ratio_draws,
    'mmr':fetch_mmr_draws,
    'art':fetch_art_draws,
    'vaccines':fetch_misc_from_disk,
    'birth_mort':fetch_misc_from_disk,
    'disk':fetch_misc_from_disk,
    'risk_standardized_mortality': fetch_risk_standardized_mortality
}
