# Imports and environment settings
from multiprocessing import Pool
import os
import sys
import time

import numpy as np
import pandas as pd

from stgpr.model import paths
from stgpr.model.config import *
from stgpr.st_gpr import helpers as hlp
from stgpr.st_gpr.querier import get_ages
from stgpr.common.constants import age_groups

np.seterr(invalid='ignore')


# Create run class
class Model:
    def __init__(self,
                 run_id,
                 output_path,
                 run_type,
                 location_group,
                 spacevar='location_id',
                 timevar='year_id',
                 agevar='age_group_id',
                 sexvar='sex_id',
                 datavar='data',
                 holdout_num=1,
                 nparallel=1,
                 param_sets='0'):

        self.run_id = run_id
        self.output_path = output_path
        self.run_type = run_type
        self.spacevar = spacevar
        self.timevar = timevar
        self.agevar = agevar
        self.orig_agevar = '{}_orig'.format(self.agevar)
        self.sexvar = sexvar
        self.datavar = datavar
        self.loc_group = location_group
        self.holdout_num = holdout_num
        self.nparallel = nparallel
        self.param_sets = hlp.separate_string_to_list(str(param_sets), int)

        # conditionals
        if self.nparallel > 1:
            self.parallel = 1
            self.outroot = '{dir}/st_temp_{ko}'.format(
                dir=self.output_path, ko=self.holdout_num)
        else:
            self.parallel = 0
            self.outroot = '{dir}/output_{ko}.h5'.format(
                dir=self.output_path, ko=self.holdout_num)

        # central keys
        self.ids = [self.spacevar, self.timevar, self.agevar, self.sexvar]

        self.intermediate_dir = os.path.dirname(
            '{root}/amp_nsv_temp/'.format(root=self.output_path))

    def enumerate_parameters(self):
        """Initialize all model parameters"""

        # pull parameters
        parameters = hlp.model_load(
            self.run_id, 'parameters', param_set=None, output_path=self.output_path)
        parameters = parameters.where((pd.notnull(parameters)), None)

        # st necessities
        self.st_version = parameters['st_version'].iat[0]
        self.location_set_id = int(parameters['location_set_id'].iat[0])
        self.gbd_round_id = int(parameters['gbd_round_id'].iat[0])
        self.decomp_step = parameters['decomp_step'].iat[0]

        # prediction vars
        self.prediction_location_ids = hlp.get_parallelization_location_group(self.location_set_id,
                                                                              self.nparallel,
                                                                              self.loc_group,
                                                                              self.gbd_round_id,
                                                                              self.decomp_step)
        self.prediction_year_ids = parameters['prediction_year_ids'].iat[0]
        self.prediction_age_group_ids = self.df[self.orig_agevar].unique(
        ).tolist()
        self.prediction_sex_ids = self.df[self.sexvar].unique().tolist()

        # age stuff
        if parameters['st_custom_age_vector'].iat[0]:
            self.custom_age_vector = [float(x) for x in
                                      parameters['st_custom_age_vector'].iat[0]
                                      .split(',')]
        else:
            self.custom_age_vector = None

        self.n_levels = int(max(self.df.level))
        self.level_cols = ['level_{i}'.format(
            i=i) for i in range(self.n_levels + 1)]

    def prep_indata(self):
        """Bring in prepped data, location hierarchy and stage1 linear model estimates
        and merge together for input into spacetime stage"""

        # Retrieve data
        self.data = hlp.model_load(
            self.run_id, 'prepped', holdout=self.holdout_num, output_path=self.output_path)
        self.stage1 = hlp.model_load(
            self.run_id, 'stage1', holdout=self.holdout_num, output_path=self.output_path)
        all_locations = hlp.model_load(self.run_id,
                                       'location_hierarchy', output_path=self.output_path).query('level >= {}'.format(NATIONAL_LEVEL))
        self.all_locations = all_locations['location_id'].tolist()

        # Merge
        df = pd.merge(self.stage1, self.data, how='outer', on=self.ids)

        # force stupid level columns to float
        lvlcols = [s for s in df.columns if "level_" in s]
        df[lvlcols] = df[lvlcols].astype(float)

        # Drop any data specified as 1 by ko_{holdout_num}
        df = hlp.drop_ko_values(df, self.holdout_num, datavar=self.datavar)

        # Create temporary age index
        df[self.orig_agevar] = df[self.agevar]
        df[self.agevar] = df.groupby(self.agevar).grouper.group_info[0]

        # Sort
        self.df = df.sort_values(by=self.ids)

    def get_age_map(self) -> None:
        """Create an age vector from either user-specified
        vector or the age midpoint of each age_group_id to be predicted over,
        to allow for more realistic distances between ages"""

        # find midpoint of each age group
        age_database = get_ages(self.prediction_age_group_ids)\
            .pipe(self._calculate_age_midpoint)

        if self.custom_age_vector:
            print('age vectoring')
            age_midpoints = self.custom_age_vector
        else:
            age_midpoints = age_database['age_midpoint']

        self.age_map = dict(
            zip(age_database[self.agevar].index, age_midpoints))

    def get_location_sex_list(self):
        """Build a list to parse out spacetime jobs to different cores based on location and sex"""

        ls_list = self.df[[self.spacevar, self.sexvar]].drop_duplicates()
        if self.parallel == 1:
            ls_list = ls_list[(ls_list.location_id.isin(
                self.prediction_location_ids))]
        ls_list = ls_list.values
        self.ls_list = tuple(map(tuple, ls_list))

    def count_loc_max_country_years(self):
        """
        Calculate the maximum number of country-years of data among
        age groups for a given country-level location and sex being modeled.
        To actually understand it, best to look at pictures, but imagine
        a bunch of time-series plots facet-wrapped by age and sex.
        If you counted the datapoints for each of those time-series plots,
        then just took the biggest one for each sex, that would be the
        maximum number of country-years for a given location/sex.

        Inputs: Pandas dataframe with location, year, age, sex, and data
        Outputs: Pandas dataframe with unique location, sex, and
        maximum country-years values
        """

        cy = self.df.dropna(subset=[self.datavar])[self.ids + [self.datavar]]
        cy = cy.drop_duplicates(subset=self.ids)

        cy = cy.drop_duplicates(subset=[self.spacevar, self.timevar,
                                        self.agevar, self.sexvar])
        cy = cy.groupby([self.spacevar, self.agevar,
                         self.sexvar]).agg('count')[self.datavar]
        cy = cy.groupby([self.spacevar, self.sexvar]).agg('max')

        location_cy = cy.to_frame(name='location_id_count').reset_index()

        # make sure all locations in frame, even ones with zero cy of data
        comps = [self.all_locations, self.prediction_sex_ids]
        names = [self.spacevar, self.sexvar]
        loc_sex_df = hlp.square_skeleton(components=comps, names=names)
        location_cy = location_cy.merge(loc_sex_df, how='outer')
        location_cy = location_cy.fillna(0).sort_values(names)

        self.location_country_years = location_cy

    def ensure_loc_id_count_in_df(self):

        if 'location_id_count' not in self.df.columns:
            self.df = self.df.merge(self.location_country_years,
                                    on=[self.spacevar, self.sexvar])

    def get_hyperparameters(self, param_set=None):
        """Set up hyperparameters for ko-runs (ko), different-hyperparams-by-data-density-runs (dd) """

        hlp.model_load(run_id, 'parameters',
                       holdout=holdout_num, param_set=param_set, output_path=self.output_path)
        store = pd.HDFStore(hlp.model_path(self.run_id, 'parameters'), 'r')
        self.hyperparams = store.get('parameters_{}'.format(param_set))
        store.close()

    def assign_hyperparameters(self, param_set):

        hype = hlp.model_load(run_id, 'parameters',
                              holdout=self.holdout_num, param_set=param_set, output_path=self.output_path)
        in_df = self.df.copy()

        if self.run_type in ['in_sample_selection', 'oos_selection']:
            hype = pd.DataFrame(dict(zip(hype.keys(), hype.values)), index=[0])

        if self.run_type != 'dd':
            hype['density_cat'] = 0
            in_df['density_cat'] = 0
        else:
            hype.sort_values(by='density_cutoffs', inplace=True)
            cutoffs = hype.density_cutoffs.tolist()
            hype['density_cat'] = list(range(0, len(cutoffs)))

            in_df = hlp.assign_density_cats(df=in_df,
                                            country_year_count_var='location_id_count',
                                            cutoffs=cutoffs)

        self.in_df = in_df.merge(hype, on='density_cat', how='left')

    def launch(self):
        p = Pool(processes=6)

        st = p.map(hlp.st_launch(self.in_df, self.run_id,
                                 self.age_map, self.st_version, self.output_path), self.ls_list)

        p.close()

        # Concatenate list
        self.spacetime_estimates = pd.concat(st)

    def clean_st_results(self):

        self.spacetime_estimates[self.agevar] = self.spacetime_estimates[self.orig_agevar]
        spacetime_estimates = self.spacetime_estimates.drop(
            self.orig_agevar, 1)

        cols = self.ids + ['st', 'scale']
        self.spacetime = spacetime_estimates[cols]

    def save_st_results(self, param_set=None):

        # output
        if self.parallel == 1:

            param_set_outpath = '{}/{}'.format(self.outroot, param_set)
            outpath = '{}/{}/{}.csv'.format(self.outroot,
                                            param_set, self.loc_group)

            if not os.path.isdir(self.outroot):
                os.system('mkdir -m 777 -p {}'.format(self.outroot))
            if not os.path.isdir(param_set_outpath):
                os.system('mkdir -m 777 -p {}'.format(param_set_outpath))

            self.spacetime.to_csv(outpath, index=False)
        else:

            hlp.model_save(self.spacetime, self.run_id, 'st', holdout=self.holdout_num,
                           param_set=param_set, mode='w', output_path=self.output_path)

    def _calculate_age_midpoint(self, age_database: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the age midpoint given columns 'age_start' and
        'age_end'. In specific age group cases where this doesn't make sense
        (ex: 80+), assign a midpoint.

        Note:
            Age smoothing relies on the assumption that age groups in
            age_database are organized in sort-order by age_group_id,
            ascending. As age group ids are mapped from their values (10, 11,
            12, 22, 388, ...)  to their indices (0, 1, 2, 3 ...), it is crucial
            midpoints are NOT sorted by age start/end and retain their original
            order instead.
        """
        age_database['age_midpoint'] = (
            age_database['age_start'] + age_database['age_end']) / 2

        specific_age_midpoints = age_groups.SPECIFIC_AGE_MIDPOINTS[self.gbd_round_id]

        for age_group_id, midpoint in specific_age_midpoints.items():
            age_database.loc[age_database[self.agevar] == age_group_id, 'age_midpoint']\
                = midpoint

        # divide by 5 to normalize for 5-year age groups
        age_database['age_midpoint'] = age_database['age_midpoint'] / 5

        return age_database


if __name__ == '__main__':
    run_id = int(sys.argv[1])
    output_path = sys.argv[2]
    holdout_num = int(sys.argv[3])
    run_type = sys.argv[4]
    param_sets = sys.argv[5]
    nparallel = int(sys.argv[6])
    if nparallel > 0:
        location_group = int(sys.argv[7])

    for i in ['run_id', 'output_path', 'holdout_num', 'run_type', 'param_sets', 'nparallel', 'location_group']:
        print('{} : {}'.format(i, eval(i)))

    start = time.time()

    m = Model(run_id=run_id,
              output_path=output_path,
              run_type=run_type,
              location_group=location_group,
              holdout_num=holdout_num,
              nparallel=nparallel,
              param_sets=param_sets,
              spacevar=SPACEVAR,
              timevar=TIMEVAR,
              agevar=AGEVAR,
              sexvar=SEXVAR,
              datavar=DATAVAR)

    print('Prepping indata')
    m.prep_indata()

    print('Getting parameters')
    m.enumerate_parameters()

    print('Determining age vector')
    m.get_age_map()

    print('Preparing for multiprocessing')
    m.get_location_sex_list()

    for i in m.param_sets:

        # Get hyperparameters and add to dataset
        m.count_loc_max_country_years()
        m.ensure_loc_id_count_in_df()
        m.assign_hyperparameters(param_set=i)

        # Launch
        print(('Running {} spacetime for param_set {}, '
               'locations {}').format(m.st_version, i, m.prediction_location_ids))
        m.launch()

        # Clean
        print('Cleaning results')
        m.clean_st_results()

        # Output
        m.save_st_results(param_set=i)
        print('Spacetime outputs saved to {path}'.format(path=m.outroot))

    end = time.time()
    print('Total time to completion: {sec} seconds'.format(sec=end - start))
