import pandas as pd
import pickle
import logging

import gbd.constants as gbd
from transmogrifier._draws import resample_df

from dalynator import get_death_data
from dalynator import get_yld_data
from dalynator import get_yll_data
from dalynator import get_daly_data
from dalynator.data_source import SuperGopherDataSource


class DataContainer(object):
    """Provide simple caching and key-based retrieval for the limited
    'input data' types [yll, yld, death, pop, paf] that are used in the
    DALYnator/Burdenator.

    Makes DataFrame access look like this...

        my_data_container['pop']
        my_data_container['yll']

    ... and helps eliminate redundant trips to the filesystem/db
    """

    def __init__(self, location_id, year_id, n_draws, gbd_round_id,
                 epi_dir=None, cod_dir=None, daly_dir=None, paf_dir=None,
                 cache_dir=None, turn_off_null_and_nan_check=False,
                 yld_metric=gbd.metrics.NUMBER):
        """Initialize the DataContainer with proper directories and
        location-year scope.

        Args:
            location_id (int): Location scope for the container
            year_id (int): Year scope for the container
            n_draws (int): Number of draws to resample raw data to (for YLD,
                YLL, Deaths, DALYs, PAFs)
            gbd_round_id (int): ID for the GBD Round (see shared.gbd_round
                table in the db for a human-readable mapping)
            epi_dir (str): Root directory where YLD data are stored
            cod_dir (str): Root directory where YLL and death data are stored
            daly_dir (str): Root directory where DALY data are stored
            paf_dir (str): Root directory where PAF data are stored
            turn_off_null_and_nan_check (bool): Throw caution to the wind
                and disable null and nan checking when retrieving data. This
                should always be False when running in Production.
        """

        self.valid_indata_types = ['yll', 'yld', 'death', 'pop', 'paf', 'daly',
                                   'age_weights', 'age_spans',
                                   'cause_hierarchy']
        self.resample_types = ['yll', 'yld', 'death', 'daly', 'paf']
        self.cached_values = {}
        # self.cached_values is almost always a data_frame - but
        # cause_hierarchy is a Tree

        self.location_id = location_id
        self.year_id = year_id
        self.gbd_round_id = gbd_round_id
        self.epi_dir = epi_dir
        self.cod_dir = cod_dir
        self.daly_dir = daly_dir
        self.paf_dir = paf_dir
        self.cache_dir = cache_dir
        self.disable_checks = turn_off_null_and_nan_check
        self.yld_metric = yld_metric
        self.n_draws = n_draws

    def __getitem__(self, key):
        """Allows dict-like retrieval... e.g. my_data_container['pop']"""
        if key not in self.cached_values:
            self._get_df_by_key(key)
        return self.cached_values[key]

    def _convert_num_to_rate(self, num_df):
        """Converts a DataFrame in rate space to number space"""

        pop = self.__getitem__('pop')

        draw_cols = list(num_df.filter(like='draw_').columns)
        index_cols = list(set(num_df.columns) - set(draw_cols))
        core_index = ['location_id', 'year_id', 'age_group_id',
                      'sex_id']

        rate_df = pd.merge(num_df, pop, on=core_index)
        rate_df['metric_id'] = gbd.metrics.RATE
        rate_df.set_index(index_cols)
        rate_df[draw_cols] = (rate_df[draw_cols].values /
                              rate_df[['pop_scaled']].values)
        rate_df.drop('pop_scaled', axis=1, inplace=True)
        return rate_df

    def _convert_rate_to_num(self, rate_df):
        """Converts a DataFrame in rate space to number space"""

        pop = self.__getitem__('pop')

        draw_cols = list(rate_df.filter(like='draw_').columns)
        index_cols = list(set(rate_df.columns) - set(draw_cols))
        core_index = ['location_id', 'year_id', 'age_group_id',
                      'sex_id']

        num_df = pd.merge(rate_df, pop, on=core_index)
        num_df['metric_id'] = gbd.metrics.NUMBER
        num_df.set_index(index_cols)
        num_df[draw_cols] = (num_df[draw_cols].values *
                             num_df[['pop_scaled']].values)
        num_df.drop('pop_scaled', axis=1, inplace=True)
        return num_df

    def _get_df_by_key(self, key):
        """Return a DataFrame for 'key' type of data"""
        if not self._is_valid_indata_type(key):
            raise KeyError("key must be one of ['{}']".format(
                "', '".join(self.valid_indata_types)))

        if key == 'yll':
            if self.cod_dir is None:
                raise NameError("cod_dir must be specified on the "
                                "DataContainer to retrieve yll data")
            df = get_yll_data.get_data_frame(
                self.location_id, self.year_id, self.cod_dir,
                self.disable_checks)
        elif key == 'yld':
            if self.epi_dir is None:
                raise NameError("epi_dir must be specified on the "
                                "DataContainer to retrieve yld data")
            yld_df = get_yld_data.get_data_frame(
                self.location_id, self.year_id, self.epi_dir,
                self.disable_checks)

            draw_cols = list(yld_df.filter(like='draw_').columns)
            index_cols = list(set(yld_df.columns) - set(draw_cols))

            # YLD draws are stored in rate space. Convert YLD draws to number
            # space, if requested
            if self.yld_metric == gbd.metrics.NUMBER:
                yld_df = self._convert_rate_to_num(yld_df)
            elif self.yld_metric != gbd.metrics.RATE:
                assert ValueError("Only RATE and NUMBER metrics are allowed "
                                  "for requesting yld data")

            yld_df = yld_df[index_cols + draw_cols]
            df = yld_df
        elif key == 'death':
            if self.cod_dir is None:
                raise NameError("cod_dir must be specified on the "
                                "DataContainer to retrieve death data")
            df = get_death_data.get_data_frame(
                self.location_id, self.year_id, self.cod_dir,
                self.disable_checks)
        elif key == 'pop':
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve pop data")
            pop_file = "{}/pop.h5".format(self.cache_dir)
            where = "location_id=={l} & year_id=={y}".format(
                l=self.location_id, y=self.year_id)
            df = pd.read_hdf(pop_file, where=where)
        elif key == 'age_weights':
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve age_weights data")
            age_weights_file = "{}/age_weights.h5".format(self.cache_dir)
            df = pd.read_hdf(age_weights_file)
        elif key == 'age_spans':
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve age_spans data")
            age_spans_file = "{}/age_spans.h5".format(self.cache_dir)
            df = pd.read_hdf(age_spans_file)
        elif key == 'cause_hierarchy':
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve cause_hierarchy"
                                " data")
            cause_hierarchy_file = "{}/cause_hierarchy.pickle".format(
                self.cache_dir)
            df = pickle.load(open(cause_hierarchy_file, "rb"))
            # this is not a dataframe, it is Tree
        elif key == 'paf':
            if self.paf_dir is None:
                raise NameError("paf_dir must be specified on the "
                                "DataContainer to retrieve paf data")
            paf_data = SuperGopherDataSource(
                'paf dta file',
                {'file_pattern': '{location_id}_{year_id}.dta',
                 'paf': 'draws'},
                self.paf_dir,
                self.disable_checks,
                location_id=self.location_id,
                year_id=self.year_id)
            index_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id',
                          'cause_id', 'rei_id']
            df = paf_data.get_data_frame(index_cols)
        elif key == 'daly':
            if self.daly_dir is None:
                raise NameError("daly_dir must be specified on the "
                                "DataContainer to retrieve daly data")
            df = get_daly_data.get_data_frame(
                self.location_id, self.year_id, self.daly_dir,
                self.disable_checks)

        df = self._resample(df, key)
        self.cached_values[key] = df

    def _resample(self, df, key):
        '''Potentially resample raw data, if dataframe has different number
        of draws from self.n_draws.
        '''
        if key not in self.resample_types:
            return df

        logger = logging.getLogger(__name__)
        logger.info(
            "Resampling {}, with df shape {}. n_draws counts are {}".format(
                key, df.shape, df.n_draws.value_counts(dropna=False).to_dict())
        )

        if key == 'paf':
            # pafs are stored wide on yld/yll so we have to call
            # resample twice and recombine
            id_cols = [c for c in df if c.endswith('id')]
            yld_cols = [c for c in df if 'yld' in c]
            yll_cols = [c for c in df if 'yll' in c]
            yld_df = resample_df(
                df[id_cols + yld_cols + ['n_draws']], self.n_draws)
            yll_df = resample_df(
                df[id_cols + yll_cols + ['n_draws']], self.n_draws)
            yll_cols = [c for c in yll_df if 'yll' in c]
            df = pd.concat([yld_df, yll_df[yll_cols]], axis=1)
        else:
            df = resample_df(df, self.n_draws)

        logger.info("Done resampling {}. New shape {}".format(key, df.shape))

        # draw_manager sticks on n_draws column to aid in resampling
        # don't want to propogate it out of data container
        df.drop("n_draws", inplace=True, axis=1)

        return df

    def _is_valid_indata_type(self, key):
        return key.lower() in self.valid_indata_types
