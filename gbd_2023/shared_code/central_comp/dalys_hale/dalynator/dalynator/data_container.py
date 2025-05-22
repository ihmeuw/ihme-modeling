import pandas as pd
import os
import pickle
import logging

import gbd.constants as gbd
from ihme_dimensions.dfutils import squaring_resample_df
from transforms.transforms import DEFAULT_MERGE_COLUMNS, transform_metric

from dalynator import get_death_data
from dalynator import get_yld_data
from dalynator import get_yll_data
from dalynator import get_daly_data
from dalynator.data_source import SuperGopherDataSource
from dalynator.lib.utils import get_index_draw_columns


logger = logging.getLogger("dalynator.data_container")


class InvalidPAFs(Exception):
    pass


def add_star_id(df, index_cols=None):
    """
    Append star_id to df, if risk_id is present but star_id is not.
    star_id will be star.UNDEFINED,
    except if rei_id is 0 (risk.TOTAL_ATTRIBUTABLE) then
    it will be star.ANY_EVIDENCE_LEVEL

    If index_cols is not null, then it will also append star_id to that if
    needed and return that list.

    Potentially mutates df and mutates index columns, in both cases adding
    'star_id'
    """
    cols = df.columns.values.tolist()
    if 'rei_id' in cols and 'star_id' not in cols:
        logger.info("Adding star_id column")
        df['star_id'] = gbd.star.UNDEFINED
        df.loc[df.eval('rei_id == @gbd.risk.TOTAL_ATTRIBUTABLE'),
               'star_id'] = gbd.star.ANY_EVIDENCE_LEVEL
        if index_cols is not None:
            return index_cols.append('star_id')
        else:
            return None
    else:
        return index_cols


def remove_unwanted_star_id_column(columns, write_out_star_ids):
    """
    If not writing out star ids, return a new list without star_id
    """
    if not write_out_star_ids and 'star_id' in columns:
        temp = columns[:]
        temp.remove('star_id')
    else:
        temp = columns
    return temp


def mangle_unwanted_star_id_column(df, write_out_star_ids=False):
    """
    If not writing out star ids, rename star_id
    column so that the writers will not write it
    """
    if not write_out_star_ids:
        df.rename(columns={'star_id': 'star_ignore'}, inplace=True)


def unmangle_unwanted_star_id_column(df, write_out_star_ids=False):
    """
    If star_ids had been changed to star_ignore, then change it back
    """
    if 'star_ignore' in df.columns:
        df.rename(columns={'star_ignore': 'star_id'}, inplace=True)


def remove_unwanted_stars(df, write_out_star_ids=False):
    """
    MUTATES df:
     drop column star_id if it is present but we should not write it out.
     MUST return df because it is a transform in data_sink
    """
    if not write_out_star_ids and 'star_id' in df:
        return df.drop(['star_id'], axis=1, inplace=True)
    return df


class DataContainer(object):
    """Provide simple caching and key-based retrieval for the limited
    'input data' types [yll, yld, death, pop, paf] that are used in the
    DALYnator/Burdenator.

    Makes DataFrame access look like this:

        my_data_container['pop']
        my_data_container['yll']

    ... and helps eliminate redundant trips to the filesystem/db
    """

    def __init__(self, cache_granularity_dict, n_draws, release_id,
            age_group_ids=None, epi_dir=None, cod_dir=None,
            cod_pattern=None, daly_dir=None, paf_dir=None, cache_dir=None,
            agg_causes=True, turn_off_null_and_nan_check=False,
            yld_metric=gbd.metrics.NUMBER, raise_on_paf_error=False,
            age_group_set_id=None, pop_merge_columns=DEFAULT_MERGE_COLUMNS):
        """Initialize the DataContainer with proper directories and
        scope as determined in the cache_granularity_dict. Usually these would
        be loc/year.

        Args:
            cache_granularity_dict (dict): dictionary outlining the scope of
                the container, usually in the form of {'location_id': loc_id,
                'year_id', year_id}
            n_draws (int): Number of draws to resample raw data to (for YLD,
                YLL, Deaths, DALYs, PAFs)
            release_id (int): The id of the GBD release
            age_group_ids (List[int]): The aggregate age groups to calculate
                for summarization, other than all-age and age-standardized
            epi_dir (str): Root directory where YLD data are stored
            cod_dir (str): Root directory where YLL and death data are stored
            daly_dir (str): Root directory where DALY data are stored
            paf_dir (str): Root directory where PAF data are stored
            cache_dir (str): Root directory where all caches are stored
            turn_off_null_and_nan_check (bool): Disable null and nan checking
                when retrieving data.
            yld_metric (int): metric_id for the metric-space ylds are stored in
            age_group_set_id (int): for specifying an explicit age group set
        """

        self.valid_indata_types = ['yll', 'yld', 'death', 'pop', 'paf', 'daly',
                                   'age_weights', 'age_spans',
                                   'cause_hierarchy', 'cause_risk_metadata']
        self.resample_types = ['yll', 'yld', 'death', 'daly', 'paf']
        self.cached_values = {}

        self.cache_granularity_dict = cache_granularity_dict
        self.release_id = release_id
        self.epi_dir = epi_dir
        self.cod_dir = cod_dir
        self.cod_pattern = cod_pattern
        self.daly_dir = daly_dir
        self.paf_dir = paf_dir
        self.cache_dir = cache_dir
        self.agg_causes = agg_causes
        self.disable_checks = turn_off_null_and_nan_check
        self.yld_metric = yld_metric
        self.n_draws = n_draws
        self.raise_on_paf_error = raise_on_paf_error
        self.age_group_ids = age_group_ids
        self.age_group_set_id = age_group_set_id
        self.pop_merge_columns = pop_merge_columns

    def __getitem__(self, key):
        """Allows dict-like retrieval."""
        if key not in self.cached_values:
            self._get_df_by_key(key)
        return self.cached_values[key]

    def validate_cache_granularity_dict(self, key):
        if key in ['yll', 'yld', 'death', 'daly', 'paf']:
            for demographic in ['location_id', 'year_id']:
                if demographic not in self.cache_granularity_dict.keys():
                    raise ValueError("cache_granularity_dict must contain {} "
                                     "when trying to get {} data"
                                     .format(demographic, key))
        else:
            pass

    def build_where_filter(self, columns):
        where_list = []
        for demographic_id in columns:
            if demographic_id in self.cache_granularity_dict:
                where_list.append("{}=={}".format(
                    demographic_id,
                    self.cache_granularity_dict[demographic_id]))
        where = ' & '.join(w for w in where_list)
        return where

    def _get_df_by_key(self, key):
        """Return a DataFrame for 'key' type of data"""
        self.validate_cache_granularity_dict(key)
        if not self._is_valid_indata_type(key) and \
            'location_hierarchy' not in key:
            raise KeyError("key must be one of ['{}']".format(
                "', '".join(self.valid_indata_types)))

        if key == 'yll':
            if self.cod_dir is None:
                raise NameError("cod_dir must be specified on the "
                                "DataContainer to retrieve yll data")
            df = get_yll_data.get_data_frame(
                self.cod_dir, self.cod_pattern, self.disable_checks,
                location_id=self.cache_granularity_dict['location_id'],
                year_id=self.cache_granularity_dict['year_id'])
        elif key == 'yld':
            if self.epi_dir is None:
                raise NameError("epi_dir must be specified on the "
                                "DataContainer to retrieve yld data")
            yld_df = get_yld_data.get_data_frame(
                self.epi_dir, self.disable_checks,
                location_id=self.cache_granularity_dict['location_id'],
                year_id=self.cache_granularity_dict['year_id'])

            index_cols, draw_cols = get_index_draw_columns(yld_df)

            # YLD draws are stored in rate space. Convert YLD draws to number
            # space, if requested
            if self.yld_metric == gbd.metrics.NUMBER:
                yld_df = transform_metric(
                    df=yld_df,
                    from_id=gbd.metrics.RATE,
                    to_id=gbd.metrics.NUMBER,
                    pop_df=self.__getitem__('pop'),
                    merge_columns=self.pop_merge_columns,
                )

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
                self.cod_dir, self.cod_pattern, self.disable_checks,
                location_id=self.cache_granularity_dict['location_id'],
                year_id=self.cache_granularity_dict['year_id'])
        elif key == 'pop':
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve pop data")
            pop_file = "{}/pop.h5".format(self.cache_dir)
            where = self.build_where_filter(
                ['location_id', 'year_id', 'age_group_id', 'sex_id'])
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
            df = pd.read_hdf(age_spans_file).sort_values(by="age_group_years_start")
        elif 'cause_hierarchy' in key:
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve cause_hierarchy"
                                " data")
            cause_hierarchy_file = "{}/{}.pickle".format(
                self.cache_dir, key)
            df = pickle.load(open(cause_hierarchy_file, "rb"))
            # this is not a dataframe, it is Tree
        elif 'location_hierarchy' in key:
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve location_hierarchy"
                                " data")
            location_hierarchy_file = "{}/{}.pickle".format(
                self.cache_dir, key)  # loc_set_id is coded into key
            df = pickle.load(open(location_hierarchy_file, "rb"))
            # this is not a dataframe, it is Tree
        elif key == 'cause_risk_metadata':
            if self.cache_dir is None:
                raise NameError("cache_dir must be specified on the "
                                "DataContainer to retrieve "
                                "cause_risk_metadata data")
            metadata_file = "{}/cause_risk_metadata.csv".format(self.cache_dir)
            df = pd.read_csv(metadata_file)
        elif key == 'paf':
            if self.paf_dir is None:
                raise NameError("paf_dir must be specified on the "
                                "DataContainer to retrieve paf data")
            if not os.path.exists('{}/all_reis.csv'.format(self.cache_dir)):
                raise NameError("all_reis must be cached in the cache_dir {} "
                                "in order to get pafs".format(self.cache_dir))
            all_reis = pd.read_csv('{}/all_reis.csv'.format(self.cache_dir)
                                   ).rei_id.unique()
            index_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id',
                          'cause_id', 'rei_id']
            paf_data = SuperGopherDataSource(
                'paf csv.gz file',
                {'file_pattern': '{location_id}_{year_id}.csv.gz',
                 'paf': 'draws'},
                self.paf_dir,
                self.disable_checks,
                desired_index=index_cols,
                location_id=self.cache_granularity_dict['location_id'],
                year_id=self.cache_granularity_dict['year_id'],
                rei_id=list(all_reis))
            df = paf_data.get_data_frame()
            self._validate_pafs(df)
            add_star_id(df)
        elif key == 'daly':
            if self.daly_dir is None:
                raise NameError("daly_dir must be specified on the "
                                "DataContainer to retrieve daly data")
            df = get_daly_data.get_data_frame(
                self.daly_dir, self.disable_checks,
                location_id=self.cache_granularity_dict['location_id'],
                year_id=self.cache_granularity_dict['year_id'])

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

        df = squaring_resample_df(df, self.n_draws)

        logger.info("Done resampling {}. New shape {}".format(key, df.shape))

        df.drop("n_draws", inplace=True, axis=1)

        return df

    def _is_valid_indata_type(self, key):
        return key.lower() in self.valid_indata_types

    def _validate_pafs(self, pafs):
        ch = self['cause_hierarchy']
        agg_cause_ids = set()
        for tree in ch:
            agg_cause_ids = agg_cause_ids | (
                set([c.id for c in tree.nodes]) -
                set([c.id for c in tree.leaves()]))
        if agg_cause_ids & set(pafs.cause_id.unique()):
            if self.raise_on_paf_error:
                logger.error("PAFs cannot contain aggregate cause_ids")
                raise InvalidPAFs("PAFs cannot contain aggregate cause_ids")
            else:
                logger.warning("PAFs cannot contain aggregate cause_ids")
