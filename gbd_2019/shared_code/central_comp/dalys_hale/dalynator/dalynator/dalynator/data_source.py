import re
import logging
import numpy as np

from dataframe_io import exceptions as ex
from db_queries import get_population
from db_tools import ezfuncs

from draw_sources.draw_sources import DrawSource

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)

_draw_cols = ["draw_" + str(i) for i in range(1000)]


class DataSource(ComputationElement):
    """A datasource returns a dataframe. For now, this has no cache - it
    "re-gets" on every call and does not keep any references to the returned
    data frame."""

    def __init__(self, name, data_frame=None, desired_index=[],
                 turn_off_null_and_nan_check=False):
        """
        Args:
        :param name: Only used for log messages, has no semantic meaning (e.g.,
        there could be duplicates)
        """
        self.name = name
        self.desired_index = desired_index
        self.turn_off_null_and_nan_check = turn_off_null_and_nan_check

        self.data_frame = data_frame

    def get_data_frame(self):
        self.data_frame = self._load_data_frame()
        self._validate()
        return self.data_frame

    def _load_data_frame(self):
        """To be overriden by subclasses. Should return an
        unvalidated/un-normalized data frame"""
        if self.data_frame is None:
            raise NotImplementedError
        return self.data_frame

    def _validate(self):
        if not self.turn_off_null_and_nan_check:
            self._check_for_bad_data()
        self._normalize_columns()

    def _check_for_bad_data(self):
        """Throws ValueError if:
        1. any value in any column is None or NaN.
        """
        if self.data_frame.isnull().values.any():
            raise ValueError(
                'DataFrame "{}" has nulls or NaNs'.format(self.name))

    def _normalize_columns(self):
        """Re-order the columns in the dataframe so that the first columns are
        the desired (demographic) index_cols, followed by draw_0 thru draw_999,
        followed by the 'extra' non-draw columns.  If desired index_cols is
        None, then any column that does not start with 'draw_' is assumed to be
        an index column"""
        # Be careful, the demographic columns and draws might be mixed up.
        # Assuming that all demographic columns are to the left of draw_0 can
        # be wrong.
        existing_index, extra_columns, number_draws = self._extract_non_draws()
        if len(self.desired_index) == 0:
            index = existing_index + _draw_cols[:number_draws] + extra_columns
        else:
            # If they are not the same length give up
            if len(existing_index) != len(self.desired_index):
                error_message = (
                    "In DataFrame {} demographic indexes don't "
                    "match (length check), actual {} vs desired "
                    "{}".format(self.name, existing_index, self.desired_index))
                logger.error(error_message)
                raise ValueError(error_message)
            index = self.desired_index + _draw_cols[:number_draws] + extra_columns

        self.data_frame = self.data_frame[index]
        return self.data_frame

    def _extract_non_draws(self):
        """If desired index_cols is None, then any column that does not start
        with 'draw_' is assumed to be an index column.

        Desired index_cols is assumed to short, so linear lookups are okay"""
        extras = []
        indexes = []
        number_draws = 0
        existing_cols = self.data_frame.columns.tolist()

        for col in existing_cols:
            if col.startswith("draw_"):
                number_draws += 1
            elif not self.desired_index or col in self.desired_index:
                indexes.append(col)
            else:
                extras.append(col)
        return indexes, extras, number_draws


class SuperGopherDataSource(DataSource):
    """Delegate the finding and reading of the file to SuperGopher.
    """

    def __init__(self, name, file_naming_conventions, dir_path,
                 turn_off_null_and_nan_check, desired_index=[], **kwargs):
        """Delegate the finding and reading of the file to SuperGopher.
        Resample to n_draws.

        Args:
            name(str): name for logger
            file_naming_conventions(Dict[str, str]): Dictionary containing at
                least 'file_pattern' key and possibly 'h5_tablename' key.
            dir_path(str): path to directory containing files to search
            turn_off_null_and_nan_check(bool): If true, don't check for
                missingness
            **kwargs: Optionally specify column name and list of ids to filter
                with. For example, column1=[id1, id2]. Get_data_frame will only
                return rows where column1 contains id1 or id2. """
        DataSource.__init__(
            self, name=name,
            turn_off_null_and_nan_check=turn_off_null_and_nan_check,
            desired_index=desired_index)
        if dir_path is None:
            raise ValueError("File path cannot be None")
        self.dir_path = dir_path
        self.file_naming_conventions = file_naming_conventions
        self.kwargs = kwargs
        logger.debug(
            ('Using SuperGopher data source "{}", turn-off-null-check '
             '{},  dir={}, filter={}'.format(
                 self.name,
                 self.turn_off_null_and_nan_check, self.dir_path,
                 self.file_naming_conventions)))
        self.ds = None

    def _load_data_frame(self):
        """If 'turn_off_null_check' is true then the null check will be skipped.
        Yuk. GBD 2015 como files have nulls caused by "other maternal" issues
        for males.  Generally it is much safer to validate data, this is
        dangerous but historically necessary.

        This will pass-through NoDrawsError exception raised by the underlying
        SuperGopher implementation if it cannot find any files.

        Will raise ValueError if no files exist. ValueError is used to be
        consistent with other DataSource methods
        """

        logger.debug('Super gopher _load_data_frame, kwargs:')
        for key, value in self.kwargs.items():
            value = list(np.atleast_1d(value))
            self.kwargs[key] = value
            logger.debug("    {} == {}".format(key, value))
        self.kwargs.update({'strict_filter_checking': True})

        try:
            pattern = self.file_naming_conventions['file_pattern']
            draw_dir = self.dir_path
            h5_tablename = self.file_naming_conventions.get('h5_tablename',
                                                            None)
            params = {'file_pattern': pattern, 'draw_dir': draw_dir}
            if h5_tablename:
                params.update({'h5_tablename': h5_tablename})

            if not self.ds:
                ds = DrawSource(
                    params=params)
                self.ds = ds
            df = ds.content(filters=self.kwargs)
            df = self._add_n_draws(df)

        except ex.InvalidFilter:
            logger.info("Super gopher '{}' found no files with file_pattern: {}"
                        ", draw_dir: {}, and filters {}. Stopping pipeline"
                        "".format(self.name, pattern, draw_dir, self.kwargs))
            raise

        logger.info(
            'Super gopher "{}" got content, shape {}'.format(
                self.name, df.shape))

        logger.debug(
        ('SuperGopher "{}" got and validated data, dir={}, filter='
         '{}'.format(self.name, self.dir_path,
                     self.file_naming_conventions)))
        return df

    @classmethod
    def _add_n_draws(self, df):
        '''Add n_draws column to dataframe to aid in resampling later'''
        # We're going to count the number of nulls per row to determine
        # n_draws count per row (num columns - num null).
        # this heuristic could fail if the draws contain nulls (due to some
        # other reason than concatenating 1k + 100 draw files)

        draw_prefixes = df.filter(like='_0').columns.str.rstrip('0')

        # PAF files are wide on measure and therefore have multiple sets
        # of draws. We're going to assume, in that case, each measure will
        # have the same number of draws.
        draw_prefix = draw_prefixes[0]
        draw_regex = re.compile("^{}\d+$".format(draw_prefix))

        draw_cols = [col for col in df.columns if draw_regex.match(col)]
        df['n_draws'] = df[draw_cols].notnull().sum(axis=1)

        return df


class PipelineDataSource(DataSource):
    """Run each DS in turn, feeding output from stage n-1 as input to stage n.
    The first stage can be a plain DataSource, the remaining stages must be
    DataFilters"""

    def __init__(self, name, pipeline, desired_index=[],
                 turn_off_null_and_nan_check=False):
        DataSource.__init__(
            self, name=name, desired_index=desired_index,
            turn_off_null_and_nan_check=turn_off_null_and_nan_check)
        self.pipeline = pipeline

    def _load_data_frame(self):
        df_out = None
        for stage in self.pipeline:
            # First one typically won't have set_input_dataframe
            # and in any case does not have an input
            if df_out is not None:
                stage.set_input_data_frame(df_out)
            df_out = stage.get_data_frame()
        return df_out


class GetPopulationDataSource(DataSource):
    """A query using db_queries.get_population. You probably want the column
    named 'population'"""

    ALL = -1

    def __init__(self, name, gbd_round_id, decomp_step, location_set_id=None,
                 location_set_version_id=None, age_group_id=ALL,
                 location_id=ALL, year_id=ALL, sex_id=ALL, desired_index=[]):
        DataSource.__init__(self, name=name, desired_index=desired_index)
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.location_set_id = location_set_id
        self.location_set_version_id = location_set_version_id
        self.age_group_id = age_group_id
        self.location_id = location_id
        self.year_id = year_id
        self.sex_id = sex_id

    def _load_data_frame(self):
        if self.location_set_id is not None:
            df_out = get_population(
                age_group_id=self.age_group_id,
                location_set_id=self.location_set_id, year_id=self.year_id,
                location_id=self.location_id,
                sex_id=self.sex_id, gbd_round_id=self.gbd_round_id)
        elif self.location_set_version_id is not None:
            df_out = get_population(
                age_group_id=self.age_group_id,
                location_set_version_id=self.location_set_version_id,
                location_id=self.location_id,
                year_id=self.year_id, sex_id=self.sex_id,
                gbd_round_id=self.gbd_round_id)
        else:
            df_out = get_population(
                age_group_id=self.age_group_id,
                location_id=self.location_id,
                year_id=self.year_id, sex_id=self.sex_id,
                gbd_round_id=self.gbd_round_id,
                decomp_step=self.decomp_step)
        if 'run_id' in df_out.columns.tolist():
            del df_out['run_id']
        df_out.rename(columns={'population': 'pop_scaled'}, inplace=True)
        return df_out
