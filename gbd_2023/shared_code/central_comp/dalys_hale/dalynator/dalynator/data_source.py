import re
import logging
import numpy as np
from typing import List, Union

from dataframe_io.api.public import InvalidFilter
from db_queries import get_population

from draw_sources.draw_sources import DrawSource

from dalynator.computation_element import ComputationElement
from dalynator.lib.utils import get_index_draw_columns

logger = logging.getLogger(__name__)


class DataSource(ComputationElement):
    """A datasource returns a dataframe."""

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
        """Throws ValueError if any value in any column is None or NaN."""

        if self.data_frame.isnull().values.any():
            raise ValueError(
                'DataFrame "{}" has nulls or NaNs'.format(self.name))

    def _normalize_columns(self):
        """Re-order the columns in the dataframe so that the first columns are
        the desired (demographic) index_cols, followed by draw_0 thru draw_999,
        followed by the 'extra' non-draw columns.  If desired index_cols is
        None, then any column that does not start with 'draw_' is assumed to be
        an index column"""
        index_cols, draw_cols = get_index_draw_columns(self.data_frame)
        if len(self.desired_index) == 0:
            existing_index = index_cols
            extra_columns = []
        else:
            existing_index = [i for i in index_cols if i in self.desired_index]
            extra_columns = list(set(index_cols) - set(existing_index))

            if len(existing_index) != len(self.desired_index):
                error_message = (
                    "In DataFrame {} demographic indexes don't "
                    "match (length check), actual {} vs desired "
                    "{}".format(self.name, existing_index, self.desired_index))
                logger.error(error_message)
                raise ValueError(error_message)

        index = existing_index + draw_cols + extra_columns
        self.data_frame = self.data_frame[index]

        return self.data_frame


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

        This will pass-through NoDrawsError exception raised by the underlying
        SuperGopher implementation if it cannot find any files.

        Will raise ValueError if no files exist.
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

        except InvalidFilter:
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

        draw_prefixes = df.filter(like='_0').columns.str.rstrip('0')

        # PAF files are wide on measure and therefore have multiple sets
        # of draws.
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
    """A query using db_queries.get_population."""

    ALL = -1

    def __init__(
        self,
        name: str,
        release_id: int,
        population_run_id: int,
        age_group_id: Union[int, List[int]] = ALL,
        location_id: Union[int, List[int]] = ALL,
        year_id: Union[int, List[int]] = ALL,
        sex_id: Union[int, List[int]] = ALL,
        desired_index: List[str] = [],
    ) -> None:
        DataSource.__init__(self, name=name, desired_index=desired_index)
        self.release_id = release_id
        self.population_run_id = population_run_id
        self.age_group_id = age_group_id
        self.location_id = location_id
        self.year_id = year_id
        self.sex_id = sex_id

    def _load_data_frame(self):
        df_out = get_population(
            age_group_id=self.age_group_id,
            location_id=self.location_id,
            year_id=self.year_id,
            sex_id=self.sex_id,
            release_id=self.release_id,
            run_id=self.population_run_id,
            use_rotation=False,
        )
        if 'run_id' in df_out.columns.tolist():
            del df_out['run_id']
        df_out.rename(columns={'population': 'pop_scaled'}, inplace=True)
        return df_out
