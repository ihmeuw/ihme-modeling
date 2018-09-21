import logging

from transmogrifier.common import exceptions as ex
from transmogrifier.common.files import draw_manager
from db_queries import get_population
from db_tools import ezfuncs

from dalynator.computation_element import ComputationElement

logger = logging.getLogger(__name__)

_draw_cols = ["draw_" + str(i) for i in range(1000)]


class DataSource(ComputationElement):
    """A datasource returns a dataframe. This has no cache - it "re-gets" on
    every call and does not keep any references to the returned data frame."""

    def __init__(self, name):
        """
        Args:
        :param name: Only used for log messages, has no semantic meaning (e.g.,
        there could be duplicates)
        """
        self.name = name


    def check_for_bad_data(self, df):
        """Throws ValueError if:
        1. any value in any column is None or NaN.
        2. <Expect to add more rules in the future>"""

        if df.isnull().values.any():
            raise ValueError(
                'DataFrame "{}" has nulls or NaNs'.format(self.name))

    @staticmethod
    def normalize_columns(df, name='anonymous', desired_index=[]):
        """Re-order the columns in the dataframe so that the first columns are the desired (demographic) index_cols,
        followed by draw_0 thru draw_999, followed by the 'extra' non-draw columns.
        If desired index_cols is None, then any column that does not start with 'draw_' is assumed
        to be an index column"""
        existing_cols = df.columns.tolist()

        existing_index, extra_columns, number_draws = DataSource.extract_non_draws(
            desired_index, existing_cols)
        if len(desired_index) == 0:
            index = existing_index + _draw_cols[:number_draws] + extra_columns
        else:
            # If they are not the same length give up
            if len(existing_index) != len(desired_index):
                error_message = \
                    "In DataFrame {} demographic indexes don't match (length check), actual {} vs desired {}".format(
                        name, existing_index, desired_index)
                logger.error(error_message)
                raise ValueError(error_message)
            index = desired_index + _draw_cols[:number_draws] + extra_columns

        df = df[index].copy(deep=True)
        return df

    @staticmethod
    def extract_non_draws(desired_index, existing_cols):
        """If desired index_cols is None, then any column that does not start with 'draw_' is assumed to be an index column.
        Desired index_cols is assumed to short, so linear lookups are okay"""
        extras = []
        indexes = []
        number_draws = 0
        for col in existing_cols:
            if col.startswith("draw_"):
                number_draws += 1
            elif not desired_index or col in desired_index:
                indexes.append(col)
            else:
                extras.append(col)
        return indexes, extras, number_draws



class SuperGopherDataSource(DataSource):
    """Delegate the finding and reading of the file to SuperGopher.
    """

    def __init__(self, name, file_naming_conventions, dir_path,
                 turn_off_null_and_nan_check, **kwargs):
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
        DataSource.__init__(self, name)
        if dir_path is None:
            raise ValueError("File path cannot be None")
        self.dir_path = dir_path
        self.file_naming_conventions = file_naming_conventions
        self.turn_off_null_and_nan_check = turn_off_null_and_nan_check
        self.kwargs = kwargs
        logger.debug(
            ('Using SuperGopher data source "{}", turn-off-null-check '
             '{},  dir={}, filter={}'.format(
                 self.name,
                 self.turn_off_null_and_nan_check, self.dir_path,
                 self.file_naming_conventions)))

    def get_data_frame(self, desired_index):
        """If 'turn_off_null_check' is true then the null check will be skipped.

        This will pass-through NoDrawsError exception raised by the underlying
        SuperGopher implementation if it cannot find any files.

        Will raise ValueError if no files exist. ValueError is used to be
        consistent with other DataSource methods
        """

        logger.debug('Super gopher get_data_frame, kwargs:')
        for key, value in self.kwargs.iteritems():
            logger.debug("    {} == {}".format(key, value))

        try:
            df = draw_manager(
                file_pattern=self.file_naming_conventions['file_pattern'],
                h5_tablename=self.file_naming_conventions.get('h5_tablename'),
                directory=self.dir_path,
                n_draws='max',  # get max available, then resample later
                num_workers=None,
                **self.kwargs)

        except ex.NoDrawsError as e:
            if "No draws exist for demographics specified" in str(e.message):
                logger.info("Super gopher '{}' found no files, "
                            "stopping pipeline by raising ValueError".format(
                                self.name))
                raise ValueError(e.message)
            else:
                # Don't lose the Exception, in this case we don't know what it
                # is!
                raise e

        logger.info(
            'Super gopher "{}" got content, shape {}'.format(
                self.name, df.shape))

        if not self.turn_off_null_and_nan_check:
            self.check_for_bad_data(df)

        df = DataSource.normalize_columns(df, self.name, desired_index)

        logger.debug(
            ('SuperGopher "{}" got and validated data, dir={}, filter='
             '{}'.format(self.name, self.dir_path,
                         self.file_naming_conventions)))
        return df


class PipelineDataSource(DataSource):
    """Run each DS in turn, feeding output from stage n-1 as input to stage n.
    The first stage can be a plain DataSource, the remaining stages must be
    DataFilters"""

    def __init__(self, name, pipeline):
        DataSource.__init__(self, name)
        self.pipeline = pipeline

    def get_data_frame(self, desired_index):
        df_out = None
        for stage in self.pipeline:
            # First one typically won't have set_input_dataframe
            # and in any case does not have an input
            if df_out is not None:
                stage.set_input_data_frame(df_out)
            df_out = stage.get_data_frame([])
        # Validate that we have the full desired index_cols
        df_out = DataSource.normalize_columns(df_out, self.name, desired_index)
        return df_out


class SqlDataSource(DataSource):
    """Represents a Sql query. Only one query, although it is executed on every call to get_data_frame"""

    def __init__(self, name, query_string, connection_definition):
        DataSource.__init__(self, name)
        self.query_string = query_string
        self.connection_definition = connection_definition

    def get_data_frame(self, desired_index):
        df_out = ezfuncs.query(self.query_string,
                               conn_def=self.connection_definition)
        df_out = DataSource.normalize_columns(df_out, self.name, desired_index)
        return df_out


class DataFrameDataSource(DataSource):
    """input a dataframe, output is dataframe"""

    def __init__(self, name, data_frame):
        DataSource.__init__(self, name)
        self.data_frame = data_frame

    def get_data_frame(self, desired_index):
        df_out = self.data_frame
        df_out = DataSource.normalize_columns(df_out, self.name, desired_index)
        return df_out


class GetPopulationDataSource(DataSource):
    """A query using db_queries.get_population. You probably want the column named 'population'"""

    ALL = -1

    def __init__(self, name, gbd_round_id, location_set_id=None,
                 location_set_version_id=None, age_group_id=ALL,
                 location_id=ALL, year_id=ALL, sex_id=ALL):
        DataSource.__init__(self, name)
        self.gbd_round_id = gbd_round_id
        self.location_set_id = location_set_id
        self.location_set_version_id = location_set_version_id
        self.age_group_id = age_group_id
        self.location_id = location_id
        self.year_id = year_id
        self.sex_id = sex_id

    def get_data_frame(self, desired_index):
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
                gbd_round_id=self.gbd_round_id)
        df_out = DataSource.normalize_columns(df_out, self.name, desired_index)
        df_out.rename(columns={'population': 'pop_scaled'}, inplace=True)
        return df_out
