import logging
import os

import pandas as pd

from codem.data.parameters import get_run_id
from db_queries import get_demographics, get_envelope, get_location_metadata
from db_tools import ezfuncs
from gbd import constants as gbd_constants
from gbd.decomp_step import decomp_step_from_decomp_step_id

import hybridizer.reference.db_connect as db_connect

logger = logging.getLogger(__name__)


class ModelData:
    """
    Stores and manipulates the data and its properties for a given
    model_version_id
    """

    def __init__(self, model_version_id, model_folder, data_draws, index_columns,
                 envelope_column, pop_column, data_columns, conn_def,
                 location_set_id, gbd_round_id, decomp_step_id, sex_id,
                 user, envelope_proc_version_id):
        # Assign inputs to attributes
        self.model_version_id = model_version_id
        self.model_folder = model_folder
        self.data_draws = data_draws
        self.index_columns = index_columns
        self.envelope_column = envelope_column
        self.pop_column = pop_column
        self.data_columns = data_columns
        self.conn_def = conn_def
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id
        self.decomp_step_id = decomp_step_id
        self.sex_id = sex_id
        self.user = user
        self.envelope_run_id = get_run_id(envelope_proc_version_id)
        self.most_detailed_ages = get_demographics(
            gbd_team="cod", gbd_round_id=gbd_round_id)['age_group_id']

        # Define other attributes
        self.location_hierarchy = self.get_location_hierarchy()
        self.data_summaries = None

        # Methods that run when ModelData object is instantiated
        self.check_missing_locations()

    def format_draws(self, data):
        """
        Drops all columns except for index columns, envelope and pop columns,
        and data columns

        :param self:
        :param data: dataframe
            pandas dataframe to edit
        :return: dataframe
            pandas dataframe with only index, envelope, pop, and data columns
            retained
        """
        logger.info("Formatting model draws.")
        keep_columns = self.index_columns + [self.envelope_column,
                                             self.pop_column] + self.data_columns
        return data[keep_columns]

    def get_location_hierarchy(self):
        """
        Reads and returns location hierarchy information from SQL

        :param self:
        :return: dataframe
            pandas dataframe with location hierarchy information from database
        """
        logger.info("Getting location hierarchy.")
        decomp_step = decomp_step_from_decomp_step_id(self.decomp_step_id)
        location_hierarchy_history = get_location_metadata(
            self.location_set_id, gbd_round_id=self.gbd_round_id,
            decomp_step=decomp_step
        )[['location_id', 'level', 'parent_id', 'is_estimate']]

        return location_hierarchy_history

    def get_estimated_locations(self):
        """
        Gets the most detailed locations that are estimates from the location
        hierarchy history

        :param self:
        :return: list of ints
            list of location_id's labelled as is_estimate
        """
        logger.info("Get most detailed locations.")
        location_hierarchy_history = self.location_hierarchy.copy(deep=True)
        location_hierarchy_history = location_hierarchy_history.loc[
            location_hierarchy_history['is_estimate'] == 1]
        return location_hierarchy_history['location_id'].drop_duplicates().tolist()

    def check_missing_locations(self):
        """
        Prints any missing locations, i.e. locations that are in estimated
        but not in draw_locations

        :param self:
        """
        logger.info("Check for missing locations.")
        draw_locations = self.data_draws['location_id'].drop_duplicates().tolist()
        estimated_locations = self.get_estimated_locations()
        if len(set(estimated_locations) - set(draw_locations)) > 0:
            logger.debug(
                "The following locations as missing from the draws {}".format(
                    ', '.join([str(x) for x in list(set(estimated_locations) -
                                                    set(draw_locations))])
                )
            )
            raise Exception("Locations missing from the draws!")
        else:
            logger.debug("No missing locations!")

    def aggregate_locations(self):
        """
        Aggregate data up the location hierarchy and assign it to the data_draws
        attribute

        :param self:
        """
        logger.info("Aggregate locations up the hierarchy.")
        # prep/clean up ModelData object
        if self.location_hierarchy is None:
            self.location_hierarchy = self.get_location_hierarchy()
        keep_columns = self.index_columns + \
            [self.envelope_column, self.pop_column] + self.data_columns
        self.data_draws = self.data_draws[keep_columns]
        self.check_missing_locations()

        data = self.data_draws.copy(deep=True)
        data = data.loc[data['location_id'].isin(self.get_estimated_locations())]

        # merge data with location hierarchy, get most granular level
        data = pd.merge(data,
                        self.get_location_hierarchy(),
                        on='location_id',
                        how='left')
        max_level = data['level'].max()

        data = self.format_draws(data)
        # loop through to aggregate data to less and less granular levels, i.e.
        # up the hierarchy
        for level in range(max_level, 0, -1):
            data = pd.merge(data,
                            self.location_hierarchy[['location_id',
                                                     'level',
                                                     'parent_id']],
                            on='location_id',
                            how='left')
            temp = data.loc[data['level'] == level].copy(deep=True)
            temp['location_id'] = temp['parent_id']
            temp = self.format_draws(temp)
            temp = temp.groupby(self.index_columns).sum().reset_index()
            data = pd.concat([self.format_draws(data), temp]).reset_index(drop=True)
        self.data_draws = data

    def save_draws(self):
        """
        Saves the draws information to an hdf

        :param self:
        """
        logger.info("Saving draws.")
        sex_dict = {1: 'male', 2: 'female'}
        draws_filepath = self.model_folder + "/draws/deaths_{sex_name}.h5".\
            format(sex_name=sex_dict[self.sex_id])
        if not os.path.exists(self.model_folder + "/draws"):
            os.makedirs(self.model_folder + "/draws")
        self.data_draws.to_hdf(draws_filepath,
                               'data',
                               mode='w',
                               format='table',
                               data_columns=['location_id',
                                             'year_id',
                                             'sex_id',
                                             'age_group_id',
                                             'cause_id'])

    def get_full_envelope(self):
        """
        For models with age restrictions, the all-ages group should use envelope
        for all ages. Pulls the envelope for most detailed age groups and
        all ages.
        """
        logger.info("Pulling in the envelope for all ages, and most detailed"
                    "age groups")
        # get the envelope, then merge in with the aggregated dfs
        env_df = get_envelope(
            location_id=self.data_draws.location_id.unique().tolist(),
            year_id=self.data_draws.year_id.unique().tolist(),
            age_group_id=self.data_draws.age_group_id.unique().tolist(),
            sex_id=self.sex_id,
            gbd_round_id=self.gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(self.decomp_step_id),
            run_id=self.envelope_run_id
        )
        env_df = env_df[['location_id', 'year_id', 'age_group_id', 'mean']]
        env_df.rename(columns={'mean': 'envelope'}, inplace=True)
        self.data_draws.drop('envelope', inplace=True, axis=1)
        self.data_draws = self.data_draws.merge(env_df, on=['location_id', 'year_id', 'age_group_id'])

    def generate_all_ages(self):
        """
        Adds the draws in all specific age groups to get the counts for all ages
        combined, then stores it in data_draws attribute

        :param self:
        """
        logger.info("Generating draws for all ages.")
        self.data_draws = self.data_draws.loc[
            self.data_draws['age_group_id'] != gbd_constants.age.ALL_AGES
        ]
        data = self.format_draws(self.data_draws)
        data = data.loc[
            data['age_group_id'].map(lambda x: x in self.most_detailed_ages)
        ]
        # sum by indices (age, sex, location) to get the sum over all age groups
        data['age_group_id'] = gbd_constants.age.ALL_AGES  # all ages
        data = data.groupby(self.index_columns).sum().reset_index()
        self.data_draws = pd.concat([self.data_draws, data])

    def generate_age_standardized(self):
        """
        Standardizes data_draws using age group weights from database

        :param self:
        """
        logger.info("Generating age-standardized draws.")
        # get age weights
        sql_query = """
                    SELECT
                        age_group_id,
                        age_group_weight_value
                    FROM
                        shared.age_group_weight agw
                    WHERE
                        gbd_round_id = :gbd_round_id
                    """
        age_standard_data = ezfuncs.query(
            sql_query, conn_def=self.conn_def, parameters={
                'gbd_round_id': self.gbd_round_id
            })
        # prep draws for merge
        self.data_draws = self.data_draws.loc[
            self.data_draws['age_group_id'] != gbd_constants.age.AGE_STANDARDIZED
        ]
        data = self.format_draws(self.data_draws)
        data = data.loc[data['age_group_id'] in self.most_detailed_ages]
        # merge on age-weights
        data = pd.merge(data,
                        age_standard_data,
                        on='age_group_id')
        # make adjusted rate
        for c in self.data_columns:
            data[c] = data[c] * data['age_group_weight_value'] / data[self.pop_column]
        # collapsing to generate ASR
        data['age_group_id'] = gbd_constants.age.AGE_STANDARDIZED
        data = data.groupby(self.index_columns).sum().reset_index()
        # merge with original data
        self.data_draws = pd.concat([self.data_draws, data])

    def generate_summaries(self):
        """
        Summarizes model data and stores summaries in data_summaries attribute

        :param self:
        """
        logger.info("Generate data summaries")
        # Copy draws
        self.data_summaries = self.data_draws.copy(deep=True)
        for column in self.data_columns:
            self.data_summaries.loc[
                self.data_summaries['age_group_id'] != gbd_constants.age.AGE_STANDARDIZED,
                column
            ] = (
                self.data_summaries.loc[
                    self.data_summaries['age_group_id'] != gbd_constants.age.AGE_STANDARDIZED,
                    column
                ] /
                self.data_summaries.loc[
                    self.data_summaries['age_group_id'] != gbd_constants.age.AGE_STANDARDIZED,
                    self.envelope_column
                ]
            )
        self.data_summaries = self.data_summaries[self.index_columns + self.data_columns]
        self.data_summaries['mean_cf'] = self.data_summaries[self.data_columns].mean(axis=1)
        self.data_summaries['lower_cf'] = self.data_summaries[self.data_columns].quantile(0.025, axis=1)
        self.data_summaries['upper_cf'] = self.data_summaries[self.data_columns].quantile(0.975, axis=1)
        # Generate other columns
        self.data_summaries['model_version_id'] = self.model_version_id
        self.data_summaries['inserted_by'] = self.user
        self.data_summaries['last_updated_by'] = self.user
        self.data_summaries['last_updated_action'] = 'INSERT'
        self.data_summaries = self.data_summaries[self.index_columns + ['model_version_id',
                                                                        'mean_cf',
                                                                        'lower_cf',
                                                                        'upper_cf',
                                                                        'inserted_by',
                                                                        'last_updated_by',
                                                                        'last_updated_action']]

    def save_summaries(self):
        """
        Saves data_summaries to a csv file

        :param self:
        """
        logger.info("Save data summaries.")
        summary_filepath = self.model_folder + "/summaries.csv"
        self.data_summaries.to_csv(summary_filepath, index=False)

    def upload_summaries(self):
        """
        Writes data_summaries to the database

        :param self:
        """
        logger.info("Upload summaries.")
        db_connect.wipe_database_upload(model_version_id=self.model_version_id,
                                        conn_def=self.conn_def)
        data = self.data_summaries[['model_version_id', 'year_id', 'location_id', 'sex_id',
                                    'age_group_id', 'mean_cf', 'lower_cf', 'upper_cf',
                                    'inserted_by',
                                    'last_updated_by', 'last_updated_action']].reset_index(drop=True)
        db_connect.write_data(df=data, db='cod', table='model', conn_def=self.conn_def)
