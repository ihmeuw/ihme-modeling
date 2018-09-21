import sqlalchemy as sql
import pandas as pd
from hybridizer.core import run_query, execute_statement, read_creds
import datetime
import logging
import os
import hybridizer.log_utilities as l

AGES_DISAGGREGATED = range(2, 21) + range(30, 33) + [235]

class ModelData(object):
    """
    Stores and manipulates the data and its properties for a given
    model_version_id
    """

    def __init__(self, model_version_id, data_draws, index_columns,
                        envelope_column, pop_column, data_columns,
                        server, location_set_id=35, gbd_round_id=4):
        # Assign inputs to attributes
        self.model_version_id = model_version_id
        self.data_draws = data_draws
        self.index_columns = index_columns
        self.envelope_column = envelope_column
        self.pop_column = pop_column
        self.data_columns = data_columns
        self.server = server
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id

        # Define other attributes
        self.location_hierarchy = self.get_location_hierarchy()

        self.data_summaries = None
        self.model_folder = None
        self.age_group_id_start = None
        self.age_group_id_end = None
        self.acause = None
        self.sex_id = None
        self.user = None

        # Methods that run when ModelData object is instatiated
        self.get_model_details()
        self.get_model_folder()
        self.check_missing_locations()

    def get_model_details(self):
        """
        Gets acause, sex_id, and user from database and stores them in self
        """
        sql_query = """
                    SELECT
                        mv.model_version_id,
                        mv.cause_id,
                        c.acause,
                        mv.sex_id,
                        mv.inserted_by
                    FROM
                        cod.model_version mv
                    JOIN
                        shared.cause c USING (cause_id)
                    WHERE
                        model_version_id = {};
                    """.format(self.model_version_id)
        model_data = run_query(sql_query, server=self.server)
        self.acause = model_data.ix[0, 'acause']
        self.sex_id = model_data.ix[0, 'sex_id']
        self.user = model_data.ix[0, 'inserted_by']

    def get_age_range(self):
        """
        Gets the min and max age group id from database and stores them in self
        """
        self.age_group_id_start = \
                self.data_draws.ix[self.data_draws['age_group_id'].map(lambda x:\
                                x in AGES_DISAGGREGATED), 'age_group_id'].min()
        self.age_group_id_end = \
                self.data_draws.ix[self.data_draws['age_group_id'].map(lambda x:\
                                x in AGES_DISAGGREGATED), 'age_group_id'].max()
        self.age_group_id_start = int(self.age_group_id_start)
        self.age_group_id_end = int(self.age_group_id_end)

    def get_model_folder(self):
        """
        Gets the folder that the the model is written to, and stores
        it as an attribute in self
        """
        self.model_folder = ["FILEPATH"]

    def format_draws(self, data):
        """
        Drops all columns except for index columns, envelope and pop columns,
        and data columns

        :param data: dataframe
            pandas dataframe to edit
        :return: dataframe
            pandas dataframe with only index, envelope, pop, and data columns
            retained
        """
        keep_columns = self.index_columns + [self.envelope_column, self.pop_column] + self.data_columns
        return data[keep_columns]

    def get_location_hierarchy(self):
        """
        Reads and returns location hierarchy information from SQL

        :return: dataframe
            pandas dataframe with location hierarchy information from database
        """
        sql_query = """SELECT
                           location_id,
                           level,
                           parent_id,
                           is_estimate
                       FROM
                           shared.location_hierarchy_history lhh
                       JOIN
                           shared.location_set_version_active lsv USING (location_set_version_id)
                       WHERE
                           lhh.location_set_id = {location_set_id} AND
                           lsv.gbd_round_id = {gbd_round_id};
                       """.format(location_set_id=self.location_set_id, gbd_round_id=self.gbd_round_id)
        location_hierarchy_history = run_query(sql_query, server=self.server)
        location_hierarchy_history.drop_duplicates(inplace=True)
        return location_hierarchy_history

    def get_estimated_locations(self):
        """
        Gets the most detailed locations that are estimates from the location
        hierarchy history
        :return: list of ints
            list of location_id's labelled as is_estimate
        """
        location_hierarchy_history = self.location_hierarchy.copy(deep=True)
        location_hierarchy_history = location_hierarchy_history.ix[\
            location_hierarchy_history['is_estimate']==1]
        return location_hierarchy_history['location_id'].\
            drop_duplicates().tolist()

    def check_missing_locations(self):
        """
        Prints any missing locations, i.e. locations that are in estimated
        but not in draw_locations

        """
        draw_locations = self.data_draws['location_id'].\
            drop_duplicates().tolist()
        estimated_locations = self.get_estimated_locations()
        if len(set(estimated_locations) - set(draw_locations)) > 0:
            print "The following locations as missing from the draws {}".\
                format(', '.join([str(x) for x in list(set(estimated_locations)\
                - set(draw_locations))]))
        else:
            print "No missing locations!"

    def aggregate_locations(self):
        """
        Aggregate data up the location hierarchy and assign it to the data_draws
        attribute
        """
        # prep/clean up ModelData object
        if self.location_hierarchy is None:
            self.location_hierarchy = self.get_location_hierarchy()
        keep_columns = self.index_columns + \
            [self.envelope_column, self.pop_column] + self.data_columns
        self.data_draws = self.data_draws[keep_columns]
        self.check_missing_locations()

        data = self.data_draws.copy(deep=True)
        data = data.ix[data['location_id'].isin(self.get_estimated_locations())]

        # merge data with location hierarchy, get most granular level
        data = pd.merge(data,
                        self.get_location_hierarchy(),
                        on='location_id',
                        how='left')
        max_level = data['level'].max()

        data = self.format_draws(data)
        # loop through to aggregate data to less and less granular levels, i.e.
        # up the hierarchy
        for level in xrange(max_level, 0, -1):
            data = pd.merge(data,
                            self.location_hierarchy[['location_id',
                                                        'level',
                                                        'parent_id']
                                                        ],
                            on='location_id',
                            how='left')
            temp = data.ix[data['level']==level].copy(deep=True)
            temp['location_id'] = temp['parent_id']
            temp = self.format_draws(temp)
            temp = temp.groupby(self.index_columns).sum().reset_index()
            data = pd.concat([self.format_draws(data), temp]).reset_index(drop=True)
        self.data_draws = data

    def save_draws(self):
        """
        Saves the draws information to an hdf
        """
        sex_dict = {1: 'male', 2: 'female'}
        draws_filepath = "FILEPATH"
        if not os.path.exists("FILEPATH"):
            os.makedirs("FILEPATH")
        self.data_draws.to_hdf(draws_filepath,
                               'data',
                               mode='w',
                               format='table',
                               data_columns=['location_id',
                                             'year_id',
                                             'sex_id',
                                             'age_group_id',
                                             'cause_id'])

    def generate_all_ages(self):
        """
        Adds the draws in all specific age groups to get the counts for all ages
        combined, then stores it in data_draws attribute
        """
        self.data_draws = self.data_draws.ix[self.data_draws['age_group_id']!=22]
        data = self.format_draws(self.data_draws)
        data = data.ix[data['age_group_id'].map(lambda x: x in AGES_DISAGGREGATED)]
        # sum by indices (age, sex, location) to get the sum over all age groups
        data['age_group_id'] = 22  # all ages
        data = data.groupby(self.index_columns).sum().reset_index()
        self.data_draws = pd.concat([self.data_draws, data])

    def generate_age_standardized(self):
        """
        Standardizes data_draws using age group weights from database
        """
        # get age weights
        sql_query = """
                    SELECT
                        age_group_id,
                        age_group_weight_value
                    FROM
                        shared.age_group_weight agw
                    WHERE
                        gbd_round_id = {gbd_round_id};
                    """.format(gbd_round_id=self.gbd_round_id)
        age_standard_data = run_query(sql_query, server=self.server)
        # prep draws for merge
        self.data_draws = self.data_draws.ix[self.data_draws['age_group_id']!=27]
        data = self.format_draws(self.data_draws)
        data = data.ix[data['age_group_id'] in AGES_DISAGGREGATED]
        # merge on age-weights
        data = pd.merge(data,
                        age_standard_data,
                        on='age_group_id')
        # make adjusted rate
        for c in self.data_columns:
            data[c] = data[c] * data['age_group_weight_value'] / data[self.pop_column]
        # collapsing to generate ASR
        data['age_group_id'] = 27
        data = data.groupby(self.index_columns).sum().reset_index()
        # merge with original data
        self.data_draws = pd.concat([self.data_draws, data])

    def generate_summaries(self):
        """
        Summarizes model data and stores summaries in data_summaries attribute
        """
        # Copy draws
        self.data_summaries = self.data_draws.copy(deep=True)
        for c in self.data_columns:
            self.data_summaries.ix[self.data_summaries['age_group_id']!=27, c] = \
                self.data_summaries.ix[self.data_summaries['age_group_id']!=27, c]\
                / self.data_summaries.ix[self.data_summaries['age_group_id']!=27, self.envelope_column]
        self.data_summaries = self.data_summaries[self.index_columns + self.data_columns]
        self.data_summaries['mean_cf'] = self.data_summaries[self.data_columns].mean(axis=1)
        self.data_summaries['lower_cf'] = self.data_summaries[self.data_columns].quantile(0.025, axis=1)
        self.data_summaries['upper_cf'] = self.data_summaries[self.data_columns].quantile(0.975, axis=1)
        # Generate other columns
        self.data_summaries['model_version_id'] = self.model_version_id
        self.data_summaries['date_inserted'] = datetime.datetime.now()
        self.data_summaries['inserted_by'] = self.user
        self.data_summaries['last_updated'] = datetime.datetime.now()
        self.data_summaries['last_updated_by'] = self.user
        self.data_summaries['last_updated_action'] = 'INSERT'
        self.data_summaries = self.data_summaries[self.index_columns + ['model_version_id',
                                                               'mean_cf',
                                                               'lower_cf',
                                                               'upper_cf',
                                                               'date_inserted',
                                                               'inserted_by',
                                                               'last_updated',
                                                               'last_updated_by',
                                                               'last_updated_action']]

    def save_summaries(self):
        """
        Saves data_summaries to a csv file
        """
        summary_filepath = self.model_folder + "/summaries.csv"
        self.data_summaries.to_csv(summary_filepath, index=False)

    def upload_summaries(self):
        """
        Writes data_summaries to the database
        """
        data = self.data_summaries[['model_version_id', 'year_id', 'location_id', 'sex_id',
                     'age_group_id', 'mean_cf', 'lower_cf', 'upper_cf',
                     'date_inserted', 'inserted_by', 'last_updated',
                     'last_updated_by', 'last_updated_action']
                    ].reset_index(drop=True)
        DB = "DATABASE"
        engine = sql.create_engine(DB)
        data.to_sql("model", engine, if_exists="append",
                                        index=False, chunksize=15000)

    def update_status(self):
        """
        Changes the status code in the model_version table of the cod DB
        """
        sql_statement = """
            UPDATE cod.model_version
            SET status = {status_code}
            WHERE model_version_id = {model_version_id}
            """.format(status_code=1, model_version_id=self.model_version_id)
        execute_statement(sql_statement, server=self.server)


if __name__ == "__main__":
    log_dir = 'FILEPATH'
