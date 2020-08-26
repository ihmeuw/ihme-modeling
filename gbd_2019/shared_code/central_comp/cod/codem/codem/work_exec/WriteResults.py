import matplotlib
matplotlib.use('Agg')

import numpy as np
import logging
import pandas as pd
from tqdm import tqdm

from db_queries import get_envelope

import codem.reference.db_connect as db_connect
from codem.db_write import submodels
from codem.metadata.step_metadata import STEP_IDS
from codem.joblaunch.args import get_step_args
import codem.db_write.plot_diagnostics as p_diags
from codem.reference.log_config import setup_logging
from codem.metadata.model_task import ModelTask

logger = logging.getLogger(__name__)


class WriteResults(ModelTask):
    def __init__(self, **kwargs):
        """
        Reads in the draws for linear and spacetime models
        and aggregates over locations and ages. Writes the aggregates
        and summaries to the database, and saves the draws to the model directory.
        Also writes predictive validity metrics for the final model to the database.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['WriteResults'])
        self.agg_dfs = {}
        self.age_aggregate_dfs = ['age_location', 'age_country', 'age_region',
                                  'age_super_region', 'global']

        self.response_list = self.pickled_inputs['response_list']
        self.draw_id = self.pickled_inputs['draw_id']

        self.draws = None
        self.country_df = None
        self.region_df = None
        self.super_region_df = None
        self.age_df = None

    def database_wipe(self):
        """
        Delete from the model results database if this task has already been run for this
        model version. If nothing is in the database yet, then 0 rows will be affected.
        """
        db_connect.call_stored_procedure(
            name='cod.delete_from_model_tables_by_request',
            args=[int(self.model_version_id)],
            connection=self.db_connection)

    def read_draws(self):
        """
        Read in the draws that are saved in the file system.
        :return:
        """
        draw_file = self.model_paths.DRAW_FILE.format(
                sex=self.model_metadata.model_parameters['sex'])
        logger.info(f"Reading in draws from {draw_file} file.")
        self.draws = pd.read_hdf(
            path_or_buf=draw_file,
            key='data'
        )

    def aggregate_draws(self):
        """
        Aggregate the draws up to the country, region, and super region levels.
        We also need to throw in the age aggregates in globally.
        """
        logger.info("Starting aggregating draws.")
        self.alerts.alert("Aggregating draws")
        # store age aggregate data frames in a dictionary for easier access later
        self.draws.drop(['measure_id', 'sex_id', 'cause_id'], inplace=True, axis=1)
        self.agg_dfs['age_location'] = \
            self.draws.groupby(['location_id', 'year_id'], as_index=False).sum()
        self.agg_dfs['age_location']["age_group_id"] = self.model_metadata.model_parameters['all_age_group_id']

        logger.info("Aggregating to the country level.")
        self.country_df = \
            self.draws.groupby(['country_id', 'age_group_id', 'year_id'], as_index=False).sum()
        self.country_df.drop('location_id', axis=1, inplace=True)
        self.country_df.rename(columns={'country_id': 'location_id'}, inplace=True)
        countries = np.setdiff1d(self.country_df.location_id.unique(),
                                 self.draws.location_id.unique())
        self.country_df = self.country_df[self.country_df["location_id"].isin(countries)]
        self.country_df.reset_index(drop=True, inplace=True)

        logger.info("Aggregating to the age-country level.")
        self.agg_dfs['age_country'] = \
            self.country_df.groupby(['location_id', 'year_id'], as_index=False).sum()
        self.agg_dfs['age_country']["age_group_id"] = self.model_metadata.model_parameters['all_age_group_id']

        logger.info("Aggregating to the region level.")
        self.region_df = \
            self.draws.groupby(['region_id', 'age_group_id', 'year_id'], as_index=False).sum()
        self.region_df.drop(['location_id', 'country_id'], axis=1, inplace=True)
        self.region_df.rename(columns={'region_id': 'location_id'}, inplace=True)

        logger.info("Aggregating to the age-region level.")
        self.agg_dfs['age_region'] = \
            self.region_df.groupby(['location_id', 'year_id'], as_index=False).sum()
        self.agg_dfs['age_region']["age_group_id"] = self.model_metadata.model_parameters['all_age_group_id']

        logger.info("Aggregating to the super region level.")
        self.super_region_df = \
            self.draws.groupby(['super_region_id', 'age_group_id', 'year_id'], as_index=False).sum()
        self.super_region_df.drop(['location_id', 'country_id', 'region_id'], axis=1, inplace=True)
        self.super_region_df.rename(columns={'super_region_id': 'location_id'}, inplace=True)

        logger.info("Aggregating to the age-super region level.")
        self.agg_dfs['age_super_region'] = \
            self.super_region_df.groupby(['location_id', 'year_id'], as_index=False).sum()
        self.agg_dfs['age_super_region']["age_group_id"] = self.model_metadata.model_parameters['all_age_group_id']

        logger.info("Aggregating to the global-age level.")
        self.age_df = \
            self.draws.groupby(['age_group_id', 'year_id'], as_index=False).sum()
        self.age_df.drop(['super_region_id', 'country_id', 'region_id'], axis=1, inplace=True)
        self.age_df.location_id = self.model_metadata.model_parameters['global_location_id']

        logger.info("Aggregating to the global level.")
        self.agg_dfs['global'] = self.draws.groupby(['year_id'], as_index=False).sum()
        self.agg_dfs['global']["location_id"] = self.model_metadata.model_parameters['global_location_id']
        self.agg_dfs['global']["age_group_id"] = self.model_metadata.model_parameters['all_age_group_id']
        self.agg_dfs['global'].drop(["super_region_id", "region_id", "country_id"], axis=1, inplace=True)

    def get_full_envelope(self):
        """
        For models with age restrictions, the all-ages group should use envelope
        for all ages. Replace the age-restricted envelope for the full envelope for
        each location-year, country-year, region-year, super-region-year, and global-year.
        """
        logger.info("Pulling in the full envelope in order to have a correct"
                    "envelope for age-restricted causes.")
        for df_type in self.age_aggregate_dfs:
            self.agg_dfs[df_type].drop('envelope', inplace=True, axis=1)
            locs = self.agg_dfs[df_type].location_id.values.tolist()
            years = self.agg_dfs[df_type].year_id.values.tolist()
            # get the envelope, then merge in with the aggregated dfs
            env_df = get_envelope(age_group_id=self.model_metadata.model_parameters['all_age_group_id'],
                                  gbd_round_id=self.model_metadata.model_parameters['gbd_round_id'],
                                  location_id=locs,
                                  sex_id=self.model_metadata.model_parameters['sex_id'],
                                  year_id=years,
                                  decomp_step=self.model_metadata.model_parameters['decomp_step'],
                                  run_id=self.model_metadata.model_parameters['env_run_id'])
            env_df = env_df[['location_id', 'year_id', 'mean']]
            env_df.rename(columns={'mean': 'envelope'}, inplace=True)
            self.agg_dfs[df_type] = self.agg_dfs[df_type].merge(env_df,
                                                                on=['location_id', 'year_id'])
    
    def write_submodel_means(self):
        """
        Write the submodel means of the codem run for all submodels which we
        have more than 50 draws for.
        
        May increase the partitions if run out of space for model_version in cod.submodel.
        """
        valid_models = [m for m in set(self.draw_id) if self.draw_id.count(m) >= 100]
        logger.info("Writing model means to the database.")
        for model in tqdm(valid_models):
            columns = ["draw_%d" % i for i in np.where(np.array(self.draw_id) == model)[0]]
            for df_true in [self.draws, self.country_df, self.region_df,
                            self.super_region_df, self.age_df, self.agg_dfs['age_location'],
                            self.agg_dfs['age_country'], self.agg_dfs['age_region'],
                            self.agg_dfs['age_super_region'], self.agg_dfs['global']]:
                df = df_true.copy()
                df["sex_id"] = self.model_metadata.model_parameters['sex_id']
                if df.shape[0] == 0:
                    continue
                df_sub = df.loc[:, ["year_id", "location_id", "sex_id", "age_group_id", "envelope"] + columns]
                df_sub.loc[:, columns] = df_sub[columns].values / df_sub["envelope"].values[..., np.newaxis]
                df_sub["mean_cf"] = df_sub[columns].mean(axis=1)
                df_sub["lower_cf"] = df_sub[columns].quantile(q=0.025, axis=1)
                df_sub["upper_cf"] = df_sub[columns].quantile(q=0.975, axis=1)
                df_sub = df_sub[["year_id", "location_id", "sex_id", "age_group_id", "mean_cf", "lower_cf", "upper_cf"]]
                df_sub["model_version_id"] = self.model_version_id
                df_sub["submodel_version_id"] = model
                # check for partitions. When there are enough partitions for this model version ID,
                # countPartition will return 1, and then we can write the df to sql
                count = db_connect.countPartition('cod', 'submodel', self.model_version_id, self.db_connection)
                if count == 0:
                    db_connect.increase_partitions(self.db_connection, 'cod', 'submodel')
                
                db_connect.write_df_to_sql(df_sub, db="cod", table="submodel", connection=self.db_connection)

    def write_model_mean(self):
        logger.info("Writing ensemble model means to the database.")
        for df_true in tqdm([self.draws, self.country_df, self.region_df,
                             self.super_region_df, self.age_df,
                             self.agg_dfs['global'], self.agg_dfs['age_location'],
                             self.agg_dfs['age_country'], self.agg_dfs['age_region'],
                             self.agg_dfs['age_super_region']]):
            if df_true.shape[0] == 0:
                continue
            submodels.write_model_output(
                df_true=df_true,
                model_version_id=self.model_version_id,
                sex_id=self.model_metadata.model_parameters['sex_id'],
                db_connection=self.db_connection
            )
        self.alerts.alert("Summaries have been written to the cod.model table for use in CodViz")

    def create_global_table(self):
        logger.info("Creating the global plot.")
        p_diags.create_global_table(
            global_df=self.agg_dfs['global'],
            filepath=self.model_paths.diagnostics_dir("global_estimates.png")
        )
    

def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['WriteResults'])

    logger.info("Initiating writing results.")
    t = WriteResults(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.database_wipe()
    t.read_draws()
    t.aggregate_draws()
    t.get_full_envelope()
    t.write_submodel_means()
    t.write_model_mean()
    t.create_global_table()
