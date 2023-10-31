import matplotlib

matplotlib.use('Agg')

import logging

import numpy as np
import pandas as pd
from tqdm import tqdm

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from db_queries import get_envelope
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from gbd import constants as gbd
from hierarchies import dbtrees

import codem.db_write.plot_diagnostics as p_diags
import codem.reference.db_connect as db_connect
from codem.db_write import submodels
from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

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

        self.response_list = self.pickled_inputs['response_list']
        self.draw_id = self.pickled_inputs['draw_id']

        self.draws = None
        self.agg_draws = None

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
        Aggregate the draws up the full location hierarchy and produce estimates
        for All ages.
        """
        logger.info("Starting aggregating draws.")
        self.alerts.alert("Aggregating draws")

        index_cols = list(self.draws.filter(like='_id').columns)
        data_cols = list(set(self.draws.columns) - set(index_cols))

        io_mock = {}
        source = DrawSource({"draw_dict": io_mock, "name": "tmp"}, mem_read_func)
        sink = DrawSink({"draw_dict": io_mock, "name": "tmp"}, mem_write_func)
        sink.push(self.draws[index_cols + data_cols])

        # location
        logger.info("Aggregating locations.")
        operator = Sum(
                index_cols=[col for col in index_cols if col != "location_id"],
                value_cols=data_cols)
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in index_cols if col != "location_id"],
            aggregate_col="location_id",
            operator=operator)
        loc_tree = dbtrees.loctree(
            location_set_version_id=self.model_metadata.model_parameters["location_set_version_id"],
            gbd_round_id=self.model_metadata.model_parameters["gbd_round_id"])
        aggregator.run(loc_tree)
        self.agg_draws  = source.content()

        # age (data isn't square on age, so not using agetree)
        logger.info("Aggregating ages.")
        index_cols.remove('age_group_id')
        all_ages = self.agg_draws.groupby(index_cols, as_index=False).sum()
        all_ages["age_group_id"] = self.model_metadata.model_parameters['all_age_group_id']
        self.agg_draws = self.agg_draws.append(all_ages).reset_index(drop=True)



    def get_full_envelope(self):
        """
        For models with age restrictions, the all-ages group should use envelope
        for all ages. Replace the age-restricted envelope for the full envelope for
        each location-year, country-year, region-year, super-region-year, and global-year.
        """
        logger.info("Pulling in the full All age envelope in order to have a correct"
                    "envelope for age-restricted causes.")
        # get the envelope, then merge in with the aggregated dfs
        env_df = get_envelope(location_id=self.agg_draws.location_id.unique().tolist(),
                              year_id=self.agg_draws.year_id.unique().tolist(),
                              age_group_id=self.agg_draws.age_group_id.unique().tolist(),
                              sex_id=self.model_metadata.model_parameters['sex_id'],
                              gbd_round_id=self.model_metadata.model_parameters['gbd_round_id'],
                              decomp_step=self.model_metadata.model_parameters['decomp_step'],
                              run_id=self.model_metadata.model_parameters['env_run_id'])
        env_df = env_df[['location_id', 'year_id', 'age_group_id', 'mean']]
        env_df.rename(columns={'mean': 'envelope'}, inplace=True)
        self.agg_draws.drop('envelope', inplace=True, axis=1)
        self.agg_draws = self.agg_draws.merge(env_df, on=['location_id', 'year_id', 'age_group_id'])

    def write_submodel_means(self):
        """
        Write the submodel means of the codem run for all submodels which we
        have more than 100 draws for.

        May increase the partitions if run out of space for model_version in cod.submodel.
        """
        valid_models = [m for m in set(self.draw_id) if self.draw_id.count(m) >= 100]
        logger.info("Writing model means to the database.")
        index_cols = ["year_id", "location_id", "sex_id", "age_group_id", "envelope"]
        for model in tqdm(valid_models):
            # check for partitions. When there are enough partitions for this model version ID,
            # countPartition will return 1, and then we can write the df to sql
            count = db_connect.countPartition('cod', 'submodel', self.model_version_id, self.db_connection)
            if count == 0:
                db_connect.increase_partitions(self.db_connection, 'cod', 'submodel')

            draw_cols = ["draw_%d" % i for i in np.where(np.array(self.draw_id) == model)[0]]
            submodels.write_model_output(
                df_true=self.agg_draws[index_cols + draw_cols],
                model_version_id=self.model_version_id,
                sex_id=self.model_metadata.model_parameters['sex_id'],
                db_connection=self.db_connection,
                submodel_version_id=model
            )

    def write_model_mean(self):
        logger.info("Writing ensemble model means to the database.")
        submodels.write_model_output(
            df_true=self.agg_draws,
            model_version_id=self.model_version_id,
            sex_id=self.model_metadata.model_parameters['sex_id'],
            db_connection=self.db_connection
        )
        self.alerts.alert("Summaries have been written to the cod.model table for use in CodViz")

    def create_global_table(self):
        logger.info("Creating the global plot.")
        if self.model_metadata.model_parameters['decomp_step'] == "usa_re":
            p_diags.create_global_table(
                global_df=self.agg_draws.query(
                    f"location_id == 102 & age_group_id == {gbd.age.ALL_AGES}"
                ),
                filepath=self.model_paths.diagnostics_file("global_estimates.png")
            )
        else:
            p_diags.create_global_table(
                global_df=self.agg_draws.query(
                    f"location_id == 1 & age_group_id == {gbd.age.ALL_AGES}"
                ),
                filepath=self.model_paths.diagnostics_file("global_estimates.png")
            )


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['WriteResults'],
                  db_connection=args.db_connection)

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
