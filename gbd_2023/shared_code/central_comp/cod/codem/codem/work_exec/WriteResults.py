import logging

import matplotlib
import numpy as np
import pandas as pd
from tqdm import tqdm

from aggregator.aggregators import AggSynchronous
from aggregator.operators import Sum
from db_queries import get_envelope
from draw_sources.draw_sources import DrawSink, DrawSource
from draw_sources.io import mem_read_func, mem_write_func
from gbd import constants as gbd_constants
from hierarchies import dbtrees

import codem.data.queryStrings as QS
import codem.db_write.plot_diagnostics as p_diags
import codem.reference.db_connect as db_connect
from codem.db_write import submodels
from codem.joblaunch.args import get_step_args
from codem.joblaunch.run_utils import change_model_status
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

matplotlib.use("Agg")
logger = logging.getLogger(__name__)


def get_pruned_loctree(
    location_set_version_id: int, locations_exclude: str, release_id: int
) -> dbtrees.loctree:
    """Get pruned location hierarchy in case of excluded locations given CoD data.

    Prune the location hierarchy given a location set version ID and a space delimited
    string of location IDs to exclude. Exluded locations (if any) come from CoD RT by
    refresh and may be at location level 3 or below. If a parent location is specified,
    all children are assumed to also be excluded.
    """
    loc_tree = dbtrees.loctree(
        location_set_version_id=location_set_version_id, release_id=release_id
    )
    # capture true most-detailed locations
    true_leaves = loc_tree.leaves()
    # remove any locations explicitly excluded from the model along with their children
    for location_id in np.array(locations_exclude.split()).astype(int):
        # If location_id is not in tree, assume node was a child location that has already
        # been pruned as a child of a pruned aggregate
        if location_id in loc_tree.node_ids:
            loc_tree.prune(location_id)
    # then also remove any aggregate locations where all children are now excluded,
    # comparing current leaves/most-detailed locations to those that were leaves orignally
    false_leaves = set(loc_tree.leaves()) - set(true_leaves)
    while false_leaves:
        for node in list(false_leaves):
            loc_tree.prune(node.id)
        false_leaves = set(loc_tree.leaves()) - set(true_leaves)
    return loc_tree


class WriteResults(ModelTask):
    def __init__(self, **kwargs):
        """
        Reads in the draws for linear and spacetime models
        and aggregates over locations and ages. Writes the aggregates
        and summaries to the database, and saves the draws to the model directory.
        Also writes predictive validity metrics for the final model to the database.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS["WriteResults"])

        self.response_list = self.pickled_inputs["response_list"]
        self.draw_id = self.pickled_inputs["draw_id"]

        self.draws = None
        self.agg_draws = None

    def database_wipe(self):
        """
        Delete results from the model and submodel tables if this task has already been run
        for this model version. If nothing is in the database yet, then 0 rows will be
        affected. The sproc `delete_from_model_tables_by_request` deletes from both tables.
        """
        # check if rows exist in cod.model or cod.submodel
        df = pd.concat(
            db_connect.execute_select(
                QS.count_rows_query.format(table=table),
                conn_def=self.conn_def,
                parameters={"model_version_id": self.model_version_id},
            )
            for table in ["model", "submodel"]
        )
        if any(df["count"] > 0):
            db_connect.call_stored_procedure(
                name="cod.delete_from_model_tables_by_request",
                args=[int(self.model_version_id)],
                conn_def=self.conn_def,
            )
            # flip model version status from soft shell delete (6) back to running (0)
            change_model_status(
                model_version_id=self.model_version_id, status=0, conn_def=self.conn_def
            )

    def read_draws(self):
        """
        Read in the draws that are saved in the file system.
        :return:
        """
        draw_file = self.model_paths.DRAW_FILE.format(
            sex=self.model_metadata.model_parameters["sex"]
        )
        logger.info(f"Reading in draws from {draw_file} file.")
        self.draws = pd.read_hdf(path_or_buf=draw_file, key="data")

    def aggregate_draws(self):
        """
        Aggregate the draws up the full location hierarchy and produce estimates
        for All ages.
        """
        logger.info("Starting aggregating draws.")
        self.alerts.alert("Aggregating draws")

        index_cols = list(self.draws.filter(like="_id").columns)
        data_cols = list(set(self.draws.columns) - set(index_cols))

        io_mock = {}
        source = DrawSource({"draw_dict": io_mock, "name": "tmp"}, mem_read_func)
        sink = DrawSink({"draw_dict": io_mock, "name": "tmp"}, mem_write_func)
        sink.push(self.draws[index_cols + data_cols])

        # location
        logger.info("Aggregating locations.")
        operator = Sum(
            index_cols=[col for col in index_cols if col != "location_id"],
            value_cols=data_cols,
        )
        aggregator = AggSynchronous(
            draw_source=source,
            draw_sink=sink,
            index_cols=[col for col in index_cols if col != "location_id"],
            aggregate_col="location_id",
            operator=operator,
        )
        loc_tree = get_pruned_loctree(
            location_set_version_id=self.model_metadata.model_parameters[
                "location_set_version_id"
            ],
            locations_exclude=self.model_metadata.model_parameters["locations_exclude"],
            release_id=self.model_metadata.model_parameters["release_id"],
        )
        aggregator.run(loc_tree)
        self.agg_draws = source.content()

        # age (data isn't square on age, so not using agetree)
        logger.info("Aggregating ages.")
        index_cols.remove("age_group_id")
        all_ages = self.agg_draws.groupby(index_cols, as_index=False).sum()
        all_ages["age_group_id"] = self.model_metadata.model_parameters["all_age_group_id"]
        self.agg_draws = pd.concat([self.agg_draws, all_ages], axis=0).reset_index(drop=True)

    def get_full_envelope(self):
        """
        For models with age restrictions, the all-ages group should use envelope
        for all ages. Replace the age-restricted envelope for the full envelope for
        each location-year, country-year, region-year, super-region-year, and global-year.
        """
        logger.info(
            "Pulling in the full All age envelope in order to have a correct "
            "envelope for age-restricted causes."
        )
        # get the envelope, then merge in with the aggregated dfs
        env_df = get_envelope(
            location_id=self.agg_draws.location_id.unique().tolist(),
            year_id=self.agg_draws.year_id.unique().tolist(),
            age_group_id=self.agg_draws.age_group_id.unique().tolist(),
            sex_id=self.model_metadata.model_parameters["sex_id"],
            with_hiv=self.model_metadata.model_parameters["with_hiv"],
            release_id=self.model_metadata.model_parameters["release_id"],
            run_id=self.model_metadata.model_parameters["env_run_id"],
        )
        env_df = env_df[["location_id", "year_id", "age_group_id", "mean"]]
        env_df.rename(columns={"mean": "envelope"}, inplace=True)
        self.agg_draws.drop("envelope", inplace=True, axis=1)
        self.agg_draws = self.agg_draws.merge(
            env_df, on=["location_id", "year_id", "age_group_id"]
        )

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
            count = db_connect.countPartition(
                "cod", "submodel", self.model_version_id, self.conn_def
            )
            if count == 0:
                db_connect.increase_partitions(self.conn_def, "cod", "submodel")

            draw_cols = ["draw_%d" % i for i in np.where(np.array(self.draw_id) == model)[0]]
            submodels.write_model_output(
                df_true=self.agg_draws[index_cols + draw_cols],
                model_version_id=self.model_version_id,
                sex_id=self.model_metadata.model_parameters["sex_id"],
                conn_def=self.conn_def,
                submodel_version_id=model,
                model_output_path=self.model_paths.SUMMARY_FILE,
            )

    def write_model_mean(self):
        logger.info("Writing ensemble model means to the database.")
        submodels.write_model_output(
            df_true=self.agg_draws,
            model_version_id=self.model_version_id,
            sex_id=self.model_metadata.model_parameters["sex_id"],
            conn_def=self.conn_def,
            model_output_path=self.model_paths.SUMMARY_FILE,
        )
        self.alerts.alert(
            "Summaries have been written to the cod.model table for use in CodViz"
        )

    def create_global_table(self):
        logger.info("Creating the global plot.")
        if self.model_metadata.model_parameters["release_id"] == gbd_constants.release.USRE:
            p_diags.create_global_table(
                global_df=self.agg_draws.query(
                    f"location_id == 102 & age_group_id == {gbd_constants.age.ALL_AGES}"
                ),
                filepath=self.model_paths.diagnostics_file("global_estimates.png"),
            )
        else:
            p_diags.create_global_table(
                global_df=self.agg_draws.query(
                    f"location_id == 1 & age_group_id == {gbd_constants.age.ALL_AGES}"
                ),
                filepath=self.model_paths.diagnostics_file("global_estimates.png"),
            )


def main():
    args = get_step_args()
    setup_logging(
        model_version_id=args.model_version_id,
        step_id=STEP_IDS["WriteResults"],
        conn_def=args.conn_def,
    )

    logger.info("Initiating writing results.")
    t = WriteResults(
        model_version_id=args.model_version_id,
        conn_def=args.conn_def,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
    )
    t.database_wipe()
    t.read_draws()
    t.aggregate_draws()
    t.get_full_envelope()
    t.write_submodel_means()
    t.write_model_mean()
    t.create_global_table()
