import logging
import os
import subprocess

import pandas as pd

from db_queries.api.internal import get_active_location_set_version
from gbd.decomp_step import decomp_step_from_decomp_step_id

import hybridizer.draws as draws
import hybridizer.utilities as utils
from hybridizer.model_data import ModelData

logger = logging.getLogger(__name__)


class Hybridizer:
    def __init__(self, model_version_id, global_model, datarich_model, conn_def,
                 base_dir):
        """
        Runs an instance of the CODEm hybridizer!
        """
        logger.info(f"Initializing the hybridizer for model version ID {model_version_id}!")
        self.model_version_id = model_version_id
        self.global_model = global_model
        self.datarich_model = datarich_model
        self.conn_def = conn_def
        self.base_dir = base_dir

        # Get attributes and model properties
        self.datarich_model_properties = utils.get_model_properties(
            self.datarich_model, conn_def=self.conn_def)
        self.global_model_properties = utils.get_model_properties(
            self.global_model, conn_def=self.conn_def)
        self.hybrid_model_properties = utils.get_model_properties(
            self.model_version_id, conn_def=self.conn_def)
        self.cause_id = self.hybrid_model_properties['cause_id']
        self.gbd_round_id = self.hybrid_model_properties['gbd_round_id']
        self.decomp_step_id = self.hybrid_model_properties['decomp_step_id']
        self.validate_model_version_types()

        self.location_set_id = 35
        self.location_set_version_id = None
        self.global_location_list = None
        self.datarich_location_list = None
        self.data_all = None

    def get_locations(self):
        logger.info("Getting locations.")
        self.location_set_version_id = get_active_location_set_version(
            self.location_set_id, gbd_round_id=self.gbd_round_id,
            decomp_step=decomp_step_from_decomp_step_id(self.decomp_step_id)
        )
        self.global_location_list, self.datarich_location_list = (
            utils.get_locations_for_models(
                self.datarich_model,
                self.location_set_version_id,
                conn_def=self.conn_def
            )
        )

    def concat_data(self):
        logger.info("Grabbing all of the data.")
        # Go through data-rich and global models to concatenate all data
        self.data_all = []
        self.data_all.append(draws.read_model_draws(self.cause_id,
                                                    self.global_model,
                                                    self.global_location_list,
                                                    gbd_round_id=self.gbd_round_id,
                                                    decomp_step_id=self.decomp_step_id))
        self.data_all.append(draws.read_model_draws(self.cause_id,
                                                    self.datarich_model,
                                                    self.datarich_location_list,
                                                    gbd_round_id=self.gbd_round_id,
                                                    decomp_step_id=self.decomp_step_id))
        self.data_all = pd.concat(self.data_all).reset_index(drop=True)
        for col in ['location_id', 'year_id', 'age_group_id']:
            self.data_all[col] = self.data_all[col].map(lambda x: int(x))

    def validate_model_version_types(self):
        model_version_type_dict = {
            1: 'global',
            2: 'data_rich'
        }
        global_match = self.global_model_properties['model_version_type_id'] != 1
        datarich_match = self.datarich_model_properties['model_version_type_id'] != 2

        if global_match or datarich_match:
            raise RuntimeError(
                f"A Hybrid model was launched with mismatching global and data_rich"
                f"model versions. Model version {self.global_model_properties['model_version_type_id']} "
                f"is a {model_version_type_dict[self.global_model_properties['model_version_type_id']]}"
                f"model, and model version {self.datarich_model_properties['model_version_type_id']} "
                f"is a {model_version_type_dict[self.datarich_model_properties['model_version_type_id']]} "
                f"model."
            )

    def run_hybridizer(self):
        logger.info(f"Running the hybridizer for model version ID {self.model_version_id}")
        self.get_locations()
        self.concat_data()

        # Create ModelData instance (uses save_results.py)
        m = ModelData(model_version_id=self.model_version_id,
                      model_folder=self.base_dir,
                      data_draws=self.data_all,
                      index_columns=['location_id', 'year_id', 'sex_id', 'age_group_id', 'cause_id'],
                      envelope_column='envelope',
                      pop_column='pop',
                      data_columns=[f"draw_{x}" for x in range(1000)],
                      conn_def=self.conn_def,
                      location_set_id=self.location_set_id,
                      gbd_round_id=self.gbd_round_id,
                      decomp_step_id=self.decomp_step_id,
                      sex_id=self.hybrid_model_properties["sex_id"],
                      user=self.hybrid_model_properties["inserted_by"],
                      envelope_proc_version_id=self.hybrid_model_properties["envelope_proc_version_id"]
        )

        # Run prep steps for upload
        logger.info("Aggregating locations")
        m.aggregate_locations()

        logger.info("Save draws")
        m.save_draws()

        logger.info("Generate all ages")
        m.generate_all_ages()

        logger.info("Pulling envelope for all ages and most detailed age groups")
        m.get_full_envelope()

        logger.info("Generate summaries")
        m.generate_summaries()

        logger.info("Save summaries")
        m.save_summaries()

        logger.info("Upload summaries")
        m.upload_summaries()

        logger.info("Finished with running ModelData.")
