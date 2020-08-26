import pandas as pd
import logging
import os
import subprocess

from hybridizer.database import acause_from_id, gbd_round_from_id
from hybridizer.model_data import ModelData
import hybridizer.utilities as utils
import hybridizer.draws as draws
from hybridizer.database import get_location_hierarchy_version

logger = logging.getLogger(__name__)


class Hybridizer:
    def __init__(self, model_version_id, global_model, developed_model, conn_def):
        """
        Runs an instance of the CODEm hybridizer!
        """
        logger.info("Starting the hybridizer for model version {}!".format(model_version_id))
        self.model_version_id = model_version_id
        self.global_model = global_model
        self.developed_model = developed_model
        self.conn_def = conn_def
        self.acause = acause_from_id(self.model_version_id, self.conn_def)
        self.gbd_round_id = gbd_round_from_id(self.model_version_id, self.conn_def)

        # Get attributes and model properties
        self.cause_data = utils.get_cause_dict(conn_def=self.conn_def)
        self.developed_model_properties = utils.get_model_properties(self.developed_model,
                                                                     conn_def=self.conn_def)
        self.global_model_properties = utils.get_model_properties(self.global_model,
                                                                  conn_def=self.conn_def)
        self.hybrid_model_properties = utils.get_model_properties(self.model_version_id,
                                                                  conn_def=self.conn_def)

        self.base_dir = None
        self.location_set_version_id = None
        self.global_location_list = None
        self.developed_location_list = None
        self.location_metadata_version_id = None
        self.data_all = None

    def file_setup(self):
        # Set up folder structure for hybrid results
        self.base_dir = "ADDRESS".format(self.acause, self.model_version_id)
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        subprocess.call("chmod 777 {base_dir} -R".format(base_dir=self.base_dir), shell=True)

    def get_locations(self):
        logger.info("Getting locations.")
        self.location_set_version_id, self.location_metadata_version_id = \
            get_location_hierarchy_version(gbd_round_id=self.gbd_round_id,
                                           conn_def=self.conn_def)
        self.global_location_list, self.developed_location_list = \
            utils.get_locations_for_models(self.developed_model, self.location_set_version_id,
                                           conn_def=self.conn_def)

    def concat_data(self):
        logger.info("Grabbing all of the data.")
        # Go through data-rich and global models to concatenate all data
        self.data_all = []
        # global
        self.data_all.append(draws.read_model_draws(self.hybrid_model_properties['cause_id'],
                                                    self.global_model,
                                                    self.global_location_list,
                                                    gbd_round_id=self.hybrid_model_properties['gbd_round_id'],
                                                    decomp_step_id=self.hybrid_model_properties['decomp_step_id']))
        self.data_all.append(draws.read_model_draws(self.hybrid_model_properties['cause_id'],
                                                    self.developed_model,
                                                    self.developed_location_list,
                                                    gbd_round_id=self.hybrid_model_properties['gbd_round_id'],
                                                    decomp_step_id=self.hybrid_model_properties['decomp_step_id']))

        self.data_all = pd.concat(self.data_all).reset_index(drop=True)
        for col in ['location_id', 'year_id', 'age_group_id']:
            self.data_all[col] = self.data_all[col].map(lambda x: int(x))

    def run_hybridizer(self):
        logger.info("Running the hybridizer for model version {}".format(self.model_version_id))
        self.file_setup()
        self.get_locations()
        self.concat_data()

        # Create ModelData instance (uses save_results.py)
        m = ModelData(model_version_id=self.model_version_id,
                      data_draws=self.data_all,
                      index_columns=['location_id', 'year_id', 'sex_id', 'age_group_id', 'cause_id'],
                      envelope_column='envelope',
                      pop_column='pop',
                      data_columns=['draw_{}'.format(x) for x in range(1000)],
                      conn_def=self.conn_def,
                      location_set_id=35,
                      gbd_round_id=self.gbd_round_id)

        # Run prep steps for upload
        logger.info("Aggregating locations")
        m.aggregate_locations()

        logger.info("Save draws")
        m.save_draws()

        logger.info("Generate all ages")
        m.generate_all_ages()

        logger.info("Generate summaries")
        m.generate_summaries()

        logger.info("Save summaries")
        m.save_summaries()

        logger.info("Upload summaries")
        m.upload_summaries()

        logger.info("Finished with running ModelData.")

