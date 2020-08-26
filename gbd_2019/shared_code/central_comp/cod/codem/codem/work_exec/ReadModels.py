import os
import logging
import pandas as pd

from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS

from codem.ensemble.all_models import All_Models
from codem.stgpr.space_time_smoothing import import_json

logger = logging.getLogger(__name__)


class ReadModels(ModelTask):
    def __init__(self, step_id, **kwargs):
        """
        Read the result of the json files from linear model builds in R
        for either linear or spacetime
        models and make basic linear model predictions based off of that.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=step_id)
        self.json_dict = None

    def read_models(self, input_json_path, output_object_name):
        logger.info("Reading the result of linear model builds.")
        self.json_dict = import_json(input_json_path)
        self.pickled_outputs[output_object_name] = All_Models(
            df=pd.concat([self.model_metadata.data_frame,
                          self.model_metadata.covariates], axis=1),
            knockouts=self.model_metadata.ko_data,
            linear_floor_rate=self.model_metadata.model_parameters['linear_floor_rate'],
            json_dict=self.json_dict,
            db_connection=self.db_connection)




