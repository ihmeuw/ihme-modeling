import logging
import numexpr as ne

from codem.metadata.model_task import ModelTask

logger = logging.getLogger(__name__)


class PV(ModelTask):
    def __init__(self, step_id, **kwargs):
        """
        General class for calculating predictive validity on
        either linear models or space-time models (they both
        go through the same process).

        This is sub-classed for both of the linear- and spacetime-
        specific tasks.

        First make predictions in a uniform space, then calculate
        RMSE and trend.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=step_id)
        ne.set_num_threads(1)

    def calculate_submodel_pv(self, input_object_name, output_object_prefix):
        self.pickled_outputs[output_object_prefix + '_pv'] = self.pickled_inputs[input_object_name]
        del self.pickled_inputs[input_object_name]

        logger.info("Creating uniform predictions.")
        self.pickled_outputs[output_object_prefix + '_pv'].uniform_predictions(
            self.model_metadata.data_frame,
            self.pickled_inputs['response_list']
        )

        logger.info("RMSE out-of-sample.")
        self.pickled_outputs[output_object_prefix + '_pv'].rmse_out(
            self.model_metadata.data_frame,
            self.model_metadata.ko_data
        )

        logger.info("Trend out-of-sample.")
        self.pickled_outputs[output_object_prefix + '_pv'].trend_out(
            self.model_metadata.data_frame,
            self.model_metadata.ko_data,
            self.model_metadata.model_parameters['rmse_window']
        )

        self.pickled_outputs[output_object_prefix + '_rmse_all'] = \
            self.pickled_outputs[output_object_prefix + '_pv'].RMSE_all
        self.pickled_outputs[output_object_prefix + '_trend_all'] = \
            self.pickled_outputs[output_object_prefix + '_pv'].trend_all

