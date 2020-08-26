from codem.joblaunch.CODEmTask import CODEmTask
from hybridizer.joblaunch.HybridTask import HybridTask
from codem.joblaunch.run_utils import new_models


class CODEmTriple:
    def __init__(self, global_model_version_id,
                 developed_model_version_id,
                 db_connection, description,
                 gbd_round_id=6,
                 decomp_step_id=1,
                 num_cores=20,
                 codem_params=None,
                 hybridizer_params=None):
        """
        Class that creates a list of tasks for a
        global, data rich, and hybrid model.

        :param global_model_version_id: (int) global model
        :param developed_model_version_id: (int) developed model
        :param db_connection: (str) database connection
        :param description: (str) description of the model versions
        :param gbd_round_id: (int) gbd round ID
        :param decomp_step_id: (int) decomp step ID
        :param codem_params: (dict) dictionary with codem cluster parameters for runs
        :param hybridizer_params: (dict) dictionary with hybridizer parameters for runs
        """
        self.old_global_model_version_id = global_model_version_id
        self.old_developed_model_version_id = developed_model_version_id
        self.db_connection = db_connection
        self.description = description
        self.gbd_round_id = gbd_round_id
        self.decomp_step_id = decomp_step_id
        self.num_cores = num_cores
        self.user = 'codem'
        self.codem_params = codem_params
        self.hybridizer_params = hybridizer_params

        self.developed_model_version_id = None
        self.global_model_version_id = None
        self.developed_task = None
        self.global_task = None
        self.hybrid_task = None

        if self.db_connection == 'ADDRESS':
            self.conn_def = 'codem'
        elif self.db_connection == 'ADDRESS':
            self.conn_def = 'codem-test'
        else:
            raise RuntimeError("Must pass a valid database connection.")

    def task_list(self):
        """
        Creates a task list of a global, developed, and hybrid model task.
        Also uploads a new model version for all of the models.
        """
        self.developed_model_version_id = new_models(self.old_developed_model_version_id,
                                                     db_connection=self.db_connection,
                                                     gbd_round_id=self.gbd_round_id,
                                                     decomp_step_id=self.decomp_step_id,
                                                     desc=self.description + ' previous version {}'.
                                                     format(self.old_developed_model_version_id))[0]
        self.developed_task = CODEmTask(model_version_id=self.developed_model_version_id,
                                        db_connection=self.db_connection,
                                        gbd_round_id=self.gbd_round_id,
                                        parameter_dict=self.codem_params,
                                        cores=self.num_cores)
        self.global_model_version_id = new_models(self.old_global_model_version_id,
                                                  db_connection=self.db_connection,
                                                  gbd_round_id=self.gbd_round_id,
                                                  decomp_step_id=self.decomp_step_id,
                                                  desc=self.description + ' previous version {}'.
                                                  format(self.old_global_model_version_id))[0]
        self.global_task = CODEmTask(model_version_id=self.global_model_version_id,
                                     db_connection=self.db_connection,
                                     gbd_round_id=self.gbd_round_id,
                                     parameter_dict=self.codem_params,
                                     cores=self.num_cores)
        self.hybrid_task = HybridTask(user=self.user,
                                      developed_model_version_id=self.developed_model_version_id,
                                      global_model_version_id=self.global_model_version_id,
                                      conn_def=self.conn_def,
                                      upstream_tasks=[self.developed_task,
                                                      self.global_task],
                                      parameter_dict=self.hybridizer_params)
        return [self.developed_task, self.global_task, self.hybrid_task]


