import matplotlib
matplotlib.use('Agg')

import logging
import pandas as pd

from codem.metadata.model_task import ModelTask
import codem.reference.emailer as email
from codem.joblaunch.args import get_step_args
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging
import codem.db_write.table_diagnostics as t_diags
import codem.data.queryStrings as QS

logger = logging.getLogger(__name__)


class Email(ModelTask):
    def __init__(self, **kwargs):
        """
        Create diagnostic plots and tables for modelers to vet.
        Attach all information + predictive validity metrics to the modeler email
        and send to the relevant modelers.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS['Email'])

        self.submodel_summary_df = pd.read_csv(self.model_paths.diagnostics_dir('submodel_table.csv'))
        self.covariate_summary_df = pd.read_csv(self.model_paths.diagnostics_dir('email_covariate_summary.csv'))
        self.model_type_df = pd.read_csv(self.model_paths.diagnostics_dir('model_type_df.csv'))
        self.covariate_table = pd.read_csv(self.model_paths.diagnostics_dir('covariate_table.csv'))

        self.email_body = None
        self.texts = None

    def create_email_body(self):
        logger.info("Getting diagnostics that will be saved as csv.")
        self.email_body = t_diags.validation_table(
            submodel_summary_df=self.submodel_summary_df,
            model_pv=self.pickled_inputs['model_pv']
        )

    def create_email_text(self):
        logger.info("Creating email text.")
        texts = QS.frame_work.format(
            user=self.model_metadata.model_parameters['inserted_by'],
            description=self.model_metadata.model_parameters['description'],
            sex=self.model_metadata.model_parameters['sex'],
            start_age=self.model_metadata.model_parameters['age_start'],
            end_age=self.model_metadata.model_parameters['age_end'],
            codx_link="ADDRESS",
            best_psi=self.pickled_inputs['model_pv']['best_psi'],
            validation_table=self.email_body,
            number_of_submodels=self.submodel_summary_df.shape[0],
            model_type_table=self.model_type_df.to_html(index=False),
            number_of_covariates=self.covariate_summary_df.shape[0],
            covariate_table=self.covariate_table.to_html(index=False),
            path_to_outputs=self.model_paths.BASE_DIR
        )
        texts = texts.replace("\n", "")
        self.texts = texts

    def send_email(self):
        """
        Send an email, and create a png of global estimates, but only if the
        user is not codem. Don't send an email to codem@uw.edu ever (it is a real person).
        """
        logger.info("Sending email to the modeler.")
        s = email.Server("ADDRESS")
        s.connect()
        e = email.Emailer(s)

        for attachment in ['global_estimates.png',
                           'covariate_draws.png',
                           'covariate_submodels.png',
                           'covariate_distributions_standard_beta.png',
                           'covariate_distributions_beta.png',
                           'submodel_table.csv',
                           'covariate_long_table.csv']:
            e.add_attachment(self.model_paths.diagnostics_dir(attachment))

        recipients = set(self.model_metadata.model_parameters['modeler'] +
                         [self.model_metadata.model_parameters['inserted_by']])
        recipients = [x for x in recipients if x not in ["codem"]]
        recipients = [x for x in recipients if x is not None]

        for recipient in recipients:
            e.add_recipient('ADDRESS' % recipient)

        self.alerts.alert(f"Email being sent to {', '.join(recipients)}")

        e.set_subject(
            'CODEm model complete for {acause}, sex {sex}, age group ids {age_start}-{age_end}, '
            '{mtype}, model version {mvid}'.format(
                acause=self.model_metadata.model_parameters["acause"],
                mvid=self.model_version_id,
                mtype=self.model_metadata.model_parameters["model_version_type"],
                sex=self.model_metadata.model_parameters["sex"],
                age_start=self.model_metadata.model_parameters["age_start"],
                age_end=self.model_metadata.model_parameters["age_end"]
            )
        )
        e.set_body(self.texts)
        e.send_email()
        s.disconnect()


def main():
    args = get_step_args()
    setup_logging(model_version_id=args.model_version_id,
                  step_id=STEP_IDS['Email'])

    logger.info("Initiating modeler email.")
    t = Email(
        model_version_id=args.model_version_id,
        db_connection=args.db_connection,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores
    )
    t.alerts.alert("Creating the email to send to modelers")
    t.create_email_body()
    t.create_email_text()
    t.send_email()
    t.alerts.alert("Check your email! :)")
