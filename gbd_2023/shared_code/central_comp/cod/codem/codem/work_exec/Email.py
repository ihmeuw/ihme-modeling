import logging
from typing import Any

import matplotlib
import pandas as pd

from gbd import constants as gbd_constants
from gbd.gbd_round import gbd_round_from_gbd_round_id

import codem.data.queryStrings as QS
import codem.db_write.table_diagnostics as t_diags
import codem.reference.emailer as email
from codem.joblaunch.args import get_step_args
from codem.metadata.model_task import ModelTask
from codem.metadata.step_metadata import STEP_IDS
from codem.reference.log_config import setup_logging

matplotlib.use("Agg")
logger = logging.getLogger(__name__)


class Email(ModelTask):
    def __init__(self, **kwargs: Any):
        """
        Create diagnostic plots and tables for modelers to vet.
        Attach all information + predictive validity metrics to the modeler email
        and send to the relevant modelers.

        :param kwargs:
        """
        super().__init__(**kwargs, step_id=STEP_IDS["Email"])

        self.submodel_summary_df = pd.read_csv(
            self.model_paths.diagnostics_file("submodel_table.csv")
        )
        self.covariate_summary_df = pd.read_csv(
            self.model_paths.diagnostics_file("email_covariate_summary.csv")
        )
        self.model_type_df = pd.read_csv(
            self.model_paths.diagnostics_file("model_type_df.csv")
        )
        self.covariate_table = pd.read_csv(
            self.model_paths.diagnostics_file("covariate_table.csv")
        )

        self.email_body = None
        self.texts = None

    def create_email_body(self) -> None:
        logger.info("Getting diagnostics that will be saved as csv.")
        self.email_body = t_diags.validation_table(
            submodel_summary_df=self.submodel_summary_df,
            model_pv=self.pickled_inputs["model_pv"],
        )

    def create_email_text(self) -> None:
        logger.info("Creating email text.")
        codx_link = "ADDRESS"
        if self.model_metadata.model_parameters["release_id"] == gbd_constants.release.USRE:
            codx_link = codx_link + "-usa"
        gbd_round = gbd_round_from_gbd_round_id(
            self.model_metadata.model_parameters["gbd_round_id"]
        )
        codx_link = f"{codx_link}-{gbd_round}"
        texts = QS.frame_work.format(
            user=", ".join(self.model_metadata.model_parameters["modeler"]),
            cause_name=self.model_metadata.model_parameters["cause_name"],
            description=self.model_metadata.model_parameters["description"],
            sex=self.model_metadata.model_parameters["sex"],
            start_age=self.model_metadata.model_parameters["age_start_name"],
            end_age=self.model_metadata.model_parameters["age_end_name"],
            codx_link=codx_link,
            best_psi=round(self.pickled_inputs["model_pv"]["best_psi"], 3),
            validation_table=self.email_body,
            number_of_submodels=self.submodel_summary_df.shape[0],
            model_type_table=self.model_type_df.fillna("--").to_html(index=False),
            number_of_covariates=self.covariate_table.shape[0],
            covariate_table=self.covariate_table.fillna("--").to_html(index=False),
            path_to_outputs=self.model_paths.BASE_DIR,
        )
        texts = texts.replace("\n", "")
        self.texts = texts

    def send_email(self) -> None:
        """
        Send an email, and create a png of global estimates, but only if the
        user is not codem. Don't send an email to USERNAME ever (it is a real person).
        """
        logger.info("Sending email to the modeler.")
        s = email.Server(smtp_server="ADDRESS", smtp_port=25)
        e = email.Emailer(
            server=s, sender="USERNAME", sender_name="CODEm"
        )

        for attachment in [
            "global_estimates.png",
            "covariate_draws.png",
            "covariate_submodels.png",
            "covariate_distributions_standard_beta.png",
            "covariate_distributions_beta.png",
            "submodel_table.csv",
            "covariate_long_table.csv",
        ]:
            e.add_attachment(self.model_paths.diagnostics_file(attachment))

        recipients = set(
            self.model_metadata.model_parameters["modeler"]
            + [self.model_metadata.model_parameters["inserted_by"]]
        )
        recipients = [x for x in recipients if x not in ["codem", "svc_codem_modeling"]]
        recipients = [x for x in recipients if x is not None]

        for recipient in recipients:
            e.add_recipient("%s@uw.edu" % recipient)

        self.alerts.alert(f"Email being sent to {', '.join(recipients)}")

        e.set_subject(
            "CODEm {acause} model complete, {sex}, {age_start}-{age_end}, "
            "{mtype} (model version ID {mvid})".format(
                acause=self.model_metadata.model_parameters["acause"],
                mvid=self.model_version_id,
                mtype=self.model_metadata.model_parameters["model_version_type"],
                sex=self.model_metadata.model_parameters["sex"].capitalize(),
                age_start=self.model_metadata.model_parameters["age_start_name"],
                age_end=self.model_metadata.model_parameters["age_end_name"],
            )
        )
        e.set_body(self.texts)
        e.send_email()
        s.disconnect()


def main() -> None:
    args = get_step_args()
    setup_logging(
        model_version_id=args.model_version_id,
        step_id=STEP_IDS["Email"],
        conn_def=args.conn_def,
    )

    logger.info("Initiating modeler email.")
    t = Email(
        model_version_id=args.model_version_id,
        conn_def=args.conn_def,
        debug_mode=args.debug_mode,
        old_covariates_mvid=args.old_covariates_mvid,
        cores=args.cores,
    )
    t.alerts.alert("Creating the email to send to modelers")
    t.create_email_body()
    t.create_email_text()
    t.send_email()
    t.alerts.alert("Check your email!")
