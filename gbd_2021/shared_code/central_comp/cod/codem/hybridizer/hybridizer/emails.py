import logging
import smtplib

from db_tools import ezfuncs

from hybridizer.database import acause_from_id

logger = logging.getLogger(__name__)


def get_modeler(acause, gbd_round_id, conn_def):
    """
    Get the modeler for an acause and gbd round
    :param acause:
    :param gbd_round_id:
    :param conn_def:
    :return:
    """
    call = """ SELECT m.username
        FROM cod.modeler m
            INNER JOIN shared.cause c
            ON m.cause_id = c.cause_id
        WHERE c.acause = :acause AND m.gbd_round_id = :gbd_round_id
        """.format(acause=acause, gbd=gbd_round_id)
    modeler = ezfuncs.query(
        call, conn_def=conn_def, parameters={
            'acause': acause, 'gbd_round_id': gbd_round_id
        }
    )
    return modeler.iloc[0, 0].split(', ')


def send_email(recipients, subject, msg_body):
    """
    Sends an email to user-specified recipients with a given subject and
    message body

    :param recipients: list of strings
        users to send the results to
    :param subject: str
        the subject of the email
    :param msg_body: str
        the content of the email
    """
    smtp_server = 'USERNAME'
    smtp_port = 587
    sender_name = "CODEm Hybridizer"
    sender = 'USERNAME'
    password = 'PASSWORD'

    headers = ["From: " + sender_name + "<" + sender + ">",
               "Subject: " + subject,
               "To: " + ', '.join(recipients),
               "MIME-Version: 1.0",
               "Content-Type: text/html"]
    headers = "\r\n".join(headers)

    msg_body = headers + "\r\n\r\n" + msg_body

    session = smtplib.SMTP(smtp_server, smtp_port)

    session.ehlo()
    session.starttls()
    session.login(sender, password)

    session.sendmail(sender, recipients, msg_body)
    session.quit()


def send_success_email(model_version_id,
                       global_model_version_id,
                       datarich_model_version_id, user,
                       gbd_round_id, conn_def):
    """
    Send success email to modeler.
    """
    logger.info("Sending success email to {}".format(user))
    acause = acause_from_id(model_version_id, conn_def)
    modeler = get_modeler(acause, gbd_round_id, conn_def)
    message_for_body = '''
                <p>Hi {user},</p>
                <p>The hybrid of Global {global_model} and Data Rich {datarich_model} for {acause} \
                        has completed and saved as model {model_version_id}.</p>
                <p>Please vet your model, and if everything looks good \
                    mark it as best.</p>
                <p></p>
                '''.format(user=", ".join(modeler),
                           global_model=global_model_version_id,
                           datarich_model=datarich_model_version_id,
                           acause=acause,
                           model_version_id=model_version_id)

    users = set(user.split() + modeler)
    users = [x for x in users if x not in ["codem"]]
    users = [x for x in users if x is not None]
    addresses = ['{}@uw.edu'.format(u) for u in users]
    send_email(addresses,
               "CODEm {acause} hybrid of {global_model} and "
               "{datarich_model} complete".format(
                   global_model=global_model_version_id,
                   datarich_model=datarich_model_version_id,
                   acause=acause
               ),
               message_for_body)


def send_failed_email(model_version_id,
                      global_model_version_id,
                      datarich_model_version_id, user, log_dir,
                      gbd_round_id, conn_def):
    """
    Send failure email to modeler.
    """
    logger.info("Sending failed email to {}".format(user))
    acause = acause_from_id(model_version_id, conn_def)
    modeler = get_modeler(acause, gbd_round_id, conn_def)

    message_for_body = '''
                        <p>Hi {user},</p>
                        <p>The hybrid of Global {global_model} and Data Rich {datarich_model} for {acause} \
                            has failed.</p>
                        <p>Please check the following log file: {log_filepath} and delete your model from CoDViz.</p>
                        <p></p>
                        '''.format(user=", ".join(modeler),
                                   log_filepath=log_dir+'hybridizer.log',
                                   global_model=global_model_version_id,
                                   datarich_model=datarich_model_version_id,
                                   acause=acause,
                                   model_version_id=model_version_id)
    users = set(user.split() + modeler)
    users = [x for x in users if x not in ["codem"]]
    users = [x for x in users if x is not None]
    addresses = ['{}@uw.edu'.format(u) for u in users]
    send_email(addresses,
               "CODEm {acause} hybrid of {global_model} and {datarich_model} failed".
               format(global_model=global_model_version_id,
                      datarich_model=datarich_model_version_id,
                      acause=acause),
               message_for_body)


def send_mismatch_email(global_model_version_id,
                        datarich_model_version_id,
                        attribute_name, user, conn_def):
    """
    Send a modeler an email that their models don't have matching parameters.
    """
    logger.info("Sending mismatch email.")
    acause = acause_from_id(global_model_version_id, conn_def)

    message_for_body = '''
        <p>Hi {user},</p>
        <p>The hybrid of Global {global_model} and Data Rich {datarich_model} for {acause} \
            has failed to start because they had different {attribute} parameters. Please resubmit this
            hybrid by selecting two models with the same {attribute}.</p>
        <p>Regards,</p>
        '''.format(user=user,
                   global_model=global_model_version_id,
                   datarich_model=datarich_model_version_id,
                   acause=acause,
                   attribute=attribute_name)

    if user != "codem":
        send_email(['USERNAME'.format(user=user)],
                   "CODEm {acause} hybrid of {global_model} and "
                   "{datarich_model} mismatched {attribute}".format(
                       global_model=global_model_version_id,
                       datarich_model=datarich_model_version_id,
                       acause=acause, attribute=attribute_name
                   ),
                   message_for_body)
