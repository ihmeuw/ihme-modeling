import logging
import smtplib

from hybridizer.database import acause_from_id
import hybridizer.reference.db_connect as db_connect

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
        WHERE c.acause = '{acause}' AND m.gbd_round_id = {gbd}
        """.format(acause=acause, gbd=gbd_round_id)
    modeler = db_connect.query(call, conn_def)
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
    smtp_server = ''
    smtp_port = "PASSWORD"
    sender_name = "CODEm Hybridizer"
    sender = ''
    password = ''

    headers = ["From: " + sender_name + "<" + sender + ">",
               "Subject: " + subject,
               "To: " + ', '.join(recipients),
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
                       developed_model_version_id, user,
                       gbd_round_id, conn_def):
    """
    Send success email to modeler.
    """
    logger.info("Sending success email to {}".format(user))
    acause = acause_from_id(model_version_id, conn_def)
    modeler = get_modeler(acause, gbd_round_id, conn_def)
    message_for_body = '''
                <p>Hello {user},</p>
                <p>The hybrid of {global_model} and {developed_model} for {acause} \
                        has completed and saved as model {model_version_id}.</p>
                <p>Please check your model and then, if everything looks good, \
                    mark it as best.</p>
                <p></p>
                <p>Regards,</p>
                <p>Your Friendly Neighborhood Hybridizer</p>
                '''.format(user=user, global_model=global_model_version_id,
                           developed_model=developed_model_version_id,
                           acause=acause,
                           model_version_id=model_version_id)

    users = set(user.split() + modeler)
    addresses = ['USERNAME'.format(u) for u in users]
    send_email(addresses,
               "Hybrid of {global_model} and {developed_model} ({acause}) has completed".
               format(global_model=global_model_version_id,
                      developed_model=developed_model_version_id,
                      acause=acause),
               message_for_body)


def send_failed_email(model_version_id,
                      global_model_version_id,
                      developed_model_version_id, user, log_dir,
                      gbd_round_id, conn_def):
    """
    Send failure email to modeler.
    """
    logger.info("Sending failed email to {}".format(user))
    acause = acause_from_id(model_version_id, conn_def)
    modeler = get_modeler(acause, gbd_round_id, conn_def)

    message_for_body = '''
                        <p>Hello {user},</p>
                        <p>The hybrid of {global_model} and {developed_model} for {acause} \
                            has failed.</p>
                        <p>Please check the following log file:</p>
                        <p>{log_filepath}</p>
                        <p>Please also delete your model from CodViz since it has failed.</p>
                        <p></p>
                        <p>Regards,</p>
                        <p>Your Friendly Neighborhood Hybridizer</p>
                        '''.format(user=user, log_filepath=log_dir+'/hybridizer.txt',
                                   global_model=global_model_version_id,
                                   developed_model=developed_model_version_id,
                                   acause=acause,
                                   model_version_id=model_version_id)
    users = set(user.split() + modeler)
    addresses = ['USERNAME'.format(u) for u in users]
    send_email(addresses,
               "Hybrid of {global_model} and {developed_model}\
               ({acause}) has failed".format(global_model=global_model_version_id,
                                             developed_model=developed_model_version_id,
                                             acause=acause),
               message_for_body)


def send_mismatch_email(global_model_version_id,
                        developed_model_version_id,
                        attribute_name, user, conn_def):
    """
    Send a modeler an email that their models don't have matching parameters.
    """
    logger.info("Sending age restriction violation email.")
    acause = acause_from_id(global_model_version_id, conn_def)

    message_for_body = '''
        <p>Hello {user},</p>
        <p>The hybrid of {global_model} and {developed_model} for {acause} \
            has failed to start because they had different {attribute} parameters. Please resubmit this
            hybrid by selecting two models with the {attribute}.</p>
        <p>Regards,</p>
        <p>Your Friendly Neighborhood Hybridizer</p>
        '''.format(user=user,
                   global_model=global_model_version_id,
                   developed_model=developed_model_version_id,
                   acause=acause,
                   attribute=attribute_name)

    send_email(['USERNAME'.format(user=user)],
               "Hybrid of {global_model} and {developed_model} ({acause}) \
                    failed due to mismatched {attribute} parameter.".
               format(global_model=global_model_version_id,
                      developed_model=developed_model_version_id,
                      acause=acause, attribute=attribute_name),
               message_for_body)
