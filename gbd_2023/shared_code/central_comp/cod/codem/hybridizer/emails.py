import logging
import smtplib
from typing import List

from gbd.release import get_gbd_round_id_from_release

from hybridizer.database import acause_from_id
from hybridizer.reference import db_connect

logger = logging.getLogger(__name__)

DONT_EMAIL_USERS = ["USERNAME", "USERNAME"]


def get_modeler(acause: str, release_id: int, conn_def: str) -> List[str]:
    """
    Get the modeler for an acause and gbd round.

    Arguments:
        acause: acause to determine modeler
        release_id: GBD release ID
        conn_def:  connection definition
    Returns:
        Modeler of acause for GBD release in a list
    """
    call = """SELECT m.username
        FROM cod.modeler m
            INNER JOIN shared.cause c
            ON m.cause_id = c.cause_id
        WHERE c.acause = :acause AND m.gbd_round_id = :gbd_round_id
        """
    modeler = db_connect.execute_select(
        call,
        conn_def=conn_def,
        parameters={
            "acause": acause,
            "gbd_round_id": get_gbd_round_id_from_release(release_id),
        },
    )
    if modeler.empty:
        modeler = []
    else:
        modeler = modeler.iloc[0, 0].split(", ")
    return modeler


def send_email(recipients: List[str], subject: str, msg_body: str) -> None:
    """
    Sends an email to user-specified recipients with a given subject and message body.

    Arguments:
        recipients: users to send the results to
        subject: the subject of the email
        msg_body: the content of the email
    """
    smtp_server = "SERVER"
    smtp_port = 25
    sender_name = "CODEm Hybridizer"
    sender = "USERNAME"

    headers = [
        "From: " + sender_name + "<" + sender + ">",
        "Subject: " + subject,
        "To: " + ", ".join(recipients),
        "Content-Type: text/html",
    ]
    headers = "\r\n".join(headers)

    msg_body = headers + "\r\n\r\n" + msg_body

    session = smtplib.SMTP(host=smtp_server, port=smtp_port)
    session.sendmail(from_addr=sender, to_addrs=recipients, msg=msg_body)
    session.quit()


def send_success_email(
    model_version_id: int,
    global_model_version_id: int,
    datarich_model_version_id: int,
    user: str,
    release_id: int,
    conn_def: str,
) -> None:
    """Send success email to modeler."""
    logger.info("Sending success email to {}".format(user))
    acause = acause_from_id(model_version_id, conn_def)
    modeler = get_modeler(acause, release_id, conn_def)
    message_for_body = """
        <p>Hi {user},</p>
        <p>The {acause} CODEm hybrid of Global {global_model} and \
           Data Rich {datarich_model} is complete! Please vet model \
           version ID {model_version_id}, and if everything looks \
           good mark it as best.</p>
        <p>CODEm Hybridizer</p>
        """.format(
        user=", ".join(modeler),
        global_model=global_model_version_id,
        datarich_model=datarich_model_version_id,
        acause=acause,
        model_version_id=model_version_id,
    )

    users = set(user.split() + modeler)
    users = [x for x in users if x not in DONT_EMAIL_USERS]
    users = [x for x in users if x is not None]
    addresses = ["{}@uw.edu".format(u) for u in users]
    send_email(
        addresses,
        "CODEm {acause} hybrid of {global_model} and "
        "{datarich_model} complete".format(
            global_model=global_model_version_id,
            datarich_model=datarich_model_version_id,
            acause=acause,
        ),
        message_for_body,
    )


def send_failed_email(
    model_version_id: int,
    global_model_version_id: int,
    datarich_model_version_id: int,
    user: str,
    log_dir: str,
    release_id: int,
    conn_def: str,
) -> None:
    """Send failure email to modeler."""
    logger.info("Sending failed email to {}".format(user))
    acause = acause_from_id(model_version_id, conn_def)
    modeler = get_modeler(acause, release_id, conn_def)

    message_for_body = """
        <p>Hi {user},</p>
        <p>The {acause} CODEm hybrid of Global {global_model} and \
           Data Rich {datarich_model} failed! Please check the log \
           file {log_filepath} and delete model version ID \
           {model_version_id} from CoDViz.</p>
        <p>CODEm Hybridizer</p>
        """.format(
        user=", ".join(modeler),
        log_filepath=log_dir + "/hybridizer.log",
        global_model=global_model_version_id,
        datarich_model=datarich_model_version_id,
        acause=acause,
        model_version_id=model_version_id,
    )
    users = set(user.split() + modeler)
    users = [x for x in users if x not in DONT_EMAIL_USERS]
    users = [x for x in users if x is not None]
    addresses = ["{}@uw.edu".format(u) for u in users]
    send_email(
        addresses,
        "CODEm {acause} hybrid of {global_model} and {datarich_model} failed".format(
            global_model=global_model_version_id,
            datarich_model=datarich_model_version_id,
            acause=acause,
        ),
        message_for_body,
    )


def send_mismatch_email(
    global_model_version_id: int,
    datarich_model_version_id: int,
    attribute_name: str,
    user: str,
    conn_def: str,
) -> None:
    """Send email that models don't have matching parameters to the modeler."""
    logger.info("Sending mismatch email.")
    acause = acause_from_id(global_model_version_id, conn_def)

    message_for_body = """
        <p>Hi {user},</p>
        <p>The {acause} CODEm hybrid of Global {global_model} and \
           Data Rich {datarich_model} failed to start because they \
           had different {attribute} parameters. Please resubmit this \
           hybrid by selecting two models with the same {attribute}.</p>
        <p>CODEm Hybridizer</p>
        """.format(
        user=user,
        global_model=global_model_version_id,
        datarich_model=datarich_model_version_id,
        acause=acause,
        attribute=attribute_name,
    )

    if user and user not in DONT_EMAIL_USERS:
        send_email(
            ["USERNAME".format(user=user)],
            "CODEm {acause} hybrid of {global_model} and "
            "{datarich_model} mismatched {attribute}".format(
                global_model=global_model_version_id,
                datarich_model=datarich_model_version_id,
                acause=acause,
                attribute=attribute_name,
            ),
            message_for_body,
        )
