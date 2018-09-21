import smtplib
import os
import locale
import sys
import glob
import numpy as np
from codem.db_connect import query
locale.setlocale(locale.LC_NUMERIC, "")
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import Encoders


class Server:
    '''
    (str) -> server

    Creates an object of class server that acts as the connection to the email
    host. The string passed to the class
    '''
    def __init__(self, smtp, user, password):
        self.connection = smtplib.SMTP(smtp)
        self.user = user
        self.password = password

    def connect(self):
        self.connection.starttls()
        self.connection.login(self.user, self.password)

    def disconnect(self):
        self.connection.quit()


class Emailer:
    '''
    (server) -> emailer

    Creates an object of class emailer from a valid server class which can
    be used to define recipients, add attachments, add text, define the subject
    line and finally send the email
    '''
    def __init__(self, server):
        self.server = server
        self.recipients = []
        self.body = ''
        self.attachments = []
        self.subject = None
        self.message = None

    def add_recipient(self, address, recipient_type='to'):
        self.recipients.append({'address': address, 'recipient_type': recipient_type})

    def set_subject(self, subject):
        self.subject = subject

    def set_body(self, body, append=False):
        if append:
            self.body += body
        else:
            self.body = body

    def add_attachment(self, file_path):
        if file_path is None: return
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(open(file_path, 'rb').read())
        Encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition',
                              'attachment; filename="{filename}"'.format(filename=os.path.basename(file_path)))
        self.attachments.append(attachment)

    def build_email(self):
        message = MIMEMultipart()
        message['Subject'] = self.subject
        message['From'] = self.server.user
        message['To'] = ','.join([i['address'] for i in self.recipients if i['recipient_type'] == 'to'])
        message['CC'] = ','.join([i['address'] for i in self.recipients if i['recipient_type'] == 'cc'])
        message['BCC'] = ','.join([i['address'] for i in self.recipients if i['recipient_type'] == 'bcc'])
        message.attach(MIMEText(self.body, 'html'))
        for a in self.attachments:
            message.attach(a)
        self.message = message

    def send_email(self):
        self.build_email()
        return self.server.connection.sendmail(from_addr=self.server.user,
                                               to_addrs=[i['address'] for i in self.recipients],
                                               msg=self.message.as_string())


def get_acause(model_version_id, connection):
    call = '''
    SELECT cause_id FROM cod.model_version
        WHERE model_version_id = {model_version_id}
    '''.format(model_version_id=model_version_id)
    cause_id = query(call, connection)["cause_id"][0]
    call = '''
    SELECT acause from shared.cause WHERE cause_id = {cause_id}
    '''.format(cause_id=cause_id)
    acause = query(call, connection)["acause"][0]
    return acause


def update_status(status, model_version_id, connection):
    '''
    :param status: int
        the target status, e.g. 0 for pending, 1 for success, 7 for failure
    :param model_version_id: int
        which model to update the status of
    :param connection: string
        the database (server) to connect to

    :return: None
    '''
    call = '''
    UPDATE cod.model_version
    SET status = {status}
    WHERE model_version_id = {mvid}
    '''.format(status=status, mvid=model_version_id)
    query(call, connection)


def get_modeler(acause, connection):
    '''
    string -> string

    Look up the modeler for the cause being modeled in the cod.modeler table
    '''
    call = """ SELECT m.username
        FROM cod.modeler m
            INNER JOIN shared.cause c
            ON m.cause_id = c.cause_id
        WHERE c.acause = '{acause}'
        """.format(acause=acause)
    modeler = query(call, connection)
    return modeler.ix[0, 0].split(', ')



frame_work = '''
<p>Hey {user},</p>
<p>Your model {model_version_id} for cause {acause} appears to have run into some complications.</p>
<p>An error log can be found attached to this email or at this path:</p>
<p>{model_code}</p>
<p>Please direct any questions that you have to USERNAME or USERNAME.</p>
<p></p>
<p>CODEm2.0</p>
'''

if __name__ == "__main__":
    user, model_version_id, db_name = sys.argv[1:]
    acause = get_acause(model_version_id, db_name)
    model_code = "FILEPATH"
    final_pickle = model_code + "FILEPATH"


    # if the model failed, send an email and update the db status to 7
    if not os.path.exists(final_pickle):
        # prep and send the email
        os.chdir(model_code)
        try:
            error_file = glob.glob("cod_*.e*")[0]
        except:
            try:
                os.chdir('FILEPATH')
                error_file = "FILEPATH"
            except:
                error_file = None
        s = Server("SERVER", 'EMAIL', 'PASSWORD')
        s.connect()
        e = Emailer(s)
        recipients = get_modeler(acause, db_name) + ['USERNAME']
        for recipient in recipients:
            e.add_recipient("{user}@uw.edu".format(user=recipient))
        e.add_attachment(error_file)
        e.set_subject('CODEm run failure')
        e.set_body(frame_work.format(user=user,
                                     model_code=model_code,
                                     model_version_id=model_version_id,
                                     acause=acause))
        e.send_email()
        s.disconnect()

        # update the db status
        update_status(7, model_version_id, db_name)

    else:
        pass
