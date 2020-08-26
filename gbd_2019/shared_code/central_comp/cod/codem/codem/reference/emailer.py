import smtplib
import os
import locale
locale.setlocale(locale.LC_NUMERIC, "")
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


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
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(open(file_path, 'rb').read())
        encoders.encode_base64(attachment)
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