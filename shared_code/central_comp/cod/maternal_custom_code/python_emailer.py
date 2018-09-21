import pandas as pd
import smtplib
import os
import locale
locale.setlocale(locale.LC_NUMERIC, "")
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import Encoders

# see https://docs.python.org/2/library/smtplib.html
class server:
    def __init__(self, smtp):
        self.connection = smtplib.SMTP(smtp)
        
    def set_user(self, user):
        self.user = user
        
    def set_password(self, password):
        self.password = password
        
    def connect(self):
        self.connection.starttls()
        self.connection.login(self.user, self.password)
    
    def disconnect(self):
        self.connection.quit()

class emailer:
    
    def __init__(self, server):
        self.server = server
        self.recipients = []
        self.body = ''
        self.attachments = []

    def add_recipient(self, address, recipient_type='to'):
        self.recipients.append({'address': address, 'recipient_type': recipient_type})
        
    def set_subject(self, subject):
        self.subject = subject
    
    def set_body(self, body, append=False):
        if append:
            self.body += body
        else:
            self.body = body
        
    def add_attachment(self, filepath):
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(open(filepath, 'rb').read())
        Encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment; filename="{filename}"'.format(filename=os.path.basename(filepath)))
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
        return self.server.connection.sendmail(from_addr=self.server.user, to_addrs=[i['address'] for i in self.recipients], msg=self.message.as_string())