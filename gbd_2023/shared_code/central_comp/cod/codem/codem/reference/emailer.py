import locale
import os
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

locale.setlocale(locale.LC_NUMERIC, "")


class Server:
    """Creates an object of class server that acts as the connection to the email host."""

    def __init__(self, smtp_server: str, smtp_port: int) -> None:
        self.connection = smtplib.SMTP(host=smtp_server, port=smtp_port)

    def disconnect(self) -> None:
        self.connection.quit()


class Emailer:
    """
    Creates an object of class emailer from a valid server class which can
    be used to define recipients, add attachments, add text, define the subject
    line and finally send the email.
    """

    def __init__(self, server: Server, sender: str, sender_name: str = "") -> None:
        self.server = server
        self.sender = sender
        self.sender_name = sender_name
        self.recipients = []
        self.body = ""
        self.attachments = []
        self.subject = None
        self.message = None

    def add_recipient(self, address: str) -> None:
        self.recipients.append({"address": address, "recipient_type": "to"})

    def set_subject(self, subject: str) -> None:
        self.subject = subject

    def set_body(self, body: str, append: bool = False) -> None:
        if append:
            self.body += body
        else:
            self.body = body

    def add_attachment(self, file_path: str) -> None:
        attachment = MIMEBase("application", "octet-stream")
        attachment.set_payload(open(file_path, "rb").read())
        encoders.encode_base64(attachment)
        attachment.add_header(
            "Content-Disposition",
            'attachment; filename="{filename}"'.format(filename=os.path.basename(file_path)),
        )
        self.attachments.append(attachment)

    def build_email(self) -> None:
        message = MIMEMultipart()
        message["From"] = self.sender_name + "<" + self.sender + ">"
        message["Subject"] = self.subject
        message["To"] = ",".join(
            [i["address"] for i in self.recipients if i["recipient_type"] == "to"]
        )
        message["Content-Type"] = "text/html"
        message.attach(MIMEText(self.body, "html"))
        for a in self.attachments:
            message.attach(a)
        self.message = message

    def send_email(self) -> None:
        self.build_email()
        return self.server.connection.sendmail(
            from_addr=self.sender,
            to_addrs=[i["address"] for i in self.recipients],
            msg=self.message.as_string(),
        )
