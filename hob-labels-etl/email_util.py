import os, sys
import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Email:

    def __init__(self, subject, reciever, attachment, body,filename):
        self.subject = subject
        self.reciever = reciever
        self.attachment = attachment
        self.body = body
        self.filename = filename
        self.server, self.port, self.sender, self.password = self.connect_attributes()

    def connect_attributes(self):
        server = os.environ['SMTP_SERVER']
        port = os.environ['SMTP_PORT']
        sender = os.environ['SENDER']
        password = os.environ['PASSWORD']
        return server, port, sender, password

    def send_email(self):
        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = self.sender
        message["To"] = self.sender
        message["Subject"] = self.subject

        # Add body to email
        message.attach(MIMEText(self.body, "plain"))

        filename = self.filename  # In same directory as script
        part = MIMEBase("application", "octet-stream")
        part.set_payload(self.attachment)

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()

        with smtplib.SMTP(self.server, self.port) as server:
            server.starttls()
            server.login(self.sender, self.password)
            server.sendmail(self.sender, [self.sender] + self.reciever, text)

