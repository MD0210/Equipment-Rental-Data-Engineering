# equipment_rental/utils/email_utils.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from equipment_rental.logger.logger import get_logger

logger = get_logger()

def send_sla_email(subject, body, to_emails, from_email, from_password, smtp_server="smtp.gmail.com", smtp_port=465):
    """
    Send SLA alert email. Non-blocking: logs any exceptions but doesn't raise.
    """
    try:
        msg = MIMEMultipart()
        msg["From"] = from_email
        msg["To"] = ", ".join(to_emails)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(from_email, from_password)
            server.sendmail(from_email, to_emails, msg.as_string())

        logger.info(f"SLA breach email sent to {to_emails}")
    except Exception as e:
        logger.error(f"Failed to send SLA email: {str(e)}")