"""
Email Notification Module for ETL Pipeline
==========================================

Sends email notifications about ETL pipeline success/failure status.
"""

import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
)

logger = logging.getLogger(__name__)


class EmailNotifier:
    """Email notification service for ETL pipeline results."""

    def __init__(self):
        """Initialize email configuration from environment variables."""
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.sender_password = os.getenv("SENDER_PASSWORD")  # App password for Gmail
        self.recipient_email = "havando1802@gmail.com" # add more people if needed

    def send_notification(self, success: bool, details: dict = None):
        """
        Send email notification about ETL pipeline result.

        Args:
            success (bool): Whether the ETL pipeline succeeded
            details (dict): Additional details about the pipeline run
        """
        if not self.sender_email or not self.sender_password:
            logger.error(
                "Email credentials not configured. Skipping email notification."
            )
            return False

        try:
            # Create message
            message = MIMEMultipart()
            message["From"] = self.sender_email
            message["To"] = self.recipient_email

            if success:
                subject = "✅ Real Estate ETL Pipeline - Success"
                body = self._create_success_email_body(details)
            else:
                subject = "❌ Real Estate ETL Pipeline - Failed"
                body = self._create_failure_email_body(details)

            message["Subject"] = subject
            message.attach(MIMEText(body, "html"))

            # Send email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(message)

            logger.info(
                f"Email notification sent successfully to {self.recipient_email}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False

    def _create_success_email_body(self, details: dict = None) -> str:
        """Create HTML email body for successful pipeline run."""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html_body = f"""
        <html>
        <body>
        <h2 style="color: #28a745;">🎉 Real Estate ETL Pipeline Completed Successfully!</h2>
        
        <p><strong>Execution Time:</strong> {current_time}</p>
        
        <h3>Pipeline Summary:</h3>
        <ul>
            <li><strong>Status:</strong> <span style="color: #28a745;">SUCCESS</span></li>
            <li><strong>Records Processed:</strong> {details.get('records_processed', 'N/A') if details else 'N/A'}</li>
            <li><strong>Duration:</strong> {details.get('duration', 'N/A') if details else 'N/A'}</li>
            <li><strong>Location:</strong> {details.get('location', 'N/A') if details else 'N/A'}</li>
        </ul>
        
        <h3>Next Steps:</h3>
        <p>Your real estate data has been successfully extracted, transformed, and loaded. 
        You can now access the updated data in your database for analysis.</p>
        
        <hr>
        <p style="font-size: 12px; color: #666;">
        This is an automated message from your Real Estate ETL Pipeline.<br>
        Pipeline Location: /Users/hado/Desktop/Career/Coding/Data Engineer /Project/real_estate_project/
        </p>
        </body>
        </html>
        """
        return html_body

    def _create_failure_email_body(self, details: dict = None) -> str:
        """Create HTML email body for failed pipeline run."""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        html_body = f"""
        <html>
        <body>
        <h2 style="color: #dc3545;">⚠️ Real Estate ETL Pipeline Failed</h2>
        
        <p><strong>Execution Time:</strong> {current_time}</p>
        
        <h3>Pipeline Summary:</h3>
        <ul>
            <li><strong>Status:</strong> <span style="color: #dc3545;">FAILED</span></li>
            <li><strong>Error:</strong> {details.get('error', 'Unknown error') if details else 'Unknown error'}</li>
            <li><strong>Location:</strong> {details.get('location', 'N/A') if details else 'N/A'}</li>
        </ul>
        
        <h3>Troubleshooting Steps:</h3>
        <ol>
            <li>Check the pipeline logs at: <code>etl_pipeline.log</code></li>
            <li>Verify API key is valid and not expired</li>
            <li>Ensure internet connection is stable</li>
            <li>Check if the Zillow API is responding</li>
        </ol>
        
        <p style="color: #dc3545;"><strong>Action Required:</strong> Please investigate and resolve the issue.</p>
        
        <hr>
        <p style="font-size: 12px; color: #666;">
        This is an automated message from your Real Estate ETL Pipeline.<br>
        Pipeline Location: /Users/hado/Desktop/Career/Coding/Data Engineer /Project/real_estate_project/
        </p>
        </body>
        </html>
        """
        return html_body


def send_test_email():
    """Send a test email to verify configuration."""
    notifier = EmailNotifier()
    test_details = {
        "records_processed": 25,
        "duration": "00:02:15",
        "location": "Los Angeles, CA",
    }

    print("Sending test email...")
    success = notifier.send_notification(success=True, details=test_details)

    if success:
        print("✅ Test email sent successfully!")
    else:
        print("❌ Failed to send test email. Check your configuration.")


if __name__ == "__main__":
    send_test_email()
