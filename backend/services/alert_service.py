"""
Enterprise ETL Platform - Alerting Service

Sends notifications via Email and Slack when pipeline
events occur (failures, warnings, quality violations).
"""
import smtplib
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

import httpx

from backend.core.config import settings
from backend.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class AlertEvent:
    """Represents an alertable event."""
    pipeline_name: str
    run_id: str
    severity: str           # info | warning | error | critical
    title: str
    message: str
    details: Optional[Dict[str, Any]] = None
    error_traceback: Optional[str] = None


class EmailAlerter:
    """Sends HTML email alerts via SMTP."""

    def send(self, event: AlertEvent, recipients: Optional[List[str]] = None) -> bool:
        """
        Send an HTML email alert.

        Args:
            event: AlertEvent describing what happened.
            recipients: Email addresses. Falls back to settings.ALERT_EMAIL.

        Returns:
            bool: True if sent successfully.
        """
        if not settings.EMAIL_ALERTS_ENABLED or not settings.SMTP_USER:
            logger.debug("Email alerts disabled or SMTP not configured")
            return False

        to_list = recipients or [settings.ALERT_EMAIL]
        subject = f"[ETL {event.severity.upper()}] {event.title}"
        body = self._build_html(event)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = settings.SMTP_USER
        msg["To"] = ", ".join(to_list)
        msg.attach(MIMEText(body, "html"))

        try:
            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                smtp.sendmail(settings.SMTP_USER, to_list, msg.as_string())

            logger.info(
                "Email alert sent",
                recipients=to_list,
                severity=event.severity,
                pipeline=event.pipeline_name,
            )
            return True

        except Exception as exc:
            logger.error("Failed to send email alert", error=str(exc), exc_info=True)
            return False

    def _build_html(self, event: AlertEvent) -> str:
        """Build HTML email body."""
        severity_colors = {
            "info": "#2196F3",
            "warning": "#FF9800",
            "error": "#f44336",
            "critical": "#9C27B0",
        }
        color = severity_colors.get(event.severity, "#607D8B")

        details_html = ""
        if event.details:
            rows = "".join(
                f"<tr><td style='padding:4px 12px;color:#555'>{k}</td>"
                f"<td style='padding:4px 12px'><b>{v}</b></td></tr>"
                for k, v in event.details.items()
            )
            details_html = f"<table style='border-collapse:collapse'>{rows}</table>"

        traceback_html = ""
        if event.error_traceback:
            traceback_html = (
                f"<pre style='background:#f5f5f5;padding:12px;"
                f"border-radius:4px;font-size:12px;overflow:auto'>"
                f"{event.error_traceback}</pre>"
            )

        return f"""
        <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto">
          <div style="background:{color};color:white;padding:16px 24px;border-radius:6px 6px 0 0">
            <h2 style="margin:0">🔔 ETL Alert: {event.title}</h2>
            <span style="opacity:.85">Severity: {event.severity.upper()}</span>
          </div>
          <div style="border:1px solid #ddd;border-top:none;padding:20px;border-radius:0 0 6px 6px">
            <p><b>Pipeline:</b> {event.pipeline_name}</p>
            <p><b>Run ID:</b> {event.run_id}</p>
            <p>{event.message}</p>
            {details_html}
            {traceback_html}
          </div>
        </body></html>
        """


class SlackAlerter:
    """Sends Slack webhook alerts."""

    def send(self, event: AlertEvent) -> bool:
        """
        Post a Slack message via incoming webhook.

        Args:
            event: AlertEvent to notify about.

        Returns:
            bool: True if posted successfully.
        """
        webhook_url = settings.SLACK_WEBHOOK_URL
        if not settings.SLACK_ALERTS_ENABLED or not webhook_url:
            logger.debug("Slack alerts disabled or webhook not configured")
            return False

        severity_emoji = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "❌",
            "critical": "🚨",
        }
        emoji = severity_emoji.get(event.severity, "📢")

        payload = {
            "attachments": [
                {
                    "color": self._severity_color(event.severity),
                    "blocks": [
                        {
                            "type": "header",
                            "text": {
                                "type": "plain_text",
                                "text": f"{emoji} ETL Alert: {event.title}",
                            },
                        },
                        {
                            "type": "section",
                            "fields": [
                                {"type": "mrkdwn", "text": f"*Pipeline:*\n{event.pipeline_name}"},
                                {"type": "mrkdwn", "text": f"*Severity:*\n{event.severity.upper()}"},
                                {"type": "mrkdwn", "text": f"*Run ID:*\n`{event.run_id}`"},
                                {"type": "mrkdwn", "text": f"*Message:*\n{event.message}"},
                            ],
                        },
                    ],
                }
            ]
        }

        # Add details fields
        if event.details:
            detail_fields = [
                {"type": "mrkdwn", "text": f"*{k}:*\n{v}"}
                for k, v in event.details.items()
            ]
            payload["attachments"][0]["blocks"].append(
                {"type": "section", "fields": detail_fields[:10]}
            )

        try:
            response = httpx.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.info(
                "Slack alert sent",
                severity=event.severity,
                pipeline=event.pipeline_name,
            )
            return True

        except Exception as exc:
            logger.error("Failed to send Slack alert", error=str(exc), exc_info=True)
            return False

    def _severity_color(self, severity: str) -> str:
        return {
            "info": "#2196F3",
            "warning": "#FF9800",
            "error": "#f44336",
            "critical": "#9C27B0",
        }.get(severity, "#607D8B")


class AlertService:
    """
    Unified alerting facade. Routes to Email and/or Slack based on config.
    """

    def __init__(self) -> None:
        self._email = EmailAlerter()
        self._slack = SlackAlerter()

    def send_pipeline_failure(
        self,
        pipeline_name: str,
        run_id: str,
        error: str,
        details: Optional[Dict[str, Any]] = None,
        traceback: Optional[str] = None,
    ) -> None:
        """Alert on pipeline failure."""
        event = AlertEvent(
            pipeline_name=pipeline_name,
            run_id=run_id,
            severity="error",
            title=f"Pipeline Failed: {pipeline_name}",
            message=f"Pipeline execution failed with error: {error}",
            details=details,
            error_traceback=traceback,
        )
        self._dispatch(event)

    def send_quality_alert(
        self,
        pipeline_name: str,
        run_id: str,
        quality_score: float,
        threshold: float,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Alert when quality score falls below threshold."""
        event = AlertEvent(
            pipeline_name=pipeline_name,
            run_id=run_id,
            severity="warning",
            title=f"Low Data Quality: {pipeline_name}",
            message=(
                f"Quality score {quality_score:.2%} is below threshold {threshold:.2%}"
            ),
            details={
                "quality_score": f"{quality_score:.4f}",
                "threshold": f"{threshold:.4f}",
                **(details or {}),
            },
        )
        self._dispatch(event)

    def send_success_notification(
        self,
        pipeline_name: str,
        run_id: str,
        records_loaded: int,
        duration_seconds: float,
    ) -> None:
        """Send success notification (info level)."""
        event = AlertEvent(
            pipeline_name=pipeline_name,
            run_id=run_id,
            severity="info",
            title=f"Pipeline Completed: {pipeline_name}",
            message=f"Successfully loaded {records_loaded:,} records.",
            details={
                "records_loaded": f"{records_loaded:,}",
                "duration": f"{duration_seconds:.1f}s",
            },
        )
        self._dispatch(event)

    def _dispatch(self, event: AlertEvent) -> None:
        """Send event to all configured channels."""
        self._email.send(event)
        self._slack.send(event)


# Singleton
alert_service = AlertService()
