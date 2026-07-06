"""
MS Teams notification helper for molflow.

One notification, sent once per run: the Final Summary, built and sent
from notify_run_summary in molflow_pipeline.py (which imports
post_to_teams from here).

Webhook URL lives in the Airflow Variable "teams_webhook_secret".
"""
import logging

import requests

logger = logging.getLogger(__name__)


def post_to_teams(webhook_url: str, payload: dict) -> None:
    try:
        response = requests.post(webhook_url, json=payload, timeout=5)
        if response.ok:
            logger.info("Notification sent to MS Teams.")
        else:
            logger.warning(
                "MS Teams notification failed with status %d: %s",
                response.status_code, response.text[:500],
            )
    except requests.exceptions.Timeout:
        logger.warning("Request to Teams webhook timed out.")
    except Exception as notify_error:
        logger.warning(
            "MS Teams notification failed: %s",
            notify_error
        )
