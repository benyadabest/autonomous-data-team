from __future__ import annotations

import httpx

from autonomous_data_team.mail import _is_retryable_agentmail_error, extract_body_text, is_authorized_sender, parse_command
from autonomous_data_team.models import MailMessage


def test_parse_command_variants() -> None:
    message = MailMessage(
        message_id="m1",
        thread_id="t1",
        sender="owner@example.com",
        subject="Run",
        labels=["unread"],
        text="RUN RECENT 5",
    )
    command = parse_command(message)
    assert command is not None
    assert command.action == "run_recent"
    assert command.arg == "5"


def test_authorized_sender(settings) -> None:
    message = MailMessage(
        message_id="m1",
        thread_id="t1",
        sender="Owner <owner@example.com>",
        subject="Run",
        labels=["unread"],
        text="RUN FULL ARCHIVE",
    )
    assert is_authorized_sender(message, settings) is True


def test_extract_body_prefers_html_when_text_missing() -> None:
    message = MailMessage(
        message_id="m2",
        thread_id="t2",
        sender="owner@example.com",
        subject="Top",
        labels=["unread"],
        html="<p>TOP 20</p>",
    )
    assert extract_body_text(message) == "TOP 20"


def test_retryable_agentmail_error_detection() -> None:
    request = httpx.Request("GET", "https://api.agentmail.to/v0/test")
    response = httpx.Response(429, request=request)
    error = httpx.HTTPStatusError("rate limited", request=request, response=response)

    assert _is_retryable_agentmail_error(error) is True
