from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .config import Settings
from .models import MailAttachment, MailCommand, MailMessage


def _is_retryable_agentmail_error(exc: BaseException) -> bool:
    if not isinstance(exc, httpx.HTTPStatusError):
        return False
    status_code = exc.response.status_code if exc.response is not None else None
    if status_code is None:
        return False
    return status_code in {408, 429} or 500 <= status_code < 600


class AgentMailAPI:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        if not settings.agentmail_api_key:
            raise RuntimeError("AGENTMAIL_API_KEY is required for inbox operations")

    def list_unread_messages(self, limit: int = 25) -> list[MailMessage]:
        data = self._request(
            "GET",
            f"/inboxes/{self.settings.agentmail_inbox_id}/messages",
            params={"labels": ["unread"], "limit": limit},
        )
        messages = []
        for message in data.get("messages", []):
            full_message = self.get_message(message["message_id"])
            messages.append(full_message)
        return messages

    def get_message(self, message_id: str) -> MailMessage:
        data = self._request(
            "GET",
            f"/inboxes/{self.settings.agentmail_inbox_id}/messages/{message_id}",
        )
        return MailMessage(
            message_id=data["message_id"],
            thread_id=data.get("thread_id", ""),
            sender=_normalize_sender(data.get("from", "")),
            subject=data.get("subject", ""),
            labels=data.get("labels", []),
            text=data.get("text"),
            html=data.get("html"),
            attachments=[
                MailAttachment(
                    attachment_id=attachment.get("attachment_id") or attachment.get("id") or "",
                    filename=attachment.get("filename") or attachment.get("name"),
                    content_type=attachment.get("content_type"),
                    size=attachment.get("size"),
                    inline=bool(attachment.get("inline", False)),
                )
                for attachment in data.get("attachments", [])
                if attachment.get("attachment_id") or attachment.get("id")
            ],
        )

    def update_labels(self, message_id: str, add_labels: list[str], remove_labels: list[str]) -> None:
        self._request(
            "PATCH",
            f"/inboxes/{self.settings.agentmail_inbox_id}/messages/{message_id}",
            json={"add_labels": add_labels, "remove_labels": remove_labels},
        )

    def reply_all(
        self,
        message_id: str,
        text: str,
        labels: Optional[list[str]] = None,
        attachments: Optional[list[dict[str, str]]] = None,
    ) -> None:
        payload = {"text": text}
        if labels:
            payload["labels"] = labels
        if attachments:
            payload["attachments"] = attachments
        self._request(
            "POST",
            f"/inboxes/{self.settings.agentmail_inbox_id}/messages/{message_id}/reply-all",
            json=payload,
        )

    def download_attachment(self, message_id: str, attachment_id: str) -> bytes:
        data = self._request(
            "GET",
            f"/inboxes/{self.settings.agentmail_inbox_id}/messages/{message_id}/attachments/{attachment_id}",
        )
        if isinstance(data, dict) and data.get("download_url"):
            with httpx.Client(timeout=60) as client:
                response = client.get(data["download_url"])
                response.raise_for_status()
                return response.content
        if isinstance(data, dict) and data.get("content"):
            return data["content"].encode()
        raise RuntimeError(f"Attachment {attachment_id} could not be downloaded")

    def _request(self, method: str, path: str, **kwargs):
        return self._request_with_retry(method, path, **kwargs)

    @retry(
        retry=retry_if_exception(_is_retryable_agentmail_error),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        stop=stop_after_attempt(4),
        reraise=True,
    )
    def _request_with_retry(self, method: str, path: str, **kwargs):
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self.settings.agentmail_api_key}"
        base_url = self.settings.agentmail_base_url.rstrip("/")
        with httpx.Client(timeout=30) as client:
            response = client.request(method, base_url + path, headers=headers, **kwargs)
            response.raise_for_status()
            if response.content:
                return response.json()
            return {}


def parse_command(message: MailMessage) -> MailCommand | None:
    body = extract_body_text(message)
    normalized = " ".join(body.strip().split())
    if not normalized:
        return None
    if re.fullmatch(r"RUN FULL ARCHIVE", normalized, re.IGNORECASE):
        return MailCommand(action="run_full_archive")
    recent = re.fullmatch(r"RUN RECENT (\d+)", normalized, re.IGNORECASE)
    if recent:
        return MailCommand(action="run_recent", arg=recent.group(1))
    edition = re.fullmatch(r"RUN EDITION (\d{4}-\d{2}-\d{2})", normalized, re.IGNORECASE)
    if edition:
        return MailCommand(action="run_edition", arg=edition.group(1))
    top = re.fullmatch(r"TOP (\d+)", normalized, re.IGNORECASE)
    if top:
        return MailCommand(action="top", arg=top.group(1))
    return None


def has_supported_dataset_attachment(message: MailMessage) -> bool:
    supported_suffixes = {".csv", ".tsv", ".json", ".parquet", ".zip"}
    for attachment in message.attachments:
        suffix = Path(attachment.filename or "").suffix.lower()
        if not attachment.inline and suffix in supported_suffixes:
            return True
    return False


def extract_body_text(message: MailMessage) -> str:
    if message.text:
        return message.text
    if message.html:
        return BeautifulSoup(message.html, "html.parser").get_text(" ", strip=True)
    return ""


def is_authorized_sender(message: MailMessage, settings: Settings) -> bool:
    if not settings.authorized_senders:
        return False
    return _normalize_sender(message.sender) in settings.authorized_senders


def _normalize_sender(value: str) -> str:
    if "<" in value and ">" in value:
        value = value.split("<", 1)[1].split(">", 1)[0]
    return value.strip().lower()
