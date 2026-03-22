from __future__ import annotations

import re
import zipfile
from pathlib import Path

from .mail import AgentMailAPI
from .models import MailAttachment, MailMessage, SavedAttachment

SUPPORTED_DATASET_SUFFIXES = {".csv", ".tsv", ".json", ".parquet", ".zip"}


def supported_message_attachments(message: MailMessage) -> list[MailAttachment]:
    return [
        attachment
        for attachment in message.attachments
        if not attachment.inline and Path(attachment.filename or "").suffix.lower() in SUPPORTED_DATASET_SUFFIXES
    ]


def save_message_attachments(
    message: MailMessage,
    client: AgentMailAPI,
    attachments_dir: Path,
) -> list[SavedAttachment]:
    attachments_dir.mkdir(parents=True, exist_ok=True)
    saved: list[SavedAttachment] = []
    for attachment in supported_message_attachments(message):
        filename = sanitize_filename(attachment.filename or attachment.attachment_id)
        payload = client.download_attachment(message.message_id, attachment.attachment_id)
        local_path = attachments_dir / filename
        local_path.write_bytes(payload)
        if local_path.suffix.lower() == ".zip":
            saved.extend(_extract_zip(local_path, attachment))
        else:
            saved.append(
                SavedAttachment(
                    attachment_id=attachment.attachment_id,
                    filename=local_path.name,
                    content_type=attachment.content_type,
                    local_path=str(local_path),
                )
            )
    return saved


def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return cleaned or "attachment.bin"


def _extract_zip(path: Path, attachment: MailAttachment) -> list[SavedAttachment]:
    extracted_dir = path.parent / f"{path.stem}_extracted"
    extracted_dir.mkdir(parents=True, exist_ok=True)
    saved: list[SavedAttachment] = []
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            suffix = Path(member.filename).suffix.lower()
            if suffix not in SUPPORTED_DATASET_SUFFIXES - {".zip"}:
                continue
            target_path = extracted_dir / sanitize_filename(Path(member.filename).name)
            with archive.open(member) as source, target_path.open("wb") as target:
                target.write(source.read())
            saved.append(
                SavedAttachment(
                    attachment_id=attachment.attachment_id,
                    filename=target_path.name,
                    content_type=attachment.content_type,
                    local_path=str(target_path),
                    extracted_from=str(path),
                )
            )
    if not saved:
        saved.append(
            SavedAttachment(
                attachment_id=attachment.attachment_id,
                filename=path.name,
                content_type=attachment.content_type,
                local_path=str(path),
            )
        )
    return saved
