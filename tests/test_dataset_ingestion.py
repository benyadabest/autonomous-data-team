from __future__ import annotations

from pathlib import Path

from autonomous_data_team.dataset_ingestion import save_message_attachments, supported_message_attachments
from autonomous_data_team.models import MailAttachment, MailMessage


class _FakeClient:
    def __init__(self, payloads):
        self.payloads = payloads

    def download_attachment(self, message_id: str, attachment_id: str) -> bytes:
        return self.payloads[attachment_id]


def test_supported_message_attachments_filters_inline_and_unsupported() -> None:
    message = MailMessage(
        message_id="m1",
        thread_id="t1",
        sender="owner@example.com",
        subject="Dataset",
        labels=["unread"],
        attachments=[
            MailAttachment(attachment_id="a1", filename="dataset.csv", inline=False),
            MailAttachment(attachment_id="a2", filename="image.png", inline=False),
            MailAttachment(attachment_id="a3", filename="inline.csv", inline=True),
        ],
    )

    supported = supported_message_attachments(message)

    assert [attachment.attachment_id for attachment in supported] == ["a1"]


def test_save_message_attachments_writes_csv(tmp_path: Path) -> None:
    message = MailMessage(
        message_id="m1",
        thread_id="t1",
        sender="owner@example.com",
        subject="Dataset",
        labels=["unread"],
        attachments=[MailAttachment(attachment_id="a1", filename="dataset.csv", inline=False)],
    )
    client = _FakeClient({"a1": b"feature,target\n1,0\n2,1\n"})

    saved = save_message_attachments(message, client, tmp_path)

    assert len(saved) == 1
    assert Path(saved[0].local_path).exists()

