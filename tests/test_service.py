from __future__ import annotations

from pathlib import Path

from autonomous_data_team.experiment_runner import heuristic_problem_frame
from autonomous_data_team.models import DatasetEntry, MailAttachment, MailMessage, ProbeResult
from autonomous_data_team.service import process_inbox_once, score_archive, sync_archive, top_opportunities
from autonomous_data_team.storage import Store


def test_sync_and_score_archive(settings, monkeypatch) -> None:
    store = Store(settings.db_path)
    repo_dir = Path("tests/fixtures/archive")
    sync_result = sync_archive(settings, store, repo_dir=repo_dir)
    assert sync_result["editions"] == 1
    assert sync_result["entries"] == 2

    monkeypatch.setattr(
        "autonomous_data_team.service.probe_entry",
        lambda entry, settings, extractor: ProbeResult(
            probe_mode_used="direct",
            downloadable=True,
            tabular_hint=True,
            row_estimate=100,
            column_estimate=5,
            basic_schema=[{"name": "feature", "dtype": "int64"}],
        ),
    )

    result = score_archive(settings, store, mode="full")

    assert int(result["scored_entries"]) == 2
    summary_path = Path(result["summary_path"])
    assert summary_path.exists()
    assert len(top_opportunities(store, limit=5)) == 2


class _FakeMailClient:
    def __init__(self, messages):
        self.messages = messages
        self.replies = []
        self.updates = []
        self.downloads = []

    def list_unread_messages(self, limit=25):
        return self.messages

    def reply_all(self, message_id, text, labels=None, attachments=None):
        self.replies.append((message_id, text, labels, attachments))

    def update_labels(self, message_id, add_labels, remove_labels):
        self.updates.append((message_id, tuple(add_labels), tuple(remove_labels)))

    def download_attachment(self, message_id, attachment_id):
        self.downloads.append((message_id, attachment_id))
        return b"feature,target\n1,0\n2,1\n3,0\n4,1\n"


def test_process_inbox_once_handles_authorization_and_commands(settings, monkeypatch) -> None:
    store = Store(settings.db_path)
    messages = [
        MailMessage(
            message_id="unauthorized",
            thread_id="t1",
            sender="intruder@example.com",
            subject="Run",
            labels=["unread"],
            text="RUN FULL ARCHIVE",
        ),
        MailMessage(
            message_id="authorized",
            thread_id="t2",
            sender="owner@example.com",
            subject="Top",
            labels=["unread"],
            text="TOP 5",
        ),
    ]
    fake_client = _FakeMailClient(messages)
    monkeypatch.setattr("autonomous_data_team.service.AgentMailAPI", lambda settings: fake_client)
    monkeypatch.setattr(
        "autonomous_data_team.service._execute_mail_command",
        lambda action, arg, settings, store: "Top opportunities:\n- Example",
    )

    result = process_inbox_once(settings, store)

    assert result == ["unauthorized:unauthorized", "authorized:processed"]
    assert fake_client.replies[0][1] == "Sender is not authorized for this inbox."
    assert "Top opportunities" in fake_client.replies[1][1]


def test_process_inbox_once_handles_dataset_attachment(settings, monkeypatch) -> None:
    store = Store(settings.db_path)
    message = MailMessage(
        message_id="attachment-message",
        thread_id="t3",
        sender="owner@example.com",
        subject="Analyze this dataset",
        labels=["unread"],
        text="Please analyze the attached dataset.",
        attachments=[MailAttachment(attachment_id="att1", filename="dataset.csv", inline=False)],
    )
    fake_client = _FakeMailClient([message])
    monkeypatch.setattr("autonomous_data_team.service.AgentMailAPI", lambda settings: fake_client)
    monkeypatch.setattr(
        "autonomous_data_team.swarm.build_problem_frame",
        lambda settings, profile, requester_notes: heuristic_problem_frame(profile),
    )

    result = process_inbox_once(settings, store)

    assert result[0].startswith("attachment-message:attachment_processed:")
    assert fake_client.replies


def test_replace_entries_for_edition_removes_stale_rows(settings) -> None:
    store = Store(settings.db_path)
    first = DatasetEntry(
        entry_id="2025-01-08-000-a",
        edition_date="2025-01-08",
        ordinal=0,
        title="First",
        description="First entry",
        source_links=["https://example.com/one.csv"],
        as_seen_in_links=[],
        raw_markdown="First",
    )
    stale = DatasetEntry(
        entry_id="2025-01-08-001-b",
        edition_date="2025-01-08",
        ordinal=1,
        title="Stale",
        description="Stale entry",
        source_links=["https://example.com/two.csv"],
        as_seen_in_links=[],
        raw_markdown="Stale",
    )
    store.replace_entries_for_edition("2025-01-08", [first, stale])
    store.replace_entries_for_edition("2025-01-08", [first])

    entries = store.list_entries(edition_date="2025-01-08")

    assert len(entries) == 1
    assert entries[0].title == "First"
