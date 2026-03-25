from __future__ import annotations

from autonomous_data_team.hosted import HostedWorkerState


def test_hosted_state_snapshot_returns_health_payload() -> None:
    state = HostedWorkerState(
        poll_interval=300,
        started_at="2026-01-01T00:00:00+00:00",
        last_poll_started_at="2026-01-01T00:01:00+00:00",
        last_poll_completed_at="2026-01-01T00:01:01+00:00",
        last_result_count=2,
    )
    payload = state.snapshot()
    assert payload["status"] == "ok"
    assert payload["poll_interval"] == 300
    assert payload["last_result_count"] == 2
