from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .config import Settings
from .service import process_inbox_once
from .storage import Store


@dataclass
class HostedWorkerState:
    poll_interval: int
    started_at: str
    last_poll_started_at: str | None = None
    last_poll_completed_at: str | None = None
    last_poll_error: str | None = None
    last_result_count: int = 0

    def snapshot(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "started_at": self.started_at,
            "poll_interval": self.poll_interval,
            "last_poll_started_at": self.last_poll_started_at,
            "last_poll_completed_at": self.last_poll_completed_at,
            "last_poll_error": self.last_poll_error,
            "last_result_count": self.last_result_count,
        }


def serve(settings: Settings, store: Store, host: str | None = None, port: int | None = None, poll_interval: int | None = None) -> None:
    state = HostedWorkerState(
        poll_interval=poll_interval or settings.worker_poll_interval,
        started_at=_utcnow(),
    )
    stop_event = threading.Event()
    server = ThreadingHTTPServer((host or settings.bind_host, port or settings.port), _build_handler(state))
    worker = threading.Thread(
        target=_worker_loop,
        args=(settings, store, state, stop_event),
        name="inbox-worker",
        daemon=True,
    )
    worker.start()
    try:
        server.serve_forever()
    finally:
        stop_event.set()
        server.shutdown()
        server.server_close()
        worker.join(timeout=1)


def _worker_loop(settings: Settings, store: Store, state: HostedWorkerState, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        state.last_poll_started_at = _utcnow()
        try:
            results = process_inbox_once(settings, store)
            state.last_result_count = len(results)
            state.last_poll_error = None
        except Exception as exc:
            state.last_poll_error = str(exc)
        finally:
            state.last_poll_completed_at = _utcnow()
        stop_event.wait(state.poll_interval)


def _build_handler(state: HostedWorkerState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path not in {"/", "/healthz"}:
                self._write_json({"status": "not_found"}, status=404)
                return
            self._write_json(state.snapshot())

        def log_message(self, format: str, *args: object) -> None:
            return

        def _write_json(self, payload: dict[str, Any], status: int = 200) -> None:
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return Handler


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
