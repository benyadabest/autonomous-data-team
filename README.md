# Autonomous Data Team

Autonomous Data Team is an email-driven data science worker. You send a dataset attachment to `autonomous-data-team@agentmail.to`, the service analyzes it, and it replies on the same thread with findings.

The service is now meant to run remotely as an always-on worker, not on your laptop. AgentMail remains the inbox, but the worker can run in a container on a server or platform-as-a-service host.

## How it works

When an allowlisted sender emails a supported dataset attachment, the service:

1. downloads the attachment
2. unpacks zip archives when needed
3. runs dataset profiling and EDA
4. optionally runs experiments
5. writes a markdown report
6. replies on the same email thread

Supported dataset attachments:

- `.csv`
- `.tsv`
- `.json`
- `.parquet`
- `.zip` containing supported files

## Email request options

The email body can control the scope of the run.

### EDA only

Any of these will trigger an EDA-only run:

- `EDA only`
- `just EDA`
- `Mode: EDA`

In this mode, the service profiles the dataset and writes an EDA report, but it does not run modeling experiments. When an OpenAI API key is configured, the EDA report also includes LLM-generated insights: practical project ideas, out-of-the-box creative ideas, ML opportunities, data quality concerns, and recommended next steps.

### EDA + experiments

This is the default. You can also state it explicitly with text like:

- `EDA + experiments`
- `EDA and experiments`

If you do not specify a mode, the system assumes `EDA + experiments`.

## Remote operation

The service no longer needs to be tied to your local machine. The intended deployment shape is:

- one always-on worker process
- one HTTP health endpoint
- persistent environment variables for secrets
- writable disk for `runs/` and SQLite

The hosted entrypoint is:

```bash
autonomous-data-team serve
```

That command does two things:

- starts the inbox polling worker
- exposes `GET /` and `GET /healthz` for uptime checks


## Configuration

Minimum required environment for the email workflow:

```env
OPENAI_API_KEY=
AGENTMAIL_API_KEY=
AGENTMAIL_INBOX_ID=autonomous-data-team@agentmail.to
AUTHORIZED_SENDERS=you@example.com
```

Useful runtime settings:

```env
OPENAI_MODEL=gpt-4.1-mini
RUNS_DIR=./runs
SWARM_ORCHESTRATOR=crewai
CREWAI_HOME_DIR=./runs/.crewai_home
MAX_DATASET_ROWS=50000
BIND_HOST=0.0.0.0
PORT=8000
WORKER_POLL_INTERVAL=300
```

If you want deterministic fallback behavior only, set:

```env
SWARM_ORCHESTRATOR=heuristic
```

## Running the hosted worker

For direct execution:

```bash
.venv/bin/autonomous-data-team serve
```

For a one-shot mailbox poll:

```bash
.venv/bin/autonomous-data-team inbox-worker --once
```

## Docker

A `Dockerfile` is included so the worker can run on a remote container host.

Build:

```bash
docker build -t autonomous-data-team .
```

Run:

```bash
docker run --env-file .env -p 8000:8000 autonomous-data-team
```

Health check:

```bash
curl http://localhost:8000/healthz
```

## Inbox behavior

If a message contains a supported non-inline dataset attachment, attachment analysis takes priority over archive commands.

Expected flow:

- unread message is relabeled to `processing`
- the swarm runs on the attachment
- the worker replies with a short summary
- a markdown summary is attached to the reply
- the message is relabeled to `processed`

If a run fails:

- the worker replies with the failure text
- the message is relabeled to `failed`

If a message has no supported dataset attachment, the worker falls back to the archive-command path that already exists in the codebase.
