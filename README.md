# Autonomous Data Team

Autonomous Data Team is an email-driven data science workflow. You send a dataset attachment to `autonomous-data-team@agentmail.to`, the system pulls it into a local worker pipeline, and it replies with findings.

The current product is local first:

- AgentMail is the inbox and reply channel
- the worker process runs on your machine
- SQLite stores runs and task state locally
- `pandas` and `scikit-learn` handle profiling and experiments
- CrewAI is optional for reasoning-oriented steps, but the core data work stays local

## How it works

When an allowlisted sender emails a supported dataset attachment, the worker:

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

In this mode, the worker profiles the dataset and writes an EDA report, but it does not run modeling experiments.

### EDA + experiments

This is the default. You can also state it explicitly with text like:

- `EDA + experiments`
- `EDA and experiments`

If you do not specify a mode, the system assumes `EDA + experiments`.

## What local first means

Yes, it can work right now if you send an email, but only if the local worker is running on your machine.

That means all of the following need to be true:

- your `.env` is populated
- your sender email is included in `AUTHORIZED_SENDERS`
- the inbox worker process is running
- the machine has outbound access to AgentMail
- if you want CrewAI-backed reasoning, the machine also needs outbound access to OpenAI

If the worker is not running, the email will just sit in the AgentMail inbox until the process starts and polls it.

## Configuration

Minimum required environment for the email workflow:

```env
OPENAI_API_KEY=
AGENTMAIL_API_KEY=
AGENTMAIL_INBOX_ID=autonomous-data-team@agentmail.to
AUTHORIZED_SENDERS=you@example.com
```

Other useful variables:

```env
OPENAI_MODEL=gpt-4.1-mini
RUNS_DIR=./runs
SWARM_ORCHESTRATOR=crewai
CREWAI_HOME_DIR=./runs/.crewai_home
MAX_DATASET_ROWS=50000
```

If you want deterministic fallback behavior only, set:

```env
SWARM_ORCHESTRATOR=heuristic
```

## Quick start

Create `.env` from the example and install the project into a Python 3.10+ virtual environment. Then you can validate the flow locally with:

```bash
.venv/bin/autonomous-data-team analyze-dataset \
  --path tests/fixtures/data/sample.csv \
  --notes "EDA only"
```

To process real emails, run:

```bash
.venv/bin/autonomous-data-team inbox-worker --poll-interval 300
```

For a one-shot poll:

```bash
.venv/bin/autonomous-data-team inbox-worker --once
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
