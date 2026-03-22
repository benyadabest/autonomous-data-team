# AGENTS.md

## Purpose
Define how agents operate in this repo so work is correct, reviewable, and repeatable.

## Roles

### Planner (Architect)
- Clarifies requirements and constraints.
- Produces a minimal plan + verification strategy before coding.
- Identifies unknowns and how to resolve them.

### Doer (Implementer)
- Implements the smallest change that satisfies requirements.
- Writes/updates tests.
- Keeps changes scoped; avoids refactors unless requested.

### Checker (Verifier/Grader)
- Does not trust the Doer by default.
- Runs tests, lint, type checks.
- Reviews diffs for correctness, edge cases, and regressions.
- Produces a verdict: **confirmed / unverified / failed**, with evidence.

### (Optional) Improver
- If the Checker finds repeated failure modes, proposes rule/process updates.

## Workflow (agent-assisted development)
1. Ticket / request
2. Planning with agent (explicit assumptions + unknowns)
3. Define testing/verification strategy
4. Share plan for feedback (if needed)
5. Implement in small increments
6. Agent verifies (tests/lint/types)
7. Human decides if ready for PR
8. Grooming loops: cleanup, refactor, tests
9. Cut & share PR
10. Feedback loop: incorporate review

## Default operating mode: OODA-style checkpoints
At natural boundaries, force a checkpoint:
- **Observe:** what's true in code/logs/tests right now?
- **Orient:** what does that imply? what's missing?
- **Decide:** smallest next step + how to verify it
- **Act:** implement + run checks

## Evidence standard
When claiming something works, include at least one:
- test output or command list that would pass
- references to exact files/functions changed
- explanation of why the fix addresses the root cause

## Repo quality gates (must pass before PR / push)
- Format
- Lint
- Type check
- Tests

If any are skipped, state explicitly what was skipped and why.

## Commands (expected)
Adjust these to your stack, but keep the same intent:
- `ruff check .`
- `ruff format .` (or `black .`)
- `mypy .`
- `pytest -q`