## Context

URL format validation cannot distinguish dead portals from live ones. A scheduled liveness probe complements static quality checks without blocking every PR with network calls.

## Goals

- Detect unreachable catalog URLs on a weekly cadence
- Produce machine-readable liveness reports for agents and maintainers
- Minimize false positives from transient failures and bot protection

## Non-Goals

- Writing liveness fields into every YAML record in phase 1 (report layer only)
- Real-time liveness API for external consumers (maintained in separate API/MCP repositories)

## Decisions

### Decision: Report layer first, schema fields in phase 2

**What**: Phase 1 writes `dataquality/liveness_report.jsonl`; phase 2 may add `liveness_status` and `last_verified_at` to catalog schema.

**Why**: Avoids mass YAML churn and merge conflicts on first rollout.

### Decision: Weekly scheduled workflow, not per-PR

**What**: GitHub Actions `schedule: cron` weekly; manual `workflow_dispatch` for on-demand runs.

**Why**: 14k HEAD requests per PR would be slow, flaky, and rate-limit prone.

### Decision: HEAD with GET fallback, 10s timeout, 2 retries

**What**: Probe `link` field; accept 2xx and 3xx as live; retry twice on timeout/5xx.

**Why**: Balances speed with resilience; matches common link-checker practice.

## Risks / Trade-offs

- **Risk**: Bot protection returns 403 for automated probes → Mitigation: mark as `inconclusive`, not `dead`
- **Risk**: Workflow runtime on 14k URLs → Mitigation: batch with concurrency limit, sample mode for dev

## Open Questions

- Whether to probe `endpoints[].url` in addition to primary `link` in phase 1
