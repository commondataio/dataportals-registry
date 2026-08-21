# Catalog URL liveness

Report-only HTTP probes of each catalog `link`. Results do **not** write `status` on YAML. Schema fields such as `liveness_status` / `last_verified_at` are not in the catalog schema yet.

Workflow: `.github/workflows/liveness.yml` (weekly Sunday 03:00 UTC, plus `workflow_dispatch`). Script: `scripts/check_liveness.py`. Output: `dataquality/liveness_report.jsonl` (uploaded as a CI artifact; not a committed export).

## Local run

```bash
python scripts/check_liveness.py --sample 10
python scripts/check_liveness.py --country US --delay 0.25
python scripts/check_liveness.py --output dataquality/liveness_report.jsonl
```

`--sample N` picks N random entity records (seed 42 by default). `--country` is an ISO code. `--timeout` defaults to 10 seconds; `--retries` defaults to 2.

Do not turn this into an internet-wide scanner. It only reads `link` values already in `data/entities/`.

## Status values

| Status | Meaning |
|--------|---------|
| `live` | Successful HTTP response for the catalog URL |
| `redirect` | HTTP redirect to another location |
| `dead` | Persistent client error (for example 404) |
| `inconclusive` | Timeout, 5xx after retries, or TLS/network noise |
| `error` | Request failed before an HTTP status was available |

Treat `inconclusive` as a probe problem, not proof the catalog is gone. Confirm in a browser before changing `status: inactive`.

## Related

- [architecture.md](architecture.md)
- [metadata-quality.md](metadata-quality.md)
- [apidetect.md](apidetect.md)
- [releasing.md](releasing.md)
