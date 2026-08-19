# Trust score

Optional 0–100 indicator of catalog credibility. Implementation: `scripts/calculate_trust_scores.py`. Background: [issue #50](https://github.com/datenoio/dataportals-registry/issues/50).

Stored on YAML as `trust_score` and `trust_score_components` when calculated. Missing scores are valid — the field is optional.

## Components

### Owner type (0–40)

| Owner type | Score |
|------------|------:|
| Academy | 40 |
| Central government | 35 |
| Regional government / International | 30 |
| Local government | 25 |
| Civil society | 15 |
| Business | 10 |
| Community | 5 |

### Catalog type (−10 to +10)

| Catalog type | Score |
|--------------|------:|
| Scientific data repository | +10 |
| Open data portal, Geoportal, Indicators, Microdata | +5 |
| Data marketplace | −5 |
| Data search engine (aggregator) | −10 |
| ML catalog, API Catalog, Other | 0 |

### License / rights (−15 to +15)

- Has `license_id`, `license_name`, or `license_url`: **+15**
- Has `rights_type` other than null/unknown: **+5**
- Missing all license information: **−15**
- `rights_type: unknown`: **−5**

### Re3Data (0–20)

- Has re3data identifier: **+10**
- Has a trust seal (CoreTrustSeal, WDS, …): **+10** more

### Operational (−5 to +5)

- Active API: **+5**
- `status: active`: **+5**
- `status: inactive`: **−5**

## Formula

```
base = owner + catalog_type + license + re3data + operational
final = min(100, max(0, base))
```

| Range | Interpretation |
|-------|----------------|
| 90–100 | Very high — certified academic / major government with licensing |
| 70–89 | High |
| 50–69 | Moderate |
| 30–49 | Low |
| 0–29 | Very low |

## Recalculate

```bash
python scripts/calculate_trust_scores.py --dry-run
python scripts/calculate_trust_scores.py
```

Scores are heuristic. They encourage complete metadata; they are not a legal or scientific quality certificate. Full notes and examples: [devdocs/trust_score_methodology.md](https://github.com/datenoio/dataportals-registry/blob/main/devdocs/trust_score_methodology.md).
