# Trust score

Optional 0–100 indicator of catalog credibility. Implementation: `scripts/calculate_trust_scores.py`. Background: [issue #50](https://github.com/datenoio/dataportals-registry/issues/50).

Stored on YAML as `trust_score` and `trust_score_components` when calculated. Missing scores are valid — the field is optional.

## Components

### Owner type (5–40)

| Owner type | Score |
|------------|------:|
| Academy | 40 |
| Central government | 35 |
| Regional government / International | 30 |
| Local government | 25 |
| Civil society / NGO | 15 |
| Business | 10 |
| Community | 5 |
| Any other type (including `Federal government`, `Other`, or missing) | 10 |

`Federal government` is a valid canonical owner type but is not in the score map, so it falls back to the 10-point default — as do `Other` and unknown/missing types.

### Catalog type (−10 to +10)

| Catalog type | Score |
|--------------|------:|
| Scientific data repository | +10 |
| Open data portal, Geoportal, Indicators catalog, Microdata catalog | +5 |
| Data marketplace | −5 |
| Data search engine (aggregator) | −10 |
| Machine learning catalog, API Catalog, Metadata catalog, Other | 0 |
| Datasets list, General research repository (unlisted) | 0 |

Types absent from the scorer map also score **0**.

### License / rights (−15 to +20)

The license and `rights_type` bonuses stack:

- Has `license_id`, `license_name`, or `license_url`: **+15**
- Has `rights_type` other than null/unknown (in addition): **+5**
- Missing all license information: **−15**
- `rights_type: unknown`: **−5**

### Re3Data (0–20)

- Has re3data identifier: **+10**
- Has a trust seal (CoreTrustSeal, WDS, …): **+10** more

### Operational (−5 to +10)

The API and status bonuses stack:

- `api: true` **and** `api_status: active`: **+5**
- `status: active` (in addition): **+5**
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
