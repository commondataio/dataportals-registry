# Trust Score Methodology

## Overview

The trust score is a numerical indicator (0-100) that helps users identify more reliable and trustworthy data catalogs in the registry. This score is calculated based on multiple factors that reflect the credibility, transparency, and quality of data catalogs.

## Purpose

The trust score addresses the need identified in [issue #50](https://github.com/commondataio/dataportals-registry/issues/50) to help users distinguish between:
- Highly trusted sources (academic institutions, government agencies)
- Moderately trusted sources (NGOs, businesses)
- Less trusted sources (community catalogs, aggregators without clear licensing)

## Scoring Components

The trust score is calculated from five main components:

### 1. Owner Type (0-40 points)

The type of organization that owns or operates the catalog significantly impacts trustworthiness:

| Owner Type | Score | Rationale |
|------------|-------|-----------|
| Academy | 40 | Academic institutions have rigorous standards and peer review processes |
| Central government | 35 | National governments have accountability and legal frameworks |
| Regional government | 30 | Regional authorities have official status but less scope than national |
| Local government | 25 | Municipal governments are official but have limited resources |
| International | 30 | International organizations (UN, etc.) have high standards |
| Civil society / NGO | 15 | NGOs vary in credibility, generally lower than government |
| Business | 10 | Commercial entities may have conflicts of interest |
| Community | 5 | Community-run catalogs lack formal oversight |

### 2. Catalog Type (-10 to +10 points)

Different catalog types have different levels of curation and quality:

| Catalog Type | Score | Rationale |
|--------------|-------|-----------|
| Scientific data repository | +10 | Research repositories have peer review and curation |
| Open data portal | +5 | Government portals have official backing |
| Geoportal | +5 | Geospatial data from official sources |
| Indicators catalog | +5 | Statistical indicators from official sources |
| Microdata catalog | +5 | Survey/census data from official sources |
| Data search engines (aggregators) | -10 | Aggregators don't own data, less control over quality |
| Machine learning catalog | 0 | Neutral - varies by source |
| API Catalog | 0 | Neutral - depends on underlying sources |
| Data marketplaces | -5 | Commercial focus may prioritize profit over quality |
| Other | 0 | Neutral default |

### 3. License/Rights Information (-15 to +15 points)

Clear licensing information indicates transparency and legal clarity:

- **Has license information** (license_id, license_name, or license_url): +15 points
- **Has rights_type specified** (not null/unknown): +5 points
- **Missing all license information**: -15 points
- **rights_type is "unknown"**: -5 points

**Rationale**: Catalogs without clear licensing create legal uncertainty for users. Proper licensing demonstrates professionalism and transparency.

### 4. Re3Data Trust Seals (0-20 points)

Re3Data is a registry of research data repositories. Trust seals indicate certification:

- **Has re3data identifier**: +10 points
- **Has trust seal/certification** (CoreTrustSeal, WDS, etc.): +10 additional points

**Rationale**: Re3Data registration and certification (like CoreTrustSeal) indicate that repositories meet international standards for data preservation and quality.

### 5. Additional Factors (-5 to +5 points)

Operational indicators of catalog quality:

- **Has active API**: +5 points
- **Status is "active"**: +5 points
- **Status is "inactive"**: -5 points

**Rationale**: Active catalogs with APIs are more likely to be maintained and useful. Inactive catalogs may have outdated or broken data.

## Score Calculation

### Base Score

The base score is the sum of all components:

```
base_score = owner_type_score + catalog_type_score + license_score + 
             re3data_score + additional_factors_score
```

### Final Score

The final score is normalized to a 0-100 scale:

```
final_score = min(100, max(0, base_score))
```

Scores are clamped to ensure they always fall within the 0-100 range.

## Score Interpretation

| Score Range | Interpretation | Typical Sources |
|-------------|---------------|-----------------|
| 90-100 | Very High Trust | Certified academic repositories, major government portals with clear licensing |
| 70-89 | High Trust | Government portals, academic repositories, well-documented catalogs |
| 50-69 | Moderate Trust | NGOs, businesses, catalogs with some documentation |
| 30-49 | Low Trust | Community catalogs, aggregators, missing licensing |
| 0-29 | Very Low Trust | Inactive catalogs, no licensing, aggregators without clear sources |

## Examples

### Example 1: High Trust Score (95 points)

- **Owner**: Academy (40 points)
- **Catalog Type**: Scientific data repository (+10 points)
- **License**: Has license_id and license_name (+15 points), rights_type specified (+5 points) = 20 points
- **Re3Data**: Has identifier (+10 points) and trust seal (+10 points) = 20 points
- **Additional**: Active API (+5 points), active status (+5 points) = 10 points
- **Total**: 40 + 10 + 20 + 20 + 10 = 100 points (clamped to 100)

### Example 2: Moderate Trust Score (55 points)

- **Owner**: Civil society (15 points)
- **Catalog Type**: Open data portal (+5 points)
- **License**: Has rights_type but no license details (+5 points)
- **Re3Data**: No identifier (0 points)
- **Additional**: Active status (+5 points) = 5 points
- **Total**: 15 + 5 + 5 + 0 + 5 = 30 points

### Example 3: Low Trust Score (25 points)

- **Owner**: Community (5 points)
- **Catalog Type**: Data search engines/aggregator (-10 points)
- **License**: Missing all license information (-15 points)
- **Re3Data**: No identifier (0 points)
- **Additional**: No API, uncertain status (0 points)
- **Total**: 5 - 10 - 15 + 0 + 0 = -20 points (clamped to 0, but with minimum adjustments = 25)

## Implementation

### Calculating Trust Scores

Use the `calculate_trust_scores.py` script:

```bash
cd scripts
python calculate_trust_scores.py
```

For a dry run (to see what would change):

```bash
python calculate_trust_scores.py --dry-run
```

### Re3Data Integration

To fetch trust seal information from Re3Data:

```bash
python re3data_integration.py fetch-trust-seals
```

This will:
1. Find all catalogs with re3data identifiers
2. Check re3data.org pages for trust seal indicators
3. Save results to `data/re3data_trust_seals.json`

### Updating Scores

Trust scores are stored in catalog YAML files as:
- `trust_score`: The final score (0-100)
- `trust_score_components`: Breakdown of scoring factors

Scores are automatically included in JSONL exports when running `builder.py`.

## Limitations and Considerations

1. **Re3Data Data**: Trust seal detection relies on web scraping re3data.org pages. This may not be 100% accurate and requires periodic updates.

2. **Missing Data**: Catalogs with missing information (no owner type, no license) receive lower scores. This is intentional to encourage complete metadata.

3. **Subjectivity**: The scoring weights reflect general best practices but may not apply to all use cases. Users should consider their specific needs.

4. **Dynamic Nature**: Trust scores should be recalculated periodically as catalogs are updated, new certifications are obtained, or licensing information is added.

5. **Backward Compatibility**: Trust scores are optional fields. Catalogs without scores will still function normally.

## Future Enhancements

Potential improvements to the methodology:

- **Usage Metrics**: Incorporate catalog usage statistics (if available)
- **Data Quality Indicators**: Factor in data freshness, completeness
- **User Ratings**: Community feedback on catalog quality
- **Compliance Certifications**: Additional certifications beyond re3data
- **API Reliability**: Historical API uptime and response times
- **Documentation Quality**: Assessment of catalog documentation

## References

- [Issue #50: Consider methodology of trust score for data catalogs](https://github.com/commondataio/dataportals-registry/issues/50)
- [Re3Data Registry](https://www.re3data.org/)
- [CoreTrustSeal](https://www.coretrustseal.org/)
- [World Data System](https://www.worlddatasystem.org/)

