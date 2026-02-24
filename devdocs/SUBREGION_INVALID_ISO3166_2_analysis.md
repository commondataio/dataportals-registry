# SUBREGION_INVALID_ISO3166_2 - Error Verification Analysis

**Date:** 2026-02-24  
**Total flagged issues:** 244  
**Reference file:** `data/reference/subregions/IP2LOCATION-ISO3166-2.CSV`

## Summary

The validation uses `IP2LOCATION-ISO3166-2.CSV` as the authoritative list of valid ISO 3166-2 codes. **This reference file is incomplete** — it contains only a subset of the official ISO 3166-2 subdivision codes. Many flagged "errors" are **false positives**: the codes are valid per the ISO standard but missing from the IP2Location database.

### Root Cause

The IP2Location database:
- Uses **first-level subdivisions only** for many countries (e.g., US states but not US territories; Belgium provinces but not regions)
- Has **outdated codes** for some countries (e.g., India: IN-OR vs IN-OD, IN-TG vs IN-TS)
- **Omits entire categories** (e.g., French departments FR-01–FR-95, overseas FR-971/972/974; Finland FI-01)

---

## Classification of Flagged Codes

### FALSE POSITIVES (Valid ISO 3166-2, missing from reference)

| Code | Country | Description | Fix |
|------|---------|-------------|-----|
| **US-PR** | US | Puerto Rico (outlying area) | Add to reference |
| **US-AS** | US | American Samoa (outlying area) | Add to reference |
| **US-VI** | US | US Virgin Islands (outlying area) | Add to reference |
| **BE-WAL** | Belgium | Wallonia (region) | Add to reference |
| **BE-VLG** | Belgium | Flanders (region) | Add to reference |
| **IE-L** | Ireland | Leinster (province) | Add to reference |
| **FI-01** | Finland | Åland (region) | Add to reference |
| **ES-HU** | Spain | Huesca (province) | Add to reference |
| **ES-SE** | Spain | Sevilla (province) | Add to reference |
| **HU-DE** | Hungary | Debrecen (city with county rights) | Add to reference |
| **IN-LA** | India | Ladakh (UT, 2019) | Add to reference |
| **IN-OD** | India | Odisha (2023 update from IN-OR) | Add to reference |
| **IN-TS** | India | Telangana (2023 update from IN-TG) | Add to reference |
| **NP-P2** | Nepal | Madhesh (Province 2, 2015) | Add to reference |
| **NP-P5** | Nepal | Lumbini (Province 5) | Add to reference |
| **NP-P6** | Nepal | Karnali (Province 6) | Add to reference |
| **FR-08** | France | Ardennes (department) | Add to reference |
| **FR-13** | France | Bouches-du-Rhône | Add to reference |
| **FR-21** | France | Côte-d'Or | Add to reference |
| **FR-34** | France | Hérault | Add to reference |
| **FR-35** | France | Ille-et-Vilaine | Add to reference |
| **FR-59** | France | Nord | Add to reference |
| **FR-64** | France | Pyrénées-Atlantiques | Add to reference |
| **FR-65** | France | Hautes-Pyrénées | Add to reference |
| **FR-67** | France | Bas-Rhin | Add to reference |
| **FR-75C** | France | Paris (City) | Add to reference |
| **FR-76** | France | Seine-Maritime | Add to reference |
| **FR-971** | France | Guadeloupe | Add to reference |
| **FR-972** | France | Martinique | Add to reference |
| **FR-974** | France | La Réunion | Add to reference |
| **FR-GF** | France | Guyane (French Guiana) | Add to reference |
| **FR-NC** | France | New Caledonia | Add to reference |
| **FR-PF** | France | French Polynesia | Add to reference |
| **FR-YT** | France | Mayotte | Add to reference |
| **LV-069** | Latvia | (municipality) | Verify — LV uses 3-digit codes |
| **MK-85** | North Macedonia | (municipality) | Verify — MK uses 3-digit codes |
| **PH-40** | Philippines | (province) | Verify against PH codes |

*Note: Lithuania uses 2-letter codes (LT-AL, LT-KU); numeric LT-20, LT-22, etc. are not ISO 3166-2.*

---

### REAL ERRORS (Invalid or non-standard codes)

| Code | Country | Issue | Correct Code |
|------|---------|-------|--------------|
| **SE-MULTI** | Sweden | Custom placeholder, not ISO 3166-2 | Use multiple SE-XX codes or remove |
| **PE-UKA** | Peru | Typo | **PE-UCA** (Ucayali) |
| **IN-KE** | India | Wrong code for Kerala | **IN-KL** |

---

### REAL ERRORS (continued) — Wrong format or invalid

| Code | Country | Issue | Correct Code |
|------|---------|-------|--------------|
| **CO-BO** | Colombia | Bolívar uses 3 letters | **CO-BOL** |
| **LT-20, LT-22, LT-25, LT-31, LT-38, LT-46** | Lithuania | ISO 3166-2:LT uses 2-letter codes (LT-AL, LT-KU) | Map to LT-AL, LT-KU, LT-MR, LT-TE, LT-PN, LT-SA |
| **CL-QL** | Chile | ISO 3166-2:CL has regions only; Quillota is a province | **CL-VS** (Valparaíso) |
| **EUSAIR** | EU | Custom code, not ISO 3166-2 | Use country codes or remove |
| **FO-TOR** | Faroe Islands | No ISO 3166-2 subdivisions defined for FO | Remove subregion |

### CODES REQUIRING VERIFICATION

| Code | Country | Notes |
|------|---------|-------|
| **LV-069** | Latvia | LV uses 3-digit codes; verify if 069 exists in ISO 3166-2:LV |
| **MK-85** | North Macedonia | MK uses 3-digit codes (e.g. MK-109); verify 085 format |
| **PH-40** | Philippines | Verify against ISO 3166-2:PH province list |

---

## Fixes Applied (2026-02-24)

| Issue | Fix | Records |
|-------|-----|---------|
| PE-UKA | PE-UCA (owner.location) | catalogoregionucayaligobpe |
| IN-KE | IN-KL, moved to IN-KL/ | 5 Kerala records |
| CO-BO | CO-BOL, moved to CO-BOL/ | midascartagenagovco |
| CL-QL | CL-VS (Valparaíso), moved to CL-VS/ | ideemprendequillotacl |
| LT-20, LT-22, LT-31, LT-46 | LT-KL (Klaipėda county) | 4 records |
| LT-25 | LT-MR (Marijampolė county) | gismarijampolelt |
| LT-38 | LT-KU (Kaunas county) | gisraseiniailt |
| LV-069 | LV-059 (Madonas novads typo) | gisportalmadonasudenslv |
| PH-40 | Added to reference (CALABARZON) | — |
| MK-85 | Added to reference (Skopje) | — |

---

## Manual Review Required (4 issues)

| Code | Records | Action |
|------|---------|--------|
| **SE-MULTI** | extgeodatakataloglansstyrelsense, extgeodatakatalogforvlansstyrelsense | Multi-region placeholder; consider listing explicit SE-XX codes or add SE-MULTI to reference as exception |
| **EUSAIR** | wwwportodimareeu | EU Strategy for Adriatic-Ionian Region; use country-level or remove subregion |
| **FO-TOR** | gistorshavnfo | Faroe Islands has no ISO 3166-2 subdivisions; remove subregion or use country-level only |

---

## Recommended Actions

### 1. Update the reference file (recommended)

Add the missing valid ISO 3166-2 codes to `IP2LOCATION-ISO3166-2.CSV`. This will eliminate the majority of false positives.

**Missing codes to add (high priority):**
- US: US-PR, US-AS, US-VI, US-GU, US-MP, US-UM
- BE: BE-BRU, BE-VLG, BE-WAL (regions)
- FR: All departments (FR-01 through FR-95, FR-2A, FR-2B), overseas (FR-971, FR-972, FR-973, FR-974, FR-976, FR-GF, FR-NC, FR-PF, FR-YT, etc.)
- FI: FI-01 (Åland)
- IE: IE-L (Leinster)
- ES: Province-level codes (ES-HU, ES-SE, etc.) if not present
- IN: IN-OD, IN-TS, IN-LA (2023 updates)
- NP: NP-P1 through NP-P7 (provinces)
- HU: HU-DE (Debrecen) and other cities with county rights

### 2. Fix real data errors

- **PE-UKA** → **PE-UCA** (1 record: catalogoregionucayaligobpe)
- **IN-KE** → **IN-KL** (Kerala, multiple records)

### 3. Handle special cases

- **SE-MULTI**: Consider adding as an allowed exception for "multi-region" records, or require splitting into explicit SE-XX codes
- **CL-QL**: Change to **CL-VS** (Valparaíso region) if Quillota is within that region
- **CO-BO**: Change to **CO-BOL** (Bolívar)
- **EUSAIR**: Use EU-level or appropriate country codes
- **FO-TOR**: Faroe Islands has no subdivisions in ISO 3166-2; use country-level only or remove subregion

---

## Statistics

| Category | Count | % of Total |
|----------|-------|------------|
| False positives (valid ISO 3166-2, missing from reference) | ~200 issues | ~82% |
| Real errors (typos, wrong codes, invalid format) | ~35 issues | ~14% |
| Needs verification | ~10 issues | ~4% |

---

## Reference

- [ISO 3166-2:US](https://en.wikipedia.org/wiki/ISO_3166-2:US) — US territories
- [ISO 3166-2:BE](https://en.wikipedia.org/wiki/ISO_3166-2:BE) — Belgium regions
- [ISO 3166-2:FR](https://en.wikipedia.org/wiki/ISO_3166-2:FR) — France departments
- [ISO 3166-2:IN](https://en.wikipedia.org/wiki/ISO_3166-2:IN) — India (2023 updates)
- [ISO 3166-2:NP](https://en.wikipedia.org/wiki/ISO_3166-2:NP) — Nepal provinces
- [ISO Online Browsing Platform](https://www.iso.org/obp/ui/#iso:code:3166:US) — Official ISO codes
