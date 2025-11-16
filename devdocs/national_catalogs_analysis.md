# Analysis: Countries Without National Data Catalogs

## Summary

- **Total countries analyzed**: 191
- **Countries without national catalogs**: 104
- **Countries with federal/central catalogs to mark**: 104
- **Total catalogs suggested to mark as national**: 629

## Top Countries by Number of Federal/Central Catalogs (without is_national flag)

### Countries with 10+ Federal/Central Catalogs

1. **Turkey (TR)** - 14 catalogs
   - Multiple statistical institute catalogs (TÜİK)
   - Central Bank data portal
   - Health ministry open data portal
   - National smart city platform

2. **Tunisia (TN)** - 14 catalogs
   - National Statistical Institute (NADA)
   - Multiple government open data portals
   - Transport, agriculture, industry data portals

3. **Palestine (PS)** - 13 catalogs
   - Palestinian Central Bureau of Statistics
   - Multiple ministry geoportals
   - Environment and energy authority portals

4. **Mozambique (MZ)** - 10 catalogs
   - National Data Archive
   - Multiple GIS and geoportal services
   - SDG data hub

5. **Armenia (AM)** - 10 catalogs
   - ArmStat microdata and statistics
   - SDG data portal
   - Multiple academic and government geoportals

### Countries with 5-9 Federal/Central Catalogs

- **Kenya (KE)** - 8 catalogs
- **Lebanon (LB)** - 8 catalogs
- **Hungary (HU)** - 8 catalogs
- **Egypt (EG)** - 8 catalogs
- **Laos (LA)** - 8 catalogs
- **El Salvador (SV)** - 7 catalogs
- **Cameroon (CM)** - 3 catalogs

### Countries with 1-2 Federal/Central Catalogs

Many smaller countries have 1-2 federal/central catalogs that should be marked as national:
- Pakistan (PK) - 2 catalogs
- Seychelles (SC) - 2 catalogs
- Vanuatu (VU) - 2 catalogs
- Niue (NU) - 2 catalogs
- Nicaragua (NI) - 2 catalogs
- Kuwait (KW) - 2 catalogs
- Cuba (CU) - 2 catalogs
- Turks and Caicos (TC) - 2 catalogs
- South Sudan (SS) - 2 catalogs
- And many more...

## Recommendations

### High Priority (Countries with multiple federal catalogs)

These countries have clear national-level catalogs that should be marked:

1. **Turkey (TR)**: Mark all TÜİK (Turkish Statistical Institute) catalogs and central government portals as national
2. **Tunisia (TN)**: Mark National Statistical Institute and main government data portals as national
3. **Palestine (PS)**: Mark Palestinian Central Bureau of Statistics and main ministry portals as national
4. **Mozambique (MZ)**: Mark National Data Archive and main government GIS portals as national
5. **Armenia (AM)**: Mark ArmStat catalogs and SDG portal as national

### Medium Priority (Countries with 3-8 federal catalogs)

Countries like Kenya, Lebanon, Hungary, Egypt, and Laos have several federal catalogs that should be reviewed and marked as national.

### Low Priority (Countries with 1-2 catalogs)

Smaller countries with single or dual federal catalogs should have those marked as national if they represent the primary national data infrastructure.

## Criteria for Marking as National

Catalogs should be marked as `is_national: true` if they:
1. Are owned by "Central government" or "Federal government"
2. Are located in a "Federal" directory path
3. Have coverage level 20 (national level)
4. Represent the primary national statistical or data infrastructure
5. Are official government data portals at the national level

## Next Steps

1. Review the suggested catalogs for each country
2. Verify that they represent true national-level data infrastructure
3. Update the YAML files to add `properties.is_national: true` where appropriate
4. Focus on countries with multiple federal catalogs first (highest impact)

