# CKAN Instances in Japan - Discovery Report

## Summary

This report documents CKAN instances in Japan that are **NOT** currently listed in the registry.

## Discovery Method

1. Loaded all existing CKAN URLs from Japan records in the registry
2. Tested potential CKAN instances by checking for `/api/3/action/package_list` endpoint
3. Verified instances are actually CKAN by checking API response format

## Found CKAN Instances NOT in Registry

### 1. Miyazaki Prefecture CKAN Portal
- **URL**: https://ckan.pref.miyazaki.lg.jp
- **API Endpoint**: https://ckan.pref.miyazaki.lg.jp/api/3/action/package_list
- **Status**: ✅ Verified as CKAN instance
- **Note**: Different from existing `data.stat.pref.miyazaki.lg.jp` record (which is already in registry)

## Existing CKAN Instances in Registry

The registry currently contains **53 CKAN instances** from Japan, including:

- Federal level:
  - data.e-gov.go.jp (Japan e-gov data portal)
  - data.env.go.jp (Ministry of the Environment)
  - ckan.odp.jig.jp
  - ckan.odpt.org (Public Transport Open Data Center)
  - www.geospatial.jp/ckan (Japan Geospatial Information Center)

- Prefecture level:
  - ckan.pref.akita.lg.jp (Akita)
  - ckan.pref.shizuoka.jp (Shizuoka)
  - gifu-opendata.pref.gifu.lg.jp (Gifu)
  - opendata.pref.saitama.lg.jp (Saitama)
  - yamaguchi-opendata.jp (Yamaguchi)
  - kmi-ckan.pref.kochi.lg.jp (Kochi)
  - data.stat.pref.miyazaki.lg.jp (Miyazaki - statistics portal)
  - And many more...

- City level:
  - Multiple cities including Tokyo, Yokohama, Osaka, Kobe, Chiba, etc.

## URLs Tested But Not Found

The following URLs were tested but either:
- Do not exist (DNS resolution failed)
- Are not CKAN instances
- Had connection errors

Most of these were speculative URLs based on common patterns. The errors suggest these domains don't exist or aren't accessible.

## Recommendations

1. **Add the discovered instance**: `ckan.pref.miyazaki.lg.jp` should be added to the registry
2. **Further investigation**: Check if there are other CKAN instances by:
   - Checking the fukuno.jig.jp/app/odp/ckan.html list (if accessible)
   - Reviewing BuiltWith's list of CKAN sites in Japan
   - Manual verification of prefecture and city websites
3. **Note on Miyazaki**: There appear to be two different CKAN instances:
   - `data.stat.pref.miyazaki.lg.jp` (already in registry - statistics portal)
   - `ckan.pref.miyazaki.lg.jp` (newly discovered - general open data portal)

## Next Steps

1. Verify the discovered instance manually
2. Gather more information about `ckan.pref.miyazaki.lg.jp`:
   - Owner information
   - Description
   - Topics/categories
   - Language settings
3. Create YAML record for the new instance
4. Continue searching for other potential instances using alternative methods

