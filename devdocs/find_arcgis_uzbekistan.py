#!/usr/bin/env python3
"""
Script to find ArcGIS Server instances in Uzbekistan not listed in records.
Uses multiple discovery methods including Shodan, Censys, web search, and manual lists.
"""

import os
import sys
import json
import time
import requests
from urllib.parse import urlparse, urljoin
from collections import defaultdict

# Add scripts directory to path for yaml import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
import yaml

ROOT_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'entities', 'UZ')

# Common ArcGIS Server endpoint paths
ARCGIS_ENDPOINTS = [
    '/rest/info?f=pjson',
    '/rest/services?f=pjson',
    '/server/rest/info?f=pjson',
    '/server/rest/services?f=pjson',
    '/arcgis/rest/info?f=pjson',
    '/arcgis/rest/services?f=pjson',
    '/arcgis/services?wsdl',
    '/services?wsdl',
]

def load_existing_arcgis_servers():
    """Load all existing ArcGIS Server instances from Uzbekistan YAML files"""
    servers = []
    urls = set()
    domains = set()
    
    if not os.path.exists(ROOT_DIR):
        return servers, urls, domains
    
    for root, dirs, files in os.walk(ROOT_DIR):
        for filename in files:
            if filename.endswith('.yaml') or filename.endswith('.yml'):
                filepath = os.path.join(root, filename)
                try:
                    with open(filepath, 'r', encoding='utf8') as f:
                        data = yaml.safe_load(f)
                        if data:
                            software = data.get('software', {})
                            if software.get('id') == 'arcgisserver':
                                server_info = {
                                    'id': data.get('id', ''),
                                    'name': data.get('name', ''),
                                    'link': data.get('link', ''),
                                    'file': filepath
                                }
                                servers.append(server_info)
                                
                                # Normalize URL for comparison
                                url = data.get('link', '').lower().rstrip('/')
                                if url:
                                    urls.add(url)
                                    parsed = urlparse(url)
                                    if parsed.netloc:
                                        domains.add(parsed.netloc.lower())
                                
                                # Also check endpoints
                                endpoints = data.get('endpoints', [])
                                for endpoint in endpoints:
                                    endpoint_url = endpoint.get('url', '')
                                    if endpoint_url:
                                        parsed = urlparse(endpoint_url)
                                        base_url = f"{parsed.scheme}://{parsed.netloc}"
                                        urls.add(base_url.lower().rstrip('/'))
                                        domains.add(parsed.netloc.lower())
                except Exception as e:
                    print(f"Error reading {filepath}: {e}")
    
    return servers, urls, domains

def check_arcgis_server(url, timeout=10):
    """
    Check if a URL hosts an ArcGIS Server instance.
    Returns (is_arcgis, info_dict) where info_dict contains server details if found.
    """
    # Try common ArcGIS Server endpoints
    test_urls = []
    
    # Normalize URL
    url = url.rstrip('/')
    if not url.startswith('http'):
        url = f"https://{url}"
    
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    
    # Try different endpoint paths
    for endpoint in ARCGIS_ENDPOINTS:
        test_urls.append(urljoin(base_url, endpoint))
    
    # Also try the base URL directly
    test_urls.append(base_url)
    
    for test_url in test_urls:
        try:
            response = requests.get(test_url, timeout=timeout, allow_redirects=True, verify=False)
            if response.status_code == 200:
                # Check if response looks like ArcGIS Server
                content_type = response.headers.get('content-type', '').lower()
                
                # Check JSON responses
                if 'json' in content_type or test_url.endswith('?f=pjson'):
                    try:
                        data = response.json()
                        # ArcGIS Server JSON responses typically have specific structure
                        if isinstance(data, dict):
                            # Check for ArcGIS Server indicators
                            if 'currentVersion' in data or 'serverVersion' in data:
                                return True, {
                                    'url': test_url,
                                    'base_url': base_url,
                                    'info': data,
                                    'endpoint': test_url
                                }
                            # Check for services list structure
                            if 'services' in data or 'folders' in data:
                                return True, {
                                    'url': test_url,
                                    'base_url': base_url,
                                    'info': data,
                                    'endpoint': test_url
                                }
                    except json.JSONDecodeError:
                        pass
                
                # Check XML/WSDL responses
                if 'xml' in content_type or test_url.endswith('.wsdl'):
                    content = response.text.lower()
                    if 'arcgis' in content or 'esri' in content or 'geoservices' in content:
                        return True, {
                            'url': test_url,
                            'base_url': base_url,
                            'info': {'type': 'wsdl'},
                            'endpoint': test_url
                        }
                
                # Check HTML responses for ArcGIS Server indicators
                if 'html' in content_type:
                    content = response.text.lower()
                    if 'arcgis' in content and ('rest' in content or 'services' in content):
                        # Try to find the actual REST endpoint
                        import re
                        rest_match = re.search(r'(/.*?rest.*?services)', content)
                        if rest_match:
                            rest_path = rest_match.group(1)
                            rest_url = urljoin(base_url, rest_path)
                            return check_arcgis_server(rest_url, timeout)
                        return True, {
                            'url': test_url,
                            'base_url': base_url,
                            'info': {'type': 'html'},
                            'endpoint': test_url
                        }
        except requests.exceptions.RequestException:
            continue
        except Exception as e:
            continue
    
    return False, None

def search_shodan(query, api_key=None):
    """Search Shodan for ArcGIS Server instances in Uzbekistan"""
    if not api_key:
        print("  Note: Shodan API key not provided. Skipping Shodan search.")
        print("  To use Shodan, set SHODAN_API_KEY environment variable.")
        return []
    
    try:
        import shodan
        api = shodan.Shodan(api_key)
        results = api.search(query, page=1)
        servers = []
        for result in results.get('matches', []):
            ip = result.get('ip_str', '')
            hostnames = result.get('hostnames', [])
            port = result.get('port', 443)
            
            # Prefer hostnames over IPs
            if hostnames:
                for hostname in hostnames:
                    if '.uz' in hostname.lower():
                        url = f"https://{hostname}"
                        servers.append(url)
            elif ip:
                # Try common ports
                if port == 443:
                    url = f"https://{ip}"
                else:
                    url = f"https://{ip}:{port}"
                servers.append(url)
        
        return servers
    except ImportError:
        print("  Note: shodan library not installed. Install with: pip install shodan")
        return []
    except Exception as e:
        print(f"  Error searching Shodan: {e}")
        return []

def search_censys(query, api_id=None, api_secret=None):
    """Search Censys for ArcGIS Server instances in Uzbekistan"""
    if not api_id or not api_secret:
        print("  Note: Censys API credentials not provided. Skipping Censys search.")
        print("  To use Censys, set CENSYS_API_ID and CENSYS_API_SECRET environment variables.")
        return []
    
    try:
        from censys.search import CensysHosts
        
        censys = CensysHosts(api_id=api_id, api_secret=api_secret)
        results = censys.search(query, per_page=100)
        
        servers = []
        for result in results:
            services = result.get('services', [])
            for service in services:
                if service.get('service_name') == 'HTTP' or service.get('service_name') == 'HTTPS':
                    port = service.get('port', 443)
                    hostnames = result.get('dns', {}).get('names', [])
                    
                    if hostnames:
                        for hostname in hostnames:
                            if '.uz' in hostname.lower():
                                if port == 443:
                                    url = f"https://{hostname}"
                                else:
                                    url = f"https://{hostname}:{port}"
                                servers.append(url)
        
        return servers
    except ImportError:
        print("  Note: censys library not installed. Install with: pip install censys")
        return []
    except Exception as e:
        print(f"  Error searching Censys: {e}")
        return []

def get_potential_domains():
    """Get a list of potential Uzbekistan government domains that might host ArcGIS Server"""
    # Common patterns for Uzbekistan government domains
    domains = [
        # Known government agencies
        'gis.gov.uz',
        'geoportal.gov.uz',
        'map.gov.uz',
        'geodata.gov.uz',
        'gis.uz',
        'geoportal.uz',
        
        # Ministry patterns
        'gis.minagro.uz',
        'gis.mintrans.uz',
        'gis.mineconomy.uz',
        'gis.minhealth.uz',
        'gis.minedu.uz',
        'gis.minwater.uz',
        'gis.minecology.uz',
        'gis.mininnovation.uz',
        
        # Agency patterns
        'gis.stat.uz',
        'gis.uzstat.uz',
        'gis.uzgeodez.uz',
        'gis.uzgeokadastr.uz',
        'gis.uzmeteo.uz',
        'gis.uzgeology.uz',
        'gis.uzgeoinform.uz',
        'gis.uzgeocadastr.uz',
        
        # State committee patterns
        'gis.uzstat.uz',
        'gis.uzgeodez.uz',
        'gis.uzmeteo.uz',
        
        # Regional patterns
        'gis.tashkent.uz',
        'gis.samarkand.uz',
        'gis.bukhara.uz',
        'gis.andijan.uz',
        'gis.fergana.uz',
        'gis.namangan.uz',
        
        # Alternative patterns
        'arcgis.gov.uz',
        'server.arcgis.uz',
        'geoserver.gov.uz',
        'geoportal.egov.uz',
        'gis.egov.uz',
        
        # Known variations from existing records
        'gis.agro.uz',  # Already known, but checking variations
        'gis.boshplan.uz',  # Already known
        'db.ngis.uz',  # Already known
        'open.ngis.uz',  # Already known
    ]
    
    return domains

def search_web_for_arcgis_uzbekistan():
    """Search the web for ArcGIS Server instances in Uzbekistan"""
    # This would use a search API like Google Custom Search, Bing, etc.
    # For now, return manual research suggestions
    print("  Note: Web search requires API keys (Google Custom Search, Bing, etc.)")
    print("  Manual research suggestions:")
    print("    - Search Google: 'ArcGIS Server site:.uz'")
    print("    - Search Google: 'geoportal Uzbekistan'")
    print("    - Search Google: 'GIS services Uzbekistan'")
    print("    - Check government ministry websites")
    print("    - Review Open Data Inception list")
    
    # Return empty list for now - can be enhanced with actual API calls
    return []

def main():
    print("=" * 80)
    print("ArcGIS Server Discovery for Uzbekistan")
    print("=" * 80)
    
    # Load existing servers
    print("\n1. Loading existing ArcGIS Server records...")
    existing_servers, existing_urls, existing_domains = load_existing_arcgis_servers()
    print(f"   Found {len(existing_servers)} existing ArcGIS Server instances:")
    for server in existing_servers:
        print(f"     - {server['name']} ({server['link']})")
    
    # Discovery methods
    discovered_urls = set()
    
    # Method 1: Shodan search
    print("\n2. Searching Shodan...")
    shodan_api_key = os.environ.get('SHODAN_API_KEY')
    shodan_query = 'country:UZ "ArcGIS Server" OR "arcgis/rest" OR "rest/services"'
    shodan_results = search_shodan(shodan_query, shodan_api_key)
    discovered_urls.update(shodan_results)
    print(f"   Found {len(shodan_results)} potential servers from Shodan")
    
    # Method 2: Censys search
    print("\n3. Searching Censys...")
    censys_api_id = os.environ.get('CENSYS_API_ID')
    censys_api_secret = os.environ.get('CENSYS_API_SECRET')
    censys_query = 'services.service_name: HTTP AND location.country_code: UZ AND (services.banner: "ArcGIS" OR services.banner: "rest/services")'
    censys_results = search_censys(censys_query, censys_api_id, censys_api_secret)
    discovered_urls.update(censys_results)
    print(f"   Found {len(censys_results)} potential servers from Censys")
    
    # Method 3: Test potential domains
    print("\n4. Testing potential Uzbekistan government domains...")
    potential_domains = get_potential_domains()
    domain_results = []
    for domain in potential_domains:
        is_arcgis, info = check_arcgis_server(domain)
        if is_arcgis:
            domain_results.append(info['base_url'] if info else domain)
            discovered_urls.add(info['base_url'] if info else domain)
        time.sleep(0.5)  # Be respectful with requests
    print(f"   Found {len(domain_results)} ArcGIS Server instances from domain testing")
    
    # Method 4: Web search
    print("\n5. Web search (manual research recommended)...")
    web_results = search_web_for_arcgis_uzbekistan()
    discovered_urls.update(web_results)
    
    # Method 5: Known patterns (manual list)
    print("\n6. Checking known patterns...")
    # These could be discovered through web research
    # Based on web search, Esri has a distributor in Uzbekistan (Data+ International)
    # and government agencies like Ministry of Agriculture and State Cadastre Agency use GIS
    known_patterns = [
        # Add any known or suspected ArcGIS Server URLs here
        # Example URLs discovered through research:
        # 'https://gis.example.gov.uz/server/rest/services',
        # 'https://geoportal.example.uz/arcgis/rest/services',
    ]
    for pattern_url in known_patterns:
        if pattern_url:  # Only add non-empty URLs
            discovered_urls.add(pattern_url)
    
    # Verify discovered servers
    print("\n7. Verifying discovered servers...")
    verified_servers = []
    for url in discovered_urls:
        # Check if already in registry
        url_normalized = url.lower().rstrip('/')
        parsed = urlparse(url_normalized)
        domain = parsed.netloc.lower()
        
        # Skip if already recorded
        if url_normalized in existing_urls or domain in existing_domains:
            continue
        
        # Verify it's actually an ArcGIS Server
        print(f"   Checking: {url}")
        is_arcgis, info = check_arcgis_server(url)
        if is_arcgis and info:
            verified_servers.append({
                'url': info['base_url'],
                'endpoint': info.get('endpoint', ''),
                'info': info.get('info', {})
            })
            print(f"     ✓ Verified ArcGIS Server")
        else:
            print(f"     ✗ Not an ArcGIS Server or unreachable")
        time.sleep(1)  # Be respectful with requests
    
    # Report results
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)
    print(f"\nExisting ArcGIS Server instances: {len(existing_servers)}")
    print(f"Discovered potential servers: {len(discovered_urls)}")
    print(f"Verified new ArcGIS Server instances: {len(verified_servers)}")
    
    if verified_servers:
        print("\n" + "-" * 80)
        print("NEW ARCGIS SERVER INSTANCES NOT IN REGISTRY:")
        print("-" * 80)
        for i, server in enumerate(verified_servers, 1):
            print(f"\n{i}. {server['url']}")
            print(f"   Endpoint: {server['endpoint']}")
            if isinstance(server['info'], dict) and 'currentVersion' in server['info']:
                print(f"   Version: {server['info'].get('currentVersion', 'Unknown')}")
            if isinstance(server['info'], dict) and 'serverVersion' in server['info']:
                print(f"   Server Version: {server['info'].get('serverVersion', 'Unknown')}")
    else:
        print("\nNo new ArcGIS Server instances found.")
    
    # Save results
    report = {
        'existing_count': len(existing_servers),
        'existing_servers': [{'name': s['name'], 'link': s['link']} for s in existing_servers],
        'discovered_count': len(discovered_urls),
        'verified_count': len(verified_servers),
        'verified_servers': verified_servers,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
    
    report_file = 'uzbekistan_arcgis_discovery.json'
    with open(report_file, 'w', encoding='utf8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\nReport saved to: {report_file}")
    
    # Recommendations
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    print("\n1. Manual verification:")
    print("   - Visit each discovered URL to verify it's publicly accessible")
    print("   - Check if the server contains useful geospatial data")
    print("   - Identify the organization/agency that operates the server")
    print("\n2. Additional discovery methods:")
    print("   - Search government websites for GIS/geoportal sections")
    print("   - Check ministry and agency websites for data/GIS links")
    print("   - Review Open Data Inception list for Uzbekistan")
    print("   - Search for 'ArcGIS' or 'geoportal' on .uz domains")
    print("\n3. To use automated search tools:")
    print("   - Set SHODAN_API_KEY environment variable for Shodan search")
    print("   - Set CENSYS_API_ID and CENSYS_API_SECRET for Censys search")
    print("   - Install required libraries: pip install shodan censys")

if __name__ == '__main__':
    # Suppress SSL warnings for self-signed certificates
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    main()

