# Configure catalog search tools for agents and LLMs

This page is how to **wire search tools into Cursor, ChatGPT, Claude, and similar agents** so they can (1) look up catalogs **already in this registry** and (2) **discover** installations that are not registered yet.

Query recipes (Google operators, Censys CenQL, Shodan filters): [discovery-search-tools.md](discovery-search-tools.md). Agent checklist: [agents/discover.md](agents/discover.md). This repository does **not** ship a production search API or MCP server — see [when-to-use.md](when-to-use.md).

## Two jobs, two tool stacks

| Job | Data the agent needs | Typical tools |
|-----|----------------------|---------------|
| **Query** catalogs already in the registry | DuckDB / Parquet / JSONL in this repo, or [dateno-api](https://github.com/datenoio/dateno-api) | Local files, SQL, optional HTTP API |
| **Discover** catalogs not yet registered | Web search + internet maps + a live GET to confirm the site | Google (or CSE/Brave), Censys, Shodan, FOFA, URLScan, crt.sh, browser |

Do not mix them up. Searching Google for “CKAN Portugal” does not tell you whether `dados.gov.pt` is already in `data/datasets/datasets.duckdb`. Duplicate-check exports **before** adding YAML ([agents/query.md](agents/query.md)).

## What to install (minimum vs full)

| Setup | Query this registry | Discover new catalogs |
|-------|---------------------|------------------------|
| **Minimum (Cursor in this repo)** | Files + DuckDB; follow [llms.txt](https://github.com/datenoio/dataportals-registry/blob/main/llms.txt) | Cursor **web search** + **browser**; paste queries from [discovery-search-tools.md](discovery-search-tools.md) |
| **Minimum (ChatGPT / Claude in the browser)** | Upload `llms.txt` + a DuckDB/CSV extract, or use dateno-api | Built-in **web search** / browsing; paste the [shared instructions](#shared-system-instructions) |
| **Full agent stack** | Same as minimum, plus dateno-api if you need HTTP | Official **Censys MCP**; Google **Programmable Search** or Brave/Tavily; optional Shodan / URLScan keys |

Start with the minimum. Add Censys MCP when Google stops listing sites (no inbound links, IP-only GeoServer, certificate names).

## Shared system instructions

Paste this into a Custom GPT, ChatGPT Project, Claude Project, Gemini Gem, Cursor user rule, or any other agent instruction field. Point the model at the live docs, not a stale copy.

```text
You help maintain dataportals-registry (https://github.com/datenoio/dataportals-registry).

Read https://datenoio.github.io/dataportals-registry/llms.txt first.
Query existing catalogs from exports (DuckDB/Parquet/JSONL), never by walking data/entities/**/*.yaml.
Discover missing catalogs with docs/agents/discover.md and docs/discovery-search-tools.md.
Scope every hunt: one country, city, TLD, or software.id. No internet-wide scans.

For each candidate: duplicate-check hostname; confirm a public catalog UI or harvestable API;
set software.id only with two matching signals (else custom); do not invent uid.
Stop on HTTP 401/403. Do not follow login forms or guess API keys.
Register the catalog homepage, not a single dataset URL.
Prefer add-single --scheduled, then assign and validate-yaml --id {id}.

Search-tool config: docs/discovery-agent-tools.md
Platform fingerprints: docs/discovery-opendata.md, discovery-geoportals.md,
discovery-scientific.md, discovery-metadata.md, discovery-indicators.md.
Software ID map: docs/software-index.md.
```

Keep secrets **out** of these instructions. Put API keys in the client’s env / MCP config / GPT Action auth, never in git.

## Secrets and environment variables

Do not commit keys. Use the user environment, Cursor MCP `env`, ChatGPT Action authentication, or a local `.env` that is gitignored.

| Variable | Tool | Where to get it |
|----------|------|-----------------|
| `GOOGLE_CSE_API_KEY` | Google Custom Search JSON API | [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Credentials |
| `GOOGLE_CSE_CX` | Same (search engine id) | [Programmable Search Engine](https://programmablesearchengine.google.com/) → Setup → Basics |
| `BRAVE_API_KEY` | Brave Search API (agent-friendly Google alternative) | [Brave Search API](https://brave.com/search/api/) |
| `TAVILY_API_KEY` | Tavily Search (LLM-oriented) | [Tavily](https://tavily.com/) |
| `CENSYS_PAT` | Censys Platform API (header auth) | Censys Platform → Account → Personal access token |
| `CENSYS_ORG_ID` | Censys org header | Censys Platform organization settings |
| `SHODAN_API_KEY` | Shodan | [account.shodan.io](https://account.shodan.io) |
| `FOFA_EMAIL` / `FOFA_KEY` | FOFA | [en.fofa.info](https://en.fofa.info) account |
| `URLSCAN_API_KEY` | urlscan.io Search API | [urlscan.io/user/signup](https://urlscan.io/user/signup) |

crt.sh needs no key. Cursor’s built-in web search and ChatGPT browsing need no extra key.

## Cursor

Cursor already has this repository’s [AGENTS.md](https://github.com/datenoio/dataportals-registry/blob/main/AGENTS.md) and [docs/agents/](agents/query.md). Open the **dataportals-registry** folder as the workspace so the agent can read exports and write YAML.

### Built-in tools (no API key)

1. **Web search** — ask the agent to run the Google queries from [discovery-search-tools.md](discovery-search-tools.md) (or Bing/DuckDuckGo if Google is blocked).
2. **Browser** — open a candidate URL, confirm it is a catalog, copy the homepage `link`.
3. **Terminal** — duplicate-check DuckDB; `add-single --scheduled`; `validate-yaml --id`.
4. **Rules** — project rule `.cursor/rules/catalog-discovery.mdc` attaches when the task is discovery. User rules can paste the [shared instructions](#shared-system-instructions) if you want them in every chat.

Example prompt:

```text
Discover CKAN open data portals in Portugal that are not in this registry.
Follow docs/agents/discover.md. Duplicate-check datasets.duckdb first.
Use Google queries from docs/discovery-opendata.md, then confirm
/api/3/action/status_show. Add verified finds with add-single --scheduled.
```

### MCP in Cursor

Cursor Settings → **Tools & MCP** → **New MCP server**, or edit `~/.cursor/mcp.json` (user-wide) or `.cursor/mcp.json` in a project.

- Prefer **user-wide** `~/.cursor/mcp.json` for API keys so they are not committed.
- If you add a project `.cursor/mcp.json`, store only public URLs (for example the Censys OAuth MCP). Never put a personal access token in the repo.
- After saving, reload MCP. CLI: `agent mcp list` (see [Cursor MCP docs](https://cursor.com/docs/mcp)).

**Censys (official, recommended for discovery beyond Google)**

OAuth (preferred). Cursor opens a consent page; pick your Censys organization. Calls count against [Censys credits](https://docs.censys.com/docs/censys-credits).

```json
{
  "mcpServers": {
    "censys-platform": {
      "url": "https://mcp.platform.censys.io/platform/mcp/"
    }
  }
}
```

Header auth (only in **user** `mcp.json`, not in git):

```json
{
  "mcpServers": {
    "censys-platform": {
      "url": "https://mcp.platform.censys.io/platform/mcp/",
      "headers": {
        "X-Organization-ID": "your-organization-id",
        "Authorization": "Bearer your-censys-personal-access-token"
      }
    }
  }
}
```

Official docs: [Platform MCP Server](https://docs.censys.com/docs/platform-mcp-server). For catalog discovery, ask the agent to use **web property** search (`generate_and_search_query` / CenQL on `web.endpoints.http.html_title` and `web.names`), not `discover_attack_surface` or the Adversary Investigation MCP. Those tools are for security hunting, not registry work.

**Shodan (optional, third-party MCP)**

There is no official Shodan MCP. Community servers exist (search GitHub for `shodan-mcp`). Review the code before installing. Typical pattern in **user** `mcp.json`:

```json
{
  "mcpServers": {
    "shodan": {
      "command": "npx",
      "args": ["-y", "mcp-shodan"],
      "env": {
        "SHODAN_API_KEY": "your-shodan-api-key"
      }
    }
  }
}
```

Package names vary. If you do not want a third-party MCP, have Cursor run the [Shodan CLI](#shodan) in the terminal instead.

**Google**

Do not add a “Google scrape” MCP. Use Cursor web search, or configure [Programmable Search](#google-programmable-search-json-api) and let the agent `curl` it with keys from the environment.

### Cursor Cloud Agents

Cloud agents do **not** automatically inherit your laptop `~/.cursor/mcp.json` or local API keys. Give them the GitHub repo, `llms.txt`, and tell them to use public web search. Attach Censys only if you add a **project** MCP with OAuth (no PAT in the repo) or run discovery locally.

## ChatGPT app

ChatGPT (chatgpt.com and the desktop/mobile app) cannot see your local `data/datasets/` tree. Give it either (a) web search + this project’s published docs, or (b) an HTTP API / uploaded extract.

### Everyday chat (web search)

On a paid plan, enable **web search** (or browsing) in the composer. Start the thread with:

```text
Read https://datenoio.github.io/dataportals-registry/llms.txt
and https://datenoio.github.io/dataportals-registry/docs/discovery-search-tools
Find GeoNetwork catalogs in Czechia not obviously on the GeoNetwork gallery.
Return name, URL, proposed software.id, and why you think it is a catalog.
Do not invent registry uids. I will duplicate-check in the repo.
```

Paste DuckDB hits if you already searched locally, so ChatGPT does not propose duplicates.

### ChatGPT Projects

Create a Project. Add the shared instructions above. Upload (or link):

- `llms.txt`
- `docs/agents/discover.md`
- `docs/discovery-search-tools.md`
- Optionally a CSV/JSONL slice of catalogs for one country (`id`, `name`, `link`, `software`)

Projects persist that context across chats. Still use web search for live discovery.

### Custom GPT

1. [chatgpt.com/gpts/editor](https://chatgpt.com/gpts/editor) → **Create**.
2. **Instructions**: paste the [shared system instructions](#shared-system-instructions).
3. **Knowledge**: upload the same files as for Projects. Keep them updated when docs change.
4. **Capabilities**: enable **Web Search**. Do not enable image generation for this job.
5. **Actions** (optional): add an OpenAPI action for Google CSE, Censys, or Shodan ([examples below](#custom-gpt-actions-openapi)). Store keys in the GPT’s authentication, not in the spec.
6. **Conversation starters**: “Find CKAN portals in Kenya”, “GeoNetwork in the Balkans”, “Dataverse installations missing from the registry”.

Custom GPTs cannot run `builder.py`. The GPT should return a candidate table; you (or Cursor) write YAML in the repo.

### Developer Mode and MCP connectors

ChatGPT can attach **remote** MCP servers (HTTPS). Local `stdio` MCP (a command on your laptop) is not supported.

1. Enable **Developer Mode** (Settings → Apps / Advanced — paid plans; write-capable MCP is limited on some consumer plans).
2. Add a connector with URL `https://mcp.platform.censys.io/platform/mcp/` and complete Censys OAuth.
3. In a chat, attach the Censys app/connector from the **+** menu and ask for web-property searches scoped by country.

OpenAI help: [Developer mode and MCP apps](https://help.openai.com/en/articles/12584461-developer-mode-and-mcp-apps-in-chatgpt). ChatGPT **Apps** in the directory are vendor-reviewed; Censys is added as a **custom connector**, not as an OpenAI-built plugin (plugins were retired).

### Custom GPT Actions (OpenAPI)

Example Google Custom Search action (replace `{cx}` or pass `cx` as a parameter). Authentication: API key query param `key`.

```yaml
openapi: 3.0.1
info:
  title: Google Custom Search
  version: 1.0.0
servers:
  - url: https://www.googleapis.com/customsearch
paths:
  /v1:
    get:
      operationId: googleCustomSearch
      summary: Search the web with a Programmable Search Engine
      parameters:
        - name: q
          in: query
          required: true
          schema: { type: string }
        - name: cx
          in: query
          required: true
          schema: { type: string }
        - name: num
          in: query
          schema: { type: integer, default: 10, maximum: 10 }
      responses:
        "200":
          description: Search results
```

In the GPT Action auth UI, choose API key, header or query `key`, and paste `GOOGLE_CSE_API_KEY`. Put your engine id in instructions: “Always pass `cx=YOUR_CX`.”

For Censys, prefer the official MCP connector over hand-written OpenAPI; the Platform schema is large and versioned at [api.platform.censys.io](https://docs.censys.com/reference/get-started).

## Claude (Desktop, Claude Code, Projects)

**Claude.ai Projects**: same pattern as ChatGPT Projects — paste shared instructions, add knowledge files / links to `llms.txt` and the discovery guides, enable web search.

**Claude Desktop MCP** (`~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "censys-platform": {
      "url": "https://mcp.platform.censys.io/platform/mcp/"
    }
  }
}
```

**Claude Code**:

```bash
claude mcp add censys-platform https://mcp.platform.censys.io/platform/mcp/
```

Then open this repository so Claude Code can duplicate-check DuckDB and add YAML.

## Other LLM clients

| Client | How to attach this registry | How to attach search tools |
|--------|-----------------------------|----------------------------|
| **VS Code Copilot** | Open the repo; Copilot reads `AGENTS.md` if present | `.vscode/mcp.json` with the same Censys URL |
| **Continue.dev** | Index the repo | `mcpServers` in Continue config ([Censys example](https://docs.censys.com/docs/platform-mcp-server)) |
| **Windsurf / Cline** | Open the repo | User MCP JSON, same schema as Cursor |
| **Gemini Gems** | Paste shared instructions; link `llms.txt` | Built-in Google Search grounding (strong for Google dorks); no Censys unless you add an API call in a larger app |
| **Perplexity** | Link the docs site | Built-in web search; paste candidate URLs back into Cursor for YAML |
| **Open WebUI / local models** | RAG over `docs/` + `llms.txt` | Add CSE/Brave/Censys as HTTP tools in the tool registry |

## Configure each search tool

Query syntax lives in [discovery-search-tools.md](discovery-search-tools.md). This section is **accounts, APIs, and how agents should call them**.

### Google (and Bing / DuckDuckGo)

**For Cursor and ChatGPT**, built-in web search is enough for most hunts. Tell the agent the exact query string (`"Powered by CKAN" inurl:/dataset site:.pt`).

Do **not** have agents scrape `google.com/search` HTML (ToS, captchas, brittle). If you need stable JSON for automation, use Programmable Search, Brave, or Tavily.

#### Google Programmable Search JSON API

1. Create a [Programmable Search Engine](https://programmablesearchengine.google.com/controlpanel/create).
2. Under **Setup → Basics**, turn **Search the entire web** on (otherwise you only search sites you listed).
3. Copy the **Search engine ID** (`cx`).
4. In Google Cloud, enable **Custom Search API** and create an API key. Restrict the key to that API.
5. Free quota is small (on the order of 100 queries/day unless you enable billing). Scope hunts tightly.

```bash
curl -sS "https://www.googleapis.com/customsearch/v1" \
  --get \
  --data-urlencode "key=$GOOGLE_CSE_API_KEY" \
  --data-urlencode "cx=$GOOGLE_CSE_CX" \
  --data-urlencode "q=Powered by CKAN inurl:/dataset site:.pt" \
  --data-urlencode "num=10"
```

Agent rule: one query per country×software, `num≤10`, then a human or browser confirms the catalog root. Parse `items[].link` and `items[].title` only.

#### Brave Search API

Useful when you want JSON without building a CSE. Header `X-Subscription-Token: $BRAVE_API_KEY`.

```bash
curl -sS "https://api.search.brave.com/res/v1/web/search" \
  -H "X-Subscription-Token: $BRAVE_API_KEY" \
  --get \
  --data-urlencode "q=intitle:GeoNetwork site:.cz"
```

#### Tavily

LLM-oriented search (`POST https://api.tavily.com/search` with `api_key` and `query`). Good inside custom agents; still duplicate-check this registry afterward.

### Censys

1. Create a [Censys Platform](https://platform.censys.io) account. API search is a **paid/credit** capability on most plans; Free may only allow lookups, not search — check [Get started with Censys APIs](https://docs.censys.com/reference/get-started).
2. Grant your user the **API Access** role for the organization.
3. Prefer **OAuth MCP** in Cursor / Claude / ChatGPT Developer Mode.
4. For raw HTTP, create a **personal access token**. Base URL: `https://api.platform.censys.io/v3/global/`.

```bash
curl -sS "https://api.platform.censys.io/v3/global/search/query" \
  -H "Authorization: Bearer $CENSYS_PAT" \
  -H "X-Organization-ID: $CENSYS_ORG_ID" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "web.location.country_code = \"PT\" and web.endpoints.http.body: \"Powered by CKAN\"",
    "page_size": 25
  }'
```

Agent rules:

- Search **web properties** for catalog UIs; use **hosts** for GeoServer/ArcGIS product hits; use **certificates** for `opendata.` names.
- `page_size` ≤ 25 per call. Do not page through the entire internet.
- Convert hostnames to `https://{name}/`, then probe. Never set `link` to a bare IP.
- MCP helpers `generate_query` and `validate_censys_query` are useful; `investigate_host` is optional and expensive — skip it unless you need to disambiguate one IP.

Legacy Search (`search.censys.io` / v1 API) is a different product. New integrations should use Platform CenQL ([query language](https://docs.censys.com/docs/censys-query-language)).

### Shodan

1. Create an account and copy the API key from [account.shodan.io](https://account.shodan.io).
2. Search filters consume **query credits** on most plans. Use `shodan count` before `shodan search`.

```bash
pip install -U --user shodan
shodan init "$SHODAN_API_KEY"
shodan count 'http.title:"CKAN" country:PT'
shodan search --limit 20 'http.title:"CKAN" country:PT'
```

REST:

```bash
curl -sS "https://api.shodan.io/shodan/host/search" \
  --get \
  --data-urlencode "key=$SHODAN_API_KEY" \
  --data-urlencode "query=http.title:\"GeoNetwork\" country:CZ"
```

Agent rules: prefer `hostname` / SSL CN in the banner over `ip_str` for `link`. Skip results with no HTTP title. Do not run `shodan scan` (active scanning) for this registry.

### FOFA

East Asian coverage. Encode the query as Base64 (`qbase64`). Agent must not log the key.

```bash
python - <<'PY'
import base64, os, urllib.parse, urllib.request
q = 'title="CKAN" && country="PT"'
qb = base64.b64encode(q.encode()).decode()
url = (
    "https://fofa.info/api/v1/search/all?"
    + urllib.parse.urlencode({
        "email": os.environ["FOFA_EMAIL"],
        "key": os.environ["FOFA_KEY"],
        "qbase64": qb,
        "size": 20,
    })
)
print(urllib.request.urlopen(url).read().decode()[:2000])
PY
```

### urlscan.io

Good for recently crawled Hub and OpenDataSoft sites.

```bash
curl -sS "https://urlscan.io/api/v1/search/" \
  -H "api-key: $URLSCAN_API_KEY" \
  --get \
  --data-urlencode "q=page.title:CKAN" \
  --data-urlencode "size=20"
```

Use `page.url` / `page.domain` from results. Search-only is enough; do not mass-submit live scans of government sites.

### crt.sh (no key)

```bash
curl -sS "https://crt.sh/?q=%.opendata.pt&output=json"
```

Treat names as **leads**. Resolve HTTPS and confirm a catalog UI.

## Which tool the agent should call

```text
User names a country / city / software
        │
        ├─► DuckDB / Parquet duplicate-check
        │
        ├─► Vendor lists (CKAN ecosystem, GeoNetwork gallery, Dataverse JSON, …)
        │
        ├─► Web search (Cursor/ChatGPT/CSE/Brave) with platform queries
        │
        ├─► If thin results: Censys web properties (MCP or API)
        │         optional Shodan / FOFA / URLScan / crt.sh
        │
        └─► Browser or GET: confirm catalog + software probe
                  then add-single --scheduled (in the git workspace)
```

If the agent has **no** Censys/Shodan credentials, it must still finish the Google + vendor-list path and report “Censys not configured” rather than inventing hosts.

## Example: Cursor vs ChatGPT for the same hunt

**Cursor (repo open, Censys MCP connected)**

```text
Find unregistered OpenDataSoft portals in Belgium.
1) SQL duplicate-check on link like '%opendatasoft%' and '%belgium%' / '.be'
2) Google: inurl:/explore site:.be OpenDataSoft
3) Censys MCP: web.names: ".be" and web.endpoints.http.body: "OpenDataSoft"
4) Confirm /api/explore/v2.1/catalog/datasets
5) add-single --scheduled for new hosts only
```

**ChatGPT (no local repo, web search + docs)**

```text
Follow https://datenoio.github.io/dataportals-registry/docs/discovery-opendata
Search for OpenDataSoft portals in Belgium. Return a markdown table:
name | url | evidence | likely duplicate of data.gov.be?
I will check the registry myself. Do not invent uids.
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Agent walks thousands of YAML files | Ignored `llms.txt` | Point it at exports; repeat [agents/query.md](agents/query.md) |
| Censys MCP does nothing | No API role / OAuth not completed / Free plan has no search | Finish consent; check credits; fall back to Google |
| `401` from Censys API | Missing PAT or org header | Use MCP OAuth, or set both `Authorization` and `X-Organization-ID` |
| Google CSE returns only your site | Entire-web toggle off | Enable “Search the entire web” on the engine |
| CSE `403` | API not enabled or key restricted | Enable Custom Search API; relax key restrictions for the agent runtime |
| Shodan empty / error | No credits or query too broad | `shodan info`; add `country:` and a title filter |
| ChatGPT proposes zenodo.org / ckan.org | No duplicate-check | Paste existing `link` values or a country CSV into the thread |
| Agent sets `link` to an IP | Censys/Shodan host record | Use certificate / web-property name; confirm HTTPS vhost |
| Keys leaked in a PR | mcp.json or GPT spec committed | Rotate the key; move secrets to user config |

## Related

- [discovery.md](discovery.md)
- [discovery-search-tools.md](discovery-search-tools.md)
- [agents/discover.md](agents/discover.md)
- [agents/query.md](agents/query.md)
- [ai-consumers.md](ai-consumers.md)
- [llms.txt](https://github.com/datenoio/dataportals-registry/blob/main/llms.txt)
