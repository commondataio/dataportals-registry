# Security Policy

## Supported versions

Security fixes are applied to the default branch of this repository. There are no long-term release branches for the reference-data registry.

## Reporting a vulnerability

If you believe you have found a security vulnerability, please report it responsibly:

1. **Preferred:** open a [GitHub Security Advisory](https://github.com/commondataio/dataportals-registry/security/advisories/new) (private disclosure).
2. **Alternative:** open a GitHub issue labeled `security` with a minimal description and request a private follow-up if the issue must stay confidential.

Please include:

- A description of the issue and potential impact
- Steps to reproduce (proof-of-concept if available)
- Affected paths (scripts, workflows, dependencies)

Do **not** open public issues with exploit details before a fix is available.

## Scope

In scope:

- Python scripts under `scripts/`
- GitHub Actions workflows under `.github/workflows/`
- Build/validation pipeline behavior that could lead to code execution, secret exposure, or supply-chain issues

Out of scope:

- Availability or content of third-party catalog URLs listed in the registry
- Vulnerabilities in downstream API/MCP services maintained in other repositories

## Response expectations

Maintainers will acknowledge reports within a reasonable timeframe and coordinate fixes on the default branch. We appreciate responsible disclosure.
