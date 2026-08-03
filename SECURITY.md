# Security Policy

## Project status

Paykit Server is pre-production software. It has no stable releases or persisted-data compatibility guarantee. Only the current `master` branch is maintained.

| Version | Supported |
| --- | --- |
| Current `master` | Yes |
| Older commits or releases | No |

## Reporting a vulnerability

Do not report security vulnerabilities in public issues, discussions, pull requests, logs, or chat transcripts.

Use GitHub's **Security → Report a vulnerability** flow for this repository. If private vulnerability reporting is unavailable, do not publish technical details; ask a repository maintainer for a private reporting channel first.

Include, where practical:

- affected commit or version;
- impact and attack prerequisites;
- minimal reproduction steps;
- whether credentials or production systems may be affected;
- a proposed mitigation, if known.

Do not include live credentials, private keys, master keys, account xpubs, database dumps, Pubky session material, auth URLs, encrypted state backups, or other user data. Use synthetic fixtures and redact identifiers.

Do not test against infrastructure or accounts you do not own or have explicit permission to assess.

## Response and disclosure

Maintainers will validate the report, determine scope, and coordinate remediation and disclosure. This pre-production project does not promise a fixed response SLA. Please allow maintainers reasonable time to investigate before public disclosure.
