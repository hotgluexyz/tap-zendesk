# tap-zendesk

A [Singer](https://www.singer.io/) tap that extracts data from **Zendesk**. It is built with [hotglue-singer-sdk](https://github.com/hotgluexyz/HotglueSingerSDK) and speaks the standard Singer message protocol on stdout, so you can pair it with any compatible target.

## Features

- **REST**-style HTTP streams (see `client.py` / `streams.py`).
- **OAuth2** with access token support via Hotglue (`access_token_support` on the tap).

- Required **`subdomain`** and **`start_date`** (see [Configuration](#configuration)).
- 14 streams ported from the pre-SDK tap, reusing its JSON schemas verbatim.

### Streams

| Stream | Endpoint | Primary key | Replication key | Pagination |
| ------ | -------- | ----------- | --------------- | ---------- |
| `tickets` | `/incremental/tickets/cursor.json` | `id` | `generated_timestamp` (epoch int) | incremental export cursor |
| `ticket_audits` | `/tickets/{ticket_id}/audits.json` | `id` | — (full table) | `next_page` link |
| `ticket_metrics` | `/tickets/{ticket_id}/metrics` | `id` | — (full table) | single object |
| `ticket_comments` | `/tickets/{ticket_id}/comments.json` | `id` | `created_at` | `next_page` link |
| `users` | `/incremental/users/cursor.json` | `id` | `updated_at` | incremental export cursor |
| `organizations` | `/incremental/organizations.json` | `id` | `updated_at` | `next_page` link |
| `groups` | `/groups` | `id` | `updated_at` | cursor (`page[after]`) |
| `group_memberships` | `/group_memberships` | `id` | `updated_at` | cursor (`page[after]`) |
| `macros` | `/macros` | `id` | `updated_at` | cursor (`page[after]`) |
| `tags` | `/tags` | `name` | — (full table) | cursor (`page[after]`) |
| `ticket_fields` | `/ticket_fields` | `id` | `updated_at` | cursor (`page[after]`) |
| `ticket_forms` | `/ticket_forms.json` | `id` | `updated_at` | `next_page` link |
| `satisfaction_ratings` | `/satisfaction_ratings` | `id` | `updated_at` | cursor (`page[after]`) |
| `sla_policies` | `/slas/policies.json` | `id` | — (full table) | `next_page` link |

`ticket_audits`, `ticket_metrics` and `ticket_comments` are fetched once per ticket
from their own endpoints, as child streams of `tickets` — the same number of
requests the pre-SDK tap made.

`organizations` and `users` additionally gain a typed property per custom field
defined on the account, fetched from `/organization_fields.json` and
`/user_fields.json` during discovery. If the token lacks the scope, the static
schema is kept and a warning is logged.

Schemas live in `tap_zendesk/schemas/`. They are the schemas from the pre-SDK tap,
including the cross-file `shared/*.json` references, which `tap_zendesk/schema.py`
resolves because the SDK does not resolve `$ref` itself.

### Pagination

Three styles, one base class each in `client.py`:

- `CursorPaginatedStream` — `page[size]` / `page[after]`, driven by `meta.has_more`.
- `OffsetPaginatedStream` — follows the query string of the `next_page` link.
- `IncrementalExportStream` — `start_time` on the first request, then an opaque
  `cursor`, until `end_of_stream`.

Each stops when its cursor fails to advance, rather than trusting a page-size proxy.

### Rate limits

The SDK retries 5xx and 429 by default. Zendesk's own error text is appended to
failure messages by `response_error_message`.

## Requirements

- Python **3.10+** (see `requires-python` in `pyproject.toml`).

## Installation

1. **Clone** this repository and `cd` into the project directory.
2. **Create `config.json`** in the project root with your credentials and settings (see [Configuration](#configuration) for the fields and an example).
3. **Create a virtual environment** and activate it:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Windows, use `.venv\Scripts\activate` instead of `source .venv/bin/activate`.

4. **Install the package** in editable mode:

```bash
pip install -e .
```

5. **Run the tap** (with the venv still activated):

```bash
tap-zendesk --help
```

## Configuration

| Setting | Type | Required | Default | Description |
| ------- | ---- | -------- | ------- | ----------- |
| `start_date` | string (datetime) | no | `2000-01-01T00:00:00Z` | Earliest record date to sync. |
| `subdomain` | string | yes | — | The `acme` in `acme.zendesk.com`. Drives both the API and OAuth token hosts. |
| `client_id` | string | yes | — | OAuth client ID. |
| `client_secret` | string | yes | — | OAuth client secret. |
| `refresh_token` | string | no | — | OAuth refresh token. Required for OAuth syncs; Zendesk rotates it on every refresh. |
| `request_timeout` | number | no | `300` | Request timeout in seconds. A falsy value falls back to the default. |
| `marketplace_name` | string | no | — | Sent as `X-Zendesk-Marketplace-Name`. All three marketplace settings must be set together. |
| `marketplace_organization` | string | no | — | Sent as `X-Zendesk-Marketplace-Organization-Id`. |
| `marketplace_app_id` | string | no | — | Sent as `X-Zendesk-Marketplace-App-Id`. |

Run `tap-zendesk --about` (or `tap-zendesk --about --format=markdown`) for the authoritative schema for your installed version.

### Example `config.json`

```json
{
  "subdomain": "acme",
  "start_date": "2000-01-01T00:00:00Z",
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "refresh_token": "YOUR_REFRESH_TOKEN"
}
```

Do not commit real credentials. Prefer environment variables or a secrets manager in production.

### Environment-based config

You can load settings from the process environment using `--config=ENV` (the SDK merges env into config). Env names follow the tap’s setting keys (see `tap-zendesk --about`).

## Usage

With your virtual environment **activated** and `config.json` in place:

Discover stream catalog:

```bash
tap-zendesk --config config.json --discover > catalog.json
```

Run a sync (with optional state):

```bash
tap-zendesk --config config.json --catalog catalog.json --state state.json
```

Pipe to any Singer target:

```bash
tap-zendesk --config config.json --catalog catalog.json | target-jsonl
```

Inspect built-in settings and stream metadata:

```bash
tap-zendesk --about
```

## API / documentation

| Host | Purpose |
| ---- | ------- |
| `https://{subdomain}.zendesk.com/api/v2` | Resource server (all streams). |
| `https://{subdomain}.zendesk.com/oauth/tokens` | OAuth token endpoint (`refresh_token` grant). |

Both are subdomain-scoped, so `subdomain` is required before either can be built.

- [Zendesk API reference](https://developer.zendesk.com/api-reference/)
- [Zendesk OAuth](https://developer.zendesk.com/documentation/ticketing/working-with-oauth/creating-and-using-oauth-tokens-with-the-api/)
- [Incremental exports](https://developer.zendesk.com/documentation/ticketing/managing-tickets/using-the-incremental-export-api/)

### Access tokens via Hotglue

The tap declares `access_token_support`, so it advertises the
`allows-fetch-access-token` capability and supports:

```bash
tap-zendesk --config config.json --access-token
```

This refreshes against Zendesk and writes the rotated `refresh_token` and
`access_token` back to the config file. Setting `_refresh_token_via_hg_api` in
config instead pulls the token from the Hotglue API.


## License
See repository files; add a `LICENSE` if you distribute this package.
