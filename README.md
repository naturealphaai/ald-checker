# ald-checker

ALD (Asset Location Database) output quality checker that validates and auto-fixes CSV files containing corporate physical asset records. Runs a 29-point validation suite covering schema, uniqueness, classification, coordinates, casing, dates, and semantic duplicate detection.

Deterministic fixes run first (casing, aliases, majority-voting); LLM classification is an optional fallback for ambiguous cases.

## Install

```bash
# Core
uv sync

# With LLM support
uv sync --extra llm

# With XLSX output
uv sync --extra xlsx

# Everything
uv sync --extra all
```

## Usage

```bash
# Validate a CSV
uv run ald-check output/final_assets.csv

# Validate and auto-fix (deterministic only)
uv run ald-check output/final_assets.csv --fix

# Auto-fix with LLM fallback for ambiguous classifications
uv run ald-check output/final_assets.csv --fix --fix-llm

# Specify LLM model (default: openai/gpt-4.1-nano)
uv run ald-check output/final_assets.csv --fix --fix-llm \
  --model bedrock/us.anthropic.claude-haiku-4-5-20251001-v1:0

# Multiple files
uv run ald-check file1.csv file2.csv --fix

# Run only specific checks
uv run ald-check data.csv --only naturesense_valid gics_valid

# Skip specific checks
uv run ald-check data.csv --skip coordinate_proximity duplicate_assets

# Dry run (show fixes without writing output)
uv run ald-check data.csv --fix --dry-run

# Skip XLSX output
uv run ald-check data.csv --fix --no-xlsx
```

### Environment variables

For LLM-powered fixes, set the relevant API key for your provider:

```bash
OPENAI_API_KEY=...        # OpenAI models (default provider)
GOOGLE_MAPS_API_KEY=...   # Geocoding for address checks
```

See `.env.example` for a template.

### Exit codes

| Code | Meaning |
|------|---------|
| `0` | All checks passed or were fixed |
| `1` | One or more checks failed |

## Checks

29 checks run in five phases:

### Phase 1 — Column structure

| Check | Fixable | Method |
|---|---|---|
| `columns` — required columns present and mapped | Yes | LLM column mapping |
| `none_strings` — replaces literal "None"/"null" with empty | Yes | Deterministic |
| `numeric_cleanup` — strips `.0` from numeric string columns | Yes | Deterministic |
| `asset_id_unique` — UUIDs unique | Yes | Generate new UUIDs |

### Phase 2 — Classification

Order matters: raw type → NatureSense → GICS.

| Check | Fixable | Method |
|---|---|---|
| `asset_type_raw_standardize` — normalize raw asset types | Yes | LLM |
| `naturesense_correct` — correct NatureSense from corp-graph | Yes | DB lookup / LLM |
| `gics_correct` — correct GICS from corp-graph | Yes | DB lookup / LLM |
| `naturesense_valid` — valid asset type categories | Yes | Case match / LLM |
| `naturesense_consistency` — same raw type → same classification | Yes | Majority vote / LLM |
| `gics_valid` — valid industry codes | Yes | LLM |
| `gics_consistency` — consistent codes per raw type | Yes | Majority vote / LLM |

### Phase 3 — Data quality

| Check | Fixable | Method |
|---|---|---|
| `coordinates` — valid lat/lon | No | — |
| `address_exists` — address field populated | Yes | Reverse geocoding |
| `entity_stake` — 0–100 range | Yes | Clamp |
| `capacity_non_negative` — positive values | No | — |
| `status_values` — valid status strings | Yes | Alias map / LLM |
| `required_fields` — non-empty required fields | No | — |
| `name_casing` — smart title case for asset names | Yes | Deterministic |
| `entity_name_casing` — smart title case for entity names | Yes | Deterministic |
| `supplementary_details` — valid JSON | Yes | Deterministic |
| `date_researched` — valid date format | Yes | Fill with today |
| `isin_format` — valid ISIN | No | — |
| `capacity_units_consistency` — consistent units per type | No | — |
| `source_url_format` — valid URLs | No | — |
| `attribution_source` — source attribution present | Yes | Fill default / LLM |

### Phase 4 — Entity structure

| Check | Fixable | Method |
|---|---|---|
| `entity_name_consistency` — normalized via cleanco | Yes | Majority vote |
| `entity_parent_consistency` — parent fields consistent | Yes | Majority vote |
| `entity_isin_valid` — ISIN exists in corp-graph | No | DB lookup |

### Phase 5 — Duplicates & proximity

| Check | Fixable | Method |
|---|---|---|
| `duplicate_assets` — no exact duplicates | No | — |
| `coordinate_proximity` — warns of likely missed dedup (100 m) | No | — |

## Output

When `--fix` is used and fixes are applied:

- **CSV** — `{filename}_checked.csv` with fixes applied, sorted by entity → asset type → name
- **XLSX** — `{filename}.xlsx` with Key (summary + audit log), Assets, and optionally Review tabs (requires `openpyxl`)

## Dependencies

| Package | Extra | Purpose |
|---|---|---|
| `cleanco` | core | Legal suffix stripping (Inc., Ltd., GmbH) |
| `litellm` | `llm` | LLM classification via any provider |
| `openpyxl` | `xlsx` | XLSX output generation |
| `psycopg` | `db` | Corp-graph database lookups |

## Testing

```bash
uv run pytest tests/
```

## License

MIT
