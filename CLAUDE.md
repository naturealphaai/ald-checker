# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An ALD (Asset Location Database) output quality checker that validates and auto-fixes CSV files containing corporate physical asset records. Implements a 29-point validation suite in five phases covering schema, classification, data quality, entity structure, and duplicate detection. Deterministic fixes run first (casing, aliases, majority-voting); LLM classification is an optional fallback for ambiguous cases.

Used by the `asset-discovery` pipeline as a post-processing validation step.

## Commands

```bash
# Install
uv sync

# Install with LLM support
uv sync --extra llm

# Install with XLSX output
uv sync --extra xlsx

# Install everything
uv sync --extra all

# Validate a CSV
uv run ald-check output/final_assets.csv

# Validate and auto-fix (deterministic: majority vote, casing, fill dates)
uv run ald-check output/final_assets.csv --fix

# Auto-fix with LLM fallback for ambiguous classifications
uv run ald-check output/final_assets.csv --fix --fix-llm

# Specify LLM model (default: openai/gpt-4.1-nano)
uv run ald-check output/final_assets.csv --fix --fix-llm --model bedrock/us.anthropic.claude-haiku-4-5-20251001-v1:0

# Multiple files
uv run ald-check file1.csv file2.csv --fix

# Run only specific checks / skip checks
uv run ald-check data.csv --only naturesense_valid gics_valid
uv run ald-check data.csv --skip coordinate_proximity

# Dry run (no output files)
uv run ald-check data.csv --fix --dry-run

# Skip XLSX output
uv run ald-check data.csv --fix --no-xlsx

# Run tests
uv run pytest tests/
```

## Architecture

### Key Files

| File | Role |
|---|---|
| `src/ald_checker/__init__.py` | Exports `ALL_CHECKS`, `CheckResult`, `run_checks` |
| `src/ald_checker/cli.py` | `ald-check` CLI entry point (argparse) |
| `src/ald_checker/checks.py` | 29 check functions + `CheckResult` + `run_checks()` orchestrator |
| `src/ald_checker/reference.py` | Loads bundled reference data: `VALID_NATURESENSE`, `VALID_GICS`, `ALD_COLUMNS`, `STATUS_ALIASES`, `EXTRA_COLUMNS` |
| `src/ald_checker/llm.py` | Optional LLM classification via litellm: `classify_naturesense()`, `classify_gics()`, `classify_status()`, `standardize_raw_types()`, `map_columns()`, `standardize_attribution()`, etc. |
| `src/ald_checker/data/naturesense_asset_types.csv` | 16 valid NatureSense asset type categories |
| `src/ald_checker/data/gics_industries.csv` | 77 valid GICS industry codes |

### The 29 Checks (5 Phases)

**Phase 1 — Column structure**

| # | Check | Fixable | Method |
|---|---|---|---|
| 1 | `check_columns` — required columns present and mapped | Yes | LLM column mapping |
| 2 | `check_none_strings` — replaces literal "None"/"null" with empty | Yes | Deterministic |
| 3 | `check_numeric_cleanup` — strips `.0` from numeric string columns | Yes | Deterministic |
| 4 | `check_asset_id_unique` — UUIDs unique | Yes | Generate new UUIDs |

**Phase 2 — Classification** (order matters: raw type → NatureSense → GICS)

| # | Check | Fixable | Method |
|---|---|---|---|
| 5 | `check_asset_type_raw_standardize` — normalize raw asset types | Yes | LLM |
| 6 | `check_naturesense_correct` — correct NatureSense from corp-graph | Yes | DB lookup / LLM |
| 7 | `check_gics_correct` — correct GICS from corp-graph | Yes | DB lookup / LLM |
| 8 | `check_naturesense_valid` — valid asset type categories | Yes | Case match / LLM |
| 9 | `check_naturesense_consistency` — same raw type → same classification | Yes | Majority vote / LLM |
| 10 | `check_gics_valid` — valid industry codes | Yes | LLM |
| 11 | `check_gics_consistency` — consistent codes per raw type | Yes | Majority vote / LLM |

**Phase 3 — Data quality**

| # | Check | Fixable | Method |
|---|---|---|---|
| 12 | `check_coordinates` — valid lat/lon | No | — |
| 13 | `check_address_exists` — address field populated | Yes | Reverse geocoding |
| 14 | `check_entity_stake` — 0–100 range | Yes | Clamp |
| 15 | `check_capacity_non_negative` — positive values | No | — |
| 16 | `check_status_values` — valid status strings | Yes | Alias map / LLM |
| 17 | `check_required_fields` — non-empty required fields | No | — |
| 18 | `check_name_casing` — smart title case | Yes | Deterministic |
| 19 | `check_entity_name_casing` — smart title case | Yes | Deterministic |
| 20 | `check_supplementary_details` — valid JSON | Yes | Deterministic |
| 21 | `check_date_researched` — valid date format | Yes | Fill with today |
| 22 | `check_isin_format` — valid ISIN | No | — |
| 23 | `check_capacity_units_consistency` — consistent units per type | No | — |
| 24 | `check_source_url_format` — valid URLs | No | — |
| 25 | `check_attribution_source` — source attribution present | Yes | Fill default / LLM |

**Phase 4 — Entity structure**

| # | Check | Fixable | Method |
|---|---|---|---|
| 26 | `check_entity_name_consistency` — normalized via cleanco | Yes | Majority vote |
| 27 | `check_entity_parent_consistency` — parent fields consistent | Yes | Majority vote |
| 28 | `check_entity_isin_valid` — ISIN exists in corp-graph | No | DB lookup |

**Phase 5 — Duplicates & proximity**

| # | Check | Fixable | Method |
|---|---|---|---|
| 29 | `check_duplicate_assets` — no exact duplicates | No | — |
| 30 | `check_coordinate_proximity` — warns of likely missed dedup (100m) | No | — |

### Key Patterns

- **Deterministic-first fixing** — All checks attempt deterministic fixes (casing, aliases, majority-voting at 70% threshold) before LLM fallback
- **Majority-vote dedup** — For inconsistent classifications, if one form has >=70% usage it wins; below that, LLM is consulted
- **Smart title-casing** — Preserves acronyms (LLC, A/S), special chars (#, numbers), and existing mixed-case words
- **Lazy LLM import** — `litellm` is optional; `_try_llm_import()` catches `ImportError` gracefully
- **Lazy DB import** — `psycopg` is optional; corp-graph lookups silently skip if unavailable
- **CSV round-trip** — Reads with `csv.DictReader`, fixes in-place, writes to `{stem}_checked.csv`
- **XLSX output** — Optional `openpyxl`-based output with Key (summary + audit log) and Assets sheets
- **Entity normalization** — Uses `cleanco` to strip legal suffixes (Inc., Ltd., GmbH) for consistency checks
- **Haversine proximity** — Coordinate proximity check at ~100m threshold for missed dedup warnings
- **Phased execution** — Checks run in dependency order (columns → classification → quality → entity → duplicates)

### Dependencies

- **Required:** `cleanco>=2.3` (legal suffix stripping)
- **Optional:** `litellm>=1.0` (LLM classification, `--extra llm`), `openpyxl>=3.1` (XLSX output, `--extra xlsx`), `psycopg>=3.1` (corp-graph DB lookups, `--extra db`)

## Naming

- Never use "TREX" anywhere in code, comments, or docs — use "ALD" instead

## Conventions

- Python 3.13+, synchronous
- No config files — all parameterized via CLI flags (`--fix`, `--fix-llm`, `--model`, `--only`, `--skip`, `--no-xlsx`, `--dry-run`)
- Standard `csv` module for I/O (not pandas)
- Reference data loaded at import time from bundled CSVs
- Exit code 0 if all checks pass/fixed, 1 if any fail
