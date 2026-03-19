# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An ALD (Asset Location Database) output quality checker that validates and auto-fixes CSV files containing corporate physical asset records. Implements a 22-point validation suite covering schema, uniqueness, classification, coordinates, casing, dates, and semantic duplicate detection. Deterministic fixes run first (casing, aliases, majority-voting); LLM classification is an optional fallback for ambiguous cases.

Used by the `asset-discovery` pipeline as a post-processing validation step.

## Commands

```bash
# Install
uv sync

# Install with LLM support
uv sync --extra llm

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

# Run tests
uv run pytest tests/
```

## Architecture

### Key Files

| File | Role |
|---|---|
| `src/ald_checker/__init__.py` | Exports `ALL_CHECKS`, `CheckResult`, `run_checks` |
| `src/ald_checker/cli.py` | `ald-check` CLI entry point (argparse) |
| `src/ald_checker/checks.py` | 22 check functions + `CheckResult` + `run_checks()` orchestrator |
| `src/ald_checker/reference.py` | Loads bundled reference data: `VALID_NATURESENSE`, `VALID_GICS`, `ALD_COLUMNS`, `STATUS_ALIASES` |
| `src/ald_checker/llm.py` | Optional LLM classification via litellm: `classify_naturesense()`, `classify_gics()`, `classify_status()` |
| `src/ald_checker/data/naturesense_asset_types.csv` | 16 valid NatureSense asset type categories |
| `src/ald_checker/data/gics_industries.csv` | 77 valid GICS industry codes |

### The 22 Checks

| # | Check | Fixable | Method |
|---|---|---|---|
| 1 | `check_columns` — required columns present | No | — |
| 2 | `check_asset_id_unique` — UUIDs unique | Yes | Generate new UUIDs |
| 3 | `check_naturesense_valid` — valid asset types | Yes | Case match / LLM |
| 4 | `check_naturesense_consistency` — same raw type → same classification | Yes | Majority vote / LLM |
| 5 | `check_gics_valid` — valid industry codes | Yes | LLM only |
| 6 | `check_gics_consistency` — consistent codes | Yes | Majority vote / LLM |
| 7 | `check_coordinates` — valid lat/lon | No | — |
| 8 | `check_entity_stake` — 0–100 range | No | — |
| 9 | `check_capacity_non_negative` — positive values | No | — |
| 10 | `check_status_values` — valid status strings | Yes | Alias map / LLM |
| 11 | `check_required_fields` — non-empty required fields | No | — |
| 12 | `check_name_casing` — smart title case | Yes | Deterministic |
| 13 | `check_entity_name_casing` — smart title case | Yes | Deterministic |
| 14 | `check_supplementary_details` — valid JSON | No | — |
| 15 | `check_duplicate_assets` — no exact duplicates | No | — |
| 16 | `check_date_researched` — valid date format | Yes | Fill with today |
| 17 | `check_isin_format` — valid ISIN | No | — |
| 18 | `check_capacity_units_consistency` — consistent units per type | No | — |
| 19 | `check_source_url_format` — valid URLs | No | — |
| 20 | `check_entity_name_consistency` — normalized via cleanco | Yes | Majority vote |
| 21 | `check_coordinate_proximity` — warns of likely missed dedup (100m) | No | — |
| 22 | `check_attribution_source` — source attribution present | Yes | Fill default |

### Key Patterns

- **Deterministic-first fixing** — All checks attempt deterministic fixes (casing, aliases, majority-voting at 70% threshold) before LLM fallback
- **Majority-vote dedup** — For inconsistent classifications, if one form has >=70% usage it wins; below that, LLM is consulted
- **Smart title-casing** — Preserves acronyms (LLC, A/S), special chars (#, numbers), and existing mixed-case words
- **Lazy LLM import** — `litellm` is optional; `_try_llm_import()` catches `ImportError` gracefully
- **CSV round-trip** — Reads with `csv.DictReader`, fixes in-place, writes to `{stem}_checked.csv`
- **Entity normalization** — Uses `cleanco` to strip legal suffixes (Inc., Ltd., GmbH) for consistency checks
- **Haversine proximity** — Coordinate proximity check at ~100m threshold for missed dedup warnings

### Dependencies

- **Required:** `cleanco>=2.3` (legal suffix stripping)
- **Optional:** `litellm>=1.0` (LLM classification, install with `uv sync --extra llm`)

## Naming

- Never use "TREX" anywhere in code, comments, or docs — use "ALD" instead

## Conventions

- Python 3.13+, synchronous
- No config files — all parameterized via CLI flags (`--fix`, `--fix-llm`, `--model`)
- Standard `csv` module for I/O (not pandas)
- Reference data loaded at import time from bundled CSVs
- Exit code 0 if all checks pass/fixed, 1 if any fail
