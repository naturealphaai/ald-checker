"""Load reference data (NatureSense types, GICS codes) bundled with the package."""
from __future__ import annotations

import csv
from pathlib import Path

_DATA_DIR = Path(__file__).resolve().parent / "data"

ALD_COLUMNS = [
    "asset_id", "entity_name", "entity_isin", "parent_name", "parent_isin",
    "name", "entity_stake_pct", "latitude", "longitude", "status",
    "capacity", "capacity_units", "asset_type_raw", "naturesense_asset_type",
    "industry_code", "date_researched", "supplementary_details", "attribution_source",
]

# Extra columns that are acceptable but not part of the core ALD schema
EXTRA_COLUMNS = {"address", "source_url", "domain_source", "qa_flag"}

VALID_STATUSES = {"Open", "Construction", "Planned", "Cancelled", ""}

STATUS_ALIASES: dict[str, str] = {
    "in operation": "Open",
    "operating": "Open",
    "active": "Open",
    "operational": "Open",
    "closed": "Cancelled",
    "shut down": "Cancelled",
    "decommissioned": "Cancelled",
    "under construction": "Construction",
    "proposed": "Planned",
    "approved": "Planned",
    "permitted": "Planned",
}


def _load_naturesense_types() -> set[str]:
    path = _DATA_DIR / "naturesense_asset_types.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return {
            row["asset_type"].strip()
            for row in csv.DictReader(f)
            if row.get("asset_type", "").strip()
        }


def _load_naturesense_reference() -> str:
    """NatureSense types with descriptions — used as LLM prompt context."""
    path = _DATA_DIR / "naturesense_asset_types.csv"
    lines = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = row.get("asset_type", "").strip()
            desc = row.get("description", "").strip()
            if name:
                entry = f"- {name}"
                if desc:
                    entry += f": {desc}"
                lines.append(entry)
    return "\n".join(lines)


def _load_gics_codes() -> set[str]:
    path = _DATA_DIR / "gics_industries.csv"
    with path.open(newline="", encoding="utf-8") as f:
        codes = set()
        for row in csv.DictReader(f):
            clean = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}
            code = clean.get("industry_code", "")
            if code:
                codes.add(code)
        return codes


def _load_gics_reference() -> str:
    """GICS codes with names — used as LLM prompt context."""
    path = _DATA_DIR / "gics_industries.csv"
    lines = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            # CSV has space-padded columns: sector_code, sector_name, group_code, group_name, industry_code, industry_name, description
            if len(row) >= 6:
                code = row[4].strip()
                name = row[5].strip().strip('"')
                if code and code.isdigit() and len(code) == 6:
                    lines.append(f"{code}: {name}")
    return "\n".join(lines)


VALID_NATURESENSE = _load_naturesense_types()
VALID_GICS = _load_gics_codes()
