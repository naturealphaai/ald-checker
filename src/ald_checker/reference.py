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

VALID_STATUSES = {
    "operational",
    "under construction",
    "planned",
    "closed",
    "mothballed",
    "temporarily closed",
    "closing",
    "under exploration",
    "",
}

STATUS_ALIASES: dict[str, str] = {
    # ALD basefile legacy
    "open": "operational",
    "operating": "operational",
    "active": "operational",
    "in operation": "operational",
    "operational": "operational",
    "construction": "under construction",
    "under_construction": "under construction",
    "under development": "under construction",
    "under construction": "under construction",
    "planned/under development": "under construction",
    "operational/under development": "under construction",
    "pre-construction": "planned",
    "pre construction": "planned",
    "proposed": "planned",
    "announced": "planned",
    "approved": "planned",
    "permitted": "planned",
    "pre-permit": "planned",
    "planned": "planned",
    # Closed variants
    "retired": "closed",
    "cancelled": "closed",
    "shut down": "closed",
    "decommissioned": "closed",
    # Mothballed / shelved
    "mothballed": "mothballed",
    "shelved": "mothballed",
    "shelved - inferred 2": "mothballed",
    "cancelled - inferred": "mothballed",
    # Temporarily closed
    "temporarily closed": "temporarily closed",
    "temporarily_closed": "temporarily closed",
    "idle": "temporarily closed",
    # Closing / winding down
    "operating pre-retire": "closing",
    "mothballed pre-retir": "closing",
    "closing": "closing",
    # Exploration / uncertain
    "under exploration": "under exploration",
    "uncertain": "under exploration",
    "unknown": "under exploration",
    "ramping": "operational",
}

# Conversion factors between area units (to_sqm multiplier)
AREA_UNIT_CONVERSIONS: dict[str, float] = {
    "sqm": 1.0,
    "sqft": 0.0929,
    "acres": 4046.86,
    "hectares": 10000.0,
}

# Plausibility bounds: (min, max) per capacity_units for a single asset
CAPACITY_PLAUSIBILITY: dict[str, tuple[float, float]] = {
    "sqm": (50, 50_000_000),
    "sqft": (500, 500_000_000),
    "acres": (0.1, 500_000),
    "hectares": (0.01, 50_000),
    "MW": (0.01, 50_000),
    "MWp": (0.01, 50_000),
    "GWh": (0.001, 500_000),
    "GWh/year": (0.001, 500_000),
    "wafers/month": (100, 500_000),
    "wafers/year": (1000, 10_000_000),
    "bpd": (1, 20_000_000),
    "tons/day": (1, 1_000_000),
    "TPD": (1, 1_000_000),
    "MMTPA": (0.01, 500),
    "rooms": (5, 50_000),
    "units": (1, 100_000),
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
