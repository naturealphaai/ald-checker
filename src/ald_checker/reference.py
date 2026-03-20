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

# Country bounding boxes: (min_lat, max_lat, min_lon, max_lon)
# Generous margins to account for overseas territories, islands, etc.
COUNTRY_BBOX: dict[str, tuple[float, float, float, float]] = {
    # Asia
    "taiwan": (21.5, 26.5, 119.0, 123.0),
    "japan": (24.0, 46.0, 122.0, 154.0),
    "china": (18.0, 54.0, 73.0, 135.0),
    "south korea": (33.0, 39.0, 124.0, 132.0),
    "korea": (33.0, 39.0, 124.0, 132.0),
    "india": (6.0, 36.0, 68.0, 98.0),
    "singapore": (1.1, 1.5, 103.6, 104.1),
    "vietnam": (8.0, 24.0, 102.0, 110.0),
    "indonesia": (-11.0, 6.0, 95.0, 141.0),
    "thailand": (5.0, 21.0, 97.0, 106.0),
    "malaysia": (0.5, 8.0, 99.0, 120.0),
    "philippines": (4.5, 21.0, 116.0, 127.0),
    # Europe
    "germany": (47.0, 55.5, 5.5, 15.5),
    "netherlands": (50.5, 54.0, 3.0, 7.5),
    "france": (41.0, 51.5, -5.5, 10.0),
    "united kingdom": (49.5, 61.0, -8.5, 2.0),
    "uk": (49.5, 61.0, -8.5, 2.0),
    "ireland": (51.0, 55.5, -11.0, -5.5),
    "italy": (36.0, 47.5, 6.5, 19.0),
    "spain": (27.5, 44.0, -19.0, 5.0),
    "switzerland": (45.5, 48.0, 5.5, 10.5),
    "sweden": (55.0, 69.5, 10.5, 24.5),
    "norway": (57.5, 71.5, 4.0, 31.5),
    "denmark": (54.5, 58.0, 8.0, 15.5),
    "finland": (59.5, 70.5, 19.0, 31.5),
    "belgium": (49.5, 51.5, 2.5, 6.5),
    "austria": (46.0, 49.0, 9.5, 17.0),
    "poland": (49.0, 55.0, 14.0, 24.5),
    "czech republic": (48.5, 51.5, 12.0, 19.0),
    "portugal": (32.0, 42.5, -31.5, -6.0),
    # Americas
    "united states": (24.0, 72.0, -180.0, -66.0),
    "us": (24.0, 72.0, -180.0, -66.0),
    "usa": (24.0, 72.0, -180.0, -66.0),
    "canada": (41.5, 84.0, -141.0, -52.0),
    "mexico": (14.0, 33.0, -118.5, -86.5),
    "brazil": (-34.0, 6.0, -74.0, -34.0),
    "argentina": (-55.5, -21.5, -73.5, -53.5),
    "chile": (-56.0, -17.0, -76.0, -66.5),
    "colombia": (-5.0, 14.0, -82.0, -66.5),
    # Oceania
    "australia": (-44.0, -10.0, 112.0, 154.0),
    "new zealand": (-47.5, -34.0, 166.0, 179.0),
    # Middle East / Africa
    "saudi arabia": (16.0, 32.5, 34.5, 56.0),
    "uae": (22.5, 26.5, 51.0, 56.5),
    "israel": (29.0, 33.5, 34.0, 36.0),
    "south africa": (-35.0, -22.0, 16.0, 33.0),
    "egypt": (22.0, 32.0, 24.5, 37.0),
    "turkey": (36.0, 42.5, 26.0, 45.0),
    "nigeria": (4.0, 14.0, 2.5, 15.0),
}

# Common address suffixes → canonical country name
COUNTRY_ALIASES: dict[str, str] = {
    "u.s.": "united states", "u.s.a.": "united states",
    "america": "united states", "états-unis": "united states",
    "the netherlands": "netherlands", "holland": "netherlands",
    "republic of korea": "south korea", "rok": "south korea",
    "prc": "china", "p.r.c.": "china", "mainland china": "china",
    "great britain": "united kingdom", "england": "united kingdom",
    "scotland": "united kingdom", "wales": "united kingdom",
    "r.o.c.": "taiwan", "chinese taipei": "taiwan",
    "deutschland": "germany", "nippon": "japan",
    "république française": "france", "italia": "italy",
    "españa": "spain", "schweiz": "switzerland",
    "brasil": "brazil", "méxico": "mexico",
}

# Continent bounding boxes for outlier detection: (min_lat, max_lat, min_lon, max_lon)
CONTINENT_BBOX: dict[str, tuple[float, float, float, float]] = {
    "North America": (7.0, 84.0, -170.0, -50.0),
    "South America": (-56.0, 14.0, -82.0, -34.0),
    "Europe": (35.0, 72.0, -25.0, 45.0),
    "Africa": (-35.0, 38.0, -18.0, 52.0),
    "Asia": (-11.0, 78.0, 25.0, 180.0),
    "Oceania": (-48.0, 0.0, 110.0, 180.0),
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
