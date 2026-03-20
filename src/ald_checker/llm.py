"""Optional LLM classification for ambiguous fixes. Requires `litellm`."""
from __future__ import annotations

import json
import re

from ald_checker.reference import VALID_NATURESENSE, _load_gics_reference, _load_naturesense_reference

DEFAULT_MODEL = "openai/gpt-4.1-nano"


def _llm_classify(prompt: str, model: str = DEFAULT_MODEL) -> str:
    import litellm
    litellm.drop_params = True
    resp = litellm.completion(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=2048,
    )
    return resp.choices[0].message.content.strip()


def _strip_fences(raw: str) -> str:
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return raw


def standardize_raw_types(raw_types: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Standardize asset_type_raw values — fix typos, normalize format, merge obvious duplicates."""
    items = "\n".join(f"- {r}" for r in raw_types)
    prompt = (
        "Standardize these asset type names. Fix typos, normalize casing and format, "
        "merge obvious duplicates of the same concept. Do NOT change meaning — "
        "keep the specificity (e.g. 'Semiconductor Fab (300mm)' stays specific, "
        "don't generalize to 'manufacturing facility').\n\n"
        "Rules:\n"
        "- Use lowercase\n"
        "- Fix obvious typos (e.g. 'seimconducter' → 'semiconductor')\n"
        "- Normalize format (e.g. 'R & D Center' → 'r&d center')\n"
        "- Merge duplicates (e.g. 'HQ' and 'head quarters' → 'corporate headquarters')\n"
        "- Keep specificity (size, capacity info, etc.)\n"
        "- If a type is already clean, return it unchanged\n"
        "- Preserve spacing around & (e.g. 'museum & visitor center' stays as is)\n\n"
        f"Asset types to standardize:\n{items}\n\n"
        'Respond with JSON only: {{"original": "standardized", ...}}\n'
        "Only include entries that changed. Omit unchanged ones. "
        "If nothing changed, respond with {}"
    )
    raw = _strip_fences(_llm_classify(prompt, model)).strip()
    return json.loads(raw) if raw else {}


def classify_naturesense(raw_types: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Classify asset_type_raw values into NatureSense types via LLM."""
    ns_ref = _load_naturesense_reference()
    items = "\n".join(f"- {r}" for r in raw_types)
    prompt = (
        "Classify each asset type into exactly one NatureSense category based on what the physical asset IS.\n\n"
        "Examples of correct classifications:\n"
        "- semiconductor fab → Heavy Industrial & Manufacturing\n"
        "- quarry → Mining Operations\n"
        "- office / headquarters / design center → Office/Housing\n"
        "- employee housing / dormitory → Office/Housing\n"
        "- warehouse / distribution center → Warehouse\n"
        "- retail store → Retail\n"
        "- wind farm / solar farm / power plant → Energy Production\n"
        "- childcare facility / recreation facility / park → Other (5km buffer area of influence)\n"
        "- museum / visitor center → Other (5km buffer area of influence)\n"
        "- r&d center / lab → R&D Facility\n\n"
        f"Valid NatureSense categories (with descriptions):\n{ns_ref}\n\n"
        f"Asset types to classify:\n{items}\n\n"
        'Respond with JSON only: {{"asset_type_raw": "NatureSense category", ...}}\n'
        "Use the EXACT NatureSense category names from the list above. Never invent categories."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def classify_gics(raw_types: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Classify asset_type_raw values into GICS codes via LLM. Batched for efficiency."""
    gics_ref = _load_gics_reference()

    # Batch in chunks of 25 to keep prompt focused
    results = {}
    for i in range(0, len(raw_types), 25):
        batch = raw_types[i:i+25]
        items = "\n".join(f"- {r}" for r in batch)
        prompt = (
            "Classify each asset type into a 6-digit GICS industry code based on what the asset IS.\n\n"
            "IMPORTANT: ONLY use codes from the valid list below. Do NOT invent codes.\n\n"
            "Key examples:\n"
            "- Any semiconductor facility (fab, foundry, packaging, cleanroom) → 453010\n"
            "- Office, headquarters, housing, dormitory, design center, r&d center → 602010\n"
            "- Quarry, mine → 151040\n"
            "- Retail store → 255040\n"
            "- Distribution center, warehouse → 203050\n"
            "- Wind farm, solar farm, power plant → 101020\n"
            "- Childcare, recreation, museum, park → 253020\n\n"
            f"Valid GICS codes (ONLY use these):\n{gics_ref}\n\n"
            f"Asset types to classify:\n{items}\n\n"
            'Respond with JSON only: {{"asset_type": "6-digit GICS code", ...}}\n'
            "ONLY use exact 6-digit codes from the valid list."
        )
        try:
            batch_results = json.loads(_strip_fences(_llm_classify(prompt, model)))
            results.update(batch_results)
        except Exception:
            pass

    return results


def map_columns(unknown_cols: list[str], known_cols: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Map unknown column names to standard ALD columns via LLM."""
    known = "\n".join(f"- {c}" for c in known_cols)
    unknown = "\n".join(f"- {c}" for c in unknown_cols)
    prompt = (
        "Map these unknown CSV column names to the correct standard ALD column name.\n\n"
        f"Standard ALD columns:\n{known}\n\n"
        f"Unknown columns to map:\n{unknown}\n\n"
        'Respond with JSON: {{"unknown_col": "standard_col_or_DROP", ...}}\n'
        'Use "DROP" if the column has no ALD equivalent and should be removed.'
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def find_entity_name_duplicates(names: list[str], model: str = DEFAULT_MODEL) -> dict[str, list[str]]:
    """Find entity names that refer to the same legal entity.

    Returns {canonical_name: [variant1, variant2, ...]} for groups that should merge.
    Does NOT merge different subsidiaries that share a parent name.
    """
    items = "\n".join(f"- {n}" for n in names)
    prompt = (
        "These are entity names from an asset database. Some may refer to the same "
        "legal entity written differently (typos, abbreviation differences).\n\n"
        "MERGE these (same legal entity, just written differently):\n"
        "- 'Atlas Copco (India) Ltd' and 'Atlas Copco (India) Ltd.' — same entity\n"
        "- 'Edwards Vacuum' and 'Edwards Vacuum LLC' — same entity\n"
        "- 'Samsung Electronics' and 'Samsung Electronics Co., Ltd.' — same entity\n\n"
        "DO NOT MERGE these (different legal entities/subsidiaries):\n"
        "- 'Atlas Copco AB' and 'Atlas Copco K.K.' — different entities (Sweden vs Japan)\n"
        "- 'Atlas Copco AB' and 'Atlas Copco s.r.o.' — different entities (Sweden vs Czech)\n"
        "- 'TSMC' and 'TSMC Arizona Corporation' — different entities (parent vs subsidiary)\n"
        "- 'Samsung Electronics' and 'Samsung SDI' — different companies\n\n"
        "Key rule: If the names differ ONLY in legal suffix (Ltd/Ltd./LLC/Inc/Co./Corp) "
        "or punctuation, they are the same entity. If they have different geographic or "
        "business identifiers (country names, K.K., s.r.o., GmbH with different base names), "
        "they are DIFFERENT entities.\n\n"
        f"Entity names:\n{items}\n\n"
        "Respond with JSON: {{\"canonical_name\": [\"variant1\", \"variant2\"], ...}}\n"
        "Only include groups where merging is needed. Use the most complete/common form "
        "as the canonical name. Return {} if no merges needed."
    )
    raw = _strip_fences(_llm_classify(prompt, model)).strip()
    return json.loads(raw) if raw else {}


def standardize_attribution(sources: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Standardize attribution source values via LLM."""
    items = "\n".join(f"- {s}" for s in sources)
    prompt = (
        "Standardize these attribution source values into canonical forms.\n\n"
        "Standard values: asset_discovery, overture_maps, places_discovery_atp, "
        "store_locator_scrape, serpapi_google_maps, manual_research, web_scrape, "
        "ald_basefile, perplexity, craft, gem, gleif\n\n"
        f"Sources to standardize:\n{items}\n\n"
        'Respond with JSON: {{"original": "standardized", ...}}\n'
        "Only include entries that changed."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def parse_dates(dates: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Parse non-standard date strings to YYYY-MM-DD format via LLM."""
    items = "\n".join(f"- {d}" for d in dates)
    prompt = (
        "Convert each date to YYYY-MM-DD format.\n\n"
        f"Dates to convert:\n{items}\n\n"
        'Respond with JSON: {{"original": "YYYY-MM-DD", ...}}\n'
        "Only include entries that changed. If a date is already YYYY-MM-DD, omit it."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def fix_capacity(assets: list[dict], model: str = DEFAULT_MODEL) -> list[dict]:
    """Fix implausible capacity + capacity_units pairs via LLM.

    Each asset dict has: row, name, asset_type, capacity, capacity_units.
    Returns list of corrections: {row, capacity, capacity_units} or {row, drop: true}.
    """
    items = "\n".join(
        f"- Row {a['row']}: \"{a['name']}\" (type: {a['asset_type'] or 'unknown'}), "
        f"capacity={a['capacity']}, units={a['capacity_units']}"
        for a in assets
    )
    prompt = (
        "These assets have implausible capacity values for their units. "
        "For each one, determine the correct capacity and capacity_units.\n\n"
        "Common errors:\n"
        "- Unit was changed (e.g. hectares→sqm) but the value wasn't converted "
        "(7 hectares = 70000 sqm, so 7 sqm is wrong)\n"
        "- Wrong unit entirely (e.g. sqm for a power plant should be MW)\n"
        "- Decimal point error (e.g. 0.5 MW should be 500 kW, reported as 500 MW)\n\n"
        "Use the asset name and type to reason about what the correct capacity and "
        "units should be. If you cannot determine the correct value with confidence, "
        'set "drop": true to clear the capacity rather than guessing.\n\n'
        f"Assets to fix:\n{items}\n\n"
        "Respond with JSON array: "
        '[{{"row": N, "capacity": X, "capacity_units": "unit"}} or '
        '{{"row": N, "drop": true}}, ...]'
    )
    raw = json.loads(_strip_fences(_llm_classify(prompt, model)))
    return raw if isinstance(raw, list) else []


def check_capacity_units_appropriate(assets: list[dict], model: str = DEFAULT_MODEL) -> list[dict]:
    """Check if capacity units are semantically appropriate for the asset type.

    Returns only rows where units are wrong for the asset type, with corrections.
    """
    # Batch in chunks of 30
    results = []
    for i in range(0, len(assets), 30):
        batch = assets[i:i+30]
        items = "\n".join(
            f"- Row {a['row']}: \"{a['name']}\" (type: {a['asset_type']}), "
            f"capacity={a['capacity']} {a['capacity_units']}"
            for a in batch
        )
        prompt = (
            "Review each asset's capacity units and determine if they are semantically "
            "appropriate for the asset type and name.\n\n"
            "CRITICAL CONTEXT: Capacity can represent EITHER:\n"
            "- Building/site size (sqm, sqft, hectares, acres) — how big the physical asset is\n"
            "- Production throughput (wafers/month, MW, tons/day, bpd) — what the asset produces\n"
            "Both are valid for the same asset. A fab with 96000 sqm is its building footprint. "
            "A fab with 100000 wafers/month is its production capacity. Neither is wrong.\n\n"
            "Examples of CLEARLY WRONG units (flag these):\n"
            "- Office with capacity in MW or wafers/month\n"
            "- Power plant with capacity in rooms or sqm\n"
            "- Warehouse with capacity in wafers/month or MW\n"
            "- Childcare facility with capacity in bpd or MW\n\n"
            "Examples of CORRECT/ACCEPTABLE units (do NOT flag):\n"
            "- Any industrial facility (fab, factory, plant, packaging) in sqm/sqft/hectares/acres = building footprint. ALWAYS acceptable.\n"
            "- Semiconductor fab in wafers/month or wafers/year = production capacity. Also acceptable.\n"
            "- Office/housing in sqm, sqft, units, or rooms\n"
            "- Power plant/solar/wind in MW, MWp, GWh/year\n"
            "- Water treatment in tons/day, m3/day, or acres\n\n"
            "IMPORTANT: Area units (sqm, sqft, hectares, acres) are ALWAYS valid for ANY "
            "physical asset as they can represent building footprint or site area. "
            "Only flag units that are CLEARLY from the wrong domain (e.g. energy units on "
            "an office, production throughput on a warehouse).\n\n"
            "ONLY return rows where units are OBVIOUSLY wrong. When in doubt, do NOT flag.\n\n"
            f"Assets to review:\n{items}\n\n"
            "Respond with JSON array of ONLY problematic rows: "
            '[{{"row": N, "issue": "brief explanation", '
            '"capacity": corrected_value_or_null, "capacity_units": "corrected_unit_or_null", '
            '"drop": true_if_should_clear}}, ...]\n'
            "Return [] if all units are appropriate."
        )
        try:
            raw = json.loads(_strip_fences(_llm_classify(prompt, model)).strip() or "[]")
            if isinstance(raw, list):
                results.extend(raw)
        except Exception:
            pass
    return results


def convert_capacity_units(assets: list[dict], model: str = DEFAULT_MODEL) -> list[dict]:
    """Convert capacity values when changing units for consistency.

    Each asset dict has: row, name, asset_type, capacity, capacity_units, target_units.
    Returns list of: {row, capacity, capacity_units} with the converted value.
    """
    items = "\n".join(
        f"- Row {a['row']}: \"{a['name']}\" (type: {a['asset_type']}), "
        f"capacity={a['capacity']} {a['capacity_units']} → convert to {a['target_units']}"
        for a in assets
    )
    prompt = (
        "Convert each asset's capacity value from its current units to the target units.\n\n"
        "CRITICAL: You must convert the numeric VALUE, not just change the label.\n"
        "For example:\n"
        "- 7 hectares → 70000 sqm (multiply by 10000)\n"
        "- 15 acres → 60702.9 sqm (multiply by 4046.86)\n"
        "- 100000 wafers/month → 1200000 wafers/year (multiply by 12)\n\n"
        "If a conversion doesn't make sense for the asset type (e.g. converting "
        "acres to tons/day for a water plant), keep the original value and units.\n\n"
        f"Assets to convert:\n{items}\n\n"
        "Respond with JSON array: "
        '[{{"row": N, "capacity": X, "capacity_units": "unit"}}, ...]'
    )
    raw = json.loads(_strip_fences(_llm_classify(prompt, model)))
    return raw if isinstance(raw, list) else []


def standardize_capacity_units(units_with_types: dict[str, list[str]], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Normalize capacity units. Input: {asset_type: [unit1, unit2, ...]}. Returns {original_unit: standardized_unit}."""
    items = "\n".join(f"- {t}: {', '.join(units)}" for t, units in units_with_types.items())
    prompt = (
        "For each asset type, the capacity units are inconsistent. "
        "Pick the most standard unit and map all variants to it.\n\n"
        f"Asset types and their units:\n{items}\n\n"
        'Respond with JSON: {{"original_unit": "standardized_unit", ...}}\n'
        "Only include entries that changed."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def classify_status(statuses: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Normalize free-text status values via LLM."""
    items = "\n".join(f"- {s}" for s in statuses)
    prompt = (
        "Normalize each asset status into exactly one of these canonical values:\n"
        "operational, under construction, planned, closed, mothballed, "
        "temporarily closed, closing, under exploration\n\n"
        "- operational = currently active, operating, open, ramping up\n"
        "- under construction = being built, under development, in development\n"
        "- planned = approved/permitted/announced/proposed but construction not started\n"
        "- closed = permanently shut down, retired, decommissioned, cancelled\n"
        "- mothballed = indefinitely paused/shelved, could potentially restart\n"
        "- temporarily closed = short-term closure (seasonal, maintenance, idle)\n"
        "- closing = in the process of winding down, pre-retirement\n"
        "- under exploration = exploratory phase, status unknown or uncertain\n\n"
        f"Statuses to normalize:\n{items}\n\n"
        'Respond with JSON only: {{"original status": "canonical status", ...}}'
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))
