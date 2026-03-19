"""Optional LLM classification for ambiguous fixes. Requires `litellm`."""
from __future__ import annotations

import json
import re

from ald_checker.reference import VALID_NATURESENSE, _load_gics_reference

DEFAULT_MODEL = "openai/gpt-4.1-nano"


def _llm_classify(prompt: str, model: str = DEFAULT_MODEL) -> str:
    import litellm
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
        "- If a type is already clean, return it unchanged\n\n"
        f"Asset types to standardize:\n{items}\n\n"
        'Respond with JSON only: {{"original": "standardized", ...}}\n'
        "Only include entries that changed. Omit unchanged ones."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def classify_naturesense(raw_types: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Classify asset_type_raw values into NatureSense types via LLM."""
    ns_list = "\n".join(f"- {t}" for t in sorted(VALID_NATURESENSE))
    items = "\n".join(f"- {r}" for r in raw_types)
    prompt = (
        "Classify each asset type into exactly one NatureSense category.\n\n"
        f"Valid NatureSense categories:\n{ns_list}\n\n"
        f"Asset types to classify:\n{items}\n\n"
        'Respond with JSON only: {{"asset_type_raw": "NatureSense category", ...}}\n'
        "Use the exact NatureSense category names from the list above."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


def classify_gics(raw_types: list[str], model: str = DEFAULT_MODEL) -> dict[str, str]:
    """Classify asset_type_raw values into GICS codes via LLM."""
    gics_ref = _load_gics_reference()
    items = "\n".join(f"- {r}" for r in raw_types)
    prompt = (
        "Classify each asset type into a 6-digit GICS industry code based on what "
        "the asset IS (not the company that owns it).\n\n"
        f"Valid GICS codes:\n{gics_ref}\n\n"
        f"Asset types to classify:\n{items}\n\n"
        'Respond with JSON only: {{"asset_type_raw": "GICS code", ...}}\n'
        "Use the exact 6-digit codes from the list above."
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))


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
        "Normalize each asset status into exactly one of: Open, Construction, Planned, Cancelled.\n\n"
        "- Open = currently active/operating\n"
        "- Construction = being built\n"
        "- Planned = approved/permitted but not started\n"
        "- Cancelled = shut down/decommissioned/closed\n\n"
        f"Statuses to normalize:\n{items}\n\n"
        'Respond with JSON only: {{"original status": "canonical status", ...}}'
    )
    return json.loads(_strip_fences(_llm_classify(prompt, model)))
