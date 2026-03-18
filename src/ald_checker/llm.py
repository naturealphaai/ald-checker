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
