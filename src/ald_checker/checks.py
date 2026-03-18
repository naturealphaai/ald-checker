"""All check functions for ALD output validation."""
from __future__ import annotations

import csv
import json
import math
import re
import uuid
from datetime import date
from pathlib import Path

from ald_checker.reference import (
    EXTRA_COLUMNS,
    STATUS_ALIASES,
    ALD_COLUMNS,
    VALID_GICS,
    VALID_NATURESENSE,
    VALID_STATUSES,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

class CheckResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = True
        self.issues: list[str] = []
        self.fixed: list[str] = []

    def fail(self, msg: str):
        self.passed = False
        self.issues.append(msg)

    def fix(self, msg: str):
        self.fixed.append(msg)


def _majority_vote(mapping: dict[str, list[int]]) -> tuple[str, float]:
    """Return (winner, confidence) where confidence = winner_count / total."""
    total = sum(len(v) for v in mapping.values())
    winner = max(mapping, key=lambda k: len(mapping[k]))
    return winner, len(mapping[winner]) / total


def _smart_title_case(s: str) -> str:
    """Title-case preserving acronyms and special patterns.

    Only converts words that are ALL-CAPS (>3 chars) or all-lowercase.
    Preserves: "A/S", "LLC", "HQ", "II", "#102", "McDonald's".
    """
    words = s.split()
    result = []
    for w in words:
        if len(w) <= 3 and w == w.upper():
            result.append(w)
        elif w != w.upper() and w != w.lower():
            result.append(w)
        elif w[0] in "#0123456789":
            result.append(w)
        elif w == w.upper() and len(w) > 3:
            result.append(w.title())
        elif w == w.lower() and len(w) > 1:
            result.append(w.title())
        else:
            result.append(w)
    return " ".join(result)


def _try_llm_import():
    """Import LLM module, returning None if litellm not installed."""
    try:
        from ald_checker import llm
        return llm
    except ImportError:
        return None


# ── Check functions ──────────────────────────────────────────────────────────

def check_columns(rows: list[dict], headers: list[str]) -> CheckResult:
    """All expected ALD columns exist."""
    result = CheckResult("all_columns_exist")
    missing = [c for c in ALD_COLUMNS if c not in headers]
    extra = [c for c in headers if c not in ALD_COLUMNS and c not in EXTRA_COLUMNS]
    if missing:
        result.fail(f"Missing columns: {missing}")
    if extra:
        result.fail(f"Unexpected columns: {extra}")
    return result


def check_asset_id_unique(rows: list[dict], fix: bool = False, **_kw) -> CheckResult:
    """All asset_ids are unique, non-empty UUIDs."""
    result = CheckResult("asset_id_unique")
    seen: dict[str, int] = {}
    empty_rows = []

    for i, row in enumerate(rows):
        aid = row.get("asset_id", "").strip()
        if not aid:
            empty_rows.append(i)
            if fix:
                row["asset_id"] = str(uuid.uuid4())
                result.fix(f"Row {i}: generated asset_id {row['asset_id']}")
            continue
        if aid in seen:
            result.fail(f"Duplicate asset_id '{aid}' at rows {seen[aid]} and {i}")
        else:
            seen[aid] = i

    if empty_rows and not fix:
        result.fail(f"Empty asset_id at {len(empty_rows)} rows: {empty_rows[:10]}{'...' if len(empty_rows) > 10 else ''}")

    for i, row in enumerate(rows):
        aid = row.get("asset_id", "").strip()
        if aid:
            try:
                uuid.UUID(aid)
            except ValueError:
                result.fail(f"Row {i}: asset_id '{aid}' is not a valid UUID")

    return result


def check_naturesense_valid(rows: list[dict], fix: bool = False, fix_llm: bool = False, model: str = "", **_kw) -> CheckResult:
    """All naturesense_asset_type values are in the official list."""
    result = CheckResult("naturesense_valid")
    invalid: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        ns = row.get("naturesense_asset_type", "").strip()
        if ns and ns not in VALID_NATURESENSE:
            invalid.setdefault(ns, []).append(i)

    if not fix and not fix_llm:
        for ns, idxs in invalid.items():
            result.fail(f"Invalid naturesense_asset_type '{ns}' at {len(idxs)} rows (first: {idxs[:5]})")
        return result

    # Deterministic fixes first
    still_invalid: dict[str, list[int]] = {}
    for ns, idxs in invalid.items():
        if ns.lower().startswith("other"):
            canonical = "Other (5km buffer area of influence)"
            for idx in idxs:
                rows[idx]["naturesense_asset_type"] = canonical
            result.fix(f"'{ns}' → '{canonical}' at {len(idxs)} rows")
        else:
            ns_lower = ns.lower()
            match = next((v for v in VALID_NATURESENSE if v.lower() == ns_lower), None)
            if match:
                for idx in idxs:
                    rows[idx]["naturesense_asset_type"] = match
                result.fix(f"Case fix '{ns}' → '{match}' at {len(idxs)} rows")
            else:
                still_invalid[ns] = idxs

    # LLM fix for remaining
    if still_invalid and fix_llm:
        llm = _try_llm_import()
        if llm:
            raw_for_ns: dict[str, str] = {}
            for ns, idxs in still_invalid.items():
                raws: dict[str, int] = {}
                for idx in idxs:
                    r = rows[idx].get("asset_type_raw", "").strip()
                    if r:
                        raws[r] = raws.get(r, 0) + 1
                raw_for_ns[ns] = max(raws, key=raws.get) if raws else ns

            try:
                llm_map = llm.classify_naturesense(list(raw_for_ns.values()), model or llm.DEFAULT_MODEL)
                for ns, idxs in still_invalid.items():
                    classified = llm_map.get(raw_for_ns[ns], "")
                    if classified in VALID_NATURESENSE:
                        for idx in idxs:
                            rows[idx]["naturesense_asset_type"] = classified
                        result.fix(f"LLM: '{ns}' → '{classified}' at {len(idxs)} rows")
                    else:
                        result.fail(f"LLM returned invalid '{classified}' for '{ns}' — skipped")
            except Exception as e:
                result.fail(f"LLM classification failed: {e}")
                for ns, idxs in still_invalid.items():
                    result.fail(f"Invalid naturesense_asset_type '{ns}' at {len(idxs)} rows")
        else:
            result.fail("--fix-llm requires litellm: pip install ald-checker[llm]")
    elif still_invalid:
        for ns, idxs in still_invalid.items():
            result.fail(f"Invalid naturesense_asset_type '{ns}' at {len(idxs)} rows (first: {idxs[:5]})")

    return result


def check_naturesense_consistency(rows: list[dict], fix: bool = False, fix_llm: bool = False, model: str = "", **_kw) -> CheckResult:
    """Same asset_type_raw always maps to the same naturesense_asset_type."""
    result = CheckResult("naturesense_consistency")
    mapping: dict[str, dict[str, list[int]]] = {}
    for i, row in enumerate(rows):
        raw = row.get("asset_type_raw", "").strip().lower()
        ns = row.get("naturesense_asset_type", "").strip()
        if raw and ns:
            mapping.setdefault(raw, {}).setdefault(ns, []).append(i)

    inconsistent = {raw: ns_map for raw, ns_map in mapping.items() if len(ns_map) > 1}
    if not inconsistent:
        return result

    if not fix and not fix_llm:
        for raw, ns_map in inconsistent.items():
            counts = {ns: len(idxs) for ns, idxs in ns_map.items()}
            result.fail(f"'{raw}' maps to multiple naturesense types: {counts}")
        return result

    needs_llm = []
    for raw, ns_map in inconsistent.items():
        winner, confidence = _majority_vote(ns_map)
        counts = {ns: len(idxs) for ns, idxs in ns_map.items()}

        if confidence >= 0.7:
            fixed_count = 0
            for ns, idxs in ns_map.items():
                if ns != winner:
                    for idx in idxs:
                        rows[idx]["naturesense_asset_type"] = winner
                    fixed_count += len(idxs)
            result.fix(f"'{raw}': majority-voted → '{winner}' (fixed {fixed_count} rows, was {counts})")
        elif fix_llm:
            needs_llm.append(raw)
        else:
            fixed_count = 0
            for ns, idxs in ns_map.items():
                if ns != winner:
                    for idx in idxs:
                        rows[idx]["naturesense_asset_type"] = winner
                    fixed_count += len(idxs)
            result.fix(f"'{raw}': majority-voted → '{winner}' (fixed {fixed_count} rows, was {counts}, low confidence {confidence:.0%})")

    if needs_llm:
        llm = _try_llm_import()
        if llm:
            try:
                llm_map = llm.classify_naturesense(needs_llm, model or llm.DEFAULT_MODEL)
                for raw in needs_llm:
                    ns_map = inconsistent[raw]
                    classified = llm_map.get(raw, "")
                    counts = {ns: len(idxs) for ns, idxs in ns_map.items()}
                    if classified in VALID_NATURESENSE:
                        fixed_count = 0
                        for ns, idxs in ns_map.items():
                            if ns != classified:
                                for idx in idxs:
                                    rows[idx]["naturesense_asset_type"] = classified
                                fixed_count += len(idxs)
                        result.fix(f"'{raw}': LLM classified → '{classified}' (fixed {fixed_count} rows, was {counts})")
                    else:
                        winner, _ = _majority_vote(ns_map)
                        fixed_count = 0
                        for ns, idxs in ns_map.items():
                            if ns != winner:
                                for idx in idxs:
                                    rows[idx]["naturesense_asset_type"] = winner
                                fixed_count += len(idxs)
                        result.fix(f"'{raw}': LLM returned invalid '{classified}', fell back to majority → '{winner}' (fixed {fixed_count} rows)")
            except Exception as e:
                result.fail(f"LLM classification failed: {e}")
                for raw in needs_llm:
                    ns_map = inconsistent[raw]
                    winner, _ = _majority_vote(ns_map)
                    counts = {ns: len(idxs) for ns, idxs in ns_map.items()}
                    fixed_count = 0
                    for ns, idxs in ns_map.items():
                        if ns != winner:
                            for idx in idxs:
                                rows[idx]["naturesense_asset_type"] = winner
                            fixed_count += len(idxs)
                    result.fix(f"'{raw}': LLM failed, majority-voted → '{winner}' (fixed {fixed_count} rows, was {counts})")
        else:
            result.fail("--fix-llm requires litellm: pip install ald-checker[llm]")

    return result


def check_gics_valid(rows: list[dict], fix_llm: bool = False, model: str = "", **_kw) -> CheckResult:
    """All industry_code values are valid 6-digit GICS codes."""
    result = CheckResult("gics_valid")
    invalid: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        code = row.get("industry_code", "").strip()
        if code and code not in VALID_GICS:
            invalid.setdefault(code, []).append(i)

    if not invalid:
        return result

    if fix_llm:
        llm = _try_llm_import()
        if llm:
            raw_for_code: dict[str, set[str]] = {}
            for code, idxs in invalid.items():
                raws = set()
                for idx in idxs:
                    r = rows[idx].get("asset_type_raw", "").strip()
                    if r:
                        raws.add(r)
                raw_for_code[code] = raws

            all_raws = list({r for raws in raw_for_code.values() for r in raws})
            if all_raws:
                try:
                    llm_map = llm.classify_gics(all_raws, model or llm.DEFAULT_MODEL)
                    for code, idxs in invalid.items():
                        for idx in idxs:
                            raw = rows[idx].get("asset_type_raw", "").strip()
                            new_code = llm_map.get(raw, "")
                            if new_code in VALID_GICS:
                                rows[idx]["industry_code"] = new_code
                            else:
                                result.fail(f"Row {idx}: LLM returned invalid GICS '{new_code}' for '{raw}'")
                        raws_str = ", ".join(raw_for_code[code])
                        result.fix(f"LLM: invalid code '{code}' reclassified for '{raws_str}' at {len(idxs)} rows")
                except Exception as e:
                    result.fail(f"LLM GICS classification failed: {e}")
                    for code, idxs in invalid.items():
                        result.fail(f"Invalid GICS code '{code}' at {len(idxs)} rows (first: {idxs[:5]})")
            else:
                for code, idxs in invalid.items():
                    result.fail(f"Invalid GICS code '{code}' at {len(idxs)} rows (first: {idxs[:5]})")
        else:
            result.fail("--fix-llm requires litellm: pip install ald-checker[llm]")
    else:
        for code, idxs in invalid.items():
            result.fail(f"Invalid GICS code '{code}' at {len(idxs)} rows (first: {idxs[:5]})")

    return result


def check_gics_consistency(rows: list[dict], fix: bool = False, fix_llm: bool = False, model: str = "", **_kw) -> CheckResult:
    """Same asset_type_raw always maps to the same industry_code."""
    result = CheckResult("gics_consistency")
    mapping: dict[str, dict[str, list[int]]] = {}
    for i, row in enumerate(rows):
        raw = row.get("asset_type_raw", "").strip().lower()
        code = row.get("industry_code", "").strip()
        if raw and code:
            mapping.setdefault(raw, {}).setdefault(code, []).append(i)

    inconsistent = {raw: code_map for raw, code_map in mapping.items() if len(code_map) > 1}
    if not inconsistent:
        return result

    if not fix and not fix_llm:
        for raw, code_map in inconsistent.items():
            counts = {code: len(idxs) for code, idxs in code_map.items()}
            result.fail(f"'{raw}' maps to multiple GICS codes: {counts}")
        return result

    needs_llm = []
    for raw, code_map in inconsistent.items():
        winner, confidence = _majority_vote(code_map)
        counts = {code: len(idxs) for code, idxs in code_map.items()}

        if confidence >= 0.7:
            fixed_count = 0
            for code, idxs in code_map.items():
                if code != winner:
                    for idx in idxs:
                        rows[idx]["industry_code"] = winner
                    fixed_count += len(idxs)
            result.fix(f"'{raw}': majority-voted → '{winner}' (fixed {fixed_count} rows, was {counts})")
        elif fix_llm:
            needs_llm.append(raw)
        else:
            fixed_count = 0
            for code, idxs in code_map.items():
                if code != winner:
                    for idx in idxs:
                        rows[idx]["industry_code"] = winner
                    fixed_count += len(idxs)
            result.fix(f"'{raw}': majority-voted → '{winner}' (fixed {fixed_count} rows, was {counts}, low confidence {confidence:.0%})")

    if needs_llm:
        llm = _try_llm_import()
        if llm:
            try:
                llm_map = llm.classify_gics(needs_llm, model or llm.DEFAULT_MODEL)
                for raw in needs_llm:
                    code_map = inconsistent[raw]
                    classified = llm_map.get(raw, "")
                    counts = {code: len(idxs) for code, idxs in code_map.items()}
                    if classified in VALID_GICS:
                        fixed_count = 0
                        for code, idxs in code_map.items():
                            if code != classified:
                                for idx in idxs:
                                    rows[idx]["industry_code"] = classified
                                fixed_count += len(idxs)
                        result.fix(f"'{raw}': LLM classified → '{classified}' (fixed {fixed_count} rows, was {counts})")
                    else:
                        winner, _ = _majority_vote(code_map)
                        fixed_count = 0
                        for code, idxs in code_map.items():
                            if code != winner:
                                for idx in idxs:
                                    rows[idx]["industry_code"] = winner
                                fixed_count += len(idxs)
                        result.fix(f"'{raw}': LLM returned invalid '{classified}', fell back to majority → '{winner}' (fixed {fixed_count} rows)")
            except Exception as e:
                result.fail(f"LLM GICS classification failed: {e}")
                for raw in needs_llm:
                    code_map = inconsistent[raw]
                    winner, _ = _majority_vote(code_map)
                    counts = {code: len(idxs) for code, idxs in code_map.items()}
                    fixed_count = 0
                    for code, idxs in code_map.items():
                        if code != winner:
                            for idx in idxs:
                                rows[idx]["industry_code"] = winner
                            fixed_count += len(idxs)
                    result.fix(f"'{raw}': LLM failed, majority-voted → '{winner}' (fixed {fixed_count} rows, was {counts})")
        else:
            result.fail("--fix-llm requires litellm: pip install ald-checker[llm]")

    return result


def check_coordinates(rows: list[dict], **_kw) -> CheckResult:
    """Lat/lon in valid ranges, not at null island, not swapped."""
    result = CheckResult("coordinates")
    for i, row in enumerate(rows):
        lat_s = row.get("latitude", "").strip()
        lon_s = row.get("longitude", "").strip()
        if not lat_s or not lon_s:
            continue
        try:
            lat, lon = float(lat_s), float(lon_s)
        except ValueError:
            result.fail(f"Row {i}: non-numeric coordinates lat='{lat_s}' lon='{lon_s}'")
            continue
        if not (-90 <= lat <= 90):
            result.fail(f"Row {i}: latitude {lat} out of range [-90, 90]")
        if not (-180 <= lon <= 180):
            result.fail(f"Row {i}: longitude {lon} out of range [-180, 180]")
        if abs(lat) < 0.01 and abs(lon) < 0.01:
            result.fail(f"Row {i}: coordinates ({lat}, {lon}) suspiciously near null island")
        if (-90 <= lon <= 90) and not (-90 <= lat <= 90) and (-180 <= lat <= 180):
            result.fail(f"Row {i}: lat={lat}, lon={lon} — possibly swapped?")
    return result


def check_entity_stake(rows: list[dict], **_kw) -> CheckResult:
    """entity_stake_pct is 0-100 when present."""
    result = CheckResult("entity_stake_pct")
    for i, row in enumerate(rows):
        val = row.get("entity_stake_pct", "").strip()
        if not val:
            continue
        try:
            pct = float(val)
        except ValueError:
            result.fail(f"Row {i}: non-numeric entity_stake_pct '{val}'")
            continue
        if not (0 <= pct <= 100):
            result.fail(f"Row {i}: entity_stake_pct {pct} out of range [0, 100]")
    return result


def check_capacity_non_negative(rows: list[dict], **_kw) -> CheckResult:
    """Capacity values are non-negative when present."""
    result = CheckResult("capacity_non_negative")
    for i, row in enumerate(rows):
        val = row.get("capacity", "").strip()
        if not val:
            continue
        try:
            cap = float(val)
        except ValueError:
            result.fail(f"Row {i}: non-numeric capacity '{val}'")
            continue
        if cap < 0:
            result.fail(f"Row {i}: negative capacity {cap}")
    return result


def check_status_values(rows: list[dict], fix: bool = False, fix_llm: bool = False, model: str = "", **_kw) -> CheckResult:
    """Status is one of: Open, Construction, Planned, Cancelled."""
    result = CheckResult("status_values")
    invalid: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        status = row.get("status", "").strip()
        if status and status not in VALID_STATUSES:
            invalid.setdefault(status, []).append(i)

    if not invalid:
        return result

    if not fix and not fix_llm:
        for status, idxs in invalid.items():
            result.fail(f"Invalid status '{status}' at {len(idxs)} rows (first: {idxs[:5]})")
        return result

    still_invalid: dict[str, list[int]] = {}
    for status, idxs in invalid.items():
        canonical = STATUS_ALIASES.get(status.lower())
        if canonical:
            for idx in idxs:
                rows[idx]["status"] = canonical
            result.fix(f"'{status}' → '{canonical}' at {len(idxs)} rows")
        else:
            still_invalid[status] = idxs

    if still_invalid and fix_llm:
        llm = _try_llm_import()
        if llm:
            try:
                llm_map = llm.classify_status(list(still_invalid.keys()), model or llm.DEFAULT_MODEL)
                for status, idxs in still_invalid.items():
                    canonical = llm_map.get(status, "")
                    if canonical in VALID_STATUSES and canonical:
                        for idx in idxs:
                            rows[idx]["status"] = canonical
                        result.fix(f"LLM: '{status}' → '{canonical}' at {len(idxs)} rows")
                    else:
                        result.fail(f"Invalid status '{status}' at {len(idxs)} rows — LLM returned '{canonical}'")
            except Exception as e:
                result.fail(f"LLM status classification failed: {e}")
                for status, idxs in still_invalid.items():
                    result.fail(f"Invalid status '{status}' at {len(idxs)} rows (first: {idxs[:5]})")
        else:
            result.fail("--fix-llm requires litellm: pip install ald-checker[llm]")
    elif still_invalid:
        for status, idxs in still_invalid.items():
            result.fail(f"Invalid status '{status}' at {len(idxs)} rows (first: {idxs[:5]}) — no known alias")

    return result


def check_required_fields(rows: list[dict], **_kw) -> CheckResult:
    """entity_name and name (asset name) are never empty."""
    result = CheckResult("required_fields")
    missing_entity = [i for i, r in enumerate(rows) if not r.get("entity_name", "").strip()]
    missing_name = [i for i, r in enumerate(rows) if not r.get("name", "").strip()]
    if missing_entity:
        result.fail(f"Empty entity_name at {len(missing_entity)} rows: {missing_entity[:10]}")
    if missing_name:
        result.fail(f"Empty name (asset name) at {len(missing_name)} rows: {missing_name[:10]}")
    return result


def check_name_casing(rows: list[dict], fix: bool = False, **_kw) -> CheckResult:
    """Asset names are title-cased, not ALL CAPS or all lowercase."""
    result = CheckResult("name_casing")
    bad_rows = []
    for i, row in enumerate(rows):
        name = row.get("name", "").strip()
        if not name:
            continue
        if (name == name.upper() and len(name) > 3) or (name == name.lower() and len(name) > 1):
            bad_rows.append(i)

    if not bad_rows:
        return result

    if fix:
        for i in bad_rows:
            rows[i]["name"] = _smart_title_case(rows[i]["name"])
        result.fix(f"Title-cased {len(bad_rows)} asset names")
    else:
        samples = [rows[i]["name"] for i in bad_rows[:5]]
        result.fail(f"Bad casing at {len(bad_rows)} rows (samples: {samples})")
    return result


def check_entity_name_casing(rows: list[dict], fix: bool = False, **_kw) -> CheckResult:
    """Entity names are title-cased, not ALL CAPS or all lowercase."""
    result = CheckResult("entity_name_casing")
    entity_rows: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        name = row.get("entity_name", "").strip()
        if name:
            entity_rows.setdefault(name, []).append(i)

    bad_entities: dict[str, list[int]] = {}
    for name, idxs in entity_rows.items():
        if (name == name.upper() and len(name) > 3) or (name == name.lower() and len(name) > 1):
            bad_entities[name] = idxs

    if not bad_entities:
        return result

    if fix:
        for name, idxs in bad_entities.items():
            fixed_name = _smart_title_case(name)
            for i in idxs:
                rows[i]["entity_name"] = fixed_name
            result.fix(f"'{name}' → '{fixed_name}' at {len(idxs)} rows")
    else:
        for name, idxs in bad_entities.items():
            result.fail(f"Bad casing: '{name}' at {len(idxs)} rows")
    return result


def check_supplementary_details(rows: list[dict], **_kw) -> CheckResult:
    """supplementary_details is valid JSON dict when present."""
    result = CheckResult("supplementary_details_json")
    for i, row in enumerate(rows):
        val = row.get("supplementary_details", "").strip()
        if not val:
            continue
        try:
            parsed = json.loads(val)
            if not isinstance(parsed, dict):
                result.fail(f"Row {i}: supplementary_details is {type(parsed).__name__}, expected dict")
        except json.JSONDecodeError:
            result.fail(f"Row {i}: supplementary_details is not valid JSON: {val[:80]}...")
    return result


def check_duplicate_assets(rows: list[dict], **_kw) -> CheckResult:
    """No duplicate asset names for the same entity."""
    result = CheckResult("duplicate_assets")
    key_rows: dict[str, list[int]] = {}
    for i, row in enumerate(rows):
        name = row.get("name", "").strip().lower()
        entity = row.get("entity_name", "").strip().lower()
        if name and entity:
            key = f"{entity}||{name}"
            key_rows.setdefault(key, []).append(i)

    for key, idxs in key_rows.items():
        if len(idxs) > 1:
            entity, name = key.split("||")
            result.fail(f"Duplicate asset name '{name}' for '{entity}' at rows {idxs}")
    return result


def check_date_researched(rows: list[dict], fix: bool = False, **_kw) -> CheckResult:
    """date_researched is a valid YYYY-MM-DD date."""
    result = CheckResult("date_researched")
    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    today = date.today().isoformat()

    empty = [i for i, r in enumerate(rows) if not r.get("date_researched", "").strip()]
    invalid = [
        (i, r.get("date_researched", ""))
        for i, r in enumerate(rows)
        if r.get("date_researched", "").strip() and not date_re.match(r["date_researched"].strip())
    ]

    if fix and empty:
        for i in empty:
            rows[i]["date_researched"] = today
        result.fix(f"Filled empty date_researched with '{today}' at {len(empty)} rows")
    elif empty:
        result.fail(f"Empty date_researched at {len(empty)} rows: {empty[:10]}")

    for i, val in invalid:
        result.fail(f"Row {i}: invalid date format '{val}' (expected YYYY-MM-DD)")
    return result


def check_isin_format(rows: list[dict], **_kw) -> CheckResult:
    """ISINs are 12 alphanumeric characters when present."""
    result = CheckResult("isin_format")
    isin_re = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
    for i, row in enumerate(rows):
        for col in ("entity_isin", "parent_isin"):
            val = row.get(col, "").strip()
            if val and not isin_re.match(val):
                result.fail(f"Row {i}: {col} '{val}' is not a valid ISIN (expected 2 letters + 10 alphanumeric)")
    return result


def check_capacity_units_consistency(rows: list[dict], **_kw) -> CheckResult:
    """Same asset_type_raw has consistent capacity_units."""
    result = CheckResult("capacity_units_consistency")
    mapping: dict[str, dict[str, list[int]]] = {}
    for i, row in enumerate(rows):
        raw = row.get("asset_type_raw", "").strip().lower()
        units = row.get("capacity_units", "").strip()
        cap = row.get("capacity", "").strip()
        if raw and units and cap:
            mapping.setdefault(raw, {}).setdefault(units, []).append(i)

    for raw, units_map in mapping.items():
        if len(units_map) > 1:
            counts = {u: len(idxs) for u, idxs in units_map.items()}
            result.fail(f"'{raw}' has mixed capacity_units: {counts}")
    return result


def check_source_url_format(rows: list[dict], **_kw) -> CheckResult:
    """Source URLs are valid http(s) URLs when present."""
    result = CheckResult("source_url_format")
    url_re = re.compile(r"^https?://\S+$")
    for i, row in enumerate(rows):
        url = row.get("source_url", "").strip()
        if url and not url_re.match(url):
            result.fail(f"Row {i}: invalid source_url '{url[:80]}'")
    return result


def check_entity_name_consistency(rows: list[dict], fix: bool = False, **_kw) -> CheckResult:
    """Entity names that resolve to the same base name are written the same way.

    Uses cleanco to strip legal suffixes (Inc., Ltd., GmbH, A/S, etc.)
    and groups by base name. Majority form wins on --fix.
    """
    from cleanco import basename

    result = CheckResult("entity_name_consistency")
    base_to_forms: dict[str, dict[str, list[int]]] = {}
    for i, row in enumerate(rows):
        name = row.get("entity_name", "").strip()
        if not name:
            continue
        base = basename(name).strip().lower()
        if not base:
            continue
        base_to_forms.setdefault(base, {}).setdefault(name, []).append(i)

    for base, forms in base_to_forms.items():
        if len(forms) <= 1:
            continue
        counts = {form: len(idxs) for form, idxs in forms.items()}
        if fix:
            winner = max(forms, key=lambda f: len(forms[f]))
            fixed_count = 0
            for form, idxs in forms.items():
                if form != winner:
                    for idx in idxs:
                        rows[idx]["entity_name"] = winner
                    fixed_count += len(idxs)
            if fixed_count:
                result.fix(f"Normalized to '{winner}' (was {counts}, fixed {fixed_count} rows)")
        else:
            result.fail(f"Same entity written as: {counts}")
    return result


def check_coordinate_proximity(rows: list[dict], **_kw) -> CheckResult:
    """Flag asset pairs within 100m of each other (likely missed dedup)."""
    result = CheckResult("coordinate_proximity")
    THRESHOLD_M = 100

    geo_rows: list[tuple[int, float, float, str]] = []
    for i, row in enumerate(rows):
        lat_s = row.get("latitude", "").strip()
        lon_s = row.get("longitude", "").strip()
        if not lat_s or not lon_s:
            continue
        try:
            lat, lon = float(lat_s), float(lon_s)
            name = row.get("name", "").strip()
            geo_rows.append((i, lat, lon, name))
        except ValueError:
            continue

    def _dist_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6_371_000
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2
             + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
             * math.sin(dlon / 2) ** 2)
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    DEG_THRESHOLD = THRESHOLD_M / 111_000 * 1.5

    for a in range(len(geo_rows)):
        idx_a, lat_a, lon_a, name_a = geo_rows[a]
        for b in range(a + 1, len(geo_rows)):
            idx_b, lat_b, lon_b, name_b = geo_rows[b]
            if abs(lat_a - lat_b) > DEG_THRESHOLD or abs(lon_a - lon_b) > DEG_THRESHOLD:
                continue
            dist = _dist_m(lat_a, lon_a, lat_b, lon_b)
            if dist < THRESHOLD_M:
                result.fail(
                    f"Rows {idx_a} & {idx_b} are {dist:.0f}m apart: "
                    f"'{name_a}' vs '{name_b}'"
                )
    return result


def check_attribution_source(rows: list[dict], fix: bool = False, **_kw) -> CheckResult:
    """attribution_source is set on every row."""
    result = CheckResult("attribution_source")
    empty = [i for i, r in enumerate(rows) if not r.get("attribution_source", "").strip()]
    if fix and empty:
        for i in empty:
            rows[i]["attribution_source"] = "asset_discovery"
        result.fix(f"Filled empty attribution_source with 'asset_discovery' at {len(empty)} rows")
    elif empty:
        result.fail(f"Empty attribution_source at {len(empty)} rows: {empty[:10]}")
    return result


# ── Check registry + runner ──────────────────────────────────────────────────

ALL_CHECKS = [
    check_columns,
    check_asset_id_unique,
    check_naturesense_valid,
    check_naturesense_consistency,
    check_gics_valid,
    check_gics_consistency,
    check_coordinates,
    check_entity_stake,
    check_capacity_non_negative,
    check_status_values,
    check_required_fields,
    check_name_casing,
    check_entity_name_casing,
    check_supplementary_details,
    check_duplicate_assets,
    check_date_researched,
    check_isin_format,
    check_capacity_units_consistency,
    check_source_url_format,
    check_entity_name_consistency,
    check_coordinate_proximity,
    check_attribution_source,
]


def run_checks(
    csv_path: str,
    fix: bool = False,
    fix_llm: bool = False,
    model: str = "",
) -> list[CheckResult]:
    """Run all checks on a CSV file. Returns list of CheckResults."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"{path} not found")

    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    print(f"\n{'='*60}")
    print(f"Checking: {path}")
    print(f"Rows: {len(rows)}")
    if fix_llm:
        print(f"LLM model: {model or 'openai/gpt-4.1-nano'}")
    print(f"{'='*60}")

    results = []
    for check_fn in ALL_CHECKS:
        if check_fn == check_columns:
            r = check_fn(rows, headers)
        else:
            r = check_fn(rows, fix=fix, fix_llm=fix_llm, model=model)
        results.append(r)

        status = "\033[32mPASS\033[0m" if r.passed else "\033[31mFAIL\033[0m"
        if r.fixed:
            status = "\033[33mFIXED\033[0m"
        print(f"  [{status}] {r.name}")
        for issue in r.issues:
            print(f"         {issue}")
        for fixed_msg in r.fixed:
            print(f"         \033[33m{fixed_msg}\033[0m")

    # Write fixed output
    if (fix or fix_llm) and any(r.fixed for r in results):
        out_path = path.with_stem(path.stem + "_checked")
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n  Fixed output written to: {out_path}")

    passed = sum(1 for r in results if r.passed and not r.fixed)
    fixed = sum(1 for r in results if r.fixed)
    failed = sum(1 for r in results if not r.passed and not r.fixed)
    print(f"\n  Summary: {passed} passed, {fixed} fixed, {failed} failed out of {len(results)} checks")

    return results
