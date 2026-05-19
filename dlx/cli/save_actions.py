from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re

from dlx.marc import Datafield, Literal


SAVE_ACTION_RULES: dict[str, dict[str, dict]] = json.loads(
    Path(__file__).with_name("save_action_rules.json").read_text()
)


@dataclass
class SaveActionSummary:
    normalized_values: int = 0
    added_fields: int = 0


def apply_save_actions(record) -> SaveActionSummary:
    summary = SaveActionSummary()
    collection = "auths" if record.record_type == "auth" else "bibs"
    rules = SAVE_ACTION_RULES.get(collection, {})

    for tag, data in rules.items():
        is_date = data.get("isDate", {})
        if is_date:
            summary.normalized_values += _normalize_dates(record, tag=tag, date_codes=is_date.keys())

        save_actions = data.get("saveActions")
        if save_actions:
            summary.added_fields += _apply_tag_save_actions(record, tag=tag, save_actions=save_actions)

    return summary


def _normalize_dates(record, *, tag: str, date_codes) -> int:
    changed = 0

    for field in record.get_fields(tag):
        for code in date_codes:
            for subfield in field.get_subfields(code):
                if subfield.value is None:
                    continue

                cleaned = subfield.value.replace(" ", "-")
                cleaned = re.sub(r"^(\d{4})(\d{2})", r"\1-\2", cleaned)
                cleaned = re.sub(r"^(\d{4})-(\d{2})(\d{2})$", r"\1-\2-\3", cleaned)

                if cleaned != subfield.value:
                    subfield.value = cleaned
                    changed += 1

    return changed


def _apply_tag_save_actions(record, *, tag: str, save_actions: dict[str, dict[str, str | None]]) -> int:
    added = 0
    matched_mappings = [mapping for criteria, mapping in save_actions.items() if _criteria_matches(record, criteria)]
    record.delete_fields(tag)

    for mapping in matched_mappings:
        field = Datafield(tag=tag, ind1=" ", ind2=" ", record_type=record.record_type)

        for code, value in sorted(mapping.items()):
            if value is None:
                continue
            field.subfields.append(Literal(code, value))

        if field.subfields:
            record.fields.append(field)
            added += 1

    return added


def _criteria_matches(record, criteria: str) -> bool:
    terms = [item for item in re.split(r"\s*(AND|OR|NOT)\s+", criteria) if item]
    modifier = ""
    last_bool = True

    for term in terms:
        if term in ("AND", "OR", "NOT"):
            modifier += term
            continue

        tag, subfield_code, value = _parse_term(term)
        evaluated = _evaluate_term(record, tag=tag, subfield_code=subfield_code, value=value)

        if modifier == "":
            last_bool = evaluated
        elif modifier == "NOT":
            last_bool = not evaluated
        elif modifier == "AND":
            last_bool = last_bool and evaluated
        elif modifier == "ANDNOT":
            last_bool = last_bool and (not evaluated)
        elif modifier == "OR":
            last_bool = last_bool or evaluated
        elif modifier == "ORNOT":
            last_bool = last_bool or (not evaluated)

        modifier = ""

    return last_bool


def _parse_term(term: str) -> tuple[str | None, str | None, str]:
    if ":" not in term:
        return None, None, ""

    field, value = term.split(":", 1)
    field_parts = field.split("__")
    tag = field_parts[0] if re.match(r"\d{3}", field_parts[0]) else None
    code = field_parts[1] if len(field_parts) > 1 else None
    return tag, code, value


def _evaluate_term(record, *, tag: str | None, subfield_code: str | None, value: str) -> bool:
    if not tag:
        return False

    rx = _rule_regex(value)

    for field in record.get_fields(tag):
        subfields = field.get_subfields(subfield_code) if subfield_code else field.subfields

        for subfield in subfields:
            if rx.search(subfield.value or ""):
                return True

    return False


@lru_cache(maxsize=512)
def _rule_regex(value: str) -> re.Pattern:
    if value.startswith("/") and value.endswith("/"):
        pattern = value[1:-1]
    else:
        pattern = value.replace("/", r"\/").replace("[", r"\[").replace("]", r"\]").replace(".", r"\.").replace("*", ".*")

    return re.compile(pattern, flags=re.IGNORECASE)
