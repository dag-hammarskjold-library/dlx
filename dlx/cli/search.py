from dataclasses import dataclass
from datetime import datetime
from collections import OrderedDict
from uuid import uuid4
import re

from dlx import DB
from dlx.marc import Auth, AuthSet, Bib, BibSet, Query
from dlx.config import Config
from dlx.cli.save_actions import apply_save_actions


class CLIConnectionError(Exception):
    pass


_INVALID_XML_CHARS = re.compile(r"[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]")
_SEARCH_SNAPSHOTS: "OrderedDict[str, SearchSnapshot]" = OrderedDict()
_MAX_SEARCH_SNAPSHOTS = 10


@dataclass
class SearchRow:
    id: int | None
    primary: str
    secondary: str | None
    updated: str | None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "primary": self.primary,
            "secondary": self.secondary,
            "updated": self.updated,
        }


@dataclass
class SearchSnapshot:
    id: str
    record_type: str
    query: str
    query_doc: dict
    total: int
    record_ids: list[int]


@dataclass
class InvalidXMLCharacter:
    record_id: int | None
    tag: str
    code: str | None
    field_place: int
    subfield_place: int | None
    position: int
    bad_char: str
    context: str
    path: str

    @property
    def codepoint(self) -> str:
        return f"U+{ord(self.bad_char):04X}"


def connect_db(uri: str | None, *, database: str | None = None) -> None:
    if uri:
        DB.connect(uri, database=database)
    elif not DB.connected:
        raise CLIConnectionError("No active DB connection. Pass --db-uri or connect with DB.connect(...) first.")


def search_records(
    query_string: str,
    *,
    record_type: str = "bib",
    limit: int = 20,
    skip: int = 0,
) -> tuple[list[SearchRow], int, dict]:
    if record_type not in ("bib", "auth"):
        raise ValueError(f'Invalid record type "{record_type}"')

    if limit < 0:
        raise ValueError("--limit must be >= 0")

    if skip < 0:
        raise ValueError("--skip must be >= 0")

    query = Query.from_string(query_string, record_type=record_type)
    query_doc = query.compile()
    set_class = BibSet if record_type == "bib" else AuthSet

    total = set_class.from_query(query_doc).count
    records = list(set_class.from_query(query_doc, skip=skip, limit=limit))
    rows = [_to_row(record, record_type=record_type) for record in records]

    return rows, total, query_doc


def create_search_snapshot(
    query_string: str,
    *,
    record_type: str = "bib",
) -> SearchSnapshot:
    if record_type not in ("bib", "auth"):
        raise ValueError(f'Invalid record type "{record_type}"')

    query = Query.from_string(query_string, record_type=record_type)
    query_doc = query.compile()
    handle = DB.bibs if record_type == "bib" else DB.auths
    find_kwargs = {"projection": {"_id": 1}}
    if DB.database_name != "testing" and Config.marc_index_default_collation:
        find_kwargs["collation"] = Config.marc_index_default_collation

    record_ids = [doc["_id"] for doc in handle.find(query_doc, sort=[("_id", 1)], **find_kwargs)]
    snapshot = SearchSnapshot(
        id=uuid4().hex,
        record_type=record_type,
        query=query_string,
        query_doc=query_doc,
        total=len(record_ids),
        record_ids=record_ids,
    )
    _store_snapshot(snapshot)
    return snapshot


def get_snapshot_page(
    snapshot_id: str,
    *,
    limit: int,
    cursor: int = 0,
) -> tuple[list[SearchRow], dict]:
    if limit < 0:
        raise ValueError("--limit must be >= 0")
    if cursor < 0:
        raise ValueError("--cursor must be >= 0")

    snapshot = _SEARCH_SNAPSHOTS.get(snapshot_id)
    if snapshot is None:
        raise ValueError("Search snapshot not found; run search again")

    _SEARCH_SNAPSHOTS.move_to_end(snapshot_id)
    start = min(cursor, snapshot.total)
    end = min(start + limit, snapshot.total)
    page_ids = snapshot.record_ids[start:end]
    rows = _fetch_rows_for_ids(page_ids, record_type=snapshot.record_type)

    page = {
        "snapshot_id": snapshot.id,
        "query": snapshot.query,
        "query_document": snapshot.query_doc,
        "record_type": snapshot.record_type,
        "total": snapshot.total,
        "cursor": start,
        "next_cursor": end if end < snapshot.total else None,
        "prev_cursor": max(0, start - limit) if start > 0 else None,
        "window_start": start + 1 if snapshot.total else 0,
        "window_end": end,
    }
    return rows, page


def snapshot_record_ids(snapshot_id: str) -> list[int]:
    snapshot = _SEARCH_SNAPSHOTS.get(snapshot_id)
    if snapshot is None:
        raise ValueError("Search snapshot not found; run search again")
    _SEARCH_SNAPSHOTS.move_to_end(snapshot_id)
    return list(snapshot.record_ids)


def _to_row(record, *, record_type: str) -> SearchRow:
    updated = getattr(record, "updated", None)
    updated_text = updated.isoformat() if isinstance(updated, datetime) else None

    if record_type == "bib":
        primary = record.title() or "(no title)"
        secondary = record.symbol()
    else:
        heading = record.heading_value("a")
        primary = heading or "(no heading)"
        secondary = record.heading_field.tag if record.heading_field else None

    return SearchRow(
        id=record.id,
        primary=primary,
        secondary=secondary,
        updated=updated_text,
    )


def _fetch_rows_for_ids(record_ids: list[int], *, record_type: str) -> list[SearchRow]:
    if not record_ids:
        return []

    handle = DB.bibs if record_type == "bib" else DB.auths
    record_class = Bib if record_type == "bib" else Auth
    find_kwargs = {}
    if DB.database_name != "testing" and Config.marc_index_default_collation:
        find_kwargs["collation"] = Config.marc_index_default_collation

    docs = list(handle.find({"_id": {"$in": record_ids}}, **find_kwargs))
    docs_by_id = {doc["_id"]: doc for doc in docs}
    rows = []

    for record_id in record_ids:
        doc = docs_by_id.get(record_id)
        if doc is None:
            continue
        rows.append(_to_row(record_class(doc), record_type=record_type))

    return rows


def _store_snapshot(snapshot: SearchSnapshot) -> None:
    _SEARCH_SNAPSHOTS[snapshot.id] = snapshot
    _SEARCH_SNAPSHOTS.move_to_end(snapshot.id)
    while len(_SEARCH_SNAPSHOTS) > _MAX_SEARCH_SNAPSHOTS:
        _SEARCH_SNAPSHOTS.popitem(last=False)


def search_payload(
    rows: list[SearchRow],
    *,
    total: int,
    record_type: str,
    query_string: str,
    query_doc: dict,
    limit: int,
    skip: int,
) -> dict:
    return {
        "record_type": record_type,
        "query": query_string,
        "query_document": query_doc,
        "total": total,
        "limit": limit,
        "skip": skip,
        "results": [row.to_dict() for row in rows],
    }


def get_record(record_id: int, *, record_type: str):
    if record_type not in ("bib", "auth"):
        raise ValueError(f'Invalid record type "{record_type}"')

    record_class = Bib if record_type == "bib" else Auth
    return record_class.from_id(record_id)


def resave_records(
    record_ids: list[int],
    *,
    record_type: str,
    user: str,
    sanitize_invalid_xml: bool = True,
    apply_save_action_rules: bool = True,
    skip_validation: bool = False,
) -> dict:
    if record_type not in ("bib", "auth"):
        raise ValueError(f'Invalid record type "{record_type}"')

    if not user or not user.strip():
        raise ValueError("--commit-user is required to re-save selected records")

    record_class = Bib if record_type == "bib" else Auth
    committed_ids = []
    missing_ids = []
    errors = []
    sanitized_record_ids = []
    sanitized_value_count = 0
    save_action_record_ids = []
    save_action_field_count = 0
    save_action_normalized_value_count = 0

    for record_id in sorted(set(record_ids)):
        record = record_class.from_id(record_id)

        if record is None:
            missing_ids.append(record_id)
            continue

        try:
            if sanitize_invalid_xml:
                changed_count = sanitize_record_xml_literals(record, replacement="")
                if changed_count:
                    sanitized_record_ids.append(record_id)
                    sanitized_value_count += changed_count

            if apply_save_action_rules:
                save_action_summary = apply_save_actions(record)
                if save_action_summary.added_fields or save_action_summary.normalized_values:
                    save_action_record_ids.append(record_id)
                    save_action_field_count += save_action_summary.added_fields
                    save_action_normalized_value_count += save_action_summary.normalized_values

            _commit_record(record, user=user, skip_validation=skip_validation)
            committed_ids.append(record_id)
        except Exception as exc:
            errors.append({"id": record_id, "error": str(exc)})

    return {
        "committed_ids": committed_ids,
        "missing_ids": missing_ids,
        "errors": errors,
        "sanitized_record_ids": sanitized_record_ids,
        "sanitized_value_count": sanitized_value_count,
        "save_action_record_ids": save_action_record_ids,
        "save_action_field_count": save_action_field_count,
        "save_action_normalized_value_count": save_action_normalized_value_count,
    }


def _commit_record(record, *, user: str, skip_validation: bool) -> None:
    if not skip_validation:
        record.commit(user=user)
        return

    original_validate = record.validate

    try:
        record.validate = lambda: None
        record.commit(user=user)
    finally:
        record.validate = original_validate


def sanitize_record_xml_literals(record, *, replacement: str = "") -> int:
    if len(replacement) > 1:
        raise ValueError("replacement must be at most one character")

    changed = 0

    for field in record.controlfields:
        if field.value is None:
            continue

        cleaned = _INVALID_XML_CHARS.sub(replacement, field.value)
        if cleaned != field.value:
            field.value = cleaned
            changed += 1

    for field in record.datafields:
        for subfield in field.subfields:
            if hasattr(subfield, "xref"):
                continue

            if Config.is_authority_controlled(record.record_type, field.tag, subfield.code):
                continue

            if subfield.value is None:
                continue

            cleaned = _INVALID_XML_CHARS.sub(replacement, subfield.value)
            if cleaned != subfield.value:
                subfield.value = cleaned
                changed += 1

    return changed


def find_invalid_xml_literals(record) -> list[InvalidXMLCharacter]:
    matches = []

    for field_place, field in enumerate(record.controlfields):
        value = field.value or ""

        for position, bad_char in _iter_invalid_chars(value):
            matches.append(
                InvalidXMLCharacter(
                    record_id=record.id,
                    tag=field.tag,
                    code=None,
                    field_place=field_place,
                    subfield_place=None,
                    position=position,
                    bad_char=bad_char,
                    context=_context_around(value, position),
                    path=f"{field.tag} (controlfield #{field_place + 1})",
                )
            )

    for field_place, field in enumerate(record.datafields):
        for subfield_place, subfield in enumerate(field.subfields):
            if hasattr(subfield, "xref"):
                continue

            if Config.is_authority_controlled(record.record_type, field.tag, subfield.code):
                continue

            value = subfield.value or ""

            for position, bad_char in _iter_invalid_chars(value):
                matches.append(
                    InvalidXMLCharacter(
                        record_id=record.id,
                        tag=field.tag,
                        code=subfield.code,
                        field_place=field_place,
                        subfield_place=subfield_place,
                        position=position,
                        bad_char=bad_char,
                        context=_context_around(value, position),
                        path=f"{field.tag}${subfield.code}",
                    )
                )

    return matches


def replace_invalid_xml_char(record, occurrence: InvalidXMLCharacter, *, replacement: str) -> bool:
    if len(replacement) > 1:
        raise ValueError("replacement must be at most one character")

    if occurrence.code is None:
        if occurrence.field_place >= len(record.controlfields):
            return False
        field = record.controlfields[occurrence.field_place]
        if field.tag != occurrence.tag:
            return False
        value = field.value or ""
        updated = _replace_at(value, occurrence.position, occurrence.bad_char, replacement)

        if updated is None:
            return False

        field.value = updated
        return True

    if occurrence.field_place >= len(record.datafields):
        return False
    field = record.datafields[occurrence.field_place]
    if field.tag != occurrence.tag:
        return False

    if occurrence.subfield_place is None or occurrence.subfield_place >= len(field.subfields):
        return False

    subfield = field.subfields[occurrence.subfield_place]
    value = subfield.value or ""
    updated = _replace_at(value, occurrence.position, occurrence.bad_char, replacement)

    if updated is None:
        return False

    subfield.value = updated
    return True


def _iter_invalid_chars(value: str):
    for i, char in enumerate(value):
        if _INVALID_XML_CHARS.match(char):
            yield i, char


def _context_around(value: str, position: int, window: int = 20) -> str:
    start = max(0, position - window)
    end = min(len(value), position + window + 1)
    before = value[start:position]
    after = value[position + 1 : end]

    return f"{before}[[INVALID:{ord(value[position]):04X}]]{after}"


def _replace_at(value: str, position: int, expected: str, replacement: str) -> str | None:
    if position < 0 or position >= len(value):
        return None

    if value[position] != expected:
        return None

    return value[:position] + replacement + value[position + 1 :]
