import json


def test_cli_search_json_bib(db, capsys):
    from dlx.cli.main import main

    capsys.readouterr()
    code = main(["search", "245__a:'This'", "--json", "--no-textual"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["record_type"] == "bib"
    assert payload["query"] == "245__a:'This'"
    assert payload["total"] == 1
    assert payload["results"][0]["id"] == 1
    assert payload["results"][0]["primary"] == "This is the title"


def test_cli_search_json_auth(db, capsys):
    from dlx.cli.main import main

    capsys.readouterr()
    code = main(
        [
            "search",
            "110__a:'Another header'",
            "--record-type",
            "auth",
            "--json",
            "--no-textual",
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 0
    assert payload["record_type"] == "auth"
    assert payload["total"] == 1
    assert payload["results"][0]["id"] == 2
    assert payload["results"][0]["primary"] == "Another header"

def test_cli_search_json_implies_no_textual(db):
    from dlx.cli.main import build_parser
    from dlx.cli.main import _run_search

    args = build_parser().parse_args(["search", "245__a:'This'", "--json"])

    assert args.json is True
    assert args.textual is True

    # runtime behavior should force --json to non-textual mode
    code = _run_search(args)

    assert code == 0
    assert args.textual is False

def test_cli_search_invalid_query(db, capsys):
    from dlx.cli.main import main

    capsys.readouterr()
    code = main(["search", "245:title NOT 500:notes", "--json", "--no-textual"])
    captured = capsys.readouterr()

    assert code == 2
    assert "Invalid query:" in captured.err


def test_resave_records_bib_with_user(db):
    from dlx.cli.search import resave_records
    from dlx.marc import Bib

    result = resave_records([1], record_type="bib", user="cli-user")
    refreshed = Bib.from_id(1)

    assert result["committed_ids"] == [1]
    assert result["missing_ids"] == []
    assert result["errors"] == []
    assert refreshed.user == "cli-user"


def test_resave_records_requires_user(db):
    import pytest
    from dlx.cli.search import resave_records

    with pytest.raises(ValueError):
        resave_records([1], record_type="bib", user="")


def test_resave_records_sanitizes_invalid_xml_chars(db):
    from dlx.cli.search import resave_records
    from dlx.marc import Bib

    bib = Bib.from_id(1)
    bib.set("245", "a", "Has bad\x01xml char")
    bib.commit(user="before")

    result = resave_records([1], record_type="bib", user="cli-user")
    refreshed = Bib.from_id(1)

    assert result["committed_ids"] == [1]
    assert result["sanitized_record_ids"] == [1]
    assert result["sanitized_value_count"] == 1
    assert refreshed.get_value("245", "a") == "Has badxml char"
    assert refreshed.user == "cli-user"


def test_find_invalid_and_replace_xml_chars(db):
    from dlx.cli.search import find_invalid_xml_literals, replace_invalid_xml_char
    from dlx.marc import Bib

    bib = Bib.from_id(1)
    bib.set("245", "a", "abc\x01def")

    occurrences = find_invalid_xml_literals(bib)
    assert len(occurrences) == 1
    assert occurrences[0].path == "245$a"
    assert occurrences[0].codepoint == "U+0001"
    assert "[[INVALID:0001]]" in occurrences[0].context

    assert replace_invalid_xml_char(bib, occurrences[0], replacement=" ") is True
    assert bib.get_value("245", "a") == "abc def"


def test_resave_records_applies_save_actions(db):
    from dlx.cli.search import resave_records
    from dlx.marc import Bib

    bib = Bib.from_id(1)
    bib.set("089", "b", "B22", ind1=" ", ind2=" ", auth_control=False, address=["+", "+"])
    bib.commit(user="before")

    result = resave_records(
        [1],
        record_type="bib",
        user="cli-user",
        sanitize_invalid_xml=False,
    )
    refreshed = Bib.from_id(1)

    assert result["committed_ids"] == [1]
    assert result["save_action_record_ids"] == [1]
    assert refreshed.get_value("989", "a") == "Speeches"


def test_resave_skip_validation_still_applies_save_actions(db):
    from dlx import DB
    from dlx.cli.search import resave_records
    from dlx.marc import Bib

    doc = DB.bibs.find_one({"_id": 1})
    doc["245"][0]["subfields"].append({"code": "AA", "value": "invalid schema code"})
    doc["089"] = [{"indicators": [" ", " "], "subfields": [{"code": "b", "value": "B22"}]}]
    DB.bibs.replace_one({"_id": 1}, doc)

    failed = resave_records(
        [1],
        record_type="bib",
        user="cli-user",
        sanitize_invalid_xml=False,
        skip_validation=False,
    )
    assert failed["committed_ids"] == []
    assert len(failed["errors"]) == 1

    succeeded = resave_records(
        [1],
        record_type="bib",
        user="cli-user",
        sanitize_invalid_xml=False,
        skip_validation=True,
    )
    refreshed = Bib.from_id(1)

    assert succeeded["committed_ids"] == [1]
    assert succeeded["errors"] == []
    assert refreshed.get_value("989", "a") == "Speeches"


def test_resave_records_save_action_criteria_use_original_fields(db):
    from dlx.cli.search import resave_records
    from dlx.marc import Bib

    bib = Bib.from_id(1)
    bib.set("989", "a", "Documents and Publications", ind1=" ", ind2=" ", auth_control=False, address=["+", "+"])
    bib.commit(user="before")

    result = resave_records(
        [1],
        record_type="bib",
        user="cli-user",
        sanitize_invalid_xml=False,
    )
    refreshed = Bib.from_id(1)

    assert result["committed_ids"] == [1]
    assert result["save_action_record_ids"] == []
    assert refreshed.get_fields("989") == []


def test_search_snapshot_pagination(db):
    from dlx import DB
    from dlx.cli.search import create_search_snapshot, get_snapshot_page

    DB.bibs.insert_one(
        {
            "_id": 3,
            "000": ["leader"],
            "245": [{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "This"}]}],
        }
    )

    snapshot = create_search_snapshot("245__a:'This'", record_type="bib")
    rows1, page1 = get_snapshot_page(snapshot.id, limit=1, cursor=0)
    rows2, page2 = get_snapshot_page(snapshot.id, limit=1, cursor=1)

    assert snapshot.total == 2
    assert len(rows1) == 1
    assert len(rows2) == 1
    assert rows1[0].id != rows2[0].id
    assert page1["next_cursor"] == 1
    assert page2["prev_cursor"] == 0


def test_search_snapshot_missing(db):
    import pytest
    from dlx.cli.search import get_snapshot_page

    with pytest.raises(ValueError):
        get_snapshot_page("missing", limit=10, cursor=0)


def test_batch_scope_toggle(db):
    """Test that batch scope toggle defaults to 'page' and can toggle to 'all-query'."""
    from dlx.cli.app import SearchResultsApp, SearchRow
    from dlx.cli.search import create_search_snapshot
    from dlx.marc import Bib

    # The db fixture already has 2 bibs (IDs 1, 2)
    # Add one more to test pagination
    bib3 = Bib()
    bib3.set("245", "a", "Title 3")
    bib3.set("245", "b", "Subtitle 3")
    bib3.commit("test_user")

    # Create a snapshot for records with 245__a (should match all 3 bibs)
    # all 3 bibs have 245__a set
    snapshot = create_search_snapshot("245__a:*", record_type="bib")
    assert snapshot.total == 3

    # Create app instance with 2 rows on the page (first page)
    rows = [
        SearchRow(id=1, primary="This", secondary="is the title", updated="2024-01-01"),
        SearchRow(id=2, primary="Another", secondary="is the title", updated="2024-01-02"),
    ]
    app = SearchResultsApp(
        rows=rows,
        total=3,
        record_type="bib",
        query="245__a:*",
        limit=2,
        skip=0,
        commit_user="test_user",
    )

    # Set snapshot
    app.snapshot_id = snapshot.id

    # Test default scope is "page"
    assert app.batch_scope == "page"

    # Test _batch_scope_label returns correct label
    assert app._batch_scope_label() == "Batch: Page"

    # Test _batch_target_ids with "page" scope returns only current page rows
    target_ids, label = app._batch_target_ids()
    assert target_ids == [1, 2]
    assert label == "page"

    # Toggle scope to "all-query"
    app.batch_scope = "all-query"
    assert app._batch_scope_label() == "Batch: All-Query"

    # Test _batch_target_ids with "all-query" scope returns all snapshot records
    target_ids, label = app._batch_target_ids()
    # Should get all 3 records
    assert sorted(target_ids) == [1, 2, 3]
    assert label == "all-query"

    # Test that selections take precedence over scope
    app.selected_ids = {1}
    target_ids, label = app._batch_target_ids()
    assert target_ids == [1]
    assert label == "selected"
