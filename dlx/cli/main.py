import argparse
import json
from datetime import datetime

from rich.console import Console
from rich.table import Table

from dlx.cli.app import SearchResultsApp
from dlx.cli.search import CLIConnectionError, connect_db, search_payload, search_records
from dlx.marc.query import InvalidQueryString


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dlx")
    subparsers = parser.add_subparsers(dest="command")

    search = subparsers.add_parser("search", help="Run a MARC query")
    search.add_argument("query", nargs="?", help="Query string (dlx Query syntax)")
    search.add_argument("--record-type", choices=("bib", "auth"), default="bib")
    search.add_argument("--limit", type=int, default=20)
    search.add_argument("--skip", type=int, default=0)
    search.add_argument("--db-uri", dest="db_uri")
    search.add_argument("--database")
    search.add_argument("--commit-user", dest="commit_user", help="User value for commit(user=...) on selected rows")
    search.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip schema validation during re-save batch actions while still applying save actions",
    )
    search.add_argument("--json", action="store_true", help="Emit JSON output")
    search.add_argument(
        "--textual",
        dest="textual",
        action="store_true",
        default=True,
        help="Use Textual UI output",
    )
    search.add_argument(
        "--no-textual",
        dest="textual",
        action="store_false",
        help="Use plain terminal table output",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "search":
        return _run_search(args)

    parser.print_help()
    return 1


def run() -> None:
    raise SystemExit(main())


def _run_search(args) -> int:
    stderr = Console(stderr=True)
    stdout = Console()

    if not args.textual and not args.query:
        stderr.print("[bold red]Error:[/bold red] Query is required with --no-textual output")
        return 1

    if args.json and not args.query:
        stderr.print("[bold red]Error:[/bold red] Query is required with --json output")
        return 1

    rows = []
    total = 0
    query_doc = {}

    try:
        connect_db(args.db_uri, database=args.database)
        if args.query and (not args.textual or args.json):
            rows, total, query_doc = search_records(
                args.query,
                record_type=args.record_type,
                limit=args.limit,
                skip=args.skip,
            )
    except InvalidQueryString as exc:
        stderr.print(f"[bold red]Invalid query:[/bold red] {exc}")
        return 2
    except (ValueError, CLIConnectionError) as exc:
        stderr.print(f"[bold red]Error:[/bold red] {exc}")
        return 1

    if args.json:
        payload = search_payload(
            rows,
            total=total,
            record_type=args.record_type,
            query_string=args.query or "",
            query_doc=query_doc,
            limit=args.limit,
            skip=args.skip,
        )
        print(json.dumps(payload, default=_json_default))
        return 0

    if args.textual:
        app = SearchResultsApp(
            rows=rows,
            total=total,
            record_type=args.record_type,
            query=args.query or "",
            limit=args.limit,
            skip=args.skip,
            commit_user=args.commit_user,
            default_skip_validation=args.skip_validation,
        )
        app.run()
        return 0

    table = Table(title="dlx search")
    table.add_column("ID")
    table.add_column("Primary")
    table.add_column("Secondary")
    table.add_column("Updated")

    for row in rows:
        table.add_row(
            str(row.id) if row.id is not None else "",
            row.primary or "",
            row.secondary or "",
            row.updated or "",
        )

    stdout.print(table)
    return 0


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
