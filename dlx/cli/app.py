import asyncio

from textual import events
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, Footer, Header, Input, Static

from dlx.cli.search import (
    SearchRow,
    create_search_snapshot,
    find_invalid_xml_literals,
    get_record,
    get_snapshot_page,
    replace_invalid_xml_char,
    resave_records,
    snapshot_record_ids,
)
from dlx.cli.save_actions import apply_save_actions


class XMLReplacementDialog(ModalScreen):
    CSS = """
    #xml-dialog {
      width: 88;
      height: auto;
      border: round $primary;
      background: $surface;
      padding: 1 2;
    }

    #xml-dialog-actions {
      padding-top: 1;
      width: 100%;
      align: right middle;
    }
    """

    def __init__(self, *, occurrence):
        super().__init__()
        self.occurrence = occurrence

    def compose(self) -> ComposeResult:
        with Vertical(id="xml-dialog"):
            yield Static("Invalid XML character found")
            yield Static(f"Path: {self.occurrence.path}")
            yield Static(f"Codepoint: {self.occurrence.codepoint}")
            yield Static(f"Context: {self.occurrence.context}")
            yield Input(
                placeholder="Replacement character (leave empty to remove)",
                id="replacement-input",
            )
            yield Static("", id="replacement-error")
            with Horizontal(id="xml-dialog-actions"):
                yield Button("Apply", variant="primary", id="apply")
                yield Button("Skip row", id="skip-row")
                yield Button("Cancel", id="cancel-all")

    def on_mount(self) -> None:
        self.query_one("#replacement-input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "replacement-input":
            self._apply()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id

        if button_id == "apply":
            self._apply()
        elif button_id == "skip-row":
            self.dismiss("__SKIP_ROW__")
        elif button_id == "cancel-all":
            self.dismiss(None)

    def _apply(self) -> None:
        value = self.query_one("#replacement-input", Input).value

        if len(value) > 1:
            self.query_one("#replacement-error", Static).update("Replacement must be 0 or 1 character")
            return

        self.dismiss(value)


class SearchResultsApp(App):
    BINDINGS = [
        ("space", "toggle_select", "Toggle row"),
        ("a", "select_all", "Select all"),
        ("x", "clear_selection", "Clear selection"),
        ("c", "toggle_batch_scope", "Toggle scope"),
        ("f", "fix_bad_chars", "Fix bad chars"),
        ("r", "resave_selected", "Re-save"),
        ("v", "resave_skip_validation", "Re-save skip validation"),
        ("z", "stop_operation", "Stop operation"),
        ("p", "previous_page", "Prev page"),
        ("n", "next_page", "Next page"),
        ("q", "quit", "Quit"),
    ]

    CSS = """
    #controls, #batch-actions {
      height: 3;
      padding: 0;
    }

    #summary {
      height: 1;
      padding: 0 1;
      border: solid $primary;
    }

    #status {
      height: 1;
      padding: 0 1;
    }

    #activity {
      height: 1;
      padding: 0 1;
      color: $warning;
    }

    #results {
      height: 1fr;
    }
    """

    def __init__(
        self,
        *,
        rows: list[SearchRow],
        total: int,
        record_type: str,
        query: str,
        limit: int,
        skip: int,
        commit_user: str | None,
        default_skip_validation: bool = False,
    ):
        super().__init__()
        self.rows = rows
        self.total = total
        self.record_type = record_type
        self.query = query or ""
        self.limit = limit
        self.skip = skip
        self.commit_user = commit_user
        self.default_skip_validation = default_skip_validation
        self.selected_ids: set[int] = set()
        self.snapshot_id: str | None = None
        self.batch_scope: str = "page"  # "page" or "all-query"
        self.status_message = "Search, select rows, then run a batch action"
        self._busy_label: str | None = None
        self._busy_dots = 0
        self._mouse_drag_active = False
        self._mouse_drag_toggle = False
        self._mouse_drag_seen_rows: set[int] = set()
        self._pending_click_toggle = False
        self._pending_click_shift = False
        self._selection_anchor_row: int | None = None
        self._next_cursor: int | None = None
        self._prev_cursor: int | None = None
        self._busy_cancellable = False
        self._cancel_requested = False
        self._operation_name: str | None = None
        self._operation_done = 0
        self._operation_total = 0

    def compose(self) -> ComposeResult:
        yield Header()
        with Container():
            with Horizontal(id="controls"):
                yield Input(value=self.query, placeholder="Query (dlx Query syntax)", id="query-input")
                yield Button("Search", id="search-btn", variant="primary")
                yield Button("Prev", id="prev-btn")
                yield Button("Next", id="next-btn")
            with Horizontal(id="batch-actions"):
                yield Button("Fix bad chars", id="fix-btn")
                yield Button("Re-save", id="resave-btn")
                yield Button("Re-save (skip validation)", id="resave-skip-btn")
                yield Button("Stop", id="stop-btn")
                yield Button(self._batch_scope_label(), id="scope-btn")
                yield Button("More options soon", id="more-btn")
            yield Static(self._summary_text(), id="summary")
            yield Static(self.status_message, id="status")
            yield Static("Ready", id="activity")
            yield DataTable(id="results")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#results", DataTable)
        table.cursor_type = "row"
        table.add_columns("Sel", "ID", "Primary", "Secondary", "Updated")
        self._render_rows()
        self.set_interval(0.35, self._animate_busy_indicator)
        self._refresh_controls()

        if self.query:
            self._start_search(reset_skip=False, use_input=False)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "query-input":
            self._start_search(reset_skip=True, use_input=True)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id

        if button_id == "search-btn":
            self._start_search(reset_skip=True, use_input=True)
        elif button_id == "prev-btn":
            self.action_previous_page()
        elif button_id == "next-btn":
            self.action_next_page()
        elif button_id == "fix-btn":
            self.action_fix_bad_chars()
        elif button_id == "resave-btn":
            self.action_resave_selected()
        elif button_id == "resave-skip-btn":
            self.action_resave_skip_validation()
        elif button_id == "stop-btn":
            self.action_stop_operation()
        elif button_id == "more-btn":
            self._set_status("Additional batch operations can be added here")

    def on_mouse_down(self, event: events.MouseDown) -> None:
        table = self.query_one("#results", DataTable)
        if self._busy_label or event.button != 1 or not self._event_targets_table(event, table):
            return

        self._mouse_drag_active = True
        self._mouse_drag_toggle = bool(event.ctrl or event.meta)
        self._mouse_drag_seen_rows.clear()
        self._pending_click_toggle = self._mouse_drag_toggle
        self._pending_click_shift = bool(event.shift)
        self.call_after_refresh(self._apply_mouse_cursor_selection)

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if event.button == 1:
            self._mouse_drag_active = False
            self._mouse_drag_seen_rows.clear()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if self._mouse_drag_active:
            self._apply_row_selection_for_mouse(event.cursor_row, drag=True)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if self._busy_label:
            return

        if self._mouse_drag_active:
            self._apply_row_selection_for_mouse(event.cursor_row, drag=True)
            return

        self._apply_row_selection_for_click(
            event.cursor_row,
            toggle=self._pending_click_toggle,
            shift=self._pending_click_shift,
        )
        self._pending_click_toggle = False
        self._pending_click_shift = False

    def _summary_text(self) -> str:
        end = min(self.total, self.skip + len(self.rows))
        if self.selected_ids:
            scope = "selected"
        elif self.batch_scope == "all-query":
            scope = "all query results"
        else:
            scope = "current page"
        return (
            f"record_type={self.record_type}  query={self.query!r}  "
            f"matches={self.total}  page={self.skip // self.limit + 1 if self.limit else 1}  "
            f"window={self.skip + 1 if self.total else 0}-{end}  "
            f"limit={self.limit}  selected={len(self.selected_ids)}  batch_scope={scope}"
        )

    def _batch_scope_label(self) -> str:
        return f"Batch: {self.batch_scope.title()}"

    def _render_rows(self) -> None:
        table = self.query_one("#results", DataTable)
        table.clear(columns=False)

        for row in self.rows:
            table.add_row(
                self._selected_marker(row.id),
                str(row.id) if row.id is not None else "",
                row.primary or "",
                row.secondary or "",
                row.updated or "",
            )

        self._refresh_summary()
        self._refresh_controls()

    def _start_search(self, *, reset_skip: bool, use_input: bool) -> None:
        query = self.query_one("#query-input", Input).value.strip() if use_input else self.query
        if not query:
            self._set_status("Enter a query to search")
            return

        self.query = query
        if reset_skip:
            self.skip = 0
            self.selected_ids.clear()
            self.rows = []
            self.total = 0
            self.snapshot_id = None
            self._render_rows()

        self._set_status("Searching...")
        self._set_busy("Searching")
        self.run_worker(
            self._search_flow(query=self.query, cursor=self.skip, reset=reset_skip),
            name="search-records",
            exclusive=True,
            exit_on_error=False,
        )

    async def _search_flow(self, *, query: str, cursor: int, reset: bool) -> None:
        try:
            if reset or not self.snapshot_id:
                snapshot = await asyncio.to_thread(
                    create_search_snapshot,
                    query,
                    record_type=self.record_type,
                )
                self.snapshot_id = snapshot.id

            rows, page = await asyncio.to_thread(
                get_snapshot_page,
                self.snapshot_id,
                limit=self.limit,
                cursor=cursor,
            )
        except Exception as exc:
            self._set_status(f"Search failed: {exc}")
            self._set_busy(None)
            return

        self.query = page["query"]
        self.skip = page["cursor"]
        self.rows = rows
        self.total = page["total"]
        self._next_cursor = page["next_cursor"]
        self._prev_cursor = page["prev_cursor"]
        self._render_rows()
        self._set_status(f"Loaded {len(rows)} / {self.total} records")
        self._set_busy(None)

    def action_toggle_select(self) -> None:
        table = self.query_one("#results", DataTable)
        row_index = table.cursor_row

        if row_index is None or not (0 <= row_index < len(self.rows)):
            return

        row = self.rows[row_index]

        if row.id is None:
            self._set_status("Cannot select a row without a record ID")
            return

        if row.id in self.selected_ids:
            self.selected_ids.remove(row.id)
        else:
            self.selected_ids.add(row.id)

        self._set_row_marker(row_index, row.id)
        self._refresh_summary()
        self._selection_anchor_row = row_index

    def action_select_all(self) -> None:
        self.selected_ids = {row.id for row in self.rows if row.id is not None}

        for row_index, row in enumerate(self.rows):
            self._set_row_marker(row_index, row.id)

        self._refresh_summary()
        self._set_status(f"Selected {len(self.selected_ids)} records")
        self._refresh_controls()

    def action_clear_selection(self) -> None:
        self.selected_ids.clear()

        for row_index, row in enumerate(self.rows):
            self._set_row_marker(row_index, row.id)

        self._refresh_summary()
        self._set_status("Selection cleared")
        self._refresh_controls()

    def action_previous_page(self) -> None:
        if self._prev_cursor is None:
            self._set_status("Already on the first page")
            return

        self.skip = self._prev_cursor
        self._start_search(reset_skip=False, use_input=False)

    def action_next_page(self) -> None:
        if self._next_cursor is None:
            self._set_status("Already on the last page")
            return

        self.skip = self._next_cursor
        self._start_search(reset_skip=False, use_input=False)

    def action_toggle_batch_scope(self) -> None:
        self.batch_scope = "all-query" if self.batch_scope == "page" else "page"
        self.query_one("#scope-btn", Button).label = self._batch_scope_label()
        self._set_status(f"Batch scope: {self.batch_scope}")
        self._refresh_summary()

    def action_fix_bad_chars(self) -> None:
        target_ids, target_label = self._batch_target_ids()
        if not self._ensure_batch_ready(target_ids):
            return

        self._start_batch_operation("Fix bad chars", total=len(target_ids))
        self.run_worker(
            self._fix_bad_chars_flow(target_ids=target_ids, target_label=target_label),
            name="fix-bad-chars",
            exclusive=True,
            exit_on_error=False,
        )

    def action_resave_selected(self) -> None:
        target_ids, _ = self._batch_target_ids()
        if not self._ensure_batch_ready(target_ids):
            return
        self._run_resave(target_ids=target_ids, skip_validation=self.default_skip_validation)

    def action_resave_skip_validation(self) -> None:
        target_ids, _ = self._batch_target_ids()
        if not self._ensure_batch_ready(target_ids):
            return
        self._run_resave(target_ids=target_ids, skip_validation=True)

    def action_stop_operation(self) -> None:
        if self._busy_label and self._busy_cancellable:
            self._cancel_requested = True
            self._set_status("Stop requested; finishing current item...")

    def _ensure_batch_ready(self, target_ids: list[int]) -> bool:
        if not target_ids:
            self._set_status("No batch target rows available")
            return False

        if not self.commit_user or not self.commit_user.strip():
            self._set_status("Batch actions require --commit-user")
            return False

        return True

    def _run_resave(self, *, target_ids: list[int], skip_validation: bool) -> None:
        self._start_batch_operation("Re-save", total=len(target_ids))
        self.run_worker(
            self._resave_flow(target_ids=target_ids, skip_validation=skip_validation),
            name="resave-selected",
            exclusive=True,
            exit_on_error=False,
        )

    async def _resave_flow(self, *, target_ids: list[int], skip_validation: bool) -> None:
        result = {
            "committed_ids": [],
            "missing_ids": [],
            "errors": [],
            "save_action_record_ids": [],
            "save_action_field_count": 0,
            "save_action_normalized_value_count": 0,
        }

        try:
            for i, record_id in enumerate(target_ids, start=1):
                if self._cancel_requested:
                    break

                item_result = await asyncio.to_thread(
                    resave_records,
                    [record_id],
                    record_type=self.record_type,
                    user=self.commit_user,
                    sanitize_invalid_xml=False,
                    apply_save_action_rules=True,
                    skip_validation=skip_validation,
                )
                self._operation_done = i

                result["committed_ids"].extend(item_result["committed_ids"])
                result["missing_ids"].extend(item_result["missing_ids"])
                result["errors"].extend(item_result["errors"])
                result["save_action_record_ids"].extend(item_result["save_action_record_ids"])
                result["save_action_field_count"] += item_result["save_action_field_count"]
                result["save_action_normalized_value_count"] += item_result["save_action_normalized_value_count"]

            cancelled = self._cancel_requested
            self._set_status(
                "Resave complete: "
                f"committed={len(result['committed_ids'])}, "
                f"save_action_records={len(result['save_action_record_ids'])}, "
                f"save_action_fields={result['save_action_field_count']}, "
                f"normalized_dates={result['save_action_normalized_value_count']}, "
                f"missing={len(result['missing_ids'])}, errors={len(result['errors'])}, "
                f"skip_validation={skip_validation}, cancelled={cancelled}"
            )
        finally:
            self._finish_batch_operation()

    async def _fix_bad_chars_flow(self, *, target_ids: list[int], target_label: str) -> None:
        committed_count = 0
        missing_count = 0
        error_count = 0
        sanitized_count = 0
        skipped_count = 0

        try:
            for i, record_id in enumerate(target_ids, start=1):
                if self._cancel_requested:
                    break

                record = get_record(record_id, record_type=self.record_type)
                self._operation_done = i

                if record is None:
                    missing_count += 1
                    continue

                skip_row = False

                while True:
                    invalid = find_invalid_xml_literals(record)
                    if not invalid:
                        break

                    replacement = await self.push_screen_wait(XMLReplacementDialog(occurrence=invalid[0]))

                    if replacement is None:
                        self._set_status("Fix bad chars cancelled")
                        return

                    if replacement == "__SKIP_ROW__":
                        skipped_count += 1
                        skip_row = True
                        break

                    if replace_invalid_xml_char(record, invalid[0], replacement=replacement):
                        sanitized_count += 1

                if skip_row:
                    continue

                try:
                    apply_save_actions(record)
                    record.commit(user=self.commit_user)
                    committed_count += 1
                except Exception:
                    error_count += 1

            self._set_status(
                f"Fix bad chars complete: committed={committed_count}, sanitized={sanitized_count}, "
                f"skipped={skipped_count}, missing={missing_count}, errors={error_count}, "
                f"scope={target_label}, cancelled={self._cancel_requested}"
            )
        finally:
            self._finish_batch_operation()

    def _selected_marker(self, row_id: int | None) -> str:
        return "●" if row_id is not None and row_id in self.selected_ids else " "

    def _set_row_marker(self, row_index: int, row_id: int | None) -> None:
        table = self.query_one("#results", DataTable)
        table.update_cell_at((row_index, 0), self._selected_marker(row_id))

    def _refresh_summary(self) -> None:
        self.query_one("#summary", Static).update(self._summary_text())
        self._refresh_controls()

    def _set_status(self, message: str) -> None:
        self.status_message = message
        self.query_one("#status", Static).update(message)

    def _refresh_controls(self) -> None:
        if not self.is_mounted:
            return

        has_query = bool(self.query_one("#query-input", Input).value.strip())
        has_batch_target = bool(self.selected_ids) or bool(self.snapshot_id and self.total)
        can_commit = bool(self.commit_user and self.commit_user.strip())
        has_prev = self._prev_cursor is not None
        has_next = self._next_cursor is not None
        is_busy = self._busy_label is not None

        self.query_one("#search-btn", Button).disabled = is_busy or not has_query
        self.query_one("#prev-btn", Button).disabled = is_busy or not has_prev
        self.query_one("#next-btn", Button).disabled = is_busy or not has_next
        self.query_one("#fix-btn", Button).disabled = is_busy or not (has_batch_target and can_commit)
        self.query_one("#resave-btn", Button).disabled = is_busy or not (has_batch_target and can_commit)
        self.query_one("#resave-skip-btn", Button).disabled = is_busy or not (has_batch_target and can_commit)
        self.query_one("#stop-btn", Button).disabled = not (is_busy and self._busy_cancellable)
        self.query_one("#more-btn", Button).disabled = is_busy

    def _set_busy(self, label: str | None, *, cancellable: bool = False) -> None:
        self._busy_label = label
        self._busy_dots = 0
        self._busy_cancellable = cancellable
        self._refresh_controls()
        self._animate_busy_indicator()

    def _animate_busy_indicator(self) -> None:
        if not self.is_mounted:
            return

        activity = self.query_one("#activity", Static)

        if not self._busy_label:
            activity.update("Ready")
            return

        self._busy_dots = (self._busy_dots + 1) % 4
        dots = "." * (self._busy_dots + 1)
        progress = f"{self._busy_label}"

        if self._operation_total:
            progress = f"({self._operation_done}/{self._operation_total}) {progress}"

        progress = f"{progress}{dots}"

        activity.update(f"Operation in progress: {progress}")

        if self._busy_label == "Searching":
            self.query_one("#status", Static).update(progress)

    def _event_targets_table(self, event: events.MouseEvent, table: DataTable) -> bool:
        widget = getattr(event, "control", None)
        while widget is not None:
            if widget is table:
                return True
            widget = widget.parent
        return False

    def _apply_mouse_cursor_selection(self) -> None:
        table = self.query_one("#results", DataTable)
        row_index = table.cursor_row
        if row_index is None:
            return
        self._apply_row_selection_for_mouse(row_index, drag=False)

    def _apply_row_selection_for_mouse(self, row_index: int, *, drag: bool) -> None:
        if not (0 <= row_index < len(self.rows)):
            return

        row_id = self.rows[row_index].id
        if row_id is None:
            return

        if drag and row_index in self._mouse_drag_seen_rows:
            return

        if self._mouse_drag_toggle:
            if row_id in self.selected_ids:
                self.selected_ids.remove(row_id)
            else:
                self.selected_ids.add(row_id)
        else:
            if not drag or not self._mouse_drag_seen_rows:
                self.selected_ids.clear()
            self.selected_ids.add(row_id)

        self._mouse_drag_seen_rows.add(row_index)
        self._selection_anchor_row = row_index
        self._set_row_marker(row_index, row_id)
        self._refresh_row_markers()
        self._refresh_summary()

    def _apply_row_selection_for_click(self, row_index: int, *, toggle: bool, shift: bool) -> None:
        if not (0 <= row_index < len(self.rows)):
            return
        if self.rows[row_index].id is None:
            return

        if shift and self._selection_anchor_row is not None:
            start = min(self._selection_anchor_row, row_index)
            end = max(self._selection_anchor_row, row_index)
            for i in range(start, end + 1):
                row_id = self.rows[i].id
                if row_id is not None:
                    self.selected_ids.add(row_id)
        elif toggle:
            row_id = self.rows[row_index].id
            if row_id in self.selected_ids:
                self.selected_ids.remove(row_id)
            else:
                self.selected_ids.add(row_id)
            self._selection_anchor_row = row_index
        else:
            self.selected_ids.clear()
            row_id = self.rows[row_index].id
            if row_id is not None:
                self.selected_ids.add(row_id)
            self._selection_anchor_row = row_index

        self._refresh_row_markers()
        self._refresh_summary()

    def _refresh_row_markers(self) -> None:
        for i, row in enumerate(self.rows):
            self._set_row_marker(i, row.id)

    def _batch_target_ids(self) -> tuple[list[int], str]:
        if self.selected_ids:
            return sorted(self.selected_ids), "selected"

        if self.batch_scope == "all-query":
            if self.snapshot_id:
                try:
                    return snapshot_record_ids(self.snapshot_id), "all-query"
                except ValueError:
                    self._set_status("Search snapshot expired; run search again")
                    return [], "none"
            return [], "none"

        # batch_scope == "page"
        page_ids = [row.id for row in self.rows if row.id is not None]
        return page_ids, "page"

    def _start_batch_operation(self, name: str, *, total: int) -> None:
        self._operation_name = name
        self._operation_total = total
        self._operation_done = 0
        self._cancel_requested = False
        self._set_busy(name, cancellable=True)

    def _finish_batch_operation(self) -> None:
        self._operation_name = None
        self._operation_total = 0
        self._operation_done = 0
        self._cancel_requested = False
        self._set_busy(None)
