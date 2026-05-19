# Prompt: Build a Python Textual TUI for MARC Operations (No `dlx-rest`)

You are implementing a terminal-first Textual TUI for MARC bibliographic and authority operations using Python and [Textual](https://github.com/Textualize/textual) (with Rich for rendering/output primitives).

## Goal

Create a developer/operator TUI experience that:

1. Uses the existing `dlx` Python package directly for MARC and file workflows.
2. Does **not** depend on, call, import, or assume the presence of `dlx-rest`.
3. Provides clear, interactive, human-friendly terminal output (tables, panels, progress, prompts).
4. Uses a Textual main page with:
   - a search bar
   - a paginated results area
   - multi-select result rows for batch operations

## Hard Constraints

- No dependency on downstream `dlx-rest` services, APIs, or schemas.
- Keep implementation inside this repository and aligned with current `dlx` patterns.
- Use existing database and model layers (`dlx.db`, `dlx.marc`, `dlx.file`) rather than duplicating business logic.
- Preserve existing behavior of core library code unless explicit CLI-specific wrapping is required.
- CLI errors must be explicit and actionable (no silent failures).
- Mirror current `dlx-rest` validation and save-action behavior in Python for TUI usage, while avoiding any runtime JS/browser dependency.

## Primary Use Cases

- Connect to MongoDB (`mongomock://` for tests, real URI for runtime).
- Query and inspect Bib/Auth records from MARC collections.
- Create/update records from structured CLI input and/or import files.
- Show record summaries, field/subfield breakdowns, and diffs before commit.
- Run index/logical-field maintenance helpers exposed in `dlx/scripts` where appropriate.
- Support file metadata workflows through `dlx.file` where feasible.

## UX Requirements (Textual-first)

- Consistent command output theme with readable colors and minimal noise.
- `--json` mode for machine-readable output on all non-interactive commands.
- Prefer Textual screens/widgets/interactions for operator workflows; use Rich tables/panels/progress where appropriate.
- Confirmation prompts for destructive actions (delete/overwrite/bulk updates).
- `--yes`/`-y` flag to bypass confirmations in automation contexts.
- Add a flag to skip validation while still applying save actions (e.g., `--skip-validation`); this must bypass validation checks only, not save-action transforms.
- Main page requirements:
  - Search bar for query entry.
  - Results area that supports selecting and deselecting multiple records for batch actions.
  - Batch actions menu that includes:
    - Fix bad characters (existing functionality).
    - Re-save selected records with validations aligned to current rules in `dlx-rest` `validation.js` (reference only, no runtime dependency): https://github.com/dag-hammarskjold-library/dlx-rest/blob/main/dlx_rest/static/js/utils/validation.js
    - Extension point for additional batch operations.
  - Pagination using existing pagination/query mechanisms in `dlx`, with controls to navigate to additional pages.
- Clear exit codes:
  - `0` success
  - non-zero on validation/runtime/connection failures

## Suggested Command Surface

If command groups are retained for non-interactive entry points, design them to map to `dlx` concepts:

- `db`: connect/test/status
- `bib`: find/show/new/update/delete
- `auth`: find/show/new/update/delete
- `marc`: validate/normalize/diff/import/export
- `index`: rebuild text/logical/tag indexes
- `file`: import/list/show (metadata-oriented first)
- `config`: inspect effective runtime settings

Prefer Textual as the primary interface framework. If command-style entry points are also provided, use a composable CLI framework (e.g., Typer or Click) that interoperates cleanly with Textual/Rich output.

## Implementation Guidance

- Create a dedicated CLI package/module (for example `dlx/cli/`).
- Centralize connection/bootstrap logic (single source for `DB.connect(...)` handling).
- Add output adapters: Textual-first renderer (with Rich components as needed) + JSON serializer from same data model.
- Reuse `dlx.util.bulk_write` where bulk operations are needed.
- Surface domain exceptions with concise operator guidance.
- Keep commands idempotent where practical and document non-idempotent behavior.
- Build the main Textual screen around search + paginated results + selectable batch operations.
- Implement batch operation wiring so "fix bad characters" and "re-save with validations" both work on selected records and can be extended with new actions.
- Support a "skip validation, still apply save actions" execution path for re-save operations via the dedicated flag.
- Add an explicit migration step for validation/save actions:
  - Reproduce `validation.js` data and `runSaveActions` behavior in `dlx` Python modules (or a shared repo-local data artifact consumed by Python), preserving current outcomes.
  - Keep this as an intermediate parity layer for TUI and future full migration, with clear boundaries so web-app-specific JS can be removed later without changing rule semantics.
  - Ensure TUI batch "re-save with validations" invokes this Python parity logic before commit.

## Testing Expectations

- Unit tests for command handlers and output adapters.
- Integration-style CLI tests using `mongomock://localhost`.
- Cover:
  - successful connect/query/update flows
  - invalid input and auth-control edge cases
  - JSON output shape stability
  - confirmation bypass/guard behavior
  - validation/save-action parity with representative cases from current `validation.js` + `runSaveActions` behavior
  - flag behavior for skipping validation while confirming save actions still run

## Deliverables

1. CLI module(s) with entry point wiring.
2. Root-level usage documentation (README section or dedicated docs page).
3. Tests for core command paths.
4. Example command snippets for common MARC workflows.
5. TUI interaction notes/screenshots (or equivalent docs) showing search, selection, batch actions, and pagination navigation.
6. A migration note documenting where validation/save-action parity logic lives in `dlx` and how it maps to legacy `validation.js`/`runSaveActions`.

## Non-Goals

- Building a web server or REST API.
- Replacing `dlx` core data model behavior.
- Tight coupling to any external downstream application.

## Definition of Done

- A user can install and run a Textual-powered TUI locally against `dlx` data stores.
- Key MARC workflows are available from the terminal with readable and JSON outputs.
- No code path requires `dlx-rest`.
- Tests pass with the project’s standard pytest workflow.
- Main page includes search input, paginated results navigation, and selectable batch operations including bad-character fixes and validation-backed re-save.
- Validation and save-action behavior used by TUI matches current `dlx-rest` outcomes for covered parity cases.
