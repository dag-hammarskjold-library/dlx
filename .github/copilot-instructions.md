# Copilot instructions for `dlx`

## Build, test, and lint commands

```bash
# Install dependencies
python -m pip install --upgrade pip
pip install -r requirements.txt

# Install this package in editable mode while developing
pip install -e .

# Run full test suite (matches CI behavior)
pytest -vvW ignore

# Run one test file
pytest tests/test_marc.py -vvW ignore

# Run one test
pytest tests/test_marc.py::test_commit -vvW ignore
```

There is no active lint step in CI right now. `.github/workflows/pythonpackage.yml` has a commented-out `flake8` block, but linting is not currently enforced.

## High-level architecture

`dlx` is a Python package for ETL-style operations on MARC bibliographic/authority records and file metadata, backed by MongoDB and (for file binaries) S3.

- **Global service handles**:
  - `dlx.db.DB` is a global/class-level connection manager. `DB.connect(...)` initializes `DB.handle`, `DB.bibs`, `DB.auths`, and `DB.files`.
  - `dlx.file.s3.S3` is also global/class-level; file uploads require `S3.connect(...)` first.
- **Core data domains**:
  - `dlx.marc` contains record models (`Bib`, `Auth`) and set/query APIs (`BibSet`, `AuthSet`, `Query`, `Condition`).
  - `dlx.file` contains file metadata and upload/import flows (`File`, `Identifier`) and writes file metadata to MongoDB plus binary content to S3.
- **Search/index strategy**:
  - Record collections (`bibs`, `auths`) store canonical MARC JSON and denormalized fields like `text`, `words`, and configured logical fields.
  - Separate `_index_<tag>` and `_index_<logical_field>` collections are used for faster text/tag search workflows in query compilation.
  - Maintenance scripts under `dlx/scripts/` rebuild or initialize these denormalized/index collections (`init_indexes`, `build_logical_fields`, `build_text_collections`).

## Key codebase conventions

- **Always connect first**: many MARC APIs are guarded by `Decorators.check_connected`; calling without `DB.connect(...)` raises immediately.
- **Test environment convention**: use `DB.connect("mongomock://localhost")` for tests. The test suite assumes global DB state and resets via the `db` fixture in `tests/conftest.py`.
- **Use `dlx.util.bulk_write` instead of direct `collection.bulk_write` in code that must run under tests**: it provides a mongomock-compatible fallback for update/delete/insert operations.
- **Authority-control behavior is config-driven**:
  - `dlx.config.Config` maps authority-controlled tag/subfield pairs for bib/auth records.
  - When constructing from tables or importing, `auth_control=True` attempts to resolve linked authority references; ambiguous/invalid values raise explicit auth exceptions.
  - By default, table import drops subfield `0` unless `delete_subfield_zero=False`.
- **Logical fields and browse indexes come from `Config` mappings**:
  - `Config.bib_logical_fields` / `Config.auth_logical_fields` drive denormalization.
  - Scripts and record logic rely on these mappings; update config and rebuild indexes together.
- **File import contracts are strict**:
  - `File.import_*` requires `identifiers` as `Identifier` objects and valid ISO 639-1 language codes.
  - Duplicate checksum cases intentionally raise `FileExists`, `FileExistsIdentifierConflict`, or `FileExistsLanguageConflict` unless `overwrite=True`.
- **Collation awareness**:
  - Default MARC collation is `Config.marc_index_default_collation`; queries/indexes often depend on it.
  - Some tests note collation differences under mongomock vs real MongoDB; avoid assuming perfect parity in test-only behavior.
