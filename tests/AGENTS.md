# CLAUDE.md

> **Purpose:** This file exists to correct consistent agent mistakes and specify required tooling — nothing more.
> Do NOT auto-generate or expand this file. If you encounter something in this codebase,
> flag it to the developer and suggest an edit here. The developer will decide whether to fix the code or update this file.

---

## Required Tooling

<!-- PLACEHOLDER: List only the non-obvious tools the agent must use.
     Example: "Always use pnpm (not npm or yarn) to run scripts."
     If the tool is detectable from package.json or config files, omit it. -->

See root AGENTS.md for project-wide tooling requirements.

---

## Consistent Mistakes to Avoid

<!-- PLACEHOLDER: Only add entries here when the agent repeatedly makes the same error
     despite the codebase structure making the correct path clear.
     Each entry should be a single, specific correction. -->

<!-- Example format:
- DO NOT use [X pattern/library] — use [Y] instead. Reason: [one sentence].
- Always run `[command]` after modifying [area of codebase].
-->

- All tests use in-memory or temp-file databases — never touch production `nba_raw_data.db`
- Shared fixtures are in `conftest.py` (e.g., `sqlite_con`, `sqlite_con_with_data`)
- Test files follow naming pattern `test_<module>_<feature>.py` or `test_<module>.py`

---

## Legacy / Deprecated Technologies

<!-- PLACEHOLDER: List technologies still present in the codebase but no longer preferred.
     This prevents the agent from reaching for outdated patterns it finds in older files. -->

<!-- Example:
- `[TechA]` — legacy only, exists in [/path]. Do not use for new code; prefer [TechB].
-->

---

## Project State Context

<!-- PLACEHOLDER: Use this section to intentionally frame the project's current state
     in a way that steers agent behavior. Update as the project matures.
     Examples of useful framings:
     - "This project is early-stage. Schema changes are welcome."
     - "This app has no production users yet. Don't generate data migration scripts."
     - "All new features must be backward-compatible — production data exists."
     -->

- Test structure mirrors `src/` structure: `test_etl_*.py` for ETL modules, `test_pipeline_*.py` for pipeline
- `conftest.py` provides SQLite and DuckDB fixtures with full schema initialized
- Integration tests are suffixed with `_integration.py` and may require external API access

---

## Agent Self-Reporting

If you encounter anything in this codebase that is surprising, ambiguous, or contradicts your expectations,
**do not silently work around it**. Instead:

1. Flag it to the developer in your response.
2. Propose a one-line addition to this file describing the confusion.

The developer will determine whether the fix belongs in the code or here.

---

<!-- MAINTENANCE REMINDER:
     - Review this file when upgrading major dependencies or refactoring architecture.
     - If a section has been empty for a long time, delete it.
     - If the model no longer makes a listed mistake, remove that entry.
     - Outdated entries actively degrade agent performance.
-->
