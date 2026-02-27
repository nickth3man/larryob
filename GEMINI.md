# CLAUDE.md

> **Purpose:** This file exists to correct consistent agent mistakes and specify required tooling — nothing more.
> Do NOT auto-generate or expand this file. If you encounter something surprising or confusing in this codebase,
> flag it to the developer and suggest an edit here. The developer will decide whether to fix the code or update this file.

---

## Required Tooling

<!-- PLACEHOLDER: List only the non-obvious tools the agent must use.
     Example: "Always use pnpm (not npm or yarn) to run scripts."
     If the tool is detectable from package.json or config files, omit it. -->

- [x] `uv` — always use `uv run` to run scripts
- [x] `ruff` — run after every change: `ruff check . && ruff format .`
- [x] `pytest` — run affected tests before marking a task complete: `uv run pytest tests/`

---

## Consistent Mistakes to Avoid

<!-- PLACEHOLDER: Only add entries here when the agent repeatedly makes the same error
     despite the codebase structure making the correct path clear.
     Each entry should be a single, specific correction. -->

<!-- Example format:
- DO NOT use [X pattern/library] — use [Y] instead. Reason: [one sentence].
- Always run `[command]` after modifying [area of codebase].
-->

- No Python file should exceed 400 lines. If a file approaches or exceeds this limit, refactor by splitting it into smaller, logically grouped modules. Note: `__init__.py` files are exempt from this rule.

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

- This project uses a modular architecture with SQL/JSON data extraction
- Schema DDL is in `src/db/schema/*.sql` files
- Analytics views are in `src/db/views/*.sql` files
- Static data is in `src/etl/data/*.json` files
- Helper modules use underscore prefix (e.g., `_helpers.py`)
- Database operations are in `src/db/operations/`, caching in `src/db/cache/`, tracking in `src/db/tracking/`
- ETL logging setup is in `src/etl/logging.py`

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