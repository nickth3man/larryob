# CLAUDE.md

> **Purpose:** This file exists to correct consistent agent mistakes and specify required tooling — nothing more.
> Do NOT auto-generate or expand this file. If you encounter something surprising or confusing in this codebase,
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

- Schema DDL lives in `.sql` files, not Python strings. Edit `src/db/schema/*.sql` for table/index definitions.
- All fact tables use NULL (not 0) for stats not tracked in early NBA eras (e.g., blocks/steals pre-1973-74).

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

- This module manages SQLite schema definitions and analytics views
- DDL statements are loaded from `schema/*.sql` files (tables.sql, indexes.sql, migrations.sql, rollback.sql)
- Analytics views are defined in `views/*.sql` files
- `schema.py` loads and executes SQL from these files — do not inline DDL in Python

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
