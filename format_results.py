import json
import os

with open(r"C:\Users\nicolas\.local\share\opencode\tool-output\tool_c839d6620001ABs6fJuXa5D38J", "r", encoding="utf-8") as f:
    data = json.load(f)

print("<analysis>")
print("**Literal Request**: Find all meaningful code patterns, file organization, and module structure in every subdirectory to score each dir for AGENTS.md placement.")
print("**Actual Need**: A comprehensive breakdown of the Python codebase (files, lines, classes, functions, exports, unusual patterns) grouped by directory, to determine the complexity and documentation needs of each module.")
print("**Success Looks Like**: A structured report detailing file metrics, directory purposes, exports, classes, functions, and anomalies, formatted exactly as requested with absolute paths.")
print("</analysis>\n")

print("<results>")
print("<files>")
for f in data['files']:
    print(f"- {f['path']} — Python source file ({f['lines']} lines)")
print("</files>\n")

print("<answer>")
print("### 1. Python Files in src/ Tree with Line Counts")
for f in data['files']:
    if 'src' in f['path']:
        print(f"- {f['path']}: {f['lines']} lines")

print("\n### 2. Subdirectories (src/ and tests/), File Counts, and Purpose")
for d, info in data['dirs'].items():
    purpose = "Unknown"
    if d.endswith("src"): purpose = "Root source directory"
    elif d.endswith("db"): purpose = "Database schema and analytics views (SQLite/DuckDB)"
    elif d.endswith("etl"): purpose = "API clients, loaders, and data transformations"
    elif d.endswith("backfill"): purpose = "Raw CSV historical data backfill pipeline"
    elif d.endswith("pipeline"): purpose = "CLI orchestration, stages, and execution"
    elif d.endswith("tests"): purpose = "Pytest suite and fixtures"
    print(f"- {d}: {info['count']} files. Purpose: {purpose}")

print("\n### 3. __init__.py Files and Exports")
for init in data['inits']:
    exports = ", ".join(init['exports']) if init['exports'] else "None"
    print(f"- {init['path']} exports: {exports}")

print("\n### 4. Class Definitions")
classes_by_file = {}
for c in data['classes']:
    classes_by_file.setdefault(c['file'], []).append(c['name'])
for f, classes in classes_by_file.items():
    print(f"- {f}: {', '.join(classes)}")

print("\n### 5. Top-Level Function Definitions")
funcs_by_file = {}
for func in data['functions']:
    funcs_by_file.setdefault(func['file'], []).append(func['name'])
for f, funcs in funcs_by_file.items():
    print(f"- {f}: {', '.join(funcs)}")

print("\n### 6. Unusual Patterns")
if data['unusual']:
    for u in data['unusual']:
        print(f"- {u['file']}:{u['line']} — {u['type']}")
else:
    print("- No unusual patterns (dynamic imports, eval, exec) detected.")

print("</answer>\n")

print("<next_steps>")
print("Use these metrics to score directory complexity. src/etl and src/etl/backfill have the highest file counts and complexity, warranting detailed AGENTS.md files. src/pipeline and src/db also have significant logic. Ready to proceed - no follow-up needed.")
print("</next_steps>")
print("</results>")
