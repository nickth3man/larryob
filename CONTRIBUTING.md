# Contributing

## Development Setup

```bash
# Install uv (package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone repository
git clone https://github.com/yourusername/larryob.git
cd larryob

# Install dependencies
uv sync

# Activate virtual environment
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows
```

## Running Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_etl_dimensions.py

# Run with coverage
uv run pytest --cov=src --cov-report=html
```

## Code Quality

```bash
# Linting
uv run ruff check src/

# Auto-fix linting issues
uv run ruff check --fix src/

# Type checking
uv run ty src/
```

## Project Guidelines

### File Size Limit
No Python file should exceed 400 lines (excluding `__init__.py`).

### Naming Conventions
- Files: `snake_case.py`
- Classes: `PascalCase`
- Functions: `snake_case`
- Constants: `UPPER_CASE`

### Private Modules
Use underscore prefix for private modules:
- `_dimensions_helpers.py`
- `_game_logs_transform.py`

## Submitting Changes

1. Create a feature branch
2. Make changes with descriptive commits
3. Run tests and linting
4. Submit a pull request

## Code Review Process

All PRs run through:
1. Automated tests (pytest)
2. Linting (ruff)
3. Type checking (ty)
4. OpenCode AI review

## Questions?

Open an issue or start a discussion.
