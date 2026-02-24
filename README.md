# LarryOB - NBA Analytics Database

## Overview
SQLite OLTP database + DuckDB OLAP analytics for NBA data.

## Quick Start

```bash
# Install dependencies
uv sync

# Load data for specific seasons
uv run ingest --seasons 2023-24 2024-25

# Load dimension tables only
uv run ingest --dims-only

# Run analytics query and export
uv run ingest --analytics player_shooting --limit 100 --output results.csv
```

## Project Structure

- `src/db/` - SQLite schema and DuckDB analytics views
- `src/etl/` - Data extraction, transformation, and loading
- `src/pipeline/` - CLI interface and pipeline orchestration
- `tests/` - Comprehensive test suite (93% coverage)

## Documentation

- `ARCHITECTURE.md` - System design and architecture
- `CONTRIBUTING.md` - Development setup and guidelines
- `PLAN.md` - Implementation roadmap

## Development

```bash
# Run tests
uv run pytest

# Run linting
uv run ruff check src/

# Type checking
uv run ty src/
```

## License

[Add your license here]
