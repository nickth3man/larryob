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
uv run ingest --analytics-view player_shooting --analytics-limit 100 --analytics-output results.csv
```

## Project Structure

- `src/db/` - SQLite schema and DuckDB analytics views
  - `schema/` - DDL files for table creation
  - `views/` - DuckDB view definitions
  - `olap.py` - DuckDB connection factory
- `src/etl/` - Data extraction, transformation, and loading
  - `schemas.py` - Pydantic validation models
  - `validation.py` - Business rule validation
- `src/pipeline/` - CLI interface and pipeline orchestration
  - `models.py` - Pipeline configuration models
  - `analytics.py` - View execution and export
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
uv run ruff check .

# Format code
uv run ruff format .

# Type checking
uv run ty src/
```

## Running Full History Ingestion

To ingest the complete NBA history (1946-47 to present), including awards, salaries,
rosters, and playoffs — all enabled by default:

```bash
uv run ingest --full-history
```

To run without optional domain data (e.g. skip salaries and playoffs):

```bash
uv run ingest --no-salaries --no-playoffs
```

## Troubleshooting

### API Rate Limiting
NBA stats.nba.com has rate limits. The built-in rate limiter will automatically retry with exponential backoff.

### Cache Issues
Clear the cache to force fresh API calls:
```bash
rm -rf .cache/
```

### Database Locked
Ensure only one process is writing to the database at a time.

## License

MIT License - see LICENSE file for details.
