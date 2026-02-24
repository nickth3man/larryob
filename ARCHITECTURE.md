# Architecture

## System Overview

LarryOB uses a **layered architecture** with a hybrid database approach:

- **SQLite (OLTP)**: Transactional data ingestion
- **DuckDB (OLAP)**: Analytical query processing

## Architecture Layers

```
┌─────────────────────────────────┐
│   Application Layer (CLI)      │  src/pipeline/
│   Orchestration, workflows      │
└────────────┬────────────────────┘
             │ depends on
┌────────────▼────────────────────┐
│   Domain Layer (ETL)           │  src/etl/
│   Data transformation, API     │
└────────────┬────────────────────┘
             │ depends on
┌────────────▼────────────────────┐
│   Infrastructure Layer (DB)    │  src/db/
│   SQLite OLTP + DuckDB OLAP    │
└─────────────────────────────────┘
```

## Data Flow

1. **Ingestion**: NBA API and Basketball-Reference → ETL loaders → SQLite
2. **Analytics**: SQLite tables → DuckDB views → Analytical queries
3. **Export**: DuckDB queries → CSV/Parquet/JSON

## Design Decisions

### Why SQLite + DuckDB?

- **SQLite**: Fast, embedded, perfect for transactional writes
- **DuckDB**: Columnar, OLAP-optimized, in-process analytics
- **Zero-copy**: DuckDB attaches SQLite directly (no ETL between them)

### Why SQL-First Schema?

- DDL statements in `src/db/schema/*.sql` files
- Version-controlled schema evolution
- Database-agnostic (easy to migrate to PostgreSQL later)

### Why src/ Layout?

- Prevents import ambiguity
- Standard for Python packages
- Better test hygiene

## Module Responsibilities

| Module | Responsibility |
|--------|---------------|
| `db/` | Schema, connections, repositories, analytics |
| `etl/` | Data extraction, transformation, loading |
| `pipeline/` | CLI, orchestration, stage execution |

## Dependency Rules

- **db**: No internal dependencies (infrastructure layer)
- **etl**: May import from db
- **pipeline**: May import from etl and db
- **tests**: May import from any module

## Database Schema

- **Dimension tables**: `dim_season`, `dim_team`, `dim_player`
- **Fact tables**: `fact_game`, `fact_player_game_log`, `fact_team_game_log`
- **Analytics views**: Player shooting, team ratings, advanced metrics

See `src/db/schema/` for complete DDL.
