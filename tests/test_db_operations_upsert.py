"""Tests for src.db.operations.upsert — the core DB write path."""

import sqlite3

import pytest

from src.db.operations.upsert import _chunked, _validate_identifier, transaction, upsert_rows

# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #


def _make_con(ddl: str) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute(ddl)
    con.commit()
    return con


# ------------------------------------------------------------------ #
# _validate_identifier                                                #
# ------------------------------------------------------------------ #


def test_validate_identifier_accepts_valid_names():
    for name in ("users", "fact_game", "dim_player2", "A", "_underscored"):
        _validate_identifier(name)  # must not raise


def test_validate_identifier_rejects_spaces():
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("my table")


def test_validate_identifier_rejects_sql_injection():
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("users; DROP TABLE users")


def test_validate_identifier_rejects_hyphen():
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("my-table")


def test_validate_identifier_rejects_empty():
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        _validate_identifier("")


# ------------------------------------------------------------------ #
# _chunked                                                            #
# ------------------------------------------------------------------ #


def test_chunked_exact_multiple():
    result = list(_chunked(range(6), 3))
    assert result == [[0, 1, 2], [3, 4, 5]]


def test_chunked_with_remainder():
    result = list(_chunked(range(7), 3))
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


def test_chunked_empty():
    assert list(_chunked([], 5)) == []


def test_chunked_chunk_larger_than_input():
    result = list(_chunked(range(3), 10))
    assert result == [[0, 1, 2]]


# ------------------------------------------------------------------ #
# upsert_rows — basic behaviour                                       #
# ------------------------------------------------------------------ #


def test_upsert_rows_empty_list_returns_zero():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    assert upsert_rows(con, "t", []) == 0


def test_upsert_rows_inserts_rows_and_returns_count():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    count = upsert_rows(con, "t", rows)
    assert count == 2
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2


def test_upsert_rows_autocommit_persists_data():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    upsert_rows(con, "t", [{"id": 1, "v": "x"}], autocommit=True)
    assert con.execute("SELECT v FROM t WHERE id=1").fetchone()[0] == "x"


def test_upsert_rows_no_autocommit_does_not_commit():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    con.isolation_level = "DEFERRED"
    upsert_rows(con, "t", [{"id": 1, "v": "x"}], autocommit=False)
    con.rollback()
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


# ------------------------------------------------------------------ #
# upsert_rows — conflict clauses                                      #
# ------------------------------------------------------------------ #


def test_upsert_rows_conflict_ignore_skips_duplicates():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    upsert_rows(con, "t", [{"id": 1, "v": "original"}])
    upsert_rows(con, "t", [{"id": 1, "v": "updated"}], conflict="IGNORE")
    assert con.execute("SELECT v FROM t WHERE id=1").fetchone()[0] == "original"


def test_upsert_rows_conflict_replace_overwrites():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    upsert_rows(con, "t", [{"id": 1, "v": "original"}])
    upsert_rows(con, "t", [{"id": 1, "v": "updated"}], conflict="REPLACE")
    assert con.execute("SELECT v FROM t WHERE id=1").fetchone()[0] == "updated"


def test_upsert_rows_invalid_conflict_raises():
    con = _make_con("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    with pytest.raises(ValueError, match="Invalid conflict clause"):
        upsert_rows(con, "t", [{"id": 1}], conflict="DELETE")


def test_upsert_rows_empty_conflict_string_inserts_plain():
    con = _make_con("CREATE TABLE t (id INTEGER, v TEXT)")
    count = upsert_rows(con, "t", [{"id": 1, "v": "a"}], conflict="")
    assert count == 1


# ------------------------------------------------------------------ #
# upsert_rows — SQL identifier validation                             #
# ------------------------------------------------------------------ #


def test_upsert_rows_rejects_bad_table_name():
    con = sqlite3.connect(":memory:")
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        upsert_rows(con, "bad-table", [{"id": 1}])


def test_upsert_rows_rejects_bad_column_name():
    con = _make_con('CREATE TABLE t ("bad col" TEXT)')
    with pytest.raises(ValueError, match="Invalid SQL identifier"):
        upsert_rows(con, "t", [{"bad col": "x"}])


# ------------------------------------------------------------------ #
# upsert_rows — missing table is handled gracefully                   #
# ------------------------------------------------------------------ #


def test_upsert_rows_missing_table_returns_zero(caplog):
    import logging

    con = sqlite3.connect(":memory:")
    with caplog.at_level(logging.WARNING, logger="src.db.operations.upsert"):
        result = upsert_rows(con, "nonexistent_table", [{"id": 1}])
    assert result == 0
    assert "missing table" in caplog.text.lower() or "nonexistent_table" in caplog.text


# ------------------------------------------------------------------ #
# upsert_rows — chunking with many columns                            #
# ------------------------------------------------------------------ #


def test_upsert_rows_large_batch_is_chunked_correctly():
    """Verify that inserting many rows across chunk boundaries all land correctly."""
    cols = ["c" + str(i) for i in range(10)]  # 10 cols → chunk_size = 90
    ddl = "CREATE TABLE wide (" + ", ".join(f"{c} INTEGER" for c in cols) + ")"
    con = _make_con(ddl)
    rows = [{c: idx for c in cols} for idx in range(200)]
    count = upsert_rows(con, "wide", rows)
    assert count == 200
    assert con.execute("SELECT COUNT(*) FROM wide").fetchone()[0] == 200


# ------------------------------------------------------------------ #
# transaction context manager                                         #
# ------------------------------------------------------------------ #


def test_transaction_commits_on_success():
    con = _make_con("CREATE TABLE t (id INTEGER)")
    with transaction(con):
        con.execute("INSERT INTO t VALUES (1)")
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1


def test_transaction_rolls_back_on_exception():
    con = _make_con("CREATE TABLE t (id INTEGER)")
    with pytest.raises(RuntimeError):
        with transaction(con):
            con.execute("INSERT INTO t VALUES (1)")
            raise RuntimeError("boom")
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


def test_transaction_reraises_original_exception():
    con = _make_con("CREATE TABLE t (id INTEGER)")
    with pytest.raises(ValueError, match="test error"):
        with transaction(con):
            raise ValueError("test error")
