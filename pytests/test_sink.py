"""Tests for the bytewax.duckdb module."""

import os
from pathlib import Path
from typing import Dict, List, Tuple, Union

import duckdb
import pytest

import bytewax.duckdb.operators as duck_op
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.duckdb import DuckDBSink
from bytewax.testing import TestingSource, run_main


# Skip the license warning in tests
@pytest.fixture(autouse=True)
def suppress_license_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress the license warning in tests."""
    monkeypatch.setitem(os.environ, "BYTEWAX_LICENSE", "1")


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Generate a temporary path for the DuckDB database file."""
    return tmp_path / "test_duckdb.db"


@pytest.fixture
def table_name() -> str:
    """Fixture for the table name."""
    return "test_table"


@pytest.fixture
def create_table_sql() -> str:
    """Fixture for the SQL statement to create a table."""
    return "CREATE TABLE test_table (id INTEGER, name TEXT)"


def test_duckdb_sink(db_path: str, table_name: str) -> None:
    flow = Dataflow("duckdb")

    def create_dict(value: int) -> Tuple[str, List[Dict[str, Union[int, str]]]]:
        return (str(value), [{"id": value, "name": "Alice"}])

    inp = op.input("inp", flow, TestingSource(range(100)))
    dict_stream = op.map("dict", inp, create_dict)
    op.output(
        "out",
        dict_stream,
        DuckDBSink(
            db_path,
            table_name,
            f"CREATE TABLE {table_name} (id INTEGER, name TEXT)",
        ),
    )
    run_main(flow)
    conn = duckdb.connect(db_path)
    assert conn.sql(f"SELECT COUNT(*) from {table_name}").fetchall() == [(100,)]


def test_duckdb_operator(db_path: str, table_name: str) -> None:
    flow = Dataflow("duckdb")

    def create_dict(value: int) -> Tuple[str, Dict[str, Union[int, str]]]:
        return (str(value), {"id": value, "name": "Alice"})

    inp = op.input("inp", flow, TestingSource(range(100)))
    dict_stream = op.map("dict", inp, create_dict)

    # Use IF NOT EXISTS to avoid duplicate table creation errors
    duck_op.output(
        "out",
        dict_stream,
        db_path,
        table_name,
        f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER, name TEXT)",
    )

    run_main(flow)

    # Connect to the database and verify the results
    conn = duckdb.connect(db_path)
    assert conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall() == [(100,)]

    # Run the flow a second time and check if data is appended correctly
    run_main(flow)
    assert conn.sql(f"SELECT COUNT(*) FROM {table_name}").fetchall() == [(200,)]
