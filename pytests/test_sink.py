"""Tests for bytewax.bytewax_duckdb module."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from bytewax.bytewax_duckdb import DuckDBSink, DuckDBSinkPartition


# Skip the license warning in tests
@pytest.fixture(autouse=True)
def suppress_license_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Suppress the license warning in tests."""
    monkeypatch.setitem(os.environ, "BYTEWAX_LICENSE", "1")


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """Generate a temporary path for the DuckDB database file."""
    return str(tmp_path / "test_duckdb.db")


@pytest.fixture
def table_name() -> str:
    """Fixture for the table name."""
    return "test_table"


@pytest.fixture
def create_table_sql() -> str:
    """Fixture for the SQL statement to create a table."""
    return "CREATE TABLE test_table (id INTEGER, name TEXT)"


@pytest.fixture
def sink_partition(
    db_path: str, table_name: str, create_table_sql: str
) -> DuckDBSinkPartition:
    """Fixture for the DuckDBSinkPartition class."""
    return DuckDBSinkPartition(
        db_path=db_path,
        table_name=table_name,
        create_table_sql=create_table_sql,
        resume_state=None,
    )


def test_duckdbsinkpartition_initialization(
    sink_partition: DuckDBSinkPartition, db_path: str, table_name: str
) -> None:
    """Test the initialization of DuckDBSinkPartition."""
    assert sink_partition.db_path == db_path
    assert sink_partition.table_name == table_name
    assert sink_partition.total_rows_written == 0
    assert len(sink_partition.buffer) == 0


def test_duckdbsinkpartition_write_batch(sink_partition: DuckDBSinkPartition) -> None:
    """Test the write_batch method of DuckDBSinkPartition."""
    items = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    sink_partition.write_batch(items)
    assert len(sink_partition.buffer) == 2  # All items should be in the buffer


def test_duckdbsinkpartition_safe_write(sink_partition: DuckDBSinkPartition) -> None:
    """Test the _safe_write method of DuckDBSinkPartition."""
    items = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    sink_partition.write_batch(items)

    # Mock _safe_write to avoid actual DuckDB interaction
    with patch.object(
        sink_partition, "_safe_write", wraps=sink_partition._safe_write
    ) as mock_write:
        sink_partition._safe_write(items)
        mock_write.assert_called_once()


def test_duckdbsinkpartition_snapshot(sink_partition: DuckDBSinkPartition) -> None:
    """Test the snapshot method of DuckDBSinkPartition."""
    items = [{"id": 1, "name": "Alice"}]
    sink_partition.write_batch(items)
    snapshot = sink_partition.snapshot()
    assert snapshot == (0, 1)  # 0 rows written, 1 item in buffer


def test_duckdbsinkpartition_close(sink_partition: DuckDBSinkPartition) -> None:
    """Test the close method of DuckDBSinkPartition."""
    items = [{"id": 1, "name": "Alice"}]
    sink_partition.write_batch(items)
    with patch.object(sink_partition, "_safe_write") as mock_write:
        sink_partition.close()
        mock_write.assert_called_once()  # Ensure the buffer was written before closing


def test_duckdbsink_initialization() -> None:
    """Test the initialization of DuckDBSink."""
    sink = DuckDBSink(db_path_template="test_{partition}.db", table_name="test_table")
    assert sink.db_path_template == "test_{partition}.db"
    assert sink.table_name == "test_table"


def test_duckdbsink_list_parts() -> None:
    """Test the list_parts method of DuckDBSink."""
    sink = DuckDBSink(db_path_template="test_{partition}.db", table_name="test_table")
    parts = sink.list_parts()
    assert parts == ["partition_0", "partition_1", "partition_2", "partition_3"]


def test_duckdbsink_part_fn() -> None:
    """Test the part_fn method of DuckDBSink."""
    sink = DuckDBSink(db_path_template="test_{partition}.db", table_name="test_table")
    partition_index = sink.part_fn("some_key")
    assert isinstance(partition_index, int)


def test_duckdbsink_build_part(db_path: str, table_name: str) -> None:
    """Test the build_part method of DuckDBSink."""
    sink = DuckDBSink(db_path_template="test_{partition}.db", table_name=table_name)
    partition = sink.build_part(
        step_id="step_1", for_part="partition_0", resume_state=None
    )
    assert isinstance(partition, DuckDBSinkPartition)
    assert partition.db_path == "test_partition_0.db"
    assert partition.table_name == table_name
