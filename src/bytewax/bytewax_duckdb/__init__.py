"""Bytewax custom sink for DuckDB."""

import os
import sys

if "BYTEWAX_LICENSE" not in os.environ:
    msg = (
        "`bytewax-duckdb` is commercially licensed "
        "with publicly available source code.\n"
        "You are welcome to prototype using this module for free, "
        "but any use on business data requires a paid license.\n"
        "See https://modules.bytewax.io/ for a license. "
        "Set the env var `BYTEWAX_LICENSE=1` to suppress this message."
    )
    print(msg, file=sys.stderr)

import zlib
from typing import Any, Dict, List, Optional, Tuple

import duckdb  # type: ignore
import pyarrow as pa  # type: ignore

from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition


class DuckDBSinkPartition(StatefulSinkPartition[Any, Tuple[int, int]]):
    """Stateful sink partition for writing data to DuckDB in batches of optimal size."""

    def __init__(
        self,
        db_path: str,
        table_name: str,
        create_table_sql: Optional[str],
        resume_state: Optional[Tuple[int, int]],
    ) -> None:
        """Initialize the DuckDB connection and set up batching.

        Args:
            db_path (str): Path to the DuckDB database file.
            table_name (str): Name of the table to write data into.
            create_table_sql (Optional[str]): SQL statement to create the table.
            resume_state (Optional[Tuple[int, int]]):
                Tuple of (total_rows_written, buffer_size).
        """
        self.db_path = db_path
        self.table_name = table_name
        self.conn = duckdb.connect(database=db_path)
        if create_table_sql:
            self.conn.execute(create_table_sql)
        self.buffer: List[Any] = []
        self.batch_size = 122_880  # Optimal batch size for DuckDB
        self.total_rows_written = 0
        if resume_state:
            self.total_rows_written, buffer_size = resume_state
            # If there's a buffer to restore, we might need to handle that
            # For simplicity, we'll assume buffer is empty on resume
        else:
            self.total_rows_written = 0

    def write_batch(self, items: List[Any]) -> None:
        """Write a batch of items to the DuckDB table.

        Args:
            items (List[Any]): List of items to write.
        """
        self.buffer.extend(items)

        while len(self.buffer) >= self.batch_size:
            batch = self.buffer[: self.batch_size]
            self._safe_write(batch)
            self.total_rows_written += len(batch)
            self.buffer = self.buffer[self.batch_size :]

    def _safe_write(self, batch: List[Any]) -> None:
        """Safely write a batch of data to DuckDB using PyArrow Tables.

        Falls back to executemany if necessary.

        Args:
            batch (List[Any]): Batch of data to write.
        """
        try:
            # Assume batch is a list of dictionaries
            pa_table = pa.Table.from_pylist(batch)
            # Insert data into the target table
            self.conn.register("temp_table", pa_table)
            self.conn.execute(f"INSERT INTO {self.table_name} SELECT * FROM temp_table")
            self.conn.unregister("temp_table")
        except Exception as e:
            # Fallback to executemany if PyArrow write fails
            print(f"PyArrow write failed: {e}. Falling back to executemany.")
            placeholders = ", ".join(["?"] * len(batch[0]))
            query = f"INSERT INTO {self.table_name} VALUES ({placeholders})"
            values = [tuple(item.values()) for item in batch]
            self.conn.executemany(query, values)

    def snapshot(self) -> Tuple[int, int]:
        """Snapshot the current state for resuming.

        Returns:
            Tuple[int, int]: (total_rows_written, buffer_size)
        """
        return (self.total_rows_written, len(self.buffer))

    def close(self) -> None:
        """Close the DuckDB connection and write any remaining data."""
        if self.buffer:
            self._safe_write(self.buffer)
            self.total_rows_written += len(self.buffer)
            self.buffer = []
        self.conn.close()


class DuckDBSink(FixedPartitionedSink):
    """Fixed partitioned sink for writing data to DuckDB efficiently."""

    def __init__(
        self,
        db_path_template: str,
        table_name: str,
        create_table_sql: Optional[str] = None,
    ) -> None:
        """Initialize the DuckDBSink.

        Args:
            db_path_template (str): Template for the DuckDB database file path.
                Use '{partition}' in the template to create unique files per partition.
            table_name (str): Name of the table to write data into.
            create_table_sql (Optional[str]): SQL statement to create the table.
        """
        self.db_path_template = db_path_template
        self.table_name = table_name
        self.create_table_sql = create_table_sql
        # Mapping of partition keys to database paths
        self.partitions: Dict[str, str] = {}

    def list_parts(self) -> List[str]:
        """List all partitions (keys). For simplicity, we'll define a fixed set.

        Returns:
            List[str]: List of partition keys.
        """
        # For this example, we'll assume partitions are predefined
        return ["partition_0", "partition_1", "partition_2", "partition_3"]

    def part_fn(self, item_key: str) -> int:
        """Map item keys to partition indices.

        Args:
            item_key (str): The key of the item.

        Returns:
            int: Partition index.
        """
        # Use a consistent hash function
        hash_value = zlib.adler32(item_key.encode("utf-8"))
        return hash_value

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[Tuple[int, int]],
    ) -> DuckDBSinkPartition:
        """Build or resume a partition.

        Args:
            step_id (str): The step ID.
            for_part (str): Partition key.
            resume_state (Optional[Tuple[int, int]]): Resume state.

        Returns:
            DuckDBSinkPartition: The partition instance.
        """
        db_path = self.db_path_template.format(partition=for_part)
        return DuckDBSinkPartition(
            db_path=db_path,
            table_name=self.table_name,
            create_table_sql=self.create_table_sql,
            resume_state=resume_state,
        )
