"""Operators for the DuckDB sink.

TODO: usage

Usage:
```
```
"""

from datetime import timedelta
from typing import List, Optional

import bytewax.operators as op
from bytewax.dataflow import operator
from bytewax.duckdb import DuckDBSink
from bytewax.operators import KeyedStream, V


@operator
def _to_sink(
    step_id: str,
    up: KeyedStream[V],
    timeout: timedelta,
    batch_size: int,
) -> KeyedStream[List[V]]:
    """Collect batches of items to be inserted into DuckDB."""
    return op.collect("batch", up, timeout=timeout, max_size=batch_size)


@operator
def output(
    step_id: str,
    up: KeyedStream[V],
    db_path: str,
    table_name: str,
    create_table_sql: Optional[str],
    timeout: timedelta = timedelta(seconds=1),
    batch_size: int = 122_880,
) -> None:
    r"""Produce to DuckDB as an output sink.

    :arg step_id: Unique ID.

    :arg up: Stream of records. Key must be a `String`
        and value must be a Python Dictionary that is
        serializable into a PyArrow table.

    :arg create_table_sql: Optional SQL statement to create DuckDB table
        if it does not already exist.

    :arg timeout: a timedelta of the amount of time to wait for
        new data before writing. Defaults to 1 second.

    :arg batch_size: the number of items to wait for before writing.
        Defaults to 122_880, an optimal size for DuckDB.

    """
    return _to_sink(
        "to_sink",
        up,
        timeout=timeout,
        batch_size=batch_size,
    ).then(
        op.output,
        "duckdb_output",
        DuckDBSink(db_path, table_name, create_table_sql),
    )
