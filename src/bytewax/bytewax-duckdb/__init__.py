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
