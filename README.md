[![Actions Status](https://github.com/bytewax/bytewax-duckdb/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax-duckdb/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax-duckdb.svg?style=flat-square)](https://pypi.org/project/bytewax-duckdb/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://bytewax-duckdb.readthedocs.io/en/latest/?badge=latest)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/6073079/195393689-7334098b-a8cd-4aaa-8791-e4556c25713e.png" width="350">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/6073079/194626697-425ade3d-3d72-4b4c-928e-47bad174a376.png" width="350">
  <img alt="Bytewax">
</picture>

## bytewax-duckdb

Bytewax sink and operator for DuckDB and MotherDuck

## Installation

We've made installation as easy as Py by making it pip-installable:

```bash
pip install bytewax-duckdb
```

This will also install the latest DuckDB and Bytewax modules.

## Storing data to DuckDB in batches through a Bytewax dataflow

When working with this integration in Bytewax, you can use it to process data in batch and write data to a target database or file in a structured way. However, there’s one essential assumption you need to know, the sink expects data in a specific tuple format, structured as:

```python
("key", List[Dict])
```
Where

`"key"`: The first element is a string identifier for the batch. Think of this as a “batch ID” that helps to organize and keep track of which group of entries belong together. Every batch you send to the sink has a unique key or identifier.

`List[Dict]`: The second element is a list of dictionaries. Each dictionary represents an individual data record, with each key-value pair in the dictionary representing fields and their corresponding values.

Together, the tuple tells the sink: “Here is a batch of data, labeled with a specific key, and this batch contains multiple data entries.”

This format is designed to let the sink write data efficiently in batches, rather than handling each entry one-by-one. By grouping data entries together with an identifier, the sink can:

* Optimize Writing: Batching data reduces the frequency of writes to the database or file, which can dramatically improve performance, especially when processing high volumes of data.

* Ensure Atomicity: By writing a batch as a single unit, we minimize the risk of partial writes, ensuring either the whole batch is written or none at all. This is especially important for maintaining data integrity.

Here is an example for a local DuckDB file.

```python
import bytewax.duckdb.operators as duck_op
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource, run_main

flow = Dataflow("duckdb")


def create_dict(value: int) -> Tuple[str, Dict[str, Union[int, str]]]:
    return ("1", {"id": value, "name": "Alice"})


inp = op.input("inp", flow, TestingSource(range(50)))
dict_stream = op.map("dict", inp, create_dict)

duck_op.output(
    "out",
    dict_stream,
    "sample.duckdb",
    "example_table",
    "CREATE TABLE IF NOT EXISTS example_table (id INTEGER, name TEXT)",
)

run_main(flow)
```

**Important**
To connect to a MotherDuck instance, ensure to [create an account](https://app.motherduck.com/?auth_flow) and [generate a token](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#creating-an-access-token). You can store this token into your environment variables.

```python
import os
import random
from typing import Dict, Tuple, Union

# Save the token in an environment variable
md_token = os.getenv("MOTHERDUCK_TOKEN")

# Initialize the dataflow
flow = Dataflow("duckdb-names-cities")

# Define sample data for names and locations
names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]


# Function to create a dictionary with more varied data
def create_dict(value: int) -> Tuple[str, Dict[str, Union[int, str]]]:
    name = random.choice(names)
    age = random.randint(20, 60)  # Random age between 20 and 60
    location = random.choice(locations)
    return ("batch_1", {"id": value, "name": name, "age": age, "location": location})


# Generate input data
inp = op.input("inp", flow, TestingSource(range(50)))
dict_stream = op.map("dict", inp, create_dict)
db_path = f"md:my_db?motherduck_token={md_token}"
# Output the data to DuckDB, creating a table with multiple columns
duck_op.output(
    "out",
    dict_stream,
    db_path,
    "names_cities",
    "CREATE TABLE IF NOT EXISTS names_cities (id INTEGER, name TEXT, age INTEGER, location TEXT)",
)

# Run the dataflow
run_main(flow)
```

## Setting up the project for development

### Install `just`

We use [`just`](https://just.systems/man/en/) as a command runner for
actions / recipes related to developing Bytewax. Please follow [the
installation
instructions](https://github.com/casey/just?tab=readme-ov-file#installation).
There's probably a package for your OS already.

### Install `pyenv` and Python 3.12

I suggest using [`pyenv`](https://github.com/pyenv/pyenv)
to manage python versions.
[the installation instructions](https://github.com/pyenv/pyenv?tab=readme-ov-file#installation).

You can also use your OS's package manager to get access to different
Python versions.

Ensure that you have Python 3.12 installed and available as a "global
shim" so that it can be run anywhere. The following will make plain
`python` run your OS-wide interpreter, but will make 3.12 available
via `python3.12`.

```console
$ pyenv global system 3.12
```

## Install `uv`

We use [`uv`](https://github.com/astral-sh/uv) as a virtual
environment creator, package installer, and dependency pin-er. There
are [a few different ways to install
it](https://github.com/astral-sh/uv?tab=readme-ov-file#getting-started),
but I recommend installing it through either
[`brew`](https://brew.sh/) on macOS or
[`pipx`](https://pipx.pypa.io/stable/).

## Install `just`

We use [`just`](https://just.systems/man/en/) as a command runner for
actions / recipes related to developing Bytewax. Please follow [the
installation
instructions](https://github.com/casey/just?tab=readme-ov-file#installation).
There's probably a package for your OS already.

## Development

We have a `just` recipe that will:

1. Set up a venv in `venvs/dev/`.

2. Install all dependencies into it in a reproducible way.

Start by adding any dependencies that are needed into [pyproject.toml](pyproject.toml) or into
[requirements/dev.in](requirements/dev.in) if they are needed for development.

Next, generate the pinned set of dependencies with

```console
> just venv-compile-all
```

## Create and activate a virtual environment

Once you have compiled your dependencies, run the following:

```console
> just get-started
```

Activate your development environment and run the development task:

```console
> . venvs/dev/bin/activate
> just develop
```

## License

`bytewax-duckdb` is commercially licensed with
publicly available source code. You are welcome to prototype using
this module for free, but any use on business data requires a paid
license. See https://modules.bytewax.io/ for a license. Please see the
full details in [LICENSE](./LICENSE.md).
