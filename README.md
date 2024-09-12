[![Actions Status](https://github.com/bytewax/bytewax-duckdb/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax-duckdb/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax-duckdb.svg?style=flat-square)](https://pypi.org/project/bytewax-duckdb/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://docs.bytewax.io/projects/bytewax-duckdb/en/stable/)

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://user-images.githubusercontent.com/6073079/195393689-7334098b-a8cd-4aaa-8791-e4556c25713e.png" width="350">
  <source media="(prefers-color-scheme: light)" srcset="https://user-images.githubusercontent.com/6073079/194626697-425ade3d-3d72-4b4c-928e-47bad174a376.png" width="350">
  <img alt="Bytewax">
</picture>

## bytewax-duckdb

* TODO: Add project documentation

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
