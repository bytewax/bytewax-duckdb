# Show this help list
help:
    @echo 'Run `just get-started` to init a development env.'
    @just --list

# Init a development env
get-started:
    @echo 'Checking that you have `uv` installed'
    @echo 'If you need it, I recommend installing `pipx` from https://pipx.pypa.io/stable/ then `pipx install uv`'
    uv --version
    @echo 'Checking that you have Python 3.12 installed'
    @echo 'If you need it, I recommend installing `pyenv` from https://github.com/pyenv/pyenv then `pyenv install 3.12`'
    @echo 'You also might need to activate the global shim with `pyenv global system 3.12`'
    python3.12 --version
    @echo 'Creating the development virtual env in `venvs/dev/`'
    mkdir -p venvs
    test -d venvs/dev/ || uv venv -p 3.12 venvs/dev/
    @echo 'Compiling all dependencies'
    just venv-compile-all
    @echo 'Installing all the tools and dependencies'
    just venv-sync dev
    @echo 'Ensuring Git pre-commit hooks are installed'
    venvs/dev/bin/pre-commit install
    @echo 'All done!'
    @echo 'Each time before you do any work in this repo you should run `. venvs/dev/bin/activate`'
    @echo 'Once the `dev` venv is activated, run:'
    @echo
    @echo '`just develop` to re-build Bytewax and install it in the venv'
    @echo '`just test-py` to run the Python test suite'
    @echo '`just lint` to lint the source code'
    @echo '`just --list` to show more advanced recipes'

# Assert we are in a venv.
_assert-venv:
    #!/usr/bin/env python
    import sys
    p = sys.prefix
    if not (p.endswith("venvs/dev") or p.endswith("venv")):
        print("You must activate the `dev` venv with `. venvs/dev/bin/activate` before running this command", file=sys.stderr)
        sys.exit(1)

# Install the library locally in an editable state
develop: _assert-venv
    @# You never need to run with `-E` / `--extras`; the `dev` and test
    @# virtualenvs already have the optional dependencies pinned.
    uv pip install -e .

# Format a Python file; automatically run via pre-commit
fmt-py *files: _assert-venv
    ruff format {{files}}

# Format code blocks in a Markdown file; automatically run via pre-commit
fmt-md *files: _assert-venv
    cbfmt -w {{files}}

# Lint the code in the repo; runs in CI
lint: _assert-venv
    vermin --config-file vermin-lib.ini src/ pytests/
    vermin --config-file vermin-dev.ini docs/ *.py
    ruff check src/ pytests/ docs/
    mypy -p bytewax.bytewax-duckdb
    mypy pytests/ docs/

# Manually check that all pre-commit hooks pass; runs in CI
lint-pc: _assert-venv
    pre-commit run --all-files --show-diff-on-failure

pytests := 'pytests/'

# Run the Python tests; runs in CI
test-py tests=pytests: _assert-venv
    pytest {{tests}}

# Test all code in the documentation; runs in CI
test-doc: _assert-venv
    cd docs/fixtures/ && sphinx-build -b doctest -E .. ../_build/

# Run all the checks that will be run in CI locally
ci-pre: lint test-py test-doc

# Start an auto-refreshing doc development server
doc-autobuild: _assert-venv
    sphinx-autobuild --ignore '**/.*' -E docs/ docs/_build/

# Build the docs into static HTML files in `docs/_build/`
doc-build: _assert-venv
    sphinx-build -b html -E docs/ docs/_build/

# Init the Read the Docs venv; only use if you are debugging RtD; use `just doc-autobuild` to write docs locally
venv-init-doc:
    mkdir -p venvs
    test -d venvs/doc/ || uv venv -p 3.12 venvs/doc/

# Sync the given venv; e.g. `dev` or `build-py3.10`
venv-sync venv:
    VIRTUAL_ENV={{justfile_directory()}}/venvs/{{venv}} uv pip sync --strict requirements/{{venv}}.txt

# Sync all venvs
venv-sync-all: (venv-sync "doc") (venv-sync "dev")

# Pin / compile all dependences for reproducible venvs; re-run this if you update any library deps or `.in` files
venv-compile-all:
    uv pip compile --generate-hashes -p 3.12 requirements/doc.in -o requirements/doc.txt

    uv pip compile --generate-hashes -p 3.8 --all-extras pyproject.toml -o requirements/lib-py3.8.txt
    uv pip compile --generate-hashes -p 3.9 --all-extras pyproject.toml -o requirements/lib-py3.9.txt
    uv pip compile --generate-hashes -p 3.10 --all-extras pyproject.toml -o requirements/lib-py3.10.txt
    uv pip compile --generate-hashes -p 3.11 --all-extras pyproject.toml -o requirements/lib-py3.11.txt
    uv pip compile --generate-hashes -p 3.12 --all-extras pyproject.toml -o requirements/lib-py3.12.txt

    uv pip compile --generate-hashes -p 3.8 requirements/build.in requirements/lib-py3.8.txt -o requirements/build-py3.8.txt
    uv pip compile --generate-hashes -p 3.9 requirements/build.in requirements/lib-py3.9.txt -o requirements/build-py3.9.txt
    uv pip compile --generate-hashes -p 3.10 requirements/build.in requirements/lib-py3.10.txt -o requirements/build-py3.10.txt
    uv pip compile --generate-hashes -p 3.11 requirements/build.in requirements/lib-py3.11.txt -o requirements/build-py3.11.txt
    uv pip compile --generate-hashes -p 3.12 requirements/build.in requirements/lib-py3.12.txt -o requirements/build-py3.12.txt

    uv pip compile --generate-hashes -p 3.12 --all-extras pyproject.toml requirements/dev.in requirements/lib-py3.12.txt -o requirements/dev.txt
