# Python Virtual Environment Setup

## 1. Purpose

The Python samples (`samples/sample_app.py` and `samples/stream_consumer.py`) need Python 3.10 or newer, a virtual environment, and `boto3`. This file walks through detecting an existing venv and, when absent, creating one and installing the project's Python dependencies.

## 2. Venv detection

```bash
test -d .venv && echo "found .venv at repo root" || true
test -d ~/venvs/extenddb-venv && echo "found extenddb-venv in ~/venvs/" || true
```

Two standard locations:

- `.venv/` at the repo root, created by `scripts/install-linux.sh` and `scripts/install-macos.sh` (recommended).
- `~/venvs/extenddb-venv/`, created by the README instructions.

Either works. If both exist, prefer `.venv/` since the install scripts maintain it.

## 3. Create venv (repo root, recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Create venv (user home, alternative)

```bash
python3 -m venv ~/venvs/extenddb-venv
source ~/venvs/extenddb-venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 5. boto3 presence check

After the venv is activated:

```bash
python3 -c "import boto3; print('boto3', boto3.__version__)"
```

Expected output: `boto3 1.XX.YY` (any 1.x version works).

If the output is an `ImportError`, `pip install -r requirements.txt` is the fix.

## 6. Deactivate

```bash
deactivate
```

Not required between sample runs, but useful to know.

## 7. Common failures

- `python3: command not found`. Install Python 3 (see ``references/setup/02-dependency-checks.md``).
- `ensurepip is not available`. On Debian or Ubuntu, run `sudo apt install python3-venv`.
- `pip install` times out. Check internet connectivity or configure a proxy.
