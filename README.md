# ZipNavigator

## Overview

ZipNavigator is a Python utility to safely navigate and extract content from `.zip` archives. It provides filesystem-like operations (`ls`, `cd`, `pwd`, `cat`, `exists`, `is_dir`, `is_file`, `info`) and a resumable, batched extraction iterator with optional CRC validation, retry policy, disk-space preflight, and persistent state on disk (`.zip_iter_state.json`).

Typical use: processing large ZIP datasets (e.g., images/videos/data dumps) where you extract only certain types, survive interruptions, log failures, and resume exactly where you left off.

## Features

* Filesystem-style navigation inside archives: `ls()`, `cd()`, `pwd()`, `cat()`, `exists()`, `is_dir()`, `is_file()`, `info()`.
* Resumable batch extraction via `initialize_iterator()`, Python iterator protocol, `iterator_status()`, `reset_iterator()`, `resume_iterator()`.
* Error policy: `on_error="skip" | "abort"`, with per-file `max_retries`.
* Optional integrity checks with `validate_crc=True`.
* Disk-space preflight for each batch.
* Safe path handling to prevent Zip Slip (rejects absolute paths, drive letters, and `..` traversal).
* Python 3.10+, standard library only.

## Installation

From source (recommended during development):

```bash
git clone https://github.com/antoniobrau/zipnavigator.git
cd zipnavigator

python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -e .
```

> If you publish to PyPI, users can install with: `pip install zipnavigator`.

## Quick Start

```python
from zipnavigator import ZipNavigator

# Always use context managers to avoid open handles (especially on Windows)
with ZipNavigator("bundle.zip") as nav:
    print("Root:", nav.ls())       # ['docs/', 'payload/', 'top.txt']

    nav.cd("payload/")             # directories use a trailing '/'
    print("Here:", nav.ls())       # e.g. ['payload/data1.csv', 'payload/data2.csv']

    # Read a text file (UTF-8 by default)
    print(nav.cat("data1.csv"))

    # Batched extraction of only CSV files
    nav.initialize_iterator(output_dir="out", batch_size=5, extensions=[".csv"])
    for batch in nav:
        print("Extracted batch:", batch)
```

## Path Semantics (important)

* ZIP archives use POSIX-style paths `/` internally.
* **Directories are represented with a trailing `/`** (e.g., `payload/`).
  `ls()` and `pwd()` always show directories with `/`.
* `cd()` is tolerant: you may pass `payload` or `payload/`; internally the working directory becomes `payload/`.
* File operations like `cat()` and `info()` must target files (no trailing `/`).

## Usage

### Basic Navigation

```python
from zipnavigator import ZipNavigator

with ZipNavigator("data/archive.zip") as nav:
    print(nav.pwd())            # "/"
    print(nav.ls())             # top-level entries

    nav.cd("docs/")             # move into a directory
    print(nav.ls())             # list inside docs/

    print(nav.exists("README.txt"))   # True/False
    print(nav.is_file("README.txt"))  # True/False

    meta = nav.info("README.txt")     # size, compress_size, CRC, compress_type, date_time
    print(meta)

    text = nav.cat("README.txt")
    print(text[:200])
```

### Resumable Batch Extraction (Iterator)

```python
from zipnavigator import ZipNavigator

with ZipNavigator("data/dataset.zip") as nav:
    nav.cd("images/")  # optional: narrow base before initializing

    nav.initialize_iterator(
        output_dir="work",               # parent folder for extraction
        batch_size=50,                   # number of files per batch
        extract_subdir="extracted_zip",  # subfolder inside output_dir
        reset=True,                      # start fresh (clears previous state for this run)
        seed=42,                         # deterministic shuffle
        extensions=[".jpg", ".png"],     # optional filter
        on_error="skip",                 # or "abort"
        max_retries=2,
        validate_crc=True                # slower, but verifies integrity
    )

    for extracted_paths in nav:
        print("Extracted:", extracted_paths)
        st = nav.iterator_status()
        print("Done:", st["extracted_so_far"], "Remaining:", st["remaining"])
```

### Resuming a Previous Run

```python
from zipnavigator import ZipNavigator

with ZipNavigator("data/dataset.zip") as nav:
    nav.resume_iterator(output_dir="work", extract_subdir="extracted_zip")
    for batch in nav:
        print("Resumed batch:", batch)
```

### Resetting Iterator State

```python
with ZipNavigator("data/dataset.zip") as nav:
    nav.reset_iterator()  # clears state file and temporary extraction files
```

## Examples

See the `examples/` folder:

* `example_quickstart.py` — minimal navigation + extraction.
* `example_iterator.py` — filtered batched extraction.
* `example_resume.py` — resume a previous run.

## Error Handling

* `on_error="skip"`: log failing members and continue; `iterator_status()` exposes `failed_so_far` and a `failed_tail`.
* `on_error="abort"`: raise at the first failing member.
* `max_retries`: per-file retry attempts (≥0).
* `validate_crc=True`: extract via a safe streaming path and rely on CRC/decompress checks to fail on corruption (slower).

## Windows Notes

* Use `with ZipNavigator(...)` to ensure file handles are closed before deleting or moving the ZIP or its temp folders.
* Avoid opening the extraction directory in Explorer with Preview Pane while running, as it can hold file locks.

## Security

* Paths are validated to prevent Zip Slip: absolute paths, drive letters, and parent traversal (`..`) are rejected.

## Contributing

1. Fork and create a feature branch.
2. Keep changes focused and add tests where relevant.
3. Ensure tests pass and code is formatted.
4. Open a PR with a clear description and rationale.

## License

MIT. See `LICENSE`.

---
