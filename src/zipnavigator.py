# -*- coding: utf-8 -*-
"""
ZipNavigator (with error handling)
==================================

- Navigation: ls(), cd(), pwd(), cat(), exists(), is_dir(), is_file(), info()
- Iterator with persistent state:
  initialize_iterator(...), next()/for, iterator_status(),
  reset_iterator(), resume_iterator()

Robustness features:
- on_error: "skip" (default) | "abort"
- max_retries: per-file retry attempts (default 1)
- validate_crc: optional; if True, manual extraction with integrity verification
- Disk space preflight per batch
- Persistent failure log (state["failed"])
"""

from __future__ import annotations
import os, json, shutil, random, posixpath
from pathlib import PurePosixPath
from typing import Iterable, Iterator, List, Optional, Dict, Any, Set, Tuple
import zipfile
import zlib

# ----------------- utils -----------------

def _is_safe_member(name: str) -> bool:
    """Reject absolute paths, Windows drive letters, and parent traversal."""
    if name.startswith(("/", "\\")):
        return False
    if len(name) >= 2 and name[1] == ":" and name[0].isalpha():
        return False
    norm = posixpath.normpath(name)
    if norm.startswith("../") or norm == "..":
        return False
    return True

def _normalize_extensions(exts: Optional[Iterable[str]]) -> Optional[Set[str]]:
    if exts is None:
        return None
    norm: Set[str] = set()
    for e in exts:
        e = e.strip().lower()
        if not e:
            continue
        if not e.startswith("."):
            e = "." + e
        norm.add(e)
    return norm or None

def _free_space_bytes(path: str) -> int:
    usage = shutil.disk_usage(path)
    return usage.free

# ----------------- class -----------------

class ZipNavigator(Iterator[List[str]]):
    """Safe ZIP navigator with filesystem-like ops and resumable batch extraction."""

    # ----- simple queries -----

    def exists(self, path: str) -> bool:
        rel = self._resolve(path)
        return self._zpath(rel).exists()

    def is_dir(self, path: str) -> bool:
        rel = self._resolve(path)
        return self._zpath(rel).is_dir()

    def is_file(self, path: str) -> bool:
        rel = self._resolve(path)
        return self._zpath(rel).is_file()

    # ----- lifecycle -----

    def __init__(self, zip_path: str):
        self.zip_path = os.fspath(zip_path)
        if not os.path.isfile(self.zip_path):
            raise FileNotFoundError(self.zip_path)
        self._zip = zipfile.ZipFile(self.zip_path, "r")
        self._cwd = ""   # current location inside the zip ("" = root)

        # Iterator state
        self._iter_active = False
        self._extract_dir: Optional[str] = None
        self._state_path: Optional[str] = None
        self._batch_size: Optional[int] = None
        self._order: Optional[List[str]] = None
        self._cursor: int = 0
        self._base_at_init: str = ""
        self._seed: Optional[int] = None
        self._extensions: Optional[Set[str]] = None

        # Robustness
        self._on_error: str = "skip"     # "skip" | "abort"
        self._max_retries: int = 1
        self._validate_crc: bool = False
        self._failed: List[str] = []     # members that permanently failed

    # ---------------- Navigation ----------------

    def _zpath(self, rel: str) -> zipfile.Path:
        """Resolve a zipfile.Path, tolerating implicit directories."""
        p = zipfile.Path(self._zip, at=rel)
        if rel and not rel.endswith("/") and (not p.is_dir() and not p.exists()):
            p2 = zipfile.Path(self._zip, at=rel + "/")
            if p2.is_dir() or p2.exists():
                return p2
        return p    

    def pwd(self) -> str:
        """Return the current working directory inside the zip."""
        return "/" + self._cwd if self._cwd else "/"

    def _dir_exists_in_zip(self, rel: str) -> bool:
        """Return True if there is at least one member under 'rel/' in the ZIP."""
        if rel == "" or rel == "/":
            return True
        prefix = rel.rstrip("/") + "/"
        for name in self._zip.namelist():
            if name.startswith(prefix):
                return True
        return False

    def _resolve(self, path: Optional[str]) -> str:
        """Normalize a user path relative to the current working directory."""
        # Normalize Windows backslashes
        if path is not None:
            path = path.replace("\\", "/")

        # Remember if the user explicitly asked for a directory with trailing '/'
        had_trailing = bool(path) and path.endswith("/")

        if not path:
            p = PurePosixPath(self._cwd)
        else:
            p = PurePosixPath(path)
            if p.is_absolute():
                p = p.relative_to("/")
            else:
                p = PurePosixPath(self._cwd) / p

        # Remove ".", "..", duplicate slashes, and trailing slash
        s = posixpath.normpath(str(p))

        # Represent logical root as empty string
        if s == ".":
            s = ""

        # Do not allow paths to escape the root
        if s.startswith(".."):
            raise ValueError("Invalid path")

        # If user had a trailing slash, restore it (except for root)
        if had_trailing and s != "" and not s.endswith("/"):
            s += "/"

        return s

    def ls(self, path: Optional[str] = None, recursive: bool = False) -> list[str]:
        """List entries under a path. Append '/' to directory names."""
        rel = self._resolve(path)
        base = self._zpath(rel)

        if not base.is_dir():
            if rel and not rel.endswith("/"):
                base2 = self._zpath(rel + "/")
                if base2.is_dir():
                    base, rel = base2, rel + "/"
                else:
                    if base.exists():
                        raise NotADirectoryError(rel)
                    raise FileNotFoundError(rel)
            else:
                if base.exists():
                    raise NotADirectoryError(rel)
                raise FileNotFoundError(rel)

        if not recursive:
            out: list[str] = []
            for c in base.iterdir():
                name = c.name + ("/" if c.is_dir() else "")
                out.append(posixpath.join(rel, name) if rel else name)
            return sorted(out)

        out: list[str] = []
        stack = [(rel, base)]
        while stack:
            cur_rel, cur = stack.pop()
            for child in cur.iterdir():
                # path relative to the zip root
                child_rel = posixpath.join(cur_rel, child.name) if cur_rel else child.name
                if child.is_dir():
                    out.append(child_rel + "/")
                    stack.append((child_rel, child))
                else:
                    out.append(child_rel)
        return sorted(out)

    def cd(self, path: str) -> str:
        """Change current directory inside the zip. Returns the new pwd()."""
        rel = self._resolve(path)
        p = self._zpath(rel)

        # root
        if rel == "":
            self._cwd = ""
            return self.pwd()

        # if there is a directory with that prefix (even implicit) → enter
        if self._dir_exists_in_zip(rel):
            self._cwd = rel.rstrip("/") + "/"
            return self.pwd()

        # if it exists (and is a file) → not a directory
        if p.exists():
            raise NotADirectoryError(rel)

        # try with a trailing slash — usually covered by _dir_exists_in_zip
        if rel and not rel.endswith("/"):
            if self._dir_exists_in_zip(rel + "/"):
                self._cwd = rel.rstrip("/") + "/"
                return self.pwd()

        # not found
        raise FileNotFoundError(rel)

    def cat(self, path: str, encoding="utf-8", errors="strict"):
        """Read a file as text (default) or bytes if encoding=None."""
        rel = self._resolve(path)
        zp = self._zpath(rel)

        if zp.is_dir():
            raise IsADirectoryError(rel)
        if not zp.exists() or not zp.is_file():
            raise FileNotFoundError(rel)

        with zp.open("rb") as f:
            data = f.read()
        return data.decode(encoding, errors=errors) if encoding else data

    def info(self, path: str) -> dict[str, Any]:
        """Return file metadata: sizes, timestamp, CRC, compression type."""
        rel = self._resolve(path)
        zp = self._zpath(rel)

        # directories (root or implicit): getinfo would fail
        if zp.is_dir():
            raise IsADirectoryError(rel)

        try:
            zi = self._zip.getinfo(rel)
        except KeyError:
            raise FileNotFoundError(rel)

        comp = zi.compress_type
        comp_name = {
            getattr(zipfile, "ZIP_STORED", None): "STORED",
            getattr(zipfile, "ZIP_DEFLATED", None): "DEFLATED",
            getattr(zipfile, "ZIP_BZIP2", None): "BZIP2",
            getattr(zipfile, "ZIP_LZMA", None): "LZMA",
        }.get(comp, str(comp))

        return {
            "filename": zi.filename,
            "file_size": zi.file_size,
            "compress_size": zi.compress_size,
            "date_time": zi.date_time,
            "CRC": zi.CRC,
            "compress_type": comp_name,
        }

    # ---------------- Iterator ----------------

    def initialize_iterator(
        self,
        output_dir: str,
        batch_size: int = 100,
        extract_subdir: str = "extracted_zip",
        reset: bool = False,
        seed: Optional[int] = None,
        extensions: Optional[Iterable[str]] = None,
        on_error: str = "skip",          # "skip" | "abort"
        max_retries: int = 1,
        validate_crc: bool = False,
    ) -> None:
        """
        Prepare batched extraction with persistent state.

        on_error:
            - "skip": failing files are skipped and logged in 'failed'.
            - "abort": stop at the first error by raising an exception.
        max_retries: per-file attempts (>=0).
        validate_crc: if True, manual extraction with integrity check (slower).
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")
        if on_error not in {"skip", "abort"}:
            raise ValueError("on_error must be 'skip' or 'abort'")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")

        self._extensions = _normalize_extensions(extensions)
        self._on_error = on_error
        self._max_retries = int(max_retries)
        self._validate_crc = bool(validate_crc)

        extract_dir = os.path.join(output_dir, extract_subdir)
        state_path = os.path.join(extract_dir, ".zip_iter_state.json")

        if reset and os.path.isdir(extract_dir):
            shutil.rmtree(extract_dir)
        os.makedirs(extract_dir, exist_ok=True)

        if (not reset) and os.path.isfile(state_path):
            state = self._load_state(state_path)
            if state.get("zip_path") != os.path.abspath(self.zip_path):
                raise RuntimeError("State belongs to a different ZIP")
            if state.get("base_at_init") != self._resolve(self._cwd):
                raise RuntimeError("Different base position inside the ZIP")
            saved_ext = set(state.get("extensions", [])) or None
            if saved_ext != self._extensions:
                raise RuntimeError("Extension filter differs from saved one")
            # load state
            self._order = state["order"]
            self._cursor = state["cursor"]
            self._batch_size = state["batch_size"]
            self._seed = state["seed"]
            self._failed = list(state.get("failed", []))
            # load policies
            self._on_error = state.get("on_error", self._on_error)
            self._max_retries = int(state.get("max_retries", self._max_retries))
            self._validate_crc = bool(state.get("validate_crc", self._validate_crc))
        else:
            base_rel = self._resolve(self._cwd)
            file_list = self._scan_all_files_under(base_rel)
            file_list = [f for f in file_list if _is_safe_member(f)]
            if self._extensions:
                file_list = [f for f in file_list if posixpath.splitext(f)[1].lower() in self._extensions]
            if not file_list:
                raise RuntimeError("No files found with the requested filter")
            order = list(file_list)
            if seed is None:
                seed = random.randrange(1, 2**63)
            rnd = random.Random(seed)
            rnd.shuffle(order)
            self._order, self._cursor, self._batch_size, self._seed = order, 0, batch_size, seed
            self._failed = []

            self._save_state(
                state_path,
                {
                    "zip_path": os.path.abspath(self.zip_path),
                    "base_at_init": base_rel,
                    "order": order,
                    "cursor": 0,
                    "batch_size": batch_size,
                    "extract_dir": os.path.abspath(extract_dir),
                    "seed": seed,
                    "extensions": sorted(self._extensions) if self._extensions else [],
                    "failed": [],
                    "on_error": self._on_error,
                    "max_retries": self._max_retries,
                    "validate_crc": self._validate_crc,
                },
            )

        self._iter_active = True
        self._extract_dir = extract_dir
        self._state_path = state_path
        self._base_at_init = self._resolve(self._cwd)

    def _scan_all_files_under(self, base_rel: str) -> List[str]:
        base = self._zpath(base_rel)
        if not base.is_dir():
            if base_rel and not base_rel.endswith("/"):
                base2 = self._zpath(base_rel + "/")
                if base2.is_dir():
                    base = base2
                else:
                    return []
            else:
                return []
        out: List[str] = []
        stack = [(base_rel, base)]
        while stack:
            cur_rel, cur = stack.pop()
            for child in cur.iterdir():
                child_rel = posixpath.join(cur_rel, child.name) if cur_rel else child.name
                if child.is_dir():
                    stack.append((child_rel, child))
                elif child.is_file():
                    out.append(child_rel)
        return out

    def _save_state(self, path, state):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)

    def _load_state(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _clear_extract_dir(self):
        for root, dirs, files in os.walk(self._extract_dir, topdown=False):
            for name in files:
                try:
                    os.remove(os.path.join(root, name))
                except FileNotFoundError:
                    pass
            for name in dirs:
                try:
                    os.rmdir(os.path.join(root, name))
                except OSError:
                    pass

    # ---- Extraction with error handling ----

    def _preflight_space(self, members: List[str]) -> None:
        """Estimate needed space and fail early if disk is clearly insufficient."""
        assert self._extract_dir and self._order is not None
        total_uncompressed = 0
        for m in members:
            try:
                zi = self._zip.getinfo(m)
                total_uncompressed += getattr(zi, "file_size", 0)
            except KeyError:
                continue
        # 5% margin + 16 MiB
        needed = int(total_uncompressed * 1.05) + 16 * 1024 * 1024
        free = _free_space_bytes(self._extract_dir)
        if free < needed:
            raise RuntimeError(
                f"Insufficient free space in extraction folder: "
                f"need ~{needed/1e6:.1f} MB, free ~{free/1e6:.1f} MB."
            )

    def _extract_one_raw(self, member: str, out_root: str) -> str:
        """Standard extraction (zipfile.extract)."""
        abs_path = self._zip.extract(member, path=out_root)
        return os.path.abspath(abs_path)

    def _extract_one_crc(self, member: str, out_root: str) -> str:
        """
        Manual extraction with integrity verification.
        If decompression or CRC fails, zipfile will raise during read/close.
        """
        # output path
        dest_path = os.path.join(out_root, *member.split("/"))
        dest_dir = os.path.dirname(dest_path)
        os.makedirs(dest_dir, exist_ok=True)
        # stream
        with self._zip.open(member, "r") as src, open(dest_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        return os.path.abspath(dest_path)

    def _extract_members(self, members: List[str]) -> Tuple[List[str], List[str]]:
        """
        Attempt to extract a list of members.
        Return (success_paths, failed_members).
        """
        assert self._extract_dir
        ok_paths: List[str] = []
        failed: List[str] = []

        for m in members:
            if not _is_safe_member(m):
                failed.append(m)
                if self._on_error == "abort":
                    raise RuntimeError(f"Unsafe ZIP member: {m}")
                continue

            # retry loop
            last_err: Optional[Exception] = None
            for _attempt in range(self._max_retries + 1):
                try:
                    if self._validate_crc:
                        outp = self._extract_one_crc(m, self._extract_dir)
                    else:
                        outp = self._extract_one_raw(m, self._extract_dir)
                    ok_paths.append(outp)
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
            if last_err is not None:
                failed.append(m)
                if self._on_error == "abort":
                    raise RuntimeError(f"Error extracting {m}: {last_err}") from last_err

        return ok_paths, failed

    # ---------- iterator protocol ----------

    def __iter__(self):
        if not self._iter_active:
            raise RuntimeError("Call initialize_iterator() first")
        return self

    def __next__(self) -> List[str]:
        if not self._iter_active or self._order is None:
            raise RuntimeError("Iterator not initialized")
        if self._cursor >= len(self._order):
            raise StopIteration

        start, end = self._cursor, min(self._cursor + self._batch_size, len(self._order))
        batch = self._order[start:end]

        # cleanup extraction folder for the new batch
        self._clear_extract_dir()

        # disk space preflight
        self._preflight_space(batch)

        # extract
        ok_paths, failed_now = self._extract_members(batch)
        # update in-memory state
        self._cursor = end
        self._failed.extend(f for f in failed_now if f not in self._failed)

        # persist state
        self._save_state(
            self._state_path,
            {
                "zip_path": os.path.abspath(self.zip_path),
                "base_at_init": self._base_at_init,
                "order": self._order,
                "cursor": self._cursor,
                "batch_size": self._batch_size,
                "extract_dir": os.path.abspath(self._extract_dir),
                "seed": self._seed,
                "extensions": sorted(self._extensions) if self._extensions else [],
                "failed": list(self._failed),
                "on_error": self._on_error,
                "max_retries": self._max_retries,
                "validate_crc": self._validate_crc,
            },
        )
        return ok_paths

    def iterator_status(self) -> Dict[str, Any]:
        """Return a dictionary with the current iterator status."""
        if not self._iter_active or self._order is None:
            return {"active": False}
        total = len(self._order)
        done = self._cursor
        remaining = max(total - done, 0)
        return {
            "active": True,
            "zip": os.path.abspath(self.zip_path),
            "base_at_init": self._base_at_init or "/",
            "batch_size": self._batch_size,
            "seed": self._seed,
            "extension_filter": sorted(self._extensions) if self._extensions else None,
            "total_files": total,
            "extracted_so_far": done,
            "remaining": remaining,
            "failed_so_far": len(self._failed),
            "failed_tail": list(self._failed[-10:]),  # last 10 for quick debug
            "extract_dir": os.path.abspath(self._extract_dir) if self._extract_dir else None,
            "state_file": os.path.abspath(self._state_path) if self._state_path else None,
            "error_policy": self._on_error,
            "max_retries": self._max_retries,
            "validate_crc": self._validate_crc,
        }

    def reset_iterator(self) -> None:
        """Clear iterator state and temporary files."""
        if self._extract_dir and os.path.isdir(self._extract_dir):
            self._clear_extract_dir()
            if self._state_path and os.path.isfile(self._state_path):
                try:
                    os.remove(self._state_path)
                except OSError:
                    pass
        self._iter_active = False
        self._extract_dir = None
        self._state_path = None
        self._batch_size = None
        self._order = None
        self._cursor = 0
        self._base_at_init = ""
        self._seed = None
        self._extensions = None
        self._failed = []
        self._on_error = "skip"
        self._max_retries = 1
        self._validate_crc = False

    def resume_iterator(self, output_dir: str, extract_subdir: str = "extracted_zip") -> None:
        """Resume a previous iterator run from the saved state file."""
        out_root = os.fspath(output_dir)
        extract_dir = os.path.join(out_root, extract_subdir)
        state_path = os.path.join(extract_dir, ".zip_iter_state.json")
        if not os.path.isfile(state_path):
            raise FileNotFoundError("No saved iterator state found.")
        state = self._load_state(state_path)
        if state.get("zip_path") != os.path.abspath(self.zip_path):
            raise RuntimeError("State belongs to a different ZIP.")
        self._base_at_init = state.get("base_at_init", "")
        if self._resolve(self._cwd) != self._base_at_init:
            self._cwd = self._base_at_init
        self._order = state["order"]
        self._cursor = int(state["cursor"])
        self._batch_size = int(state["batch_size"])
        self._seed = state.get("seed")
        exts = state.get("extensions", [])
        self._extensions = set(exts) if exts else None
        self._failed = list(state.get("failed", []))
        self._on_error = state.get("on_error", "skip")
        self._max_retries = int(state.get("max_retries", 1))
        self._validate_crc = bool(state.get("validate_crc", False))
        self._iter_active = True
        self._extract_dir = extract_dir
        self._state_path = state_path

    # ---------------- context manager ----------------

    def close(self):
        """Close the underlying zip file."""
        try:
            self._zip.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
