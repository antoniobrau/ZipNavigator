# tests/test_zip_navigator_iter_medium.py
import os
import sys
import zipfile
import posixpath
from pathlib import Path

import pytest

from src.zipnavigator import ZipNavigator


# ---------------------- helpers to create ZIPs ----------------------

def make_medium_zip(tmp_path: Path, depth: int = 3) -> tuple[Path, set[str]]:
    """
    Create a "medium" zip with dozens of files and nested directories.
    Returns (zip_path, set_of_all_member_names).
    """
    zpath = tmp_path / "medium.zip"
    members = set()

    def add(z: zipfile.ZipFile, name: str, data: bytes):
        z.writestr(name, data)
        # always normalize to POSIX
        members.add(posixpath.normpath(name))

    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # .txt block
        for i in range(20):
            add(z, f"data/text/part{i % depth}/file_{i}.txt", f"txt-{i}\n".encode())

        # .md block
        for j in range(10):
            add(z, f"docs/section{j % depth}/doc_{j}.md", f"# doc {j}\n".encode())

        # .bin block
        for k in range(15):
            add(z, f"bin/chunk{k % depth}/blob_{k}.bin", bytes([k % 256]) * (32 + k))

        # extra deep chain
        for t in range(5):
            add(z, f"nested/l1/l2/l3/l4{t % 2}/deep_{t}.txt", f"deep-{t}\n".encode())

        # mixed top-level
        add(z, "README.md", b"# readme\n")
        add(z, "top.txt", b"root file\n")

    return zpath, members


def _corrupt_first_occurrence(zip_path: Path, marker: bytes) -> None:
    ba = bytearray(zip_path.read_bytes())
    idx = ba.find(marker)
    assert idx != -1, "marker not found in zip; ensure compress_type=ZIP_STORED"
    ba[idx] ^= 0xFF  # flip one byte
    zip_path.write_bytes(bytes(ba))


def make_zip_with_corrupted_member(tmp_path: Path) -> Path:
    zpath = tmp_path / "corrupt.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.writestr("ok.txt", b"all good\n", compress_type=zipfile.ZIP_STORED)
        z.writestr("corrupt/bad.txt", b"this will fail crc\n", compress_type=zipfile.ZIP_STORED)
    # Corrupt the second file's data (ZIP_STORED -> plaintext in archive)
    _corrupt_first_occurrence(zpath, b"this will fail crc\n")
    return zpath


# ---------------------- navigation / edge cases ----------------------

def test_navigation_edgecases_medium(tmp_path):
    """
    Navigation over a medium zip:
    - ls() shows directories with trailing "/"
    - cd() works with and without trailing slash
    - cd() on a file -> NotADirectoryError
    - cd() on a non-existent path -> FileNotFoundError
    - cd("..") from root -> ValueError (escape protection)
    """
    zf, members = make_medium_zip(tmp_path)
    with ZipNavigator(str(zf)) as zn:
        root = zn.ls()
        # must include some top-level dirs with '/'
        assert any(x.endswith("/") for x in root)

        # cd without slash from root
        assert zn.cd("docs") == "/docs/"
        # back to root and repeat with slash
        assert zn.cd("/") == "/"
        assert zn.cd("docs/") == "/docs/"
        # absolute (with or without slash)
        assert zn.cd("/") == "/"
        assert zn.cd("/docs") == "/docs/"
        assert zn.cd("/") == "/"
        assert zn.cd("/docs/") == "/docs/"

        # cd on a file (must exist → NotADirectoryError)
        assert zn.cd("/") == "/"
        with pytest.raises(NotADirectoryError):
            zn.cd("top.txt")
        # absolute form as well:
        with pytest.raises(NotADirectoryError):
            zn.cd("/top.txt")

        # non-existent
        with ZipNavigator(str(zf)) as zn2:
            with pytest.raises(FileNotFoundError):
                zn2.cd("does_not_exist/")

        # ".." not allowed
        with ZipNavigator(str(zf)) as zn3:
            with pytest.raises(ValueError):
                zn3.cd("..")


# ---------------------- full iteration tests ----------------------

def test_iterates_all_files_medium(tmp_path):
    """
    Iterate over a medium zip with .txt/.md filter:
    - Covers all expected files across multiple batches (order shuffled via seed)
    - No duplicates
    - Final status reports remaining=0
    """
    zf, members = make_medium_zip(tmp_path)
    expected = {m for m in members if posixpath.splitext(m)[1].lower() in {".txt", ".md"}}
    out_dir = tmp_path / "out"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        zn.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=7,                    # small batches to force multiple steps
            extract_subdir="extracted_zip",
            seed=123,
            extensions=[".txt", ".md"],
            on_error="skip",
            max_retries=1,
            validate_crc=False,
        )

        seen_rel = set()
        total = 0
        for batch_paths in zn:
            total += len(batch_paths)
            for p in batch_paths:
                # map to path inside the archive
                rel = os.path.relpath(p, str(out_dir / "extracted_zip")).replace(os.sep, "/")
                seen_rel.add(posixpath.normpath(rel))
                # each extracted file must exist
                assert os.path.isfile(p)

        # iterated all .txt/.md
        assert total == len(expected)
        assert seen_rel == expected

        st = zn.iterator_status()
        assert st["remaining"] == 0


def test_resume_after_interrupt_medium(tmp_path):
    """
    Simulate process interruption:
    - 1st instance: initialize and consume 1 batch
    - 2nd instance: resume_iterator and consume the rest
    - End: extracted files (union of batches) == expected
    """
    zf, members = make_medium_zip(tmp_path)
    expected = {m for m in members if posixpath.splitext(m)[1].lower() in {".txt", ".md"}}
    out_dir = tmp_path / "out_resume"; out_dir.mkdir()

    # First "session": one batch
    with ZipNavigator(str(zf)) as zn:
        zn.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=5,
            extract_subdir="extracted_zip",
            seed=42,
            extensions=[".txt", ".md"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )

        first_batch = next(zn)
        assert len(first_batch) > 0
        # map first batch paths to internal zip paths
        first_seen = set()
        for p in first_batch:
            rel = os.path.relpath(p, str(out_dir / "extracted_zip")).replace(os.sep, "/")
            first_seen.add(posixpath.normpath(rel))

    # Second "session": resume and complete
    seen_rel = set()
    with ZipNavigator(str(zf)) as zn2:
        zn2.resume_iterator(output_dir=str(out_dir), extract_subdir="extracted_zip")
        for batch_paths in zn2:
            for p in batch_paths:
                rel = os.path.relpath(p, str(out_dir / "extracted_zip")).replace(os.sep, "/")
                seen_rel.add(posixpath.normpath(rel))

        st = zn2.iterator_status()
        assert st["remaining"] == 0

    # Union: first batch + post-resume batches
    seen_rel |= first_seen
    assert seen_rel == expected


# ---------------------- iterator edge cases ----------------------

def test_iterator_no_matches_raises(tmp_path):
    """
    Extension filter that yields nothing -> initialize_iterator must raise RuntimeError.
    """
    zf, _ = make_medium_zip(tmp_path)
    out_dir = tmp_path / "out3"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        with pytest.raises(RuntimeError, match="No files found"):
            zn.initialize_iterator(
                output_dir=str(out_dir),
                batch_size=3,
                extract_subdir="extracted_zip",
                seed=1,
                extensions=[".zzz"],   # nonexistent extensions
            )


# ---------------------- corrupted member handling ----------------------

def test_corrupted_member_on_error_skip(tmp_path):
    """
    ZIP with one corrupted file:
    - with on_error="skip" and validate_crc=True, extraction continues
    - the corrupted file is recorded in 'failed'
    """
    zf = make_zip_with_corrupted_member(tmp_path)
    out_dir = tmp_path / "out_corrupt"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        zn.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir="extracted_zip",
            seed=0,
            extensions=[".txt"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )
        # consume all
        total = 0
        for batch in zn:
            total += len(batch)

        st = zn.iterator_status()
        # we have 2 .txt: ok.txt and corrupt/bad.txt -> one must fail
        assert st["failed_so_far"] >= 1
        # the tail of failures should contain the corrupted member (best effort)
        assert any("corrupt/bad.txt" in x for x in st["failed_tail"])


def test_corrupted_member_on_error_abort(tmp_path):
    """
    ZIP with only a corrupted file:
    - with on_error="abort" the first batch raises.
    """
    zpath = tmp_path / "only_corrupt.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.writestr("bad.txt", b"boom\n", compress_type=zipfile.ZIP_STORED)
    _corrupt_first_occurrence(zpath, b"boom\n")

    out_dir = tmp_path / "out_abort"; out_dir.mkdir()
    with ZipNavigator(str(zpath)) as zn:
        zn.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=1,
            extract_subdir="extracted_zip",
            seed=0,
            extensions=[".txt"],
            on_error="abort",
            max_retries=0,
            validate_crc=True,
        )
        with pytest.raises(RuntimeError):
            _ = next(zn)
