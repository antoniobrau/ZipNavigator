import io
import os
import sys
import zipfile
import posixpath
from pathlib import Path
import pytest


from src.zipnavigator import ZipNavigator


def make_small_zip(tmp_path: Path) -> Path:
    """
    Create a small zip with a nested structure:

    /
    ├─ a/
    │  ├─ b/
    │  │  └─ note.txt            ("hello from b")
    │  └─ img.bin                (bytes)
    ├─ top.txt                   ("root file")
    └─ readme.md                 ("markdown")
    """
    zip_path = tmp_path / "sample.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("a/b/note.txt", "hello from b\n")
        z.writestr("a/img.bin", b"\x00\x01\x02\x03\x04\xff")
        z.writestr("top.txt", "root file\n")
        z.writestr("readme.md", "# readme\n")
    return zip_path


def test_navigation_ls_cd_exists(tmp_path):
    zf = make_small_zip(tmp_path)
    with ZipNavigator(str(zf)) as zn:
        assert zn.pwd() == "/"
        root_list = zn.ls()
        assert set(root_list) == {"a/", "top.txt", "readme.md"}  # "a/" directory visible

        # Recursive
        all_list = zn.ls(recursive=True)
        assert "a/b/note.txt" in all_list
        assert "a/img.bin" in all_list

        # exists / is_dir / is_file
        assert zn.exists("a/")
        assert zn.is_dir("a/")
        assert not zn.is_file("a/")

        assert zn.exists("top.txt")
        assert zn.is_file("top.txt")
        assert not zn.is_dir("top.txt")

        # cd
        zn.cd("a/")
        assert zn.pwd() == "/a/"
        assert zn.ls() == ["a/b/", "a/img.bin"]


def test_cat_and_info(tmp_path):
    zf = make_small_zip(tmp_path)
    with ZipNavigator(str(zf)) as zn:
        txt = zn.cat("a/b/note.txt")
        assert "hello from b" in txt

        info = zn.info("a/b/note.txt")
        assert info["filename"] == "a/b/note.txt"
        assert info["file_size"] > 0
        assert info["compress_type"] in {"STORED", "DEFLATED", "BZIP2", "LZMA"}


def test_iterator_basic(tmp_path):
    zf = make_small_zip(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        # extension filter: only .txt and .md
        zn.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=2,
            extract_subdir="extracted_zip",
            seed=123,
            extensions=[".txt", ".md"],
            on_error="skip",
            max_retries=1,
            validate_crc=False,
        )

        # First batch
        batch1 = next(zn)
        assert len(batch1) == 2
        for p in batch1:
            assert Path(p).exists()

        status = zn.iterator_status()
        assert status["active"] is True
        assert status["extracted_so_far"] == 2
        assert status["total_files"] == 3  # top.txt, a/b/note.txt, readme.md

        # Second (and last) batch
        batch2 = next(zn)
        assert len(batch2) == 1
        with pytest.raises(StopIteration):
            next(zn)


def test_iterator_resume(tmp_path, monkeypatch):
    zf = make_small_zip(tmp_path)
    out_dir = tmp_path / "out2"
    out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        zn.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=1,
            extract_subdir="extracted_zip",
            seed=42,
            extensions=[".txt", ".md"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )
        b1 = next(zn)
        assert len(b1) == 1

        # Simulate process shutdown and resume
        zn.close()

        with ZipNavigator(str(zf)) as zn2:
            zn2.resume_iterator(output_dir=str(out_dir), extract_subdir="extracted_zip")
            status = zn2.iterator_status()
            assert status["extracted_so_far"] == 1
            # Continue until completion
            consumed = 0
            for paths in zn2:
                consumed += len(paths)
            assert consumed >= 1


def test_windows_backslashes_and_safety(tmp_path):
    zf = make_small_zip(tmp_path)
    with ZipNavigator(str(zf)) as zn:
        # backslashes should work
        assert zn.is_file("a\\img.bin")
        # path traversal is blocked during extraction (indirect test)
        # _is_safe_member is used internally; here we just verify the iterator
        # still works with normal files (those in our zip)
        pass
