# tests/test_cat_and_info.py
import os, sys
from pathlib import Path


from src.zipnavigator import ZipNavigator  # noqa



def test_cat_text_and_bytes(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        assert nav.cat("top.txt") == "hello\n"
        assert nav.cat("docs/readme.md") == "# readme\n"
        data = nav.cat("payload/sub/nested/b.bin", encoding=None)
        assert isinstance(data, (bytes, bytearray))
        assert data[:4] == b"\x00\x01\x02\x03"


def test_cat_errors(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        # directory => IsADirectoryError
        try:
            nav.cat("payload/")
            raise AssertionError("Expected IsADirectoryError")
        except IsADirectoryError:
            pass
        # non esiste => FileNotFoundError
        try:
            nav.cat("nope.txt")
            raise AssertionError("Expected FileNotFoundError")
        except FileNotFoundError:
            pass


def test_info_metadata(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        meta = nav.info("top.txt")
        assert meta["filename"] == "top.txt"
        assert meta["file_size"] == len("hello\n")
        assert meta["compress_size"] >= 0
        assert meta["file_size"] >= 0
        assert meta["compress_type"] in {"STORED", "DEFLATED", "BZIP2", "LZMA"}
        assert isinstance(meta["CRC"], int)
        # dir => IsADirectoryError
        try:
            nav.info("payload/")
            raise AssertionError("Expected IsADirectoryError")
        except IsADirectoryError:
            pass
