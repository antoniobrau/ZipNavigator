# tests/test_navigation.py
import os, sys, posixpath
from pathlib import Path



from src.zipnavigator import ZipNavigator  # noqa



def test_ls_root_and_recursive(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        root = set(nav.ls())
        # directory implicite con '/' + un file
        assert "payload/" in root
        assert "docs/" in root
        assert "top.txt" in root

        rec = set(nav.ls(recursive=True))
        # devono comparire file e dir (le dir con '/')
        assert "payload/" in rec
        assert "payload/sub/" in rec
        assert "payload/sub/nested/" in rec
        assert "payload/data1.csv" in rec
        assert "payload/sub/a.txt" in rec
        assert "payload/sub/nested/b.bin" in rec
        assert "docs/readme.md" in rec
        assert "top.txt" in rec


def test_cd_with_and_without_trailing_slash(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        # senza slash
        nav.cd("payload")
        assert nav.pwd().endswith("/payload/")
        # con slash
        nav.cd("sub/")
        assert nav.pwd().endswith("/payload/sub/")


def test_exists_is_dir_is_file(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        assert nav.exists("payload")  # _zpath tollera dir implicite
        assert nav.is_dir("payload")  # True anche senza '/'
        assert nav.exists("payload/")
        assert nav.is_dir("payload/")

        assert nav.exists("top.txt")
        assert nav.is_file("top.txt")
        assert not nav.is_dir("top.txt")

        assert not nav.exists("nope/")
        assert not nav.exists("nope.txt")


def test_ls_tolerant_argument_without_slash(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        # ls("payload") deve funzionare grazie a _zpath tollerante
        listing = set(nav.ls("payload"))
        assert "payload/data1.csv" in listing
        assert "payload/data2.csv" in listing
