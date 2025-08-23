# tests/test_public_api_edges.py
import os, sys
from pathlib import Path

from src.zipnavigator import ZipNavigator  # noqa



def test_pwd_root_and_change(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        assert nav.pwd() == "/"
        nav.cd("docs/")
        assert nav.pwd().endswith("/docs/")
        nav.cd("")  # torna a root
        assert nav.pwd() == "/"
