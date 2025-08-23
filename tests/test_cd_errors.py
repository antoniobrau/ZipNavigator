# tests/test_cd_errors.py
import os, sys
from pathlib import Path



from src.zipnavigator import ZipNavigator  # noqa



def test_cd_to_file_and_missing(make_sample_zip):
    zf = make_sample_zip()
    with ZipNavigator(str(zf)) as nav:
        # cd verso file => NotADirectoryError
        try:
            nav.cd("top.txt")
            raise AssertionError("Expected NotADirectoryError")
        except NotADirectoryError:
            pass

        # cd verso dir inesistente
        try:
            nav.cd("nope/")
            raise AssertionError("Expected FileNotFoundError")
        except FileNotFoundError:
            pass
