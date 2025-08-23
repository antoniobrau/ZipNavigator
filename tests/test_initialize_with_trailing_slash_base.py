# tests/test_initialize_with_trailing_slash_base.py
import os
import sys
import zipfile
import posixpath
from pathlib import Path

import pytest

from src.zipnavigator import ZipNavigator


def _make_csv_zip(tmp_path: Path) -> Path:
    """Create a tiny ZIP with two CSV files under 'payload/' and some extras."""
    zpath = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("payload/data1.csv", "a,b,c\n1,2,3\n")
        z.writestr("payload/data2.csv", "x,y,z\n4,5,6\n")
        z.writestr("top.txt", "hello\n")
        z.writestr("docs/readme.md", "# readme\n")
    return zpath


@pytest.mark.parametrize("base_path", ["payload/", "payload"])  # trailing and non-trailing slash
def test_initialize_iterator_with_trailing_or_not_base(make_sample_zip, tmp_path, base_path):
    """
    Ensure initialize_iterator() finds files when the working base is a directory
    specified either with or without a trailing slash.
    """
    zf = _make_csv_zip(tmp_path)
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    extract_subdir = "batch"

    with ZipNavigator(str(zf)) as nav:
        # Sanity: the base directory is visible
        root = nav.ls()
        assert any(x.startswith("payload") for x in root)

        # Change base (trailing slash vs non-trailing slash)
        nav.cd(base_path)
        assert nav.pwd().endswith("/payload/")

        # Initialize iterator to extract only CSV files within the base
        print(out_dir)
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir=extract_subdir,
            reset=True,
            seed=42,
            extensions=[".csv"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )

        seen_rel = set()
        for batch in nav:
            for p in batch:
                rel = os.path.relpath(p, str(out_dir / extract_subdir)).replace(os.sep, "/")
                seen_rel.add(posixpath.normpath(rel))

        # Expect exactly the two CSVs under payload/
        assert seen_rel == {"payload/data1.csv", "payload/data2.csv"}

        st = nav.iterator_status()
        assert st["remaining"] == 0
        assert st["total_files"] == 2

