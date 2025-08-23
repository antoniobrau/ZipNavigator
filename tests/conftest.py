# tests/conftest.py
import zipfile
from pathlib import Path
import pytest

@pytest.fixture
def make_sample_zip(tmp_path: Path):
    """
    Ritorna una factory che crea uno zip di esempio e ritorna il Path allo zip.
    Uso nei test: zf = make_sample_zip()
    """
    def _make() -> Path:
        zpath = tmp_path / "sample.zip"
        with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("payload/data1.csv", "a,b,c\n1,2,3\n")
            z.writestr("payload/data2.csv", "x,y,z\n4,5,6\n")
            z.writestr("payload/sub/a.txt", "hello sub\n")
            z.writestr("payload/sub/nested/b.bin", b"\x00\x01\x02\x03")
            z.writestr("top.txt", "hello\n")
            z.writestr("docs/readme.md", "# readme\n")
        return zpath
    return _make

@pytest.fixture
def make_three_csv_zip(tmp_path: Path):
    """Factory per uno zip con tre CSV sotto payload/"""
    def _make() -> Path:
        zpath = tmp_path / "three_csv.zip"
        with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("payload/a.csv", "a\n")
            z.writestr("payload/b.csv", "b\n")
            z.writestr("payload/c.csv", "c\n")
        return zpath
    return _make

@pytest.fixture
def outdir(tmp_path: Path):
    d = tmp_path / "out"
    d.mkdir()
    return d
