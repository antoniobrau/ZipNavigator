# tests/test_iterator_errors_and_preflight.py
import os, sys, types
from pathlib import Path

import pytest

import src.zipnavigator as zn  # per monkeypatch funzioni modulo
from src.zipnavigator import ZipNavigator  # noqa



def _rel(abs_path: str, out_dir: Path, sub: str) -> str:
    return os.path.relpath(abs_path, str(out_dir / sub)).replace(os.sep, "/")


def test_preflight_space_failure(make_sample_zip, tmp_path, monkeypatch):
    zf = make_sample_zip()
    out_dir = tmp_path / "o"; out_dir.mkdir()

    # forza spazio libero ~ 0
    monkeypatch.setattr(zn, "_free_space_bytes", lambda _: 0)

    with ZipNavigator(str(zf)) as nav:
        nav.cd("payload/")
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir="b",
            reset=True,
            seed=0,
            extensions=[".csv"],
            on_error="skip",
            max_retries=0,
            validate_crc=False,
        )
        with pytest.raises(RuntimeError, match="Insufficient free space"):
            next(nav)


def test_on_error_skip_continues(make_sample_zip, tmp_path, monkeypatch):
    zf = make_sample_zip()
    out_dir = tmp_path / "o"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as nav:
        nav.cd("payload/")
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir="b",
            reset=True,
            seed=0,
            extensions=[".csv"],
            on_error="skip",
            max_retries=0,
            validate_crc=False,
        )

        # monkeypatch: fallisce estrazione di data1.csv
        orig = nav._extract_one_raw
        def failing(member, out_root):
            if member.endswith("data1.csv"):
                raise RuntimeError("boom")
            return orig(member, out_root)
        nav._extract_one_raw = failing  # type: ignore

        batch = next(nav)  # estrae batch con uno che fallisce
        # con skip, restituisce i successi
        rels = {_rel(p, out_dir, "b") for p in batch}
        assert rels == {"payload/data2.csv"}  # solo il secondo presente

        st = nav.iterator_status()
        assert st["failed_so_far"] == 1
        assert st["remaining"] == 0


def test_on_error_abort_raises(make_sample_zip, tmp_path, monkeypatch):
    zf = make_sample_zip()
    out_dir = tmp_path / "o"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as nav:
        nav.cd("payload/")
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir="b",
            reset=True,
            seed=0,
            extensions=[".csv"],
            on_error="abort",   # <---
            max_retries=0,
            validate_crc=False,
        )
        # fallisci sempre
        nav._extract_one_raw = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore
        with pytest.raises(RuntimeError, match="Error extracting"):
            next(nav)


def test_validate_crc_path_is_used(make_sample_zip, tmp_path, monkeypatch):
    zf = make_sample_zip()
    out_dir = tmp_path / "o"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as nav:
        nav.cd("payload/")
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir="b",
            reset=True,
            seed=0,
            extensions=[".csv"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,   # <--- attiva percorso CRC
        )

        called = {"crc": 0}
        orig_crc = nav._extract_one_crc
        def spy_crc(member, out_root):
            called["crc"] += 1
            return orig_crc(member, out_root)
        nav._extract_one_crc = spy_crc  # type: ignore

        list(iter(nav))  # consuma tutto
        assert called["crc"] == 2  # due csv
