# tests/test_iterator_seed_and_state.py
import os, sys
from pathlib import Path

from src.zipnavigator import ZipNavigator  # noqa


def _collect_basenames(batches):
    out = []
    for batch in batches:
        out.extend([os.path.basename(p).replace(os.sep, "/") for p in batch])
    return out

def test_seed_deterministic_order(make_three_csv_zip, tmp_path):
    zf = make_three_csv_zip()
    out1 = tmp_path / "out1"; out1.mkdir()
    out2 = tmp_path / "out2"; out2.mkdir()

    # stesso seed → stesso ordine
    with ZipNavigator(str(zf)) as nav1, ZipNavigator(str(zf)) as nav2:
        for nav, out_dir in [(nav1, out1), (nav2, out2)]:
            nav.cd("payload/")
            nav.initialize_iterator(
                output_dir=str(out_dir),
                batch_size=2,
                extract_subdir="b",
                reset=True,
                seed=123,
                extensions=[".csv"],
                on_error="skip",
                max_retries=0,
                validate_crc=False,
            )
        seq1 = _collect_basenames(list(iter(nav1)))
        seq2 = _collect_basenames(list(iter(nav2)))
        assert seq1 == seq2 and set(seq1) == {"a.csv", "b.csv", "c.csv"}

def test_resume_and_reset(make_three_csv_zip, tmp_path):
    zf = make_three_csv_zip()
    out_dir = tmp_path / "out"; out_dir.mkdir()

    # step 1: estrai un batch e lascia stato
    with ZipNavigator(str(zf)) as nav:
        nav.cd("payload/")
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=1,
            extract_subdir="b",
            reset=True,
            seed=42,
            extensions=[".csv"],
            on_error="skip",
            max_retries=0,
            validate_crc=False,
        )
        first = next(nav)  # estrae 1 file
        st = nav.iterator_status()
        assert st["extracted_so_far"] == 1
        state_file = Path(st["state_file"])
        assert state_file.is_file()

    # step 2: nuovo oggetto, riprende
    with ZipNavigator(str(zf)) as nav2:
        nav2.resume_iterator(str(out_dir), extract_subdir="b")
        # completa
        rest = list(iter(nav2))
        st2 = nav2.iterator_status()
        assert st2["remaining"] == 0
        assert st2["extracted_so_far"] == st2["total_files"]

        # reset pulisce
        nav2.reset_iterator()
        assert not state_file.exists()
        assert nav2.iterator_status()["active"] is False
