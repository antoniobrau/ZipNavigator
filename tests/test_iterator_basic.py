# tests/test_iterator_basic.py
import os, sys, posixpath
from pathlib import Path



from src.zipnavigator import ZipNavigator  # noqa



def _rel_from_out(out_dir: Path, extract_subdir: str, abs_path: str) -> str:
    rel = os.path.relpath(abs_path, str(out_dir / extract_subdir)).replace(os.sep, "/")
    return posixpath.normpath(rel)

def test_iterator_extract_csv_under_payload(make_sample_zip, tmp_path):
    zf = make_sample_zip()
    out_dir = tmp_path / "out"; out_dir.mkdir()
    extract_subdir = "batch"

    with ZipNavigator(str(zf)) as nav:
        nav.cd("payload/")
        nav.initialize_iterator(
            output_dir=str(out_dir),
            batch_size=10,
            extract_subdir=extract_subdir,
            reset=True,
            seed=42,
            extensions=[".csv"],
            on_error="skip",
            max_retries=0,
            validate_crc=False,
        )

        seen = set()
        for batch in nav:
            for p in batch:
                seen.add(_rel_from_out(out_dir, extract_subdir, p))

        assert seen == {"payload/data1.csv", "payload/data2.csv"}
        st = nav.iterator_status()
        assert st["total_files"] == 2
        assert st["remaining"] == 0
        assert st["failed_so_far"] == 0
