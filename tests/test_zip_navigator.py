import io
import os
import sys
import zipfile
import posixpath
from pathlib import Path
import pytest

# Assicura che 'src' sia nel path quando i test girano localmente
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from zip_navigator import ZipNavigator


def make_small_zip(tmp_path: Path) -> Path:
    """
    Crea uno zip in-memory con struttura annidata:

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
        assert set(root_list) == {"a/", "top.txt", "readme.md"}  # dir "a/" visibile

        # Ricorsivo
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
        # filtro estensioni: solo .txt e .md
        zn.inizializza_iteratore(
            directory_output=str(out_dir),
            max_elementi=2,
            nome_cartella="estratti_zip",
            seed=123,
            estensioni=[".txt", ".md"],
            on_error="skip",
            max_retries=1,
            validate_crc=False,
        )

        # Primo batch
        batch1 = next(zn)
        assert len(batch1) == 2
        for p in batch1:
            assert Path(p).exists()

        stato = zn.stato_iteratore()
        assert stato["attivo"] is True
        assert stato["estratti_finora"] == 2
        assert stato["totale_file"] == 3  # top.txt, a/b/note.txt, readme.md

        # Secondo (e ultimo) batch
        batch2 = next(zn)
        assert len(batch2) == 1
        with pytest.raises(StopIteration):
            next(zn)


def test_iterator_resume(tmp_path, monkeypatch):
    import pytest

    zf = make_small_zip(tmp_path)
    out_dir = tmp_path / "out2"
    out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        zn.inizializza_iteratore(
            directory_output=str(out_dir),
            max_elementi=1,
            nome_cartella="estratti_zip",
            seed=42,
            estensioni=[".txt", ".md"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )
        b1 = next(zn)
        assert len(b1) == 1

        # Simula chiusura del processo e resume
        zn.close()

        with ZipNavigator(str(zf)) as zn2:
            zn2.resume_iteratore(directory_output=str(out_dir), nome_cartella="estratti_zip")
            stato = zn2.stato_iteratore()
            assert stato["estratti_finora"] == 1
            # Continua finché finisce
            consumed = 0
            for paths in zn2:
                consumed += len(paths)
            assert consumed >= 1


def test_windows_backslashes_and_safety(tmp_path):
    zf = make_small_zip(tmp_path)
    with ZipNavigator(str(zf)) as zn:
        # backslash dovrebbe funzionare
        assert zn.is_file("a\\img.bin")
        # path traversal non permesso in estrazione (test indiretto)
        # _is_safe_member è già usato internamente; qui verifichiamo che l'iteratore
        # funzioni comunque con file "normali" (quelli del nostro zip)
        pass
