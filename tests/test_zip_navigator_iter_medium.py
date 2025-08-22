# tests/test_zip_navigator_iter_medium.py
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


# ---------------------- helper per creare ZIP ----------------------

def make_medium_zip(tmp_path: Path, depth: int = 3) -> tuple[Path, set[str]]:
    """
    Crea uno zip "medio" con decine di file e directory annidate.
    Ritorna (zip_path, insieme_di_tutti_i_member_names).
    """
    zpath = tmp_path / "medium.zip"
    members = set()

    def add(z: zipfile.ZipFile, name: str, data: bytes):
        z.writestr(name, data)
        # normalizziamo sempre a posix
        members.add(posixpath.normpath(name))

    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # blocco .txt
        for i in range(20):
            add(z, f"data/text/part{i % depth}/file_{i}.txt", f"txt-{i}\n".encode())

        # blocco .md
        for j in range(10):
            add(z, f"docs/section{j % depth}/doc_{j}.md", f"# doc {j}\n".encode())

        # blocco .bin
        for k in range(15):
            add(z, f"bin/chunk{k % depth}/blob_{k}.bin", bytes([k % 256]) * (32 + k))

        # catena annidata extra
        for t in range(5):
            add(z, f"nested/l1/l2/l3/l4{t % 2}/deep_{t}.txt", f"deep-{t}\n".encode())

        # top-level misti
        add(z, "README.md", b"# readme\n")
        add(z, "top.txt", b"root file\n")

    return zpath, members

def _corrupt_first_occurrence(zip_path: Path, marker: bytes) -> None:
    ba = bytearray(zip_path.read_bytes())
    idx = ba.find(marker)
    assert idx != -1, "marker non trovato dentro lo zip; accertati che compress_type=ZIP_STORED"
    ba[idx] ^= 0xFF  # flipping 1 byte
    zip_path.write_bytes(bytes(ba))


def make_zip_with_corrupted_member(tmp_path: Path) -> Path:
    zpath = tmp_path / "corrupt.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.writestr("ok.txt", b"all good\n", compress_type=zipfile.ZIP_STORED)
        z.writestr("corrupt/bad.txt", b"this will fail crc\n", compress_type=zipfile.ZIP_STORED)
    # Corrompi i dati del secondo file (ZIP_STORED -> testo in chiaro nell'archivio)
    _corrupt_first_occurrence(zpath, b"this will fail crc\n")
    return zpath


# ---------------------- test di navigazione / edge cases ----------------------

def test_navigation_edgecases_medium(tmp_path):
    """
    Verifica navigazione su zip medio:
    - ls() mostra directory con "/"
    - cd() funziona con e senza trailing slash
    - cd() su file -> NotADirectoryError
    - cd() su percorso inesistente -> FileNotFoundError
    - cd("..") dalla root -> ValueError (protezione escape root)
    """
    zf, members = make_medium_zip(tmp_path)
    with ZipNavigator(str(zf)) as zn:
        root = zn.ls()
        # Devono comparire almeno alcune dir top-level con '/'
        assert any(x.endswith("/") for x in root)

        # cd senza slash partendo dalla root
        assert zn.cd("docs") == "/docs/"
        # torna alla root e ripeti, stavolta con slash
        assert zn.cd("/") == "/"
        assert zn.cd("docs/") == "/docs/"
        # verifica anche l'assoluto (con o senza slash)
        assert zn.cd("/") == "/"
        assert zn.cd("/docs") == "/docs/"
        assert zn.cd("/") == "/"
        assert zn.cd("/docs/") == "/docs/"

        # cd su file (deve esistere → NotADirectoryError)
        assert zn.cd("/") == "/"
        with pytest.raises(NotADirectoryError):
            zn.cd("top.txt")        # relativo dalla root
        # e anche in forma assoluta:
        with pytest.raises(NotADirectoryError):
            zn.cd("/top.txt")

        # inesistente
        with ZipNavigator(str(zf)) as zn2:
            with pytest.raises(FileNotFoundError):
                zn2.cd("does_not_exist/")

        # ".." non consentito
        with ZipNavigator(str(zf)) as zn3:
            with pytest.raises(ValueError):
                zn3.cd("..")


# ---------------------- test iterazioni complete ----------------------

def test_iterates_all_files_medium(tmp_path):
    """
    Iterazione su zip medio con filtro estensioni .txt/.md:
    - Copre tutti i file attesi, in più batch (ordine shuffle via seed)
    - Nessun duplicato
    - Lo stato finale riporta rimanenti=0
    """
    zf, members = make_medium_zip(tmp_path)
    expected = {m for m in members if posixpath.splitext(m)[1].lower() in {".txt", ".md"}}
    out_dir = tmp_path / "out"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        zn.inizializza_iteratore(
            directory_output=str(out_dir),
            max_elementi=7,                     # batch piccoli per forzare più step
            nome_cartella="estratti_zip",
            seed=123,
            estensioni=[".txt", ".md"],
            on_error="skip",
            max_retries=1,
            validate_crc=False,
        )

        seen_rel = set()
        total = 0
        for batch_paths in zn:
            total += len(batch_paths)
            for p in batch_paths:
                # mappiamo al percorso dentro l'archivio
                rel = os.path.relpath(p, str(out_dir / "estratti_zip")).replace(os.sep, "/")
                seen_rel.add(posixpath.normpath(rel))
                # ogni file estratto deve esistere
                assert os.path.isfile(p)

        # ha iterato tutti i .txt/.md
        assert total == len(expected)
        assert seen_rel == expected

        st = zn.stato_iteratore()
        assert st["rimanenti"] == 0


def test_resume_after_interrupt_medium(tmp_path):
    """
    Simula interruzione del processo:
    - 1° istanza: inizializza e consuma 1 batch
    - 2° istanza: resume_iteratore e consuma il resto
    - Alla fine: file estratti (unione batch) == attesi
    """
    zf, members = make_medium_zip(tmp_path)
    expected = {m for m in members if posixpath.splitext(m)[1].lower() in {".txt", ".md"}}
    out_dir = tmp_path / "out_resume"; out_dir.mkdir()

    # Prima "sessione": un batch
    with ZipNavigator(str(zf)) as zn:
        zn.inizializza_iteratore(
            directory_output=str(out_dir),
            max_elementi=5,
            nome_cartella="estratti_zip",
            seed=42,
            estensioni=[".txt", ".md"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )

        first_batch = next(zn)
        assert len(first_batch) > 0
        # mappa i path del primo batch alla path interna allo zip
        first_seen = set()
        for p in first_batch:
            rel = os.path.relpath(p, str(out_dir / "estratti_zip")).replace(os.sep, "/")
            first_seen.add(posixpath.normpath(rel))

    # Seconda "sessione": resume e completa
    seen_rel = set()
    with ZipNavigator(str(zf)) as zn2:
        zn2.resume_iteratore(directory_output=str(out_dir), nome_cartella="estratti_zip")
        for batch_paths in zn2:
            for p in batch_paths:
                rel = os.path.relpath(p, str(out_dir / "estratti_zip")).replace(os.sep, "/")
                seen_rel.add(posixpath.normpath(rel))

        st = zn2.stato_iteratore()
        assert st["rimanenti"] == 0

    # Unione: primo batch + batch post-resume
    seen_rel |= first_seen
    assert seen_rel == expected


# ---------------------- test casi limite iteratore ----------------------

def test_iterator_no_matches_raises(tmp_path):
    """
    Filtro estensioni che non trova nulla -> inizializza_iteratore deve alzare RuntimeError.
    """
    zf, _ = make_medium_zip(tmp_path)
    out_dir = tmp_path / "out3"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        with pytest.raises(RuntimeError, match="Nessun file trovato"):
            zn.inizializza_iteratore(
                directory_output=str(out_dir),
                max_elementi=3,
                nome_cartella="estratti_zip",
                seed=1,
                estensioni=[".zzz"],   # estensioni inesistenti
            )


# ---------------------- test gestione file corrotto ----------------------

def test_corrupted_member_on_error_skip(tmp_path):
    """
    ZIP con un file corrotto:
    - con on_error="skip" e validate_crc=True l'estrazione continua
    - il file corrotto viene registrato in 'failed'
    """
    zf = make_zip_with_corrupted_member(tmp_path)
    out_dir = tmp_path / "out_corrupt"; out_dir.mkdir()

    with ZipNavigator(str(zf)) as zn:
        zn.inizializza_iteratore(
            directory_output=str(out_dir),
            max_elementi=10,
            nome_cartella="estratti_zip",
            seed=0,
            estensioni=[".txt"],
            on_error="skip",
            max_retries=0,
            validate_crc=True,
        )
        # consuma tutto
        total = 0
        for batch in zn:
            total += len(batch)

        st = zn.stato_iteratore()
        # abbiamo 2 .txt: ok.txt e corrupt/bad.txt -> uno deve fallire
        assert st["falliti_finora"] >= 1
        # gli ultimi falliti riportano il nome del corrotto (best-effort)
        assert any("corrupt/bad.txt" in x for x in st["elenco_falliti"])


def test_corrupted_member_on_error_abort(tmp_path):
    """
    ZIP con *solo* file corrotto:
    - con on_error="abort" l'estrazione del primo batch solleva un errore.
    """
    zpath = tmp_path / "only_corrupt.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        z.writestr("bad.txt", b"boom\n", compress_type=zipfile.ZIP_STORED)
    _corrupt_first_occurrence(zpath, b"boom\n")

    out_dir = tmp_path / "out_abort"; out_dir.mkdir()
    with ZipNavigator(str(zpath)) as zn:
        zn.inizializza_iteratore(
            directory_output=str(out_dir),
            max_elementi=1,
            nome_cartella="estratti_zip",
            seed=0,
            estensioni=[".txt"],
            on_error="abort",
            max_retries=0,
            validate_crc=True,
        )
        with pytest.raises(RuntimeError):
            _ = next(zn)
