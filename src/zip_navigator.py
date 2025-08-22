# -*- coding: utf-8 -*-
"""
ZipNavigator (con gestione errori)
==================================

- Navigazione: ls(), cd(), pwd(), cat(), exists(), is_dir(), is_file(), info()
- Iteratore con stato persistente:
  inizializza_iteratore(...), next_batch()/for, stato_iteratore(),
  reset_iteratore(), resume_iteratore()

Novità su errori/robustezza:
- on_error: "skip" (default) | "abort"
- max_retries: tentativi per file (default 1)
- validate_crc: opzionale; se True, estrazione "a mano" con verifica integrità
- Preflight spazio su disco
- Log persistente dei fallimenti (state["failed"])
"""

from __future__ import annotations
import os, json, shutil, random, posixpath, io
from pathlib import PurePosixPath
from typing import Iterable, Iterator, List, Optional, Dict, Any, Set, Tuple
import zipfile
import zlib

# ----------------- util -----------------

def _is_safe_member(name: str) -> bool:
    if name.startswith(("/", "\\")):
        return False
    if len(name) >= 2 and name[1] == ":" and name[0].isalpha():
        return False
    norm = posixpath.normpath(name)
    if norm.startswith("../") or norm == "..":
        return False
    return True

def _normalize_extensions(exts: Optional[Iterable[str]]) -> Optional[Set[str]]:
    if exts is None:
        return None
    norm: Set[str] = set()
    for e in exts:
        e = e.strip().lower()
        if not e:
            continue
        if not e.startswith("."):
            e = "." + e
        norm.add(e)
    return norm or None

def _free_space_bytes(path: str) -> int:
    usage = shutil.disk_usage(path)
    return usage.free

# ----------------- classe -----------------

class ZipNavigator(Iterator[List[str]]):
    def exists(self, path: str) -> bool:
        rel = self._resolve(path)
        return self._zpath(rel).exists()

    def is_dir(self, path: str) -> bool:
        rel = self._resolve(path)
        return self._zpath(rel).is_dir()

    def is_file(self, path: str) -> bool:
        rel = self._resolve(path)
        return self._zpath(rel).is_file()
    

    def __init__(self, zip_path: str):
        self.zip_path = os.fspath(zip_path)
        if not os.path.isfile(self.zip_path):
            raise FileNotFoundError(self.zip_path)
        self._zip = zipfile.ZipFile(self.zip_path, "r")
        self._cwd = ""   # posizione corrente nello zip

        # Stato iteratore
        self._iter_active = False
        self._extract_dir: Optional[str] = None
        self._state_path: Optional[str] = None
        self._batch_size: Optional[int] = None
        self._order: Optional[List[str]] = None
        self._cursor: int = 0
        self._base_at_init: str = ""
        self._seed: Optional[int] = None
        self._extensions: Optional[Set[str]] = None

        # Robustezza
        self._on_error: str = "skip"     # "skip" | "abort"
        self._max_retries: int = 1
        self._validate_crc: bool = False
        self._failed: List[str] = []     # membri che hanno fallito in modo definitivo

    # ---------------- Navigazione ----------------


    def _zpath(self, rel: str) -> zipfile.Path:
        p = zipfile.Path(self._zip, at=rel)
        if rel and not rel.endswith("/") and (not p.is_dir() and not p.exists()):
            p2 = zipfile.Path(self._zip, at=rel + "/")
            if p2.is_dir() or p2.exists():
                return p2
        return p    

    def pwd(self) -> str:
        return "/" + self._cwd if self._cwd else "/"


    def _dir_exists_in_zip(self, rel: str) -> bool:
        """Ritorna True se esiste almeno un membro sotto 'rel/' nello ZIP."""
        if rel == "" or rel == "/":
            return True
        prefix = rel.rstrip("/") + "/"
        for name in self._zip.namelist():
            if name.startswith(prefix):
                return True
        return False

    def _resolve(self, path: Optional[str]) -> str:
        # Normalizza backslash Windows
        if path is not None:
            path = path.replace("\\", "/")

        # Ricorda se l’utente ha chiesto esplicitamente una directory con '/'
        had_trailing = bool(path) and path.endswith("/")

        if not path:
            p = PurePosixPath(self._cwd)
        else:
            p = PurePosixPath(path)
            if p.is_absolute():
                p = p.relative_to("/")
            else:
                p = PurePosixPath(self._cwd) / p

        s = posixpath.normpath(str(p))  # toglie ".", "..", slash multipli, slash finali

        # Interpreta la root logica come stringa vuota
        if s == ".":
            s = ""

        # Non permettere di uscire dalla root
        if s.startswith(".."):
            raise ValueError("Percorso non valido")

        # Se l'utente aveva messo lo slash finale, ripristinalo (tranne root)
        if had_trailing and s != "" and not s.endswith("/"):
            s += "/"

        return s

    def ls(self, path: Optional[str] = None, recursive: bool = False) -> list[str]:
        rel = self._resolve(path)
        base = self._zpath(rel)

        if not base.is_dir():
            if rel and not rel.endswith("/"):
                base2 = self._zpath(rel + "/")
                if base2.is_dir():
                    base, rel = base2, rel + "/"
                else:
                    if base.exists():
                        raise NotADirectoryError(rel)
                    raise FileNotFoundError(rel)
            else:
                if base.exists():
                    raise NotADirectoryError(rel)
                raise FileNotFoundError(rel)

        if not recursive:
            out: list[str] = []
            for c in base.iterdir():
                name = c.name + ("/" if c.is_dir() else "")
                out.append(posixpath.join(rel, name) if rel else name)
            return sorted(out)

        out: list[str] = []
        stack = [(rel, base)]
        while stack:
            cur_rel, cur = stack.pop()
            for child in cur.iterdir():
                # path relativo rispetto alla root zip
                child_rel = posixpath.join(cur_rel, child.name) if cur_rel else child.name
                if child.is_dir():
                    out.append(child_rel + "/")
                    stack.append((child_rel, child))
                else:
                    out.append(child_rel)
        return sorted(out)



    def cd(self, path: str) -> str:
        rel = self._resolve(path)
        p = self._zpath(rel)

        # root
        if rel == "":
            self._cwd = ""
            return self.pwd()

        # se esiste una directory con quel prefisso (anche implicita) → entra
        if self._dir_exists_in_zip(rel):
            self._cwd = rel.rstrip("/") + "/"
            return self.pwd()

        # se esiste (ed è file) → errore “non directory”
        if p.exists():
            raise NotADirectoryError(rel)

        # prova variante con slash (se mancava) — già coperto da _dir_exists_in_zip in pratica
        if rel and not rel.endswith("/"):
            if self._dir_exists_in_zip(rel + "/"):
                self._cwd = rel.rstrip("/") + "/"
                return self.pwd()

        # non trovato
        raise FileNotFoundError(rel)



    def cat(self, path: str, encoding="utf-8", errors="strict"):
        rel = self._resolve(path)
        zp = self._zpath(rel)

        if zp.is_dir():
            raise IsADirectoryError(rel)
        if not zp.exists() or not zp.is_file():
            raise FileNotFoundError(rel)

        with zp.open("rb") as f:
            data = f.read()
        return data.decode(encoding, errors=errors) if encoding else data

    def info(self, path: str) -> dict[str, Any]:
        rel = self._resolve(path)
        zp = self._zpath(rel)

        # directory (root o implicita): getinfo fallirebbe
        if zp.is_dir():
            raise IsADirectoryError(rel)

        try:
            zi = self._zip.getinfo(rel)
        except KeyError:
            raise FileNotFoundError(rel)

        # mappa tipo compressione in modo leggibile
        comp = zi.compress_type
        comp_name = {
            getattr(zipfile, "ZIP_STORED", None): "STORED",
            getattr(zipfile, "ZIP_DEFLATED", None): "DEFLATED",
            getattr(zipfile, "ZIP_BZIP2", None): "BZIP2",
            getattr(zipfile, "ZIP_LZMA", None): "LZMA",
        }.get(comp, str(comp))

        return {
            "filename": zi.filename,
            "file_size": zi.file_size,
            "compress_size": zi.compress_size,
            "date_time": zi.date_time,
            "CRC": zi.CRC,
            "compress_type": comp_name,
        }

    # ---------------- Iteratore ----------------

    def inizializza_iteratore(
        self,
        directory_output: str,
        max_elementi: int = 100,
        nome_cartella: str = "estratti_zip",
        reset: bool = False,
        seed: Optional[int] = None,
        estensioni: Optional[Iterable[str]] = None,
        on_error: str = "skip",          # "skip" | "abort"
        max_retries: int = 1,
        validate_crc: bool = False,
    ) -> None:
        """
        Prepara l'estrazione a lotti con stato persistente.

        on_error:
            - "skip": i file che falliscono vengono saltati e loggati in 'failed'.
            - "abort": al primo errore l'estrazione si interrompe con eccezione.
        max_retries: tentativi per singolo file (>=0)
        validate_crc: se True, estrazione manuale con verifica integrità (più lenta).
        """
        if max_elementi <= 0:
            raise ValueError("max_elementi deve essere > 0")
        if on_error not in {"skip", "abort"}:
            raise ValueError("on_error deve essere 'skip' o 'abort'")
        if max_retries < 0:
            raise ValueError("max_retries deve essere >= 0")

        self._extensions = _normalize_extensions(estensioni)
        self._on_error = on_error
        self._max_retries = int(max_retries)
        self._validate_crc = bool(validate_crc)

        extract_dir = os.path.join(directory_output, nome_cartella)
        state_path = os.path.join(extract_dir, ".zip_iter_state.json")

        if reset and os.path.isdir(extract_dir):
            shutil.rmtree(extract_dir)
        os.makedirs(extract_dir, exist_ok=True)

        if (not reset) and os.path.isfile(state_path):
            state = self._load_state(state_path)
            if state.get("zip_path") != os.path.abspath(self.zip_path):
                raise RuntimeError("Stato di un altro ZIP")
            if state.get("base_at_init") != self._resolve(self._cwd):
                raise RuntimeError("Posizione nello ZIP differente")
            saved_ext = set(state.get("extensions", [])) or None
            if saved_ext != self._extensions:
                raise RuntimeError("Filtro estensioni diverso da quello salvato")
            # carica stato
            self._order = state["order"]
            self._cursor = state["cursor"]
            self._batch_size = state["batch_size"]
            self._seed = state["seed"]
            self._failed = list(state.get("failed", []))
            # carica policy
            self._on_error = state.get("on_error", self._on_error)
            self._max_retries = int(state.get("max_retries", self._max_retries))
            self._validate_crc = bool(state.get("validate_crc", self._validate_crc))
        else:
            base_rel = self._resolve(self._cwd)
            file_list = self._scan_all_files_under(base_rel)
            file_list = [f for f in file_list if _is_safe_member(f)]
            if self._extensions:
                file_list = [f for f in file_list if posixpath.splitext(f)[1].lower() in self._extensions]
            if not file_list:
                raise RuntimeError("Nessun file trovato con il filtro richiesto")
            order = list(file_list)
            if seed is None:
                seed = random.randrange(1, 2**63)
            rnd = random.Random(seed)
            rnd.shuffle(order)
            self._order, self._cursor, self._batch_size, self._seed = order, 0, max_elementi, seed
            self._failed = []

            self._save_state(
                state_path,
                {
                    "zip_path": os.path.abspath(self.zip_path),
                    "base_at_init": base_rel,
                    "order": order,
                    "cursor": 0,
                    "batch_size": max_elementi,
                    "extract_dir": os.path.abspath(extract_dir),
                    "seed": seed,
                    "extensions": sorted(self._extensions) if self._extensions else [],
                    "failed": [],
                    "on_error": self._on_error,
                    "max_retries": self._max_retries,
                    "validate_crc": self._validate_crc,
                },
            )

        self._iter_active = True
        self._extract_dir = extract_dir
        self._state_path = state_path
        self._base_at_init = self._resolve(self._cwd)

    def _scan_all_files_under(self, base_rel: str) -> List[str]:
        base = self._zpath(base_rel)
        if not base.is_dir():
            if base_rel and not base_rel.endswith("/"):
                base2 = self._zpath(base_rel + "/")
                if base2.is_dir():
                    base = base2
                else:
                    return []
            else:
                return []
        out: List[str] = []
        stack = [(base_rel, base)]
        while stack:
            cur_rel, cur = stack.pop()
            for child in cur.iterdir():
                child_rel = posixpath.join(cur_rel, child.name) if cur_rel else child.name
                if child.is_dir():
                    stack.append((child_rel, child))
                elif child.is_file():
                    out.append(child_rel)
        return out



    def _save_state(self, path, state):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)

    def _load_state(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _clear_extract_dir(self):
        for root, dirs, files in os.walk(self._extract_dir, topdown=False):
            for name in files:
                try:
                    os.remove(os.path.join(root, name))
                except FileNotFoundError:
                    pass
            for name in dirs:
                try:
                    os.rmdir(os.path.join(root, name))
                except OSError:
                    pass

    # ---- Estrazione con gestione errori ----

    def _preflight_space(self, members: List[str]) -> None:
        """Controllo spazio minimo: se free << somma file_size, avvisa/ferma."""
        assert self._extract_dir and self._order is not None
        total_uncompressed = 0
        for m in members:
            try:
                zi = self._zip.getinfo(m)
                total_uncompressed += getattr(zi, "file_size", 0)
            except KeyError:
                continue
        # Margine 5% + 16 MiB
        needed = int(total_uncompressed * 1.05) + 16 * 1024 * 1024
        free = _free_space_bytes(self._extract_dir)
        if free < needed:
            raise RuntimeError(
                f"Spazio insufficiente nella cartella di estrazione: "
                f"richiesti ~{needed/1e6:.1f} MB, liberi ~{free/1e6:.1f} MB."
            )

    def _extract_one_raw(self, member: str, out_root: str) -> str:
        """Estrazione standard (zipfile.extract)."""
        abs_path = self._zip.extract(member, path=out_root)
        return os.path.abspath(abs_path)

    def _extract_one_crc(self, member: str, out_root: str) -> str:
        """
        Estrazione con validazione CRC: stream dal zip al file su disco.
        Se la decompressione fallisce (zlib.error) o CRC mismatch → eccezione.
        """
        zi = self._zip.getinfo(member)
        # path di output
        dest_path = os.path.join(out_root, *member.split("/"))
        dest_dir = os.path.dirname(dest_path)
        os.makedirs(dest_dir, exist_ok=True)
        # stream
        with self._zip.open(member, "r") as src, open(dest_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        # se ci sono corruzioni, zipfile/zlib solleva in read()
        return os.path.abspath(dest_path)

    def _extract_members(self, members: List[str]) -> Tuple[List[str], List[str]]:
        """
        Tenta l'estrazione dei membri.
        Ritorna (success_paths, failed_members)
        """
        assert self._extract_dir
        ok_paths: List[str] = []
        failed: List[str] = []

        for m in members:
            if not _is_safe_member(m):
                failed.append(m)
                if self._on_error == "abort":
                    raise RuntimeError(f"Membro non sicuro: {m}")
                continue

            # retry loop
            last_err: Optional[Exception] = None
            for attempt in range(self._max_retries + 1):
                try:
                    if self._validate_crc:
                        outp = self._extract_one_crc(m, self._extract_dir)
                    else:
                        outp = self._extract_one_raw(m, self._extract_dir)
                    ok_paths.append(outp)
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
            if last_err is not None:
                # fallito definitivamente
                failed.append(m)
                if self._on_error == "abort":
                    raise RuntimeError(f"Errore estraendo {m}: {last_err}") from last_err

        return ok_paths, failed

    # ---------- API iterazione ----------

    def __iter__(self):
        if not self._iter_active:
            raise RuntimeError("Chiama prima inizializza_iteratore")
        return self

    def __next__(self) -> List[str]:
        if not self._iter_active or self._order is None:
            raise RuntimeError("Iteratore non inizializzato")
        if self._cursor >= len(self._order):
            raise StopIteration

        start, end = self._cursor, min(self._cursor + self._batch_size, len(self._order))
        batch = self._order[start:end]

        # pulizia cartella
        self._clear_extract_dir()

        # preflight spazio
        self._preflight_space(batch)

        # estrazione
        ok_paths, failed_now = self._extract_members(batch)
        # aggiorna stato in memoria
        self._cursor = end
        self._failed.extend(f for f in failed_now if f not in self._failed)

        # salva stato
        self._save_state(
            self._state_path,
            {
                "zip_path": os.path.abspath(self.zip_path),
                "base_at_init": self._base_at_init,
                "order": self._order,
                "cursor": self._cursor,
                "batch_size": self._batch_size,
                "extract_dir": os.path.abspath(self._extract_dir),
                "seed": self._seed,
                "extensions": sorted(self._extensions) if self._extensions else [],
                "failed": list(self._failed),
                "on_error": self._on_error,
                "max_retries": self._max_retries,
                "validate_crc": self._validate_crc,
            },
        )
        return ok_paths

    def stato_iteratore(self) -> Dict[str, Any]:
        if not self._iter_active or self._order is None:
            return {"attivo": False}
        total = len(self._order)
        done = self._cursor
        remaining = max(total - done, 0)
        return {
            "attivo": True,
            "zip": os.path.abspath(self.zip_path),
            "base_at_init": self._base_at_init or "/",
            "batch_size": self._batch_size,
            "seed": self._seed,
            "filtro_estensioni": sorted(self._extensions) if self._extensions else None,
            "totale_file": total,
            "estratti_finora": done,
            "rimanenti": remaining,
            "falliti_finora": len(self._failed),
            "elenco_falliti": list(self._failed[-10:]),  # ultimi 10 per debug veloce
            "cartella_estrazione": os.path.abspath(self._extract_dir) if self._extract_dir else None,
            "state_file": os.path.abspath(self._state_path) if self._state_path else None,
            "policy_errori": self._on_error,
            "max_retries": self._max_retries,
            "validate_crc": self._validate_crc,
        }

    def reset_iteratore(self) -> None:
        if self._extract_dir and os.path.isdir(self._extract_dir):
            self._clear_extract_dir()
            if self._state_path and os.path.isfile(self._state_path):
                try:
                    os.remove(self._state_path)
                except OSError:
                    pass
        self._iter_active = False
        self._extract_dir = None
        self._state_path = None
        self._batch_size = None
        self._order = None
        self._cursor = 0
        self._base_at_init = ""
        self._seed = None
        self._extensions = None
        self._failed = []
        self._on_error = "skip"
        self._max_retries = 1
        self._validate_crc = False

    def resume_iteratore(self, directory_output: str, nome_cartella: str = "estratti_zip") -> None:
        out_root = os.fspath(directory_output)
        extract_dir = os.path.join(out_root, nome_cartella)
        state_path = os.path.join(extract_dir, ".zip_iter_state.json")
        if not os.path.isfile(state_path):
            raise FileNotFoundError("Nessuno stato trovato da riprendere.")
        state = self._load_state(state_path)
        if state.get("zip_path") != os.path.abspath(self.zip_path):
            raise RuntimeError("Stato di un altro ZIP.")
        self._base_at_init = state.get("base_at_init", "")
        if self._resolve(self._cwd) != self._base_at_init:
            self._cwd = self._base_at_init
        self._order = state["order"]
        self._cursor = int(state["cursor"])
        self._batch_size = int(state["batch_size"])
        self._seed = state.get("seed")
        exts = state.get("extensions", [])
        self._extensions = set(exts) if exts else None
        self._failed = list(state.get("failed", []))
        self._on_error = state.get("on_error", "skip")
        self._max_retries = int(state.get("max_retries", 1))
        self._validate_crc = bool(state.get("validate_crc", False))
        self._iter_active = True
        self._extract_dir = extract_dir
        self._state_path = state_path

    # ---------------- contesto ----------------

    def close(self):
        try:
            self._zip.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
