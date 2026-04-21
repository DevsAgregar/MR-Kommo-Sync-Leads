#!/usr/bin/env python3
"""One-shot migration: encrypt existing plaintext SQLite databases with SQLCipher.

Run this exactly once per machine that has legacy plaintext ``.sqlite3`` files
so that future script runs (which now use ``db_util.connect``) can open them.

Idempotent: detects already-encrypted files by reading the first 16 bytes
and skips them. Keeps a ``.plain.bak`` backup of the original unless
``--no-backup`` is given.

Expects ``DB_ENCRYPTION_KEY`` in the environment or in a local ``.env``.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Iterable

from env_config import load_env_file
from sqlcipher3 import dbapi2 as sqlcipher


ROOT = Path(__file__).resolve().parent
DEFAULT_DBS = [
    ROOT / "mirella_pacientes.sqlite3",
    ROOT / "mirella_kommo_leads.sqlite3",
]
SQLITE_MAGIC = b"SQLite format 3\x00"


def _is_plaintext(path: Path) -> bool:
    with path.open("rb") as fh:
        header = fh.read(16)
    return header == SQLITE_MAGIC


def _encrypt_database(source: Path, key_hex: str, keep_backup: bool) -> None:
    tmp_fd, tmp_name = tempfile.mkstemp(
        prefix=source.stem + "_", suffix=".enc", dir=str(source.parent)
    )
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    tmp_path.unlink()  # sqlcipher_export will (re)create it

    conn = sqlcipher.connect(str(source))
    try:
        # sqlcipher_export needs the target attached with the desired key.
        conn.execute(
            f"ATTACH DATABASE ? AS encrypted KEY \"x'{key_hex}'\";",
            (str(tmp_path),),
        )
        conn.execute("SELECT sqlcipher_export('encrypted');")
        conn.execute("DETACH DATABASE encrypted;")
    finally:
        conn.close()

    if keep_backup:
        backup = source.with_suffix(source.suffix + ".plain.bak")
        if backup.exists():
            backup.unlink()
        shutil.copy2(source, backup)

    # Atomic replace: on Windows, os.replace handles the existing target.
    os.replace(tmp_path, source)


def _verify(source: Path, key_hex: str) -> None:
    conn = sqlcipher.connect(str(source))
    try:
        conn.execute(f"PRAGMA key = \"x'{key_hex}'\";")
        conn.execute("SELECT count(*) FROM sqlite_master;").fetchone()
    finally:
        conn.close()


def migrate(paths: Iterable[Path], keep_backup: bool) -> int:
    load_env_file(ROOT / ".env")
    key_hex = os.environ.get("DB_ENCRYPTION_KEY", "").strip()
    if not key_hex:
        print("DB_ENCRYPTION_KEY ausente. Rode build_secrets.py primeiro.", file=sys.stderr)
        return 1

    migrated = 0
    for path in paths:
        if not path.exists():
            print(f"   (ignorado) {path.name} nao existe")
            continue
        if not _is_plaintext(path):
            print(f"   (ignorado) {path.name} ja esta cifrado")
            continue
        print(f"-> cifrando {path.name}")
        _encrypt_database(path, key_hex, keep_backup)
        _verify(path, key_hex)
        migrated += 1

    print(f"Concluido. {migrated} banco(s) migrado(s).")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", nargs="*", type=Path, help="Arquivos .sqlite3 alvo (padrao: ambos os DBs do projeto)")
    parser.add_argument("--no-backup", action="store_true", help="Nao manter copia .plain.bak do arquivo original")
    args = parser.parse_args()

    targets = [Path(p).resolve() for p in args.paths] if args.paths else DEFAULT_DBS
    rc = migrate(targets, keep_backup=not args.no_backup)
    sys.exit(rc)


if __name__ == "__main__":
    main()
