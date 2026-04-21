"""Shared SQLCipher helpers for Mirella Kommo Sync.

All backend scripts open their SQLite databases through ``connect`` so the
PRAGMA key is applied consistently. The key itself is supplied via the
``DB_ENCRYPTION_KEY`` environment variable (set by the Tauri host for
packaged builds, and by the local .env for developer runs).
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from sqlcipher3 import dbapi2 as sqlite3  # noqa: F401 — re-exported on purpose


_HEX_KEY = re.compile(r"^[0-9A-Fa-f]{64}$")


def _resolve_key() -> str:
    key = os.environ.get("DB_ENCRYPTION_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "DB_ENCRYPTION_KEY ausente. Rode build_secrets.py ou defina a chave no .env."
        )
    if not _HEX_KEY.match(key):
        raise RuntimeError(
            "DB_ENCRYPTION_KEY invalido. Esperado 64 caracteres hex (32 bytes)."
        )
    return key


def connect(path: str | Path) -> sqlite3.Connection:
    """Open ``path`` with SQLCipher, applying the shared PRAGMA key."""
    conn = sqlite3.connect(str(path))
    key = _resolve_key()
    # Raw-byte key form avoids PBKDF2 derivation and keeps dev/prod deterministic.
    conn.execute(f"PRAGMA key = \"x'{key}'\";")
    return conn
