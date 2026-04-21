"""Transparent AES-256-GCM wrapper around Kommo session state.

The Kommo sidecar scripts hand ``state_path`` to Playwright (which reads/writes
a JSON file directly) and to ``json.loads`` in the HTTP session builder. To
keep those call sites unchanged while still storing the state cipher-text at
rest, we decrypt ``path.enc`` into a plaintext sibling at startup and
re-encrypt + delete the plaintext at shutdown via ``atexit``.

Reuses ``DB_ENCRYPTION_KEY`` from the environment — same key material that
protects the SQLCipher databases, shipped inside secrets.enc.
"""
from __future__ import annotations

import atexit
import os
import secrets
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


_NONCE_LEN = 12
_ENC_SUFFIX = ".enc"


def _key() -> bytes:
    value = os.environ.get("DB_ENCRYPTION_KEY", "").strip()
    if not value:
        raise RuntimeError("DB_ENCRYPTION_KEY ausente para estado Kommo.")
    return bytes.fromhex(value)


def encrypt_bytes(plain: bytes) -> bytes:
    nonce = secrets.token_bytes(_NONCE_LEN)
    return nonce + AESGCM(_key()).encrypt(nonce, plain, None)


def decrypt_bytes(blob: bytes) -> bytes:
    nonce, ct = blob[:_NONCE_LEN], blob[_NONCE_LEN:]
    return AESGCM(_key()).decrypt(nonce, ct, None)


def _plain_sibling(enc_path: Path) -> Path:
    return enc_path.with_suffix(".runtime.json")


def prepare(enc_path: Path) -> Path:
    """Decrypt ``enc_path`` (if present) into a plaintext sibling; return it.

    If the incoming path is not ``.enc`` the caller is opted-out of encryption
    (e.g. legacy dev runs) and the path is returned as-is.
    """
    enc_path = Path(enc_path)
    if enc_path.suffix != _ENC_SUFFIX:
        return enc_path
    plain = _plain_sibling(enc_path)
    if enc_path.exists():
        plain.parent.mkdir(parents=True, exist_ok=True)
        plain.write_bytes(decrypt_bytes(enc_path.read_bytes()))
    return plain


def seal(enc_path: Path) -> None:
    """Re-encrypt the plaintext sibling of ``enc_path`` and remove it."""
    enc_path = Path(enc_path)
    if enc_path.suffix != _ENC_SUFFIX:
        return
    plain = _plain_sibling(enc_path)
    if not plain.exists():
        return
    enc_path.parent.mkdir(parents=True, exist_ok=True)
    enc_path.write_bytes(encrypt_bytes(plain.read_bytes()))
    try:
        plain.unlink()
    except OSError:
        pass


def discard(enc_path: Path) -> None:
    """Remove the plaintext sibling without updating the encrypted state."""
    enc_path = Path(enc_path)
    if enc_path.suffix != _ENC_SUFFIX:
        return
    plain = _plain_sibling(enc_path)
    try:
        plain.unlink()
    except OSError:
        pass


def is_encrypted_path(path: Path) -> bool:
    return Path(path).suffix == _ENC_SUFFIX


def activate(enc_path: Path) -> Path:
    """Prepare the plaintext state and register an atexit seal. Returns the
    plaintext path the caller should pass to Playwright / json.loads.
    """
    plain = prepare(enc_path)
    if Path(enc_path).suffix == _ENC_SUFFIX:
        atexit.register(seal, Path(enc_path))
    return plain


def activate_manual(enc_path: Path) -> Path:
    """Prepare plaintext without registering automatic seal.

    Callers that must validate the state before persisting it should use this
    and then explicitly call ``seal`` or ``discard``.
    """
    return prepare(enc_path)
