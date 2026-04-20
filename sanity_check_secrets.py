#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List


ROOT = Path(__file__).resolve().parent

PATTERNS = [
    ("known_clinica_email", re.compile(r"consultores@agregarnegocios\.com\.br", re.I)),
    ("known_clinica_password", re.compile(r"@Agregar12")),
    ("known_kommo_email", re.compile(r"clinicamirellarabelo@gmail\.com", re.I)),
    ("known_kommo_password", re.compile(r"Mrclinica&2025")),
    ("bearer_token", re.compile(r"Bearer\s+[A-Za-z0-9._\-]{20,}")),
    ("openai_key", re.compile(r"sk-[A-Za-z0-9_-]{20,}")),
    ("aws_access_key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("jwt_like", re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}")),
    ("session_id_assignment", re.compile(r"session_id\s*[:=]\s*[\"'][^\"']{8,}[\"']", re.I)),
    ("refresh_token_assignment", re.compile(r"refresh_token\s*[:=]\s*[\"'][^\"']{16,}[\"']", re.I)),
    ("access_token_assignment", re.compile(r"access_token\s*[:=]\s*[\"'][^\"']{16,}[\"']", re.I)),
    ("csrf_token_assignment", re.compile(r"csrf_token\s*[:=]\s*[\"'][^\"']{16,}[\"']", re.I)),
    ("env_secret_assignment", re.compile(r"^(MIRELLA_(EMAIL|SENHA|COOKIE)|KOMMO_(EMAIL|PASSWORD|ACCESS_TOKEN))=.+$", re.I | re.M)),
]

EXCLUDED_SUFFIXES = {
    ".sqlite3",
    ".db",
    ".zip",
    ".xlsx",
    ".xls",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".mp4",
    ".mov",
    ".webm",
    ".wav",
}

EXCLUDED_PATH_PARTS = {
    ".git",
    "__pycache__",
    "exports",
    "output",
    "profiles",
    ".venv",
    "venv",
}
EXCLUDED_FILE_NAMES = {"sanity_check_secrets.py"}


def _git_list_files() -> List[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    raw_paths = [item for item in result.stdout.decode("utf-8", errors="ignore").split("\0") if item]
    return [ROOT / item for item in raw_paths]


def _should_skip(path: Path) -> bool:
    if any(part in EXCLUDED_PATH_PARTS for part in path.parts):
        return True
    if path.name in EXCLUDED_FILE_NAMES:
        return True
    if path.suffix.lower() in EXCLUDED_SUFFIXES:
        return True
    return False


def _is_text_file(path: Path) -> bool:
    try:
        data = path.read_bytes()
    except OSError:
        return False
    if b"\x00" in data:
        return False
    return True


def _scan_file(path: Path) -> List[str]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []

    findings: List[str] = []
    for name, pattern in PATTERNS:
        for match in pattern.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            snippet = match.group(0).strip().replace("\n", " ")[:160]
            findings.append(f"{path.relative_to(ROOT)}:{line}: {name}: {snippet}")
    return findings


def run() -> int:
    findings: List[str] = []
    for path in _git_list_files():
        if _should_skip(path):
            continue
        if not path.is_file():
            continue
        if not _is_text_file(path):
            continue
        findings.extend(_scan_file(path))

    if findings:
        print("Secret sanity check failed:")
        for finding in findings:
            print(f" - {finding}")
        return 1

    print("Secret sanity check passed: no suspicious secrets found in tracked/nonignored files.")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan tracked and nonignored files for likely secrets.")
    parser.parse_args()
    raise SystemExit(run())


if __name__ == "__main__":
    main()
