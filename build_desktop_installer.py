#!/usr/bin/env python3
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RESOURCES_DIR = ROOT / "src-tauri" / "resources"
BACKEND_DIR = RESOURCES_DIR / "backend"
RUNTIME_DIR = RESOURCES_DIR / "runtime"
BUILD_DIR = ROOT / "build" / "desktop_installer"

BACKEND_SCRIPTS = [
    "login.py",
    "clinic_operational_fields_sync.py",
    "kommo_leads_sqlite.py",
    "clinic_kommo_payload_preview.py",
    "sanity_check_secrets.py",
    "apply_kommo_safe_payloads.py",
]

RUNTIME_FILES = [
    (ROOT / ".env", RUNTIME_DIR / ".env"),
    (ROOT / ".env.example", RUNTIME_DIR / ".env.example"),
    (ROOT / "mirella_pacientes.sqlite3", RUNTIME_DIR / "mirella_pacientes.sqlite3"),
    (ROOT / "mirella_kommo_leads.sqlite3", RUNTIME_DIR / "mirella_kommo_leads.sqlite3"),
    (ROOT / "profiles" / "kommo_state.json", RUNTIME_DIR / "profiles" / "kommo_state.json"),
    (
        ROOT / "exports" / "kommo" / "kommo_leads_latest.sql",
        RUNTIME_DIR / "exports" / "kommo" / "kommo_leads_latest.sql",
    ),
    (
        ROOT / "exports" / "sync_preview" / "clinic_kommo_preview_summary.json",
        RUNTIME_DIR / "exports" / "sync_preview" / "clinic_kommo_preview_summary.json",
    ),
    (
        ROOT / "exports" / "sync_preview" / "clinic_kommo_preview_summary.md",
        RUNTIME_DIR / "exports" / "sync_preview" / "clinic_kommo_preview_summary.md",
    ),
    (
        ROOT / "exports" / "sync_preview" / "clinic_kommo_safe_payloads.json",
        RUNTIME_DIR / "exports" / "sync_preview" / "clinic_kommo_safe_payloads.json",
    ),
    (
        ROOT / "exports" / "sync_preview" / "clinic_kommo_safe_rows.csv",
        RUNTIME_DIR / "exports" / "sync_preview" / "clinic_kommo_safe_rows.csv",
    ),
    (
        ROOT / "exports" / "sync_preview" / "clinic_kommo_review_rows.csv",
        RUNTIME_DIR / "exports" / "sync_preview" / "clinic_kommo_review_rows.csv",
    ),
    (
        ROOT / "exports" / "sync_preview" / "clinic_kommo_all_actions.csv",
        RUNTIME_DIR / "exports" / "sync_preview" / "clinic_kommo_all_actions.csv",
    ),
]


def run(cmd: list[str], cwd: Path | None = None) -> None:
    print(">", " ".join(cmd))
    subprocess.run(cmd, cwd=cwd or ROOT, check=True)


def resolve_command(*names: str) -> str:
    for name in names:
        resolved = shutil.which(name)
        if resolved:
            return resolved
    raise FileNotFoundError(f"Could not resolve any command from: {', '.join(names)}")


def prepare_directories() -> None:
    if RESOURCES_DIR.exists():
        shutil.rmtree(RESOURCES_DIR)
    BACKEND_DIR.mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "mappings").mkdir(parents=True, exist_ok=True)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)


def copy_runtime_files() -> None:
    for source, target in RUNTIME_FILES:
        if not source.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    mappings_dir = ROOT / "mappings"
    if mappings_dir.exists():
        for csv_file in mappings_dir.glob("*.csv"):
            shutil.copy2(csv_file, RUNTIME_DIR / "mappings" / csv_file.name)


def build_backend() -> None:
    dist_dir = BUILD_DIR / "backend_dist"
    work_dir = BUILD_DIR / "backend_work"
    spec_dir = BUILD_DIR / "backend_spec"
    for directory in (dist_dir, work_dir, spec_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for script_name in BACKEND_SCRIPTS:
        script_path = ROOT / script_name
        exe_name = script_path.stem
        cmd = [
            resolve_command("py.exe", "py"),
            "-3",
            "-m",
            "PyInstaller",
            "--noconfirm",
            "--clean",
            "--onefile",
            "--console",
            "--name",
            exe_name,
            "--distpath",
            str(dist_dir),
            "--workpath",
            str(work_dir / exe_name),
            "--specpath",
            str(spec_dir),
            str(script_path),
        ]
        run(cmd)
        built_exe = dist_dir / f"{exe_name}.exe"
        shutil.copy2(built_exe, BACKEND_DIR / built_exe.name)


def build_installer() -> None:
    npm = resolve_command("npm.cmd", "npm")
    run([npm, "run", "build"])
    run([npm, "run", "tauri:build"])


def main() -> None:
    prepare_directories()
    copy_runtime_files()
    build_backend()
    build_installer()


if __name__ == "__main__":
    main()
