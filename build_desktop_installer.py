#!/usr/bin/env python3
"""Build the Mirella Kommo Sync desktop installer.

Produces a production-ready bundle containing only the runtime files and
backend executables the packaged app actually needs. Every step prints a
concise summary so it is easy to audit what ends up inside the installer.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RESOURCES_DIR = ROOT / "src-tauri" / "resources"
BACKEND_DIR = RESOURCES_DIR / "backend"
RUNTIME_DIR = RESOURCES_DIR / "runtime"
BUILD_DIR = ROOT / "build" / "desktop_installer"
FRONTEND_DIST = ROOT / "dist"

BACKEND_SCRIPTS = [
    "login.py",
    "clinic_operational_fields_sync.py",
    "kommo_leads_sqlite.py",
    "clinic_kommo_payload_preview.py",
    "sanity_check_secrets.py",
    "apply_kommo_safe_payloads.py",
]

# Heavy stdlib / third-party modules none of the backend scripts import.
# Excluding them keeps the sidecar executables small and speeds up startup.
PYINSTALLER_EXCLUDES = [
    "tkinter",
    "_tkinter",
    "test",
    "tests",
    "unittest",
    "pydoc",
    "pydoc_data",
    "idlelib",
    "turtle",
    "turtledemo",
    "ensurepip",
    "distutils",
    "doctest",
    "IPython",
    "notebook",
    "pandas",
    "numpy",
    "matplotlib",
    "scipy",
]

RUNTIME_FILES = [
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

# Extensions that never belong inside the installer.
SKIP_EXTENSIONS = {".pyc", ".pyo", ".pyd", ".log", ".tmp"}


def _fmt_size(bytes_: int) -> str:
    size = float(bytes_)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


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
    print("== Limpando artefatos antigos ==")
    if RESOURCES_DIR.exists():
        shutil.rmtree(RESOURCES_DIR)
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    if FRONTEND_DIST.exists():
        shutil.rmtree(FRONTEND_DIST)
    BACKEND_DIR.mkdir(parents=True, exist_ok=True)
    (RUNTIME_DIR / "mappings").mkdir(parents=True, exist_ok=True)
    BUILD_DIR.mkdir(parents=True, exist_ok=True)


def copy_runtime_files() -> None:
    print("== Copiando arquivos de runtime ==")
    copied = 0
    missing = []
    for source, target in RUNTIME_FILES:
        if not source.exists():
            missing.append(source.name)
            continue
        if source.suffix.lower() in SKIP_EXTENSIONS:
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied += 1

    mappings_dir = ROOT / "mappings"
    if mappings_dir.exists():
        for csv_file in mappings_dir.glob("*.csv"):
            shutil.copy2(csv_file, RUNTIME_DIR / "mappings" / csv_file.name)
            copied += 1

    print(f"   Arquivos copiados: {copied}")
    if missing:
        print(f"   Ausentes (ignorados): {', '.join(missing)}")


def encrypt_secrets() -> None:
    print("== Cifrando segredos (.env -> secrets.enc) ==")
    script = ROOT / "build_secrets.py"
    if not script.exists():
        raise FileNotFoundError(f"build_secrets.py ausente: {script}")
    run([resolve_command("py.exe", "py"), "-3", str(script)])


def _pyinstaller_cmd(
    script_path: Path,
    exe_name: str,
    dist_dir: Path,
    work_dir: Path,
    spec_dir: Path,
) -> list[str]:
    cmd = [
        resolve_command("py.exe", "py"),
        "-3",
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--onefile",
        "--console",
        "--log-level",
        "WARN",
        "--name",
        exe_name,
        "--distpath",
        str(dist_dir),
        "--workpath",
        str(work_dir),
        "--specpath",
        str(spec_dir),
    ]
    for module in PYINSTALLER_EXCLUDES:
        cmd.extend(["--exclude-module", module])
    cmd.append(str(script_path))
    return cmd


def build_backend() -> None:
    print("== Empacotando backend Python (PyInstaller) ==")
    dist_dir = BUILD_DIR / "backend_dist"
    work_dir = BUILD_DIR / "backend_work"
    spec_dir = BUILD_DIR / "backend_spec"
    for directory in (dist_dir, work_dir, spec_dir):
        directory.mkdir(parents=True, exist_ok=True)

    for script_name in BACKEND_SCRIPTS:
        script_path = ROOT / script_name
        if not script_path.exists():
            raise FileNotFoundError(f"Script backend ausente: {script_path}")
        exe_name = script_path.stem
        cmd = _pyinstaller_cmd(
            script_path,
            exe_name,
            dist_dir,
            work_dir / exe_name,
            spec_dir,
        )
        print(f"-- {exe_name}.exe")
        run(cmd)
        built_exe = dist_dir / f"{exe_name}.exe"
        if not built_exe.exists():
            raise FileNotFoundError(f"PyInstaller nao gerou {built_exe}")
        shutil.copy2(built_exe, BACKEND_DIR / built_exe.name)

    total = _dir_size(BACKEND_DIR)
    print(f"   Backend total: {_fmt_size(total)}")


def build_installer() -> None:
    print("== Compilando frontend (Vite + Tauri) ==")
    npm = resolve_command("npm.cmd", "npm")
    run([npm, "run", "build"])
    run([npm, "run", "tauri:build"])


def _print_summary() -> None:
    print("\n== Resumo do build ==")
    print(f"Backend:  {_fmt_size(_dir_size(BACKEND_DIR))}  ({BACKEND_DIR})")
    print(f"Runtime:  {_fmt_size(_dir_size(RUNTIME_DIR))}  ({RUNTIME_DIR})")
    print(f"Frontend: {_fmt_size(_dir_size(FRONTEND_DIST))}  ({FRONTEND_DIST})")
    bundle_dir = ROOT / "src-tauri" / "target" / "release" / "bundle"
    if bundle_dir.exists():
        print("\nInstaladores gerados:")
        for installer in bundle_dir.rglob("*"):
            if installer.is_file() and installer.suffix.lower() in {".exe", ".msi"}:
                print(f"   {_fmt_size(installer.stat().st_size):>10}  {installer}")


def main() -> None:
    prepare_directories()
    copy_runtime_files()
    encrypt_secrets()
    build_backend()
    build_installer()
    _print_summary()


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(f"\nBuild falhou: comando retornou codigo {error.returncode}")
        sys.exit(error.returncode)
    except Exception as error:  # noqa: BLE001
        print(f"\nBuild falhou: {error}")
        sys.exit(1)
