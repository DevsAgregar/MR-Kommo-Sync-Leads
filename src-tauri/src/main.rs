#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn repo_root() -> Result<PathBuf, String> {
    let current = std::env::current_dir().map_err(|error| error.to_string())?;
    if current.file_name().and_then(|name| name.to_str()) == Some("src-tauri") {
        return current
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| "Could not resolve repository root".to_string());
    }
    Ok(current)
}

fn read_json_if_exists(path: &Path) -> Option<Value> {
    let text = fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn count_csv_rows(path: &Path) -> usize {
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    text.lines().skip(1).filter(|line| !line.trim().is_empty()).count()
}

fn read_mapping_count(path: &Path) -> usize {
    count_csv_rows(path)
}

fn file_meta(path: &Path) -> Value {
    match fs::metadata(path) {
        Ok(metadata) => {
            let modified_unix = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs());
            json!({
                "exists": true,
                "bytes": metadata.len(),
                "modifiedUnix": modified_unix
            })
        }
        Err(_) => json!({
            "exists": false,
            "bytes": 0,
            "modifiedUnix": null
        }),
    }
}

#[tauri::command]
fn get_dashboard_snapshot() -> Result<Value, String> {
    let root = repo_root()?;
    let preview_dir = root.join("exports").join("sync_preview");
    let summary_path = preview_dir.join("clinic_kommo_preview_summary.json");
    let safe_payloads_path = preview_dir.join("clinic_kommo_safe_payloads.json");
    let safe_rows_path = preview_dir.join("clinic_kommo_safe_rows.csv");
    let review_rows_path = preview_dir.join("clinic_kommo_review_rows.csv");

    let preview_summary = read_json_if_exists(&summary_path);
    let safe_payload_count = read_json_if_exists(&safe_payloads_path)
        .and_then(|value| value.as_array().map(|items| items.len()))
        .unwrap_or(0);

    let snapshot = json!({
        "repoRoot": root,
        "previewSummary": preview_summary,
        "safePayloadCount": safe_payload_count,
        "safeRowsCount": count_csv_rows(&safe_rows_path),
        "reviewRowsCount": count_csv_rows(&review_rows_path),
        "mappings": {
            "originRows": read_mapping_count(&root.join("mappings").join("clinic_kommo_origin_mapping.csv")),
            "serviceRows": read_mapping_count(&root.join("mappings").join("clinic_kommo_service_mapping.csv"))
        },
        "localFiles": {
            "env": file_meta(&root.join(".env")),
            "kommoState": file_meta(&root.join("profiles").join("kommo_state.json")),
            "patientDb": file_meta(&root.join("mirella_pacientes.sqlite3")),
            "kommoDb": file_meta(&root.join("mirella_kommo_leads.sqlite3")),
            "safePayloads": file_meta(&safe_payloads_path),
            "reviewRows": file_meta(&review_rows_path)
        }
    });

    Ok(snapshot)
}

fn run_python_command(script: &str, args: &[&str]) -> Result<String, String> {
    let root = repo_root()?;
    let mut command = Command::new("py");
    command.arg("-3").arg(script).args(args).current_dir(root);
    let output = command
        .output()
        .map_err(|error| error.to_string())?;

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if output.status.success() {
        Ok(stdout)
    } else {
        Err(if stderr.trim().is_empty() { stdout } else { stderr })
    }
}

#[tauri::command]
fn run_preview() -> Result<Value, String> {
    let stdout = run_python_command("clinic_kommo_payload_preview.py", &[])?;
    Ok(json!({
        "stdout": stdout,
        "snapshot": get_dashboard_snapshot()?
    }))
}

#[tauri::command]
fn run_secret_check() -> Result<Value, String> {
    let stdout = run_python_command("sanity_check_secrets.py", &[])?;
    Ok(json!({ "stdout": stdout }))
}

fn sync_steps(task: &str) -> Result<Vec<(&'static str, &'static str, Vec<&'static str>)>, String> {
    let steps = match task {
        "clinic" => vec![("Atualizar Clínica", "login.py", vec!["--sem-input", "--reprocessar-pacientes"])],
        "operational" => vec![("Extrair campos operacionais", "clinic_operational_fields_sync.py", vec![])],
        "kommo" => vec![("Atualizar Kommo", "kommo_leads_sqlite.py", vec![])],
        "preview" => vec![("Gerar prévia", "clinic_kommo_payload_preview.py", vec![])],
        "all" => vec![
            ("Atualizar Clínica", "login.py", vec!["--sem-input", "--reprocessar-pacientes"]),
            ("Extrair campos operacionais", "clinic_operational_fields_sync.py", vec![]),
            ("Atualizar Kommo", "kommo_leads_sqlite.py", vec![]),
            ("Gerar prévia", "clinic_kommo_payload_preview.py", vec![]),
        ],
        _ => return Err(format!("Unknown sync task: {task}")),
    };
    Ok(steps)
}

#[tauri::command]
fn run_sync_task(task: String) -> Result<Value, String> {
    let mut logs: Vec<Value> = Vec::new();
    for (label, script, args) in sync_steps(&task)? {
        let stdout = run_python_command(script, &args)?;
        logs.push(json!({
            "label": label,
            "script": script,
            "stdout": stdout
        }));
    }
    Ok(json!({
        "task": task,
        "logs": logs,
        "snapshot": get_dashboard_snapshot()?
    }))
}

#[tauri::command]
fn read_mapping(kind: String) -> Result<String, String> {
    let root = repo_root()?;
    let file_name = match kind.as_str() {
        "origin" => "clinic_kommo_origin_mapping.csv",
        "service" => "clinic_kommo_service_mapping.csv",
        _ => return Err("Unknown mapping kind".to_string()),
    };
    fs::read_to_string(root.join("mappings").join(file_name)).map_err(|error| error.to_string())
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            get_dashboard_snapshot,
            run_preview,
            run_secret_check,
            run_sync_task,
            read_mapping
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
