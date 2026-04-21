#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod secret_key;
mod secrets;

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Mutex, OnceLock},
    thread,
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{path::BaseDirectory, AppHandle, Emitter, Manager, Runtime};
#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

const RUNTIME_FILES: &[(&str, &str)] = &[
    ("resources/runtime/app_auth.json", "app_auth.json"),
    ("resources/runtime/mirella_pacientes.sqlite3", "mirella_pacientes.sqlite3"),
    ("resources/runtime/mirella_kommo_leads.sqlite3", "mirella_kommo_leads.sqlite3"),
    (
        "resources/runtime/mappings/clinic_kommo_origin_mapping.csv",
        "mappings/clinic_kommo_origin_mapping.csv",
    ),
    (
        "resources/runtime/mappings/clinic_kommo_service_mapping.csv",
        "mappings/clinic_kommo_service_mapping.csv",
    ),
    ("resources/runtime/profiles/kommo_state.enc", "profiles/kommo_state.enc"),
    (
        "resources/runtime/exports/kommo/kommo_leads_latest.sql",
        "exports/kommo/kommo_leads_latest.sql",
    ),
    (
        "resources/runtime/exports/sync_preview/clinic_kommo_preview_summary.json",
        "exports/sync_preview/clinic_kommo_preview_summary.json",
    ),
    (
        "resources/runtime/exports/sync_preview/clinic_kommo_preview_summary.md",
        "exports/sync_preview/clinic_kommo_preview_summary.md",
    ),
    (
        "resources/runtime/exports/sync_preview/clinic_kommo_safe_payloads.json",
        "exports/sync_preview/clinic_kommo_safe_payloads.json",
    ),
    (
        "resources/runtime/exports/sync_preview/clinic_kommo_safe_rows.csv",
        "exports/sync_preview/clinic_kommo_safe_rows.csv",
    ),
    (
        "resources/runtime/exports/sync_preview/clinic_kommo_review_rows.csv",
        "exports/sync_preview/clinic_kommo_review_rows.csv",
    ),
    (
        "resources/runtime/exports/sync_preview/clinic_kommo_all_actions.csv",
        "exports/sync_preview/clinic_kommo_all_actions.csv",
    ),
];

#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Clone, Serialize)]
struct ProgressPayload {
    flow: String,
    task: String,
    step: String,
    status: String,
    message: String,
}

#[derive(Clone, Serialize)]
struct LogPayload {
    flow: String,
    step: String,
    stream: String,
    line: String,
    #[serde(rename = "tsMs")]
    ts_ms: u128,
}

#[derive(Clone)]
struct AuthSession {
    username: String,
    authenticated_at: u64,
}

#[derive(Clone, Deserialize)]
struct LocalAuthFile {
    auth: LocalAuthConfig,
}

#[derive(Clone, Deserialize)]
struct LocalAuthConfig {
    enabled: bool,
    gist_url: String,
    gist_file: Option<String>,
}

#[derive(Deserialize)]
struct GithubGistResponse {
    files: HashMap<String, GithubGistFile>,
}

#[derive(Deserialize)]
struct GithubGistFile {
    filename: Option<String>,
    content: Option<String>,
    raw_url: Option<String>,
    truncated: Option<bool>,
}

#[derive(Deserialize)]
struct GistAuthRoot {
    mirella_kommo_sync: GistAuthConfig,
}

#[derive(Deserialize)]
struct GistAuthConfig {
    enabled: bool,
    users: Vec<GistAuthUser>,
}

#[derive(Deserialize)]
struct GistAuthUser {
    username: String,
    password_sha256: Option<String>,
    password: Option<String>,
}

static AUTH_SESSION: OnceLock<Mutex<Option<AuthSession>>> = OnceLock::new();
static ACTIVE_CHILDREN: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();

fn active_children() -> &'static Mutex<HashSet<u32>> {
    ACTIVE_CHILDREN.get_or_init(|| Mutex::new(HashSet::new()))
}

fn register_child(pid: u32) {
    if let Ok(mut children) = active_children().lock() {
        children.insert(pid);
    }
}

fn unregister_child(pid: u32) {
    if let Ok(mut children) = active_children().lock() {
        children.remove(&pid);
    }
}

#[cfg(target_os = "windows")]
fn kill_process_tree(pid: u32) {
    let mut command = Command::new("taskkill");
    command
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command.creation_flags(CREATE_NO_WINDOW);
    let _ = command.status();
}

#[cfg(not(target_os = "windows"))]
fn kill_process_tree(pid: u32) {
    let _ = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn kill_active_children() {
    let pids: Vec<u32> = active_children()
        .lock()
        .map(|children| children.iter().copied().collect())
        .unwrap_or_default();
    for pid in pids {
        kill_process_tree(pid);
        unregister_child(pid);
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn auth_session() -> &'static Mutex<Option<AuthSession>> {
    AUTH_SESSION.get_or_init(|| Mutex::new(None))
}

fn auth_file_path<R: Runtime>(handle: &AppHandle<R>) -> Result<PathBuf, String> {
    let root = ensure_runtime_seeded(handle)?;
    if is_dev_mode() {
        let dev_config = root.join("config").join("app_auth.json");
        if dev_config.exists() {
            return Ok(dev_config);
        }
    }
    Ok(root.join("app_auth.json"))
}

fn read_local_auth_config<R: Runtime>(handle: &AppHandle<R>) -> Result<LocalAuthConfig, String> {
    let path = auth_file_path(handle)?;
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("Falha ao ler config de login {}: {}", path.display(), error))?;
    let config: LocalAuthFile = serde_json::from_str(&text)
        .map_err(|error| format!("Config de login invalida {}: {}", path.display(), error))?;
    Ok(config.auth)
}

fn extract_gist_id(input: &str) -> Option<String> {
    let mut best = String::new();
    let mut current = String::new();
    for ch in input.chars() {
        if ch.is_ascii_hexdigit() {
            current.push(ch);
            continue;
        }
        if current.len() > best.len() {
            best = current.clone();
        }
        current.clear();
    }
    if current.len() > best.len() {
        best = current;
    }
    if best.len() >= 20 {
        Some(best)
    } else {
        None
    }
}

fn fetch_text(client: &Client, url: &str) -> Result<String, String> {
    let response = client
        .get(url)
        .header("User-Agent", "Mirella-Kommo-Sync")
        .send()
        .map_err(|error| format!("Falha HTTP ao carregar config de login: {}", error))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("GitHub retornou HTTP {} ao carregar config de login.", status));
    }
    response
        .text()
        .map_err(|error| format!("Falha ao ler resposta do GitHub: {}", error))
}

fn fetch_gist_auth_config(config: &LocalAuthConfig) -> Result<GistAuthConfig, String> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .map_err(|error| format!("Falha ao preparar cliente HTTP: {}", error))?;

    let content = if config.gist_url.contains("gist.githubusercontent.com") {
        fetch_text(&client, &config.gist_url)?
    } else {
        let gist_id = extract_gist_id(&config.gist_url)
            .ok_or_else(|| "Nao consegui extrair o ID do Gist da config de login.".to_string())?;
        let api_url = format!("https://api.github.com/gists/{gist_id}");
        let text = fetch_text(&client, &api_url)?;
        let gist: GithubGistResponse = serde_json::from_str(&text)
            .map_err(|error| format!("Resposta do Gist invalida: {}", error))?;
        let selected = if let Some(file_name) = &config.gist_file {
            gist.files
                .get(file_name)
                .or_else(|| gist.files.values().find(|file| file.filename.as_deref() == Some(file_name)))
        } else {
            gist.files
                .values()
                .find(|file| file.filename.as_deref().map(|name| name.ends_with(".json")).unwrap_or(false))
        }
        .ok_or_else(|| "Nenhum arquivo JSON de login encontrado no Gist.".to_string())?;

        if selected.truncated.unwrap_or(false) {
            let raw_url = selected
                .raw_url
                .as_deref()
                .ok_or_else(|| "Arquivo do Gist truncado e sem raw_url.".to_string())?;
            fetch_text(&client, raw_url)?
        } else {
            selected
                .content
                .clone()
                .ok_or_else(|| "Arquivo de login do Gist esta vazio.".to_string())?
        }
    };

    let root: GistAuthRoot = serde_json::from_str(&content)
        .map_err(|error| format!("JSON de login do Gist invalido: {}", error))?;
    Ok(root.mirella_kommo_sync)
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn is_authenticated() -> Option<AuthSession> {
    auth_session().lock().ok()?.clone()
}

fn ensure_authenticated<R: Runtime>(handle: &AppHandle<R>) -> Result<(), String> {
    let config = read_local_auth_config(handle)?;
    if !config.enabled {
        return Ok(());
    }
    if is_authenticated().is_some() {
        Ok(())
    } else {
        Err("Login obrigatorio. Abra o app e autentique antes de continuar.".to_string())
    }
}

fn emit_log<R: Runtime>(
    handle: &AppHandle<R>,
    flow: &str,
    step: &str,
    stream: &str,
    line: &str,
) {
    let _ = handle.emit(
        "process-log",
        LogPayload {
            flow: flow.to_string(),
            step: step.to_string(),
            stream: stream.to_string(),
            line: line.to_string(),
            ts_ms: now_ms(),
        },
    );
}

fn dev_repo_root() -> Result<PathBuf, String> {
    let current = std::env::current_dir().map_err(|error| error.to_string())?;
    if current.file_name().and_then(|name| name.to_str()) == Some("src-tauri") {
        return current
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| "Could not resolve repository root".to_string());
    }
    Ok(current)
}

fn is_dev_mode() -> bool {
    cfg!(debug_assertions)
}

fn runtime_root<R: Runtime>(handle: &AppHandle<R>) -> Result<PathBuf, String> {
    if is_dev_mode() {
        return dev_repo_root();
    }
    let app_data = handle
        .path()
        .app_data_dir()
        .map_err(|error| error.to_string())?;
    fs::create_dir_all(&app_data).map_err(|error| error.to_string())?;
    Ok(app_data)
}

fn ensure_runtime_seeded<R: Runtime>(handle: &AppHandle<R>) -> Result<PathBuf, String> {
    let runtime_dir = runtime_root(handle)?;
    if is_dev_mode() {
        return Ok(runtime_dir);
    }

    for (resource_name, runtime_name) in RUNTIME_FILES {
        let target = runtime_dir.join(runtime_name);
        if target.exists() {
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let resource = handle
            .path()
            .resolve(resource_name, BaseDirectory::Resource)
            .map_err(|error| error.to_string())?;
        if !resource.exists() {
            continue;
        }
        fs::copy(&resource, &target).map_err(|error| error.to_string())?;
    }
    Ok(runtime_dir)
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

fn sidecar_path<R: Runtime>(handle: &AppHandle<R>, executable_name: &str) -> Result<PathBuf, String> {
    let resource = handle
        .path()
        .resolve(
            format!("resources/backend/{executable_name}.exe"),
            BaseDirectory::Resource,
        )
        .map_err(|error| error.to_string())?;
    Ok(resource)
}

fn emit_progress<R: Runtime>(
    handle: &AppHandle<R>,
    flow: &str,
    task: &str,
    step: &str,
    status: &str,
    message: &str,
) {
    let _ = handle.emit(
        "process-progress",
        ProgressPayload {
            flow: flow.to_string(),
            task: task.to_string(),
            step: step.to_string(),
            status: status.to_string(),
            message: message.to_string(),
        },
    );
}

fn run_backend_command<R: Runtime>(
    handle: &AppHandle<R>,
    executable_name: &str,
    script_name: &str,
    args: &[&str],
    flow: &str,
    step: &str,
) -> Result<String, String> {
    let runtime_dir = ensure_runtime_seeded(handle)?;
    if !runtime_dir.exists() {
        return Err(format!(
            "Diretorio de runtime nao encontrado: {}",
            runtime_dir.display()
        ));
    }
    let mut command = if is_dev_mode() {
        let mut cmd = Command::new("py");
        cmd.arg("-3").arg(script_name);
        cmd
    } else {
        let sidecar = sidecar_path(handle, executable_name)?;
        if !sidecar.exists() {
            return Err(format!("Backend nao encontrado: {}", sidecar.display()));
        }
        Command::new(sidecar)
    };

    command
        .args(args)
        .current_dir(&runtime_dir)
        .env("MIRELLA_RUNTIME_ROOT", &runtime_dir)
        .env("PYTHONUNBUFFERED", "1")
        .env("PYTHONIOENCODING", "utf-8")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if !is_dev_mode() {
        let secrets_map = secrets::load(handle)?;
        for (key, value) in secrets_map {
            command.env(key, value);
        }
    }

    #[cfg(target_os = "windows")]
    command.creation_flags(CREATE_NO_WINDOW);

    let mut child = command.spawn().map_err(|error| error.to_string())?;
    let child_pid = child.id();
    register_child(child_pid);
    let stdout_pipe = match child.stdout.take() {
        Some(pipe) => pipe,
        None => {
            unregister_child(child_pid);
            let _ = child.kill();
            return Err("Falha ao abrir stdout".to_string());
        }
    };
    let stderr_pipe = match child.stderr.take() {
        Some(pipe) => pipe,
        None => {
            unregister_child(child_pid);
            let _ = child.kill();
            return Err("Falha ao abrir stderr".to_string());
        }
    };

    let stdout_handle = handle.clone();
    let stdout_flow = flow.to_string();
    let stdout_step = step.to_string();
    let stdout_reader = thread::spawn(move || {
        let reader = BufReader::new(stdout_pipe);
        let mut collected = String::new();
        for line in reader.lines() {
            match line {
                Ok(text) => {
                    emit_log(&stdout_handle, &stdout_flow, &stdout_step, "stdout", &text);
                    collected.push_str(&text);
                    collected.push('\n');
                }
                Err(_) => break,
            }
        }
        collected
    });

    let stderr_handle = handle.clone();
    let stderr_flow = flow.to_string();
    let stderr_step = step.to_string();
    let stderr_reader = thread::spawn(move || {
        let reader = BufReader::new(stderr_pipe);
        let mut collected = String::new();
        for line in reader.lines() {
            match line {
                Ok(text) => {
                    emit_log(&stderr_handle, &stderr_flow, &stderr_step, "stderr", &text);
                    collected.push_str(&text);
                    collected.push('\n');
                }
                Err(_) => break,
            }
        }
        collected
    });

    let status = match child.wait() {
        Ok(status) => status,
        Err(error) => {
            unregister_child(child_pid);
            return Err(error.to_string());
        }
    };
    unregister_child(child_pid);
    let stdout_text = stdout_reader.join().unwrap_or_default();
    let stderr_text = stderr_reader.join().unwrap_or_default();

    if status.success() {
        Ok(stdout_text)
    } else {
        Err(if stderr_text.trim().is_empty() {
            stdout_text
        } else {
            stderr_text
        })
    }
}

#[tauri::command]
fn get_auth_state<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    let config = read_local_auth_config(&handle)?;
    let session = is_authenticated();
    Ok(json!({
        "required": config.enabled,
        "authenticated": !config.enabled || session.is_some(),
        "username": session.as_ref().map(|item| item.username.clone()),
        "authenticatedAt": session.as_ref().map(|item| item.authenticated_at),
        "gistConfigured": !config.gist_url.trim().is_empty(),
        "gistUrl": config.gist_url,
        "gistFile": config.gist_file,
    }))
}

#[tauri::command]
fn login_app<R: Runtime>(handle: AppHandle<R>, username: String, password: String) -> Result<Value, String> {
    let config = read_local_auth_config(&handle)?;
    if !config.enabled {
        return get_auth_state(handle);
    }

    let gist_config = fetch_gist_auth_config(&config)?;
    if !gist_config.enabled {
        return Err("Login desabilitado pela configuracao remota.".to_string());
    }

    let username_input = username.trim();
    let password_hash = sha256_hex(&password);
    let matched = gist_config.users.iter().any(|user| {
        user.username == username_input
            && (
                user.password_sha256
                    .as_ref()
                    .map(|hash| hash.eq_ignore_ascii_case(&password_hash))
                    .unwrap_or(false)
                || user.password.as_deref() == Some(password.as_str())
            )
    });

    if !matched {
        return Err("Usuario ou senha invalidos.".to_string());
    }

    *auth_session()
        .lock()
        .map_err(|_| "Falha ao registrar sessao autenticada.".to_string())? = Some(AuthSession {
        username: username_input.to_string(),
        authenticated_at: now_secs(),
    });
    get_auth_state(handle)
}

#[tauri::command]
fn logout_app<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    *auth_session()
        .lock()
        .map_err(|_| "Falha ao encerrar sessao autenticada.".to_string())? = None;
    get_auth_state(handle)
}

#[tauri::command]
fn get_dashboard_snapshot<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    ensure_authenticated(&handle)?;
    let root = ensure_runtime_seeded(&handle)?;
    let preview_dir = root.join("exports").join("sync_preview");
    let summary_path = preview_dir.join("clinic_kommo_preview_summary.json");
    let safe_payloads_path = preview_dir.join("clinic_kommo_safe_payloads.json");
    let safe_rows_path = preview_dir.join("clinic_kommo_safe_rows.csv");
    let review_rows_path = preview_dir.join("clinic_kommo_review_rows.csv");
    let all_actions_path = preview_dir.join("clinic_kommo_all_actions.csv");

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
            "kommoState": file_meta(&root.join("profiles").join("kommo_state.enc")),
            "patientDb": file_meta(&root.join("mirella_pacientes.sqlite3")),
            "kommoDb": file_meta(&root.join("mirella_kommo_leads.sqlite3")),
            "safePayloads": file_meta(&safe_payloads_path),
            "reviewRows": file_meta(&review_rows_path),
            "allActions": file_meta(&all_actions_path)
        }
    });

    Ok(snapshot)
}

#[tauri::command]
async fn run_preview<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    let worker_handle = handle.clone();
    tauri::async_runtime::spawn_blocking(move || {
        ensure_authenticated(&worker_handle)?;
        emit_progress(
            &worker_handle,
            "sync",
            "preview",
            "Gerar prévia",
            "started",
            "Gerando prévia atualizada...",
        );
        let stdout = run_backend_command(
            &worker_handle,
            "clinic_kommo_payload_preview",
            "clinic_kommo_payload_preview.py",
            &[],
            "sync",
            "Gerar prévia",
        )?;
        emit_progress(
            &worker_handle,
            "sync",
            "preview",
            "Gerar prévia",
            "completed",
            "Prévia gerada com sucesso.",
        );
        emit_progress(
            &worker_handle,
            "sync",
            "preview",
            "Gerar prévia",
            "done",
            "Prévia concluída.",
        );
        Ok(json!({
            "stdout": stdout,
            "snapshot": get_dashboard_snapshot(worker_handle)?
        }))
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn run_secret_check<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    ensure_authenticated(&handle)?;
    let stdout = run_backend_command(
        &handle,
        "sanity_check_secrets",
        "sanity_check_secrets.py",
        &[],
        "sync",
        "Verificar segredos",
    )?;
    Ok(json!({ "stdout": stdout }))
}

fn sync_steps(task: &str) -> Result<Vec<(&'static str, &'static str, &'static str, Vec<&'static str>)>, String> {
    let steps = match task {
        "quick" => vec![
            ("Atualizar Clínica", "login", "login.py", vec!["--sem-input"]),
            (
                "Extrair campos operacionais",
                "clinic_operational_fields_sync",
                "clinic_operational_fields_sync.py",
                vec!["--patient-scope", "matched", "--workers", "4"],
            ),
            (
                "Atualizar Kommo",
                "kommo_leads_sqlite",
                "kommo_leads_sqlite.py",
                vec!["--sync-mode", "incremental"],
            ),
            (
                "Gerar prévia",
                "clinic_kommo_payload_preview",
                "clinic_kommo_payload_preview.py",
                vec![],
            ),
        ],
        "full" => vec![
            ("Atualizar Clínica", "login", "login.py", vec!["--sem-input", "--reprocessar-pacientes"]),
            (
                "Extrair campos operacionais",
                "clinic_operational_fields_sync",
                "clinic_operational_fields_sync.py",
                vec!["--patient-scope", "all", "--workers", "4"],
            ),
            (
                "Atualizar Kommo",
                "kommo_leads_sqlite",
                "kommo_leads_sqlite.py",
                vec!["--sync-mode", "full"],
            ),
            (
                "Gerar prévia",
                "clinic_kommo_payload_preview",
                "clinic_kommo_payload_preview.py",
                vec![],
            ),
        ],
        "clinic" => vec![("Atualizar Clínica", "login", "login.py", vec!["--sem-input", "--reprocessar-pacientes"])],
        "operational" => vec![(
            "Extrair campos operacionais",
            "clinic_operational_fields_sync",
            "clinic_operational_fields_sync.py",
            vec!["--patient-scope", "matched", "--workers", "4"],
        )],
        "kommo" => vec![("Atualizar Kommo", "kommo_leads_sqlite", "kommo_leads_sqlite.py", vec!["--sync-mode", "incremental"])],
        "preview" => vec![("Gerar prévia", "clinic_kommo_payload_preview", "clinic_kommo_payload_preview.py", vec![])],
        _ => return Err(format!("Unknown sync task: {task}")),
    };
    Ok(steps)
}

#[tauri::command]
async fn run_sync_task<R: Runtime>(handle: AppHandle<R>, task: String) -> Result<Value, String> {
    let worker_handle = handle.clone();
    tauri::async_runtime::spawn_blocking(move || {
        ensure_authenticated(&worker_handle)?;
        let mut logs: Vec<Value> = Vec::new();
        let steps = sync_steps(&task)?;
        for (label, executable_name, script_name, args) in steps {
            emit_progress(&worker_handle, "sync", &task, label, "started", &format!("{label}..."));
            match run_backend_command(&worker_handle, executable_name, script_name, &args, "sync", label) {
                Ok(stdout) => {
                    logs.push(json!({
                        "label": label,
                        "script": script_name,
                        "stdout": stdout
                    }));
                    emit_progress(&worker_handle, "sync", &task, label, "completed", &format!("{label} concluído."));
                }
                Err(error) => {
                    emit_progress(&worker_handle, "sync", &task, label, "failed", &error);
                    return Err(error);
                }
            }
        }
        emit_progress(
            &worker_handle,
            "sync",
            &task,
            "Fluxo finalizado",
            "done",
            "Atualização finalizada com sucesso.",
        );
        Ok(json!({
            "task": task,
            "logs": logs,
            "snapshot": get_dashboard_snapshot(worker_handle)?
        }))
    })
    .await
    .map_err(|error| error.to_string())?
}

#[tauri::command]
fn read_mapping<R: Runtime>(handle: AppHandle<R>, kind: String) -> Result<String, String> {
    ensure_authenticated(&handle)?;
    let root = ensure_runtime_seeded(&handle)?;
    let file_name = match kind.as_str() {
        "origin" => "clinic_kommo_origin_mapping.csv",
        "service" => "clinic_kommo_service_mapping.csv",
        _ => return Err("Unknown mapping kind".to_string()),
    };
    fs::read_to_string(root.join("mappings").join(file_name)).map_err(|error| error.to_string())
}

#[tauri::command]
fn read_review_rows<R: Runtime>(handle: AppHandle<R>) -> Result<String, String> {
    ensure_authenticated(&handle)?;
    let root = ensure_runtime_seeded(&handle)?;
    fs::read_to_string(
        root.join("exports")
            .join("sync_preview")
            .join("clinic_kommo_review_rows.csv"),
    )
    .map_err(|error| error.to_string())
}

#[tauri::command]
fn read_apply_results<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    ensure_authenticated(&handle)?;
    let root = ensure_runtime_seeded(&handle)?;
    let dir = root.join("exports").join("apply_safe");
    if !dir.exists() {
        return Ok(json!({ "runId": null, "modifiedUnix": null, "items": [] }));
    }
    let mut latest: Option<(PathBuf, SystemTime)> = None;
    let entries = fs::read_dir(&dir).map_err(|error| error.to_string())?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.ends_with("_result.json"))
            .unwrap_or(false)
        {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .unwrap_or(UNIX_EPOCH);
        if latest.as_ref().map_or(true, |(_, best)| modified > *best) {
            latest = Some((path, modified));
        }
    }
    let Some((path, modified)) = latest else {
        return Ok(json!({ "runId": null, "modifiedUnix": null, "items": [] }));
    };
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let items: Value = serde_json::from_str(&contents).map_err(|error| error.to_string())?;
    let run_id = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(|stem| stem.trim_end_matches("_result").to_string());
    let modified_unix = modified
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    Ok(json!({
        "runId": run_id,
        "modifiedUnix": modified_unix,
        "items": items,
    }))
}

#[tauri::command]
async fn apply_safe_payloads<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    let worker_handle = handle.clone();
    tauri::async_runtime::spawn_blocking(move || {
        ensure_authenticated(&worker_handle)?;
        let steps = [
            (
                "Aplicar no Kommo",
                "apply_kommo_safe_payloads",
                "apply_kommo_safe_payloads.py",
                vec!["--apply"],
            ),
            (
                "Atualizar espelho Kommo",
                "kommo_leads_sqlite",
                "kommo_leads_sqlite.py",
                vec!["--sync-mode", "full"],
            ),
            (
                "Gerar nova prévia",
                "clinic_kommo_payload_preview",
                "clinic_kommo_payload_preview.py",
                vec![],
            ),
        ];
        let mut logs: Vec<Value> = Vec::new();
        for (label, executable_name, script_name, args) in steps {
            emit_progress(&worker_handle, "apply", "apply", label, "started", &format!("{label}..."));
            match run_backend_command(&worker_handle, executable_name, script_name, &args, "apply", label) {
                Ok(stdout) => {
                    logs.push(json!({ "label": label, "stdout": stdout }));
                    emit_progress(&worker_handle, "apply", "apply", label, "completed", &format!("{label} concluído."));
                }
                Err(error) => {
                    emit_progress(&worker_handle, "apply", "apply", label, "failed", &error);
                    return Err(error);
                }
            }
        }
        emit_progress(
            &worker_handle,
            "apply",
            "apply",
            "Fluxo finalizado",
            "done",
            "Aplicação concluída com sucesso.",
        );
        Ok(json!({
            "logs": logs,
            "snapshot": get_dashboard_snapshot(worker_handle)?
        }))
    })
    .await
    .map_err(|error| error.to_string())?
}

fn main() {
    tauri::Builder::default()
        .on_window_event(|_window, event| {
            if matches!(event, tauri::WindowEvent::CloseRequested { .. }) {
                kill_active_children();
            }
        })
        .invoke_handler(tauri::generate_handler![
            get_auth_state,
            login_app,
            logout_app,
            get_dashboard_snapshot,
            run_preview,
            run_secret_check,
            run_sync_task,
            read_mapping,
            read_review_rows,
            read_apply_results,
            apply_safe_payloads
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
