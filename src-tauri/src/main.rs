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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, OnceLock,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tauri::{
    menu::{MenuBuilder, MenuItemBuilder},
    path::BaseDirectory,
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    AppHandle, Emitter, Manager, Runtime, WindowEvent,
};
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
static SCHEDULER: OnceLock<Arc<(Mutex<SchedulerRuntime>, Condvar)>> = OnceLock::new();
static PIPELINE_BUSY: OnceLock<AtomicBool> = OnceLock::new();

fn pipeline_busy_flag() -> &'static AtomicBool {
    PIPELINE_BUSY.get_or_init(|| AtomicBool::new(false))
}

fn try_acquire_pipeline() -> bool {
    pipeline_busy_flag()
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
}

fn release_pipeline() {
    pipeline_busy_flag().store(false, Ordering::Release);
}

struct PipelineGuard;

impl Drop for PipelineGuard {
    fn drop(&mut self) {
        release_pipeline();
    }
}

fn acquire_pipeline_guard() -> Result<PipelineGuard, String> {
    if try_acquire_pipeline() {
        Ok(PipelineGuard)
    } else {
        Err("Outra atualização já está em andamento. Aguarde ela terminar.".to_string())
    }
}

const SCHEDULER_MIN_INTERVAL: u64 = 30;
const SCHEDULER_MAX_INTERVAL: u64 = 240;
const SCHEDULER_DEFAULT_INTERVAL: u64 = 60;

#[derive(Clone)]
struct SchedulerRuntime {
    enabled: bool,
    interval_minutes: u64,
    next_run_unix: Option<u64>,
    last_run_unix: Option<u64>,
    last_status: SchedulerStatus,
    last_error: Option<String>,
    running: bool,
    paused_reason: Option<String>,
    force_trigger: bool,
    shutdown: bool,
}

impl SchedulerRuntime {
    fn initial() -> Self {
        Self {
            enabled: false,
            interval_minutes: SCHEDULER_DEFAULT_INTERVAL,
            next_run_unix: None,
            last_run_unix: None,
            last_status: SchedulerStatus::Idle,
            last_error: None,
            running: false,
            paused_reason: None,
            force_trigger: false,
            shutdown: false,
        }
    }
}

#[derive(Clone, Copy)]
enum SchedulerStatus {
    Idle,
    Ok,
    Error,
}

impl SchedulerStatus {
    fn as_str(self) -> &'static str {
        match self {
            SchedulerStatus::Idle => "idle",
            SchedulerStatus::Ok => "ok",
            SchedulerStatus::Error => "error",
        }
    }
}

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

fn scheduler() -> &'static Arc<(Mutex<SchedulerRuntime>, Condvar)> {
    SCHEDULER.get_or_init(|| Arc::new((Mutex::new(SchedulerRuntime::initial()), Condvar::new())))
}

fn clamp_interval(minutes: u64) -> u64 {
    minutes
        .max(SCHEDULER_MIN_INTERVAL)
        .min(SCHEDULER_MAX_INTERVAL)
}

fn scheduler_snapshot() -> Value {
    let (lock, _) = &**scheduler();
    let state = match lock.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    json!({
        "enabled": state.enabled,
        "intervalMinutes": state.interval_minutes,
        "nextRunUnix": state.next_run_unix,
        "lastRunUnix": state.last_run_unix,
        "lastStatus": state.last_status.as_str(),
        "lastError": state.last_error.clone(),
        "running": state.running,
        "pausedReason": state.paused_reason.clone(),
    })
}

fn emit_scheduler_state<R: Runtime>(handle: &AppHandle<R>) {
    let _ = handle.emit("scheduler-state", scheduler_snapshot());
}

fn update_scheduler<F>(mutate: F)
where
    F: FnOnce(&mut SchedulerRuntime),
{
    let (lock, cvar) = &**scheduler();
    let mut state = match lock.lock() {
        Ok(state) => state,
        Err(poisoned) => poisoned.into_inner(),
    };
    mutate(&mut state);
    cvar.notify_all();
}

fn scheduler_flow_steps() -> Vec<(&'static str, &'static str, &'static str, &'static str, Vec<&'static str>)>
{
    vec![
        ("sync", "Atualizar Clínica", "login", "login.py", vec!["--sem-input"]),
        (
            "sync",
            "Extrair campos operacionais",
            "clinic_operational_fields_sync",
            "clinic_operational_fields_sync.py",
            vec!["--patient-scope", "matched", "--workers", "4"],
        ),
        (
            "sync",
            "Atualizar Kommo",
            "kommo_leads_sqlite",
            "kommo_leads_sqlite.py",
            vec!["--sync-mode", "incremental"],
        ),
        (
            "sync",
            "Gerar prévia",
            "clinic_kommo_payload_preview",
            "clinic_kommo_payload_preview.py",
            vec![],
        ),
        (
            "apply",
            "Aplicar no Kommo",
            "apply_kommo_safe_payloads",
            "apply_kommo_safe_payloads.py",
            vec!["--apply"],
        ),
        (
            "apply",
            "Atualizar espelho Kommo",
            "kommo_leads_sqlite",
            "kommo_leads_sqlite.py",
            vec!["--sync-mode", "full"],
        ),
        (
            "apply",
            "Gerar nova prévia",
            "clinic_kommo_payload_preview",
            "clinic_kommo_payload_preview.py",
            vec![],
        ),
    ]
}

fn run_scheduler_once<R: Runtime>(handle: &AppHandle<R>) -> Result<(), String> {
    ensure_authenticated(handle)?;
    let _guard = acquire_pipeline_guard()?;
    let steps = scheduler_flow_steps();
    let mut current_flow = "";
    let mut current_task = "";
    for (flow, label, executable_name, script_name, args) in steps {
        let task = if flow == "apply" { "apply" } else { "quick" };
        if flow != current_flow {
            current_flow = flow;
            current_task = task;
        } else if task != current_task {
            current_task = task;
        }
        emit_progress(handle, flow, task, label, "started", &format!("{label}..."));
        match run_backend_command(handle, executable_name, script_name, &args, flow, label) {
            Ok(_) => {
                emit_progress(
                    handle,
                    flow,
                    task,
                    label,
                    "completed",
                    &format!("{label} concluído."),
                );
            }
            Err(error) => {
                emit_progress(handle, flow, task, label, "failed", &error);
                return Err(error);
            }
        }
    }
    emit_progress(
        handle,
        "apply",
        "apply",
        "Fluxo finalizado",
        "done",
        "Automação concluída com sucesso.",
    );
    Ok(())
}

fn spawn_scheduler_worker<R: Runtime>(handle: AppHandle<R>) {
    thread::spawn(move || {
        let sched = scheduler().clone();
        loop {
            let (should_run, shutdown) = {
                let (lock, cvar) = &*sched;
                let mut state = match lock.lock() {
                    Ok(state) => state,
                    Err(poisoned) => poisoned.into_inner(),
                };
                loop {
                    if state.shutdown {
                        break (false, true);
                    }
                    if state.force_trigger && !state.running {
                        state.force_trigger = false;
                        break (true, false);
                    }
                    if !state.enabled || state.running {
                        state = match cvar.wait(state) {
                            Ok(state) => state,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        continue;
                    }
                    let now = now_secs();
                    let next = state.next_run_unix.unwrap_or(now);
                    if next <= now {
                        break (true, false);
                    }
                    let wait = Duration::from_secs(next - now);
                    let (next_state, _timeout) = match cvar.wait_timeout(state, wait) {
                        Ok(result) => result,
                        Err(poisoned) => {
                            let result = poisoned.into_inner();
                            (result.0, result.1)
                        }
                    };
                    state = next_state;
                }
            };

            if shutdown {
                break;
            }
            if !should_run {
                continue;
            }

            update_scheduler(|state| {
                state.running = true;
                state.last_error = None;
                state.last_status = SchedulerStatus::Idle;
            });
            emit_scheduler_state(&handle);

            let result = run_scheduler_once(&handle);
            let finished_at = now_secs();
            update_scheduler(|state| {
                state.running = false;
                state.last_run_unix = Some(finished_at);
                match &result {
                    Ok(()) => {
                        state.last_status = SchedulerStatus::Ok;
                        state.last_error = None;
                        let interval = clamp_interval(state.interval_minutes);
                        state.next_run_unix = Some(finished_at + interval * 60);
                    }
                    Err(error) => {
                        state.last_status = SchedulerStatus::Error;
                        state.last_error = Some(error.clone());
                        state.enabled = false;
                        state.paused_reason = Some(error.clone());
                        state.next_run_unix = None;
                    }
                }
            });
            emit_scheduler_state(&handle);
            if let Err(error) = &result {
                let _ = handle.emit("scheduler-error", error.clone());
            }
        }
    });
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
    let password_input = password.trim();
    let password_hash = sha256_hex(password_input);
    let matched = gist_config.users.iter().any(|user| {
        user.username == username_input
            && (
                user.password_sha256
                    .as_ref()
                    .map(|hash| hash.eq_ignore_ascii_case(&password_hash))
                    .unwrap_or(false)
                || user.password.as_deref() == Some(password_input)
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
    update_scheduler(|state| {
        if state.enabled {
            state.enabled = false;
            state.paused_reason = Some("Sessão encerrada.".to_string());
            state.next_run_unix = None;
        }
    });
    emit_scheduler_state(&handle);
    get_auth_state(handle)
}

#[tauri::command]
fn get_scheduler_state_cmd<R: Runtime>(_handle: AppHandle<R>) -> Result<Value, String> {
    Ok(scheduler_snapshot())
}

#[tauri::command]
fn set_scheduler_config<R: Runtime>(
    handle: AppHandle<R>,
    enabled: bool,
    interval_minutes: u64,
) -> Result<Value, String> {
    if enabled {
        ensure_authenticated(&handle)?;
    }
    let interval = clamp_interval(interval_minutes);
    update_scheduler(|state| {
        let was_enabled = state.enabled;
        state.interval_minutes = interval;
        state.enabled = enabled;
        if enabled {
            state.paused_reason = None;
            if !was_enabled || state.next_run_unix.is_none() {
                state.next_run_unix = Some(now_secs() + interval * 60);
            }
        } else {
            state.next_run_unix = None;
        }
    });
    emit_scheduler_state(&handle);
    Ok(scheduler_snapshot())
}

#[tauri::command]
fn scheduler_run_now<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    ensure_authenticated(&handle)?;
    if pipeline_busy_flag().load(Ordering::Acquire) {
        return Err("Outra atualização já está em andamento. Aguarde ela terminar.".to_string());
    }
    update_scheduler(|state| {
        state.force_trigger = true;
        state.paused_reason = None;
    });
    emit_scheduler_state(&handle);
    Ok(scheduler_snapshot())
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
        let _guard = acquire_pipeline_guard()?;
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
fn read_apply_history<R: Runtime>(
    handle: AppHandle<R>,
    limit: Option<usize>,
) -> Result<Value, String> {
    ensure_authenticated(&handle)?;
    let root = ensure_runtime_seeded(&handle)?;
    let dir = root.join("exports").join("apply_safe");
    if !dir.exists() {
        return Ok(json!({ "runs": [] }));
    }
    let limit = limit.unwrap_or(50).min(200);
    let entries = fs::read_dir(&dir).map_err(|error| error.to_string())?;
    let mut collected: Vec<(String, u64, Value)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let file_name = match path.file_name().and_then(|name| name.to_str()) {
            Some(name) if name.ends_with("_result.json") => name.to_string(),
            _ => continue,
        };
        let modified_unix = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        let contents = match fs::read_to_string(&path) {
            Ok(text) => text,
            Err(_) => continue,
        };
        let items: Value = match serde_json::from_str(&contents) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let run_id = file_name.trim_end_matches("_result.json").to_string();
        collected.push((run_id, modified_unix, items));
    }
    collected.sort_by(|a, b| b.1.cmp(&a.1));
    let runs: Vec<Value> = collected
        .into_iter()
        .take(limit)
        .map(|(run_id, modified_unix, items)| {
            let array = items.as_array().cloned().unwrap_or_default();
            let ok_count = array
                .iter()
                .filter(|value| value.get("ok").and_then(Value::as_bool).unwrap_or(false))
                .count();
            let total = array.len();
            let err_count = total.saturating_sub(ok_count);
            let mapped: Vec<Value> = array
                .into_iter()
                .map(|value| {
                    json!({
                        "id": value.get("id").cloned().unwrap_or(Value::Null),
                        "leadName": value.get("lead_name").cloned().unwrap_or(Value::Null),
                        "ok": value.get("ok").and_then(Value::as_bool).unwrap_or(false),
                        "error": value.get("error").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect();
            json!({
                "runId": run_id,
                "modifiedUnix": modified_unix,
                "okCount": ok_count,
                "errCount": err_count,
                "total": total,
                "items": mapped,
            })
        })
        .collect();
    Ok(json!({ "runs": runs }))
}

#[tauri::command]
fn read_safe_payload_preview<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    ensure_authenticated(&handle)?;
    let root = ensure_runtime_seeded(&handle)?;
    let path = root
        .join("exports")
        .join("sync_preview")
        .join("clinic_kommo_safe_payloads.json");
    if !path.exists() {
        return Ok(json!({ "items": [] }));
    }
    let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
    let payloads: Value = serde_json::from_str(&contents).map_err(|error| error.to_string())?;
    let items = payloads
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let field_count = row
                        .get("custom_fields_values")
                        .and_then(|value| value.as_array())
                        .map(|values| values.len())
                        .unwrap_or(0)
                        + usize::from(row.get("price").is_some());
                    json!({
                        "id": row.get("id").cloned().unwrap_or(Value::Null),
                        "lead_name": row.get("lead_name").cloned().unwrap_or(Value::Null),
                        "field_count": field_count,
                        "has_price": row.get("price").is_some(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(json!({ "items": items }))
}

#[tauri::command]
async fn apply_safe_payloads<R: Runtime>(handle: AppHandle<R>) -> Result<Value, String> {
    let worker_handle = handle.clone();
    tauri::async_runtime::spawn_blocking(move || {
        ensure_authenticated(&worker_handle)?;
        let _guard = acquire_pipeline_guard()?;
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

fn show_main_window<R: Runtime>(app: &AppHandle<R>) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.show();
        let _ = window.unminimize();
        let _ = window.set_focus();
    }
}

fn quit_application<R: Runtime>(app: &AppHandle<R>) {
    update_scheduler(|state| {
        state.enabled = false;
        state.shutdown = true;
        state.next_run_unix = None;
    });
    kill_active_children();
    app.exit(0);
}

fn main() {
    tauri::Builder::default()
        .setup(|app| {
            let handle = app.handle().clone();

            let open_item = MenuItemBuilder::with_id("tray-open", "Abrir").build(app)?;
            let toggle_item =
                MenuItemBuilder::with_id("tray-toggle", "Ativar/pausar automação").build(app)?;
            let run_now_item =
                MenuItemBuilder::with_id("tray-run-now", "Executar agora").build(app)?;
            let quit_item = MenuItemBuilder::with_id("tray-quit", "Sair").build(app)?;
            let menu = MenuBuilder::new(app)
                .items(&[&open_item, &toggle_item, &run_now_item, &quit_item])
                .build()?;

            let mut tray_builder = TrayIconBuilder::with_id("main-tray")
                .tooltip("Mirella Kommo Sync")
                .menu(&menu);
            if let Some(icon) = app.default_window_icon().cloned() {
                tray_builder = tray_builder.icon(icon);
            }
            tray_builder
                .on_menu_event({
                    let handle = handle.clone();
                    move |app, event| match event.id.as_ref() {
                        "tray-open" => show_main_window(app),
                        "tray-toggle" => {
                            update_scheduler(|state| {
                                if state.enabled {
                                    state.enabled = false;
                                    state.paused_reason =
                                        Some("Pausado manualmente pelo tray.".to_string());
                                    state.next_run_unix = None;
                                } else if is_authenticated().is_some() {
                                    state.enabled = true;
                                    state.paused_reason = None;
                                    let interval = clamp_interval(state.interval_minutes);
                                    state.next_run_unix = Some(now_secs() + interval * 60);
                                } else {
                                    state.paused_reason = Some(
                                        "Abra o app e faça login antes de retomar.".to_string(),
                                    );
                                }
                            });
                            emit_scheduler_state(&handle);
                        }
                        "tray-run-now" => {
                            if is_authenticated().is_none() {
                                show_main_window(app);
                                return;
                            }
                            if pipeline_busy_flag().load(Ordering::Acquire) {
                                return;
                            }
                            update_scheduler(|state| {
                                state.force_trigger = true;
                                state.paused_reason = None;
                            });
                            emit_scheduler_state(&handle);
                        }
                        "tray-quit" => quit_application(app),
                        _ => {}
                    }
                })
                .on_tray_icon_event({
                    move |tray, event| {
                        if let TrayIconEvent::Click {
                            button: MouseButton::Left,
                            button_state: MouseButtonState::Up,
                            ..
                        } = event
                        {
                            show_main_window(tray.app_handle());
                        }
                    }
                })
                .build(app)?;

            spawn_scheduler_worker(handle);
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                if window.label() == "main" {
                    api.prevent_close();
                    let _ = window.hide();
                }
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
            read_apply_history,
            read_safe_payload_preview,
            apply_safe_payloads,
            get_scheduler_state_cmd,
            set_scheduler_config,
            scheduler_run_now,
            quit_app
        ])
        .build(tauri::generate_context!())
        .expect("error while running tauri application")
        .run(|_app_handle, event| {
            if let tauri::RunEvent::ExitRequested { .. } = event {
                update_scheduler(|state| {
                    state.shutdown = true;
                    state.enabled = false;
                });
                kill_active_children();
            }
        });
}

#[tauri::command]
fn quit_app<R: Runtime>(handle: AppHandle<R>) {
    quit_application(&handle);
}
