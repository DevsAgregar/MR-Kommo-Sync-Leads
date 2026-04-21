use std::collections::HashMap;
use std::fs;
use std::sync::OnceLock;

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use tauri::{path::BaseDirectory, AppHandle, Manager, Runtime};

use crate::secret_key;

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

static CACHE: OnceLock<HashMap<String, String>> = OnceLock::new();

pub(crate) fn load<R: Runtime>(
    handle: &AppHandle<R>,
) -> Result<&'static HashMap<String, String>, String> {
    if let Some(cache) = CACHE.get() {
        return Ok(cache);
    }

    let path = handle
        .path()
        .resolve("resources/runtime/secrets.enc", BaseDirectory::Resource)
        .map_err(|error| format!("Nao foi possivel localizar secrets.enc: {error}"))?;
    let blob = fs::read(&path)
        .map_err(|error| format!("Falha ao ler {}: {error}", path.display()))?;
    if blob.len() < NONCE_LEN + TAG_LEN {
        return Err("Arquivo de segredos invalido ou truncado.".into());
    }

    let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
    let key_bytes = secret_key::recover();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_bytes));
    let nonce = Nonce::from_slice(nonce_bytes);
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| "Falha ao decifrar segredos.".to_string())?;

    let text = String::from_utf8(plaintext)
        .map_err(|error| format!("Segredos decifrados nao sao UTF-8 validos: {error}"))?;

    let map = parse_env(&text);
    let _ = CACHE.set(map);
    Ok(CACHE.get().expect("cache just populated"))
}

fn parse_env(text: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let value = unquote(value.trim());
        map.insert(key.to_string(), value.to_string());
    }
    map
}

fn unquote(value: &str) -> &str {
    let bytes = value.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &value[1..value.len() - 1];
        }
    }
    value
}
