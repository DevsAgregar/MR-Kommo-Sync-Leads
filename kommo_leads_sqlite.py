from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
from db_util import connect as db_connect, sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import requests
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright
from env_config import load_env_file
import state_util


DEFAULT_SUBDOMAIN = "mirellarabelo"
DEFAULT_BASE_URL = f"https://{DEFAULT_SUBDOMAIN}.kommo.com"
DEFAULT_LOGIN_PIPELINE_ID = "9715568"
DEFAULT_DB_PATH = Path("mirella_kommo_leads.sqlite3")
DEFAULT_OUTPUT_DIR = Path("exports") / "kommo"
DEFAULT_STATE_PATH = Path("profiles") / "kommo_state.enc"
DEFAULT_TIMEOUT_MS = 60_000
DEFAULT_SYNC_MODE = "full"
DEFAULT_INCREMENTAL_LOOKBACK_SECONDS = 86_400


class KommoAuthError(RuntimeError):
    pass

def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _row_hash(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _slug(text: Optional[str], fallback: str) -> str:
    raw = (text or fallback).strip().lower()
    raw = raw.encode("ascii", "ignore").decode("ascii")
    raw = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    return raw or fallback


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    return "'" + str(value).replace("'", "''") + "'"


def _iso_from_timestamp(value: Any) -> Optional[str]:
    if value in (None, "", 0):
        return None
    try:
        return datetime.fromtimestamp(int(value)).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError):
        return None


def _extract_display_values(values: Any) -> Tuple[Optional[str], bool]:
    if not isinstance(values, list) or not values:
        return None, False

    rendered: List[str] = []
    filled = False
    for item in values:
        if not isinstance(item, dict):
            if item not in (None, ""):
                rendered.append(str(item))
                filled = True
            continue

        if "value" in item:
            value = item.get("value")
            if value not in (None, ""):
                rendered.append(str(value))
                filled = True
                continue

        enum_code = item.get("enum_code")
        enum_id = item.get("enum_id")
        if enum_code not in (None, ""):
            rendered.append(str(enum_code))
            filled = True
        elif enum_id not in (None, ""):
            rendered.append(str(enum_id))
            filled = True

    text = "; ".join(rendered) if rendered else None
    return text, filled


def _normalize_custom_field(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "field_id": int(raw.get("id") or raw.get("field_id")),
        "field_name": raw.get("name") or raw.get("field_name") or "",
        "field_code": raw.get("code") or raw.get("field_code"),
        "field_type": raw.get("type") or raw.get("field_type"),
        "sort": raw.get("sort"),
        "group_id": raw.get("group_id"),
        "is_api_only": raw.get("is_api_only"),
        "enums_json": _json_dumps(raw.get("enums")) if raw.get("enums") is not None else None,
        "raw_json": _json_dumps(raw),
    }


def _request_json(session: requests.Session, url: str, logger: logging.Logger, attempts: int = 5) -> Dict[str, Any]:
    for attempt in range(1, attempts + 1):
        response = session.get(url, timeout=60)
        status = response.status_code
        if status == 429 and attempt < attempts:
            retry_after = response.headers.get("retry-after")
            delay = int(retry_after) if retry_after and retry_after.isdigit() else attempt * 3
            logger.info("Rate limit Kommo 429. Aguardando %ss antes de tentar novamente.", delay)
            time.sleep(delay)
            continue
        if status in (200, 201):
            return response.json()
        if status == 204:
            return {}
        if status in (401, 403):
            raise KommoAuthError(f"Kommo retornou HTTP {status} para {url}")
        body = response.text
        raise RuntimeError(f"Kommo retornou HTTP {status} para {url}: {body[:500]}")
    raise RuntimeError(f"Falha ao consultar Kommo depois de {attempts} tentativas: {url}")


def _determine_incremental_from(db_path: Path, lookback_seconds: int) -> Optional[int]:
    if not db_path.exists():
        return None
    conn = db_connect(db_path)
    try:
        row = conn.execute("SELECT MAX(updated_at) FROM kommo_leads").fetchone()
    finally:
        conn.close()
    if row is None or row[0] in (None, 0):
        return None
    return max(0, int(row[0]) - int(lookback_seconds))


def _fill_if_visible(page: Page, selectors: Sequence[str], value: str) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() and locator.is_visible(timeout=2_000):
                locator.fill(value)
                return True
        except Exception:
            continue
    return False


def _login_if_needed(
    context: BrowserContext,
    page: Page,
    base_url: str,
    email: Optional[str],
    password: Optional[str],
    state_path: Path,
    logger: logging.Logger,
) -> None:
    page.goto(f"{base_url}/leads/list/pipeline/{DEFAULT_LOGIN_PIPELINE_ID}/?skip_filter=Y", wait_until="domcontentloaded")
    page.wait_for_timeout(1_000)

    title = page.title().lower()
    if _browser_has_api_access(page, base_url, logger):
        context.storage_state(path=str(state_path))
        return

    if not email or not password:
        raise RuntimeError(
            "Sessao Kommo expirada. Defina KOMMO_EMAIL e KOMMO_PASSWORD para o login automatico."
        )

    logger.info("Login Kommo necessario; autenticando pelo navegador.")
    login_ok = _fill_if_visible(page, ['input[name="USER_LOGIN"]', 'input[name="login"]', 'input[type="email"]', 'input[placeholder="Login"]'], email)
    password_ok = _fill_if_visible(page, ['input[name="USER_PASSWORD"]', 'input[name="password"]', 'input[type="password"]', 'input[placeholder="Password"]'], password)
    if not login_ok or not password_ok:
        raise RuntimeError("Nao consegui localizar os campos de login/senha do Kommo.")

    button = page.get_by_role("button", name=re.compile("login", re.I))
    if button.count():
        button.first.click()
    else:
        page.keyboard.press("Enter")

    login_finished = False
    for _ in range(30):
        page.wait_for_timeout(1_000)
        try:
            title = page.title().lower()
            current_url = page.url
        except Exception:
            continue
        if "authorization" not in title and "/oauth2/authorize" not in current_url:
            login_finished = True
            break

    if not login_finished:
        raise RuntimeError("Login Kommo nao foi concluido. Verifique credenciais ou validacao adicional.")

    if not _browser_has_api_access(page, base_url, logger):
        raise RuntimeError(
            "Login Kommo aparentemente concluiu, mas a API continuou sem autorizacao. "
            "Pode haver captcha, 2FA, bloqueio de sessao ou credenciais invalidas."
        )

    state_path.parent.mkdir(parents=True, exist_ok=True)
    context.storage_state(path=str(state_path))


def _browser_has_api_access(page: Page, base_url: str, logger: logging.Logger) -> bool:
    try:
        response = page.request.get(f"{base_url}/api/v4/account", timeout=15_000)
        if response.status == 200:
            return True
        logger.info("Validacao de sessao Kommo via navegador retornou HTTP %s.", response.status)
        return False
    except Exception as exc:
        logger.info("Falha ao validar sessao Kommo via navegador: %s", exc)
        return False


def _create_http_session(
    base_url: str,
    state_path: Path,
    access_token: Optional[str],
) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
        }
    )

    if access_token:
        session.headers["Authorization"] = f"Bearer {access_token}"
        return session

    if not state_path.exists():
        return session

    state = json.loads(state_path.read_text(encoding="utf-8"))
    for cookie in state.get("cookies", []):
        domain = cookie.get("domain")
        if not domain:
            continue
        session.cookies.set(
            name=cookie["name"],
            value=cookie["value"],
            domain=domain,
            path=cookie.get("path") or "/",
        )
    return session


def _refresh_session_with_browser(
    base_url: str,
    email: Optional[str],
    password: Optional[str],
    state_path: Path,
    headed: bool,
    logger: logging.Logger,
) -> None:
    logger.info("Sessao HTTP sem autorizacao; renovando cookie pelo navegador.")
    state_path.parent.mkdir(parents=True, exist_ok=True)
    if state_path.exists():
        try:
            state_path.unlink()
        except OSError:
            pass
    with sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch(channel="msedge", headless=not headed)
        except Exception as exc:
            logger.warning(
                "Falha ao iniciar Microsoft Edge do sistema; tentando Chromium padrao. Detalhe: %s",
                exc,
            )
            browser = playwright.chromium.launch(headless=not headed)
        context = _create_browser_context(browser, state_path)
        page = context.new_page()
        try:
            _login_if_needed(context, page, base_url, email, password, state_path, logger)
        finally:
            context.close()
            browser.close()

    session = _create_http_session(base_url, state_path, None)
    try:
        _request_json(session, f"{base_url}/api/v4/account", logger, attempts=1)
    except KommoAuthError as exc:
        try:
            state_path.unlink()
        except OSError:
            pass
        raise RuntimeError(
            "Renovacao da sessao Kommo falhou: estado de navegador renovado nao passou em /api/v4/account."
        ) from exc


class KommoSQLiteStore:
    def __init__(self, db_path: Path, logger: logging.Logger) -> None:
        self.db_path = Path(db_path)
        self.logger = logger
        self.conn = db_connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_schema()

    def close(self) -> None:
        self.conn.close()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS kommo_sync_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                imported_at TEXT NOT NULL,
                base_url TEXT NOT NULL,
                pipeline_id TEXT,
                total_leads INTEGER NOT NULL,
                total_fields INTEGER NOT NULL,
                total_field_rows INTEGER NOT NULL,
                sql_dump_path TEXT
            );

            CREATE TABLE IF NOT EXISTS kommo_leads (
                lead_id INTEGER PRIMARY KEY,
                name TEXT,
                price INTEGER,
                price_with_minor_units INTEGER,
                responsible_user_id INTEGER,
                group_id INTEGER,
                status_id INTEGER,
                pipeline_id INTEGER,
                loss_reason_id INTEGER,
                created_by INTEGER,
                updated_by INTEGER,
                created_at INTEGER,
                created_at_iso TEXT,
                updated_at INTEGER,
                updated_at_iso TEXT,
                closed_at INTEGER,
                closed_at_iso TEXT,
                closest_task_at INTEGER,
                closest_task_at_iso TEXT,
                is_deleted INTEGER,
                score INTEGER,
                account_id INTEGER,
                labor_cost INTEGER,
                tags_json TEXT,
                contacts_json TEXT,
                companies_json TEXT,
                source_json TEXT,
                raw_json TEXT NOT NULL,
                row_hash TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_kommo_leads_pipeline ON kommo_leads(pipeline_id);
            CREATE INDEX IF NOT EXISTS idx_kommo_leads_status ON kommo_leads(status_id);
            CREATE INDEX IF NOT EXISTS idx_kommo_leads_updated_at ON kommo_leads(updated_at);

            CREATE TABLE IF NOT EXISTS kommo_lead_versions (
                lead_id INTEGER NOT NULL,
                row_hash TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                PRIMARY KEY (lead_id, row_hash)
            );

            CREATE TABLE IF NOT EXISTS kommo_lead_custom_fields (
                field_id INTEGER PRIMARY KEY,
                field_name TEXT NOT NULL,
                field_code TEXT,
                field_type TEXT,
                sort INTEGER,
                group_id TEXT,
                is_api_only INTEGER,
                enums_json TEXT,
                raw_json TEXT NOT NULL,
                imported_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_kommo_fields_name ON kommo_lead_custom_fields(field_name);
            CREATE INDEX IF NOT EXISTS idx_kommo_fields_type ON kommo_lead_custom_fields(field_type);

            CREATE TABLE IF NOT EXISTS kommo_lead_field_values (
                lead_id INTEGER NOT NULL,
                field_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                field_code TEXT,
                field_type TEXT,
                value_text TEXT,
                values_json TEXT,
                is_filled INTEGER NOT NULL DEFAULT 0,
                imported_at TEXT NOT NULL,
                PRIMARY KEY (lead_id, field_id),
                FOREIGN KEY (lead_id) REFERENCES kommo_leads(lead_id) ON DELETE CASCADE,
                FOREIGN KEY (field_id) REFERENCES kommo_lead_custom_fields(field_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_kommo_values_field ON kommo_lead_field_values(field_id);
            CREATE INDEX IF NOT EXISTS idx_kommo_values_filled ON kommo_lead_field_values(is_filled);

            CREATE VIEW IF NOT EXISTS vw_kommo_leads_campos_completos AS
            SELECT
                l.lead_id,
                l.name AS lead_name,
                l.pipeline_id,
                l.status_id,
                l.responsible_user_id,
                f.field_id,
                f.field_name,
                f.field_code,
                f.field_type,
                v.value_text,
                v.values_json,
                v.is_filled,
                l.imported_at
            FROM kommo_leads l
            JOIN kommo_lead_custom_fields f
            LEFT JOIN kommo_lead_field_values v
                ON v.lead_id = l.lead_id
               AND v.field_id = f.field_id;
            """
        )
        self.conn.commit()

    def upsert_all(
        self,
        leads: Sequence[Dict[str, Any]],
        fields: Sequence[Dict[str, Any]],
        base_url: str,
        pipeline_id: Optional[str],
    ) -> Dict[str, int]:
        imported_at = datetime.now().isoformat(timespec="seconds")
        cursor = self.conn.cursor()

        normalized_fields: Dict[int, Dict[str, Any]] = {}
        for raw_field in fields:
            normalized = _normalize_custom_field(raw_field)
            normalized_fields[normalized["field_id"]] = normalized

        for lead in leads:
            for field_value in lead.get("custom_fields_values") or []:
                field_id = field_value.get("field_id")
                if field_id is None:
                    continue
                field_id = int(field_id)
                if field_id not in normalized_fields:
                    normalized_fields[field_id] = _normalize_custom_field(field_value)

        for field in sorted(normalized_fields.values(), key=lambda item: (item.get("sort") is None, item.get("sort") or 0, item["field_id"])):
            cursor.execute(
                """
                INSERT INTO kommo_lead_custom_fields (
                    field_id, field_name, field_code, field_type, sort, group_id,
                    is_api_only, enums_json, raw_json, imported_at
                ) VALUES (
                    :field_id, :field_name, :field_code, :field_type, :sort, :group_id,
                    :is_api_only, :enums_json, :raw_json, :imported_at
                )
                ON CONFLICT(field_id) DO UPDATE SET
                    field_name = excluded.field_name,
                    field_code = excluded.field_code,
                    field_type = excluded.field_type,
                    sort = excluded.sort,
                    group_id = excluded.group_id,
                    is_api_only = excluded.is_api_only,
                    enums_json = excluded.enums_json,
                    raw_json = excluded.raw_json,
                    imported_at = excluded.imported_at
                """,
                {**field, "imported_at": imported_at},
            )

        lead_count = 0
        version_count = 0
        field_row_count = 0
        for lead in leads:
            lead_id = int(lead["id"])
            embedded = lead.get("_embedded") or {}
            payload_json = _json_dumps(lead)
            hash_value = _row_hash(lead)

            cursor.execute(
                """
                INSERT INTO kommo_leads (
                    lead_id, name, price, price_with_minor_units, responsible_user_id, group_id,
                    status_id, pipeline_id, loss_reason_id, created_by, updated_by,
                    created_at, created_at_iso, updated_at, updated_at_iso,
                    closed_at, closed_at_iso, closest_task_at, closest_task_at_iso,
                    is_deleted, score, account_id, labor_cost, tags_json, contacts_json,
                    companies_json, source_json, raw_json, row_hash,
                    imported_at, first_seen_at, last_seen_at
                ) VALUES (
                    :lead_id, :name, :price, :price_with_minor_units, :responsible_user_id, :group_id,
                    :status_id, :pipeline_id, :loss_reason_id, :created_by, :updated_by,
                    :created_at, :created_at_iso, :updated_at, :updated_at_iso,
                    :closed_at, :closed_at_iso, :closest_task_at, :closest_task_at_iso,
                    :is_deleted, :score, :account_id, :labor_cost, :tags_json, :contacts_json,
                    :companies_json, :source_json, :raw_json, :row_hash,
                    :imported_at, :first_seen_at, :last_seen_at
                )
                ON CONFLICT(lead_id) DO UPDATE SET
                    name = excluded.name,
                    price = excluded.price,
                    price_with_minor_units = excluded.price_with_minor_units,
                    responsible_user_id = excluded.responsible_user_id,
                    group_id = excluded.group_id,
                    status_id = excluded.status_id,
                    pipeline_id = excluded.pipeline_id,
                    loss_reason_id = excluded.loss_reason_id,
                    created_by = excluded.created_by,
                    updated_by = excluded.updated_by,
                    created_at = excluded.created_at,
                    created_at_iso = excluded.created_at_iso,
                    updated_at = excluded.updated_at,
                    updated_at_iso = excluded.updated_at_iso,
                    closed_at = excluded.closed_at,
                    closed_at_iso = excluded.closed_at_iso,
                    closest_task_at = excluded.closest_task_at,
                    closest_task_at_iso = excluded.closest_task_at_iso,
                    is_deleted = excluded.is_deleted,
                    score = excluded.score,
                    account_id = excluded.account_id,
                    labor_cost = excluded.labor_cost,
                    tags_json = excluded.tags_json,
                    contacts_json = excluded.contacts_json,
                    companies_json = excluded.companies_json,
                    source_json = excluded.source_json,
                    raw_json = excluded.raw_json,
                    row_hash = excluded.row_hash,
                    imported_at = excluded.imported_at,
                    first_seen_at = kommo_leads.first_seen_at,
                    last_seen_at = excluded.last_seen_at
                """,
                {
                    "lead_id": lead_id,
                    "name": lead.get("name"),
                    "price": lead.get("price"),
                    "price_with_minor_units": lead.get("price_with_minor_units"),
                    "responsible_user_id": lead.get("responsible_user_id"),
                    "group_id": lead.get("group_id"),
                    "status_id": lead.get("status_id"),
                    "pipeline_id": lead.get("pipeline_id"),
                    "loss_reason_id": lead.get("loss_reason_id"),
                    "created_by": lead.get("created_by"),
                    "updated_by": lead.get("updated_by"),
                    "created_at": lead.get("created_at"),
                    "created_at_iso": _iso_from_timestamp(lead.get("created_at")),
                    "updated_at": lead.get("updated_at"),
                    "updated_at_iso": _iso_from_timestamp(lead.get("updated_at")),
                    "closed_at": lead.get("closed_at"),
                    "closed_at_iso": _iso_from_timestamp(lead.get("closed_at")),
                    "closest_task_at": lead.get("closest_task_at"),
                    "closest_task_at_iso": _iso_from_timestamp(lead.get("closest_task_at")),
                    "is_deleted": 1 if lead.get("is_deleted") else 0,
                    "score": lead.get("score"),
                    "account_id": lead.get("account_id"),
                    "labor_cost": lead.get("labor_cost"),
                    "tags_json": _json_dumps(embedded.get("tags")) if embedded.get("tags") is not None else None,
                    "contacts_json": _json_dumps(embedded.get("contacts")) if embedded.get("contacts") is not None else None,
                    "companies_json": _json_dumps(embedded.get("companies")) if embedded.get("companies") is not None else None,
                    "source_json": _json_dumps(embedded.get("source")) if embedded.get("source") is not None else None,
                    "raw_json": payload_json,
                    "row_hash": hash_value,
                    "imported_at": imported_at,
                    "first_seen_at": imported_at,
                    "last_seen_at": imported_at,
                },
            )

            version_result = cursor.execute(
                """
                INSERT OR IGNORE INTO kommo_lead_versions (lead_id, row_hash, imported_at, raw_json)
                VALUES (?, ?, ?, ?)
                """,
                (lead_id, hash_value, imported_at, payload_json),
            )
            if version_result.rowcount:
                version_count += 1

            values_by_field: Dict[int, Dict[str, Any]] = {}
            for field_value in lead.get("custom_fields_values") or []:
                field_id = field_value.get("field_id")
                if field_id is not None:
                    values_by_field[int(field_id)] = field_value

            for field_id, field in normalized_fields.items():
                field_value = values_by_field.get(field_id)
                values = field_value.get("values") if field_value else []
                value_text, is_filled = _extract_display_values(values)
                cursor.execute(
                    """
                    INSERT INTO kommo_lead_field_values (
                        lead_id, field_id, field_name, field_code, field_type,
                        value_text, values_json, is_filled, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(lead_id, field_id) DO UPDATE SET
                        field_name = excluded.field_name,
                        field_code = excluded.field_code,
                        field_type = excluded.field_type,
                        value_text = excluded.value_text,
                        values_json = excluded.values_json,
                        is_filled = excluded.is_filled,
                        imported_at = excluded.imported_at
                    """,
                    (
                        lead_id,
                        field_id,
                        field["field_name"],
                        field["field_code"],
                        field["field_type"],
                        value_text,
                        _json_dumps(values) if values else None,
                        1 if is_filled else 0,
                        imported_at,
                    ),
                )
                field_row_count += 1

            lead_count += 1

        self._rebuild_wide_table(normalized_fields)
        cursor.execute(
            """
            INSERT INTO kommo_sync_runs (
                imported_at, base_url, pipeline_id, total_leads,
                total_fields, total_field_rows, sql_dump_path
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (imported_at, base_url, pipeline_id, lead_count, len(normalized_fields), field_row_count),
        )
        self.conn.commit()
        return {
            "leads": lead_count,
            "fields": len(normalized_fields),
            "field_rows": field_row_count,
            "versions_added": version_count,
        }

    def _rebuild_wide_table(self, fields: Dict[int, Dict[str, Any]]) -> None:
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS kommo_leads_wide")

        used_columns = {
            "lead_id",
            "name",
            "price",
            "responsible_user_id",
            "status_id",
            "pipeline_id",
            "created_at_iso",
            "updated_at_iso",
            "closed_at_iso",
            "tags_json",
            "contacts_json",
        }
        field_columns: List[Tuple[int, str]] = []
        for field_id, field in sorted(fields.items(), key=lambda item: (item[1].get("sort") is None, item[1].get("sort") or 0, item[0])):
            base = f"cf_{field_id}_{_slug(field.get('field_name'), f'field_{field_id}')}"
            column = base[:120]
            index = 2
            while column in used_columns:
                suffix = f"_{index}"
                column = (base[: 120 - len(suffix)] + suffix)
                index += 1
            used_columns.add(column)
            field_columns.append((field_id, column))

        standard_columns = [
            "lead_id INTEGER PRIMARY KEY",
            "name TEXT",
            "price INTEGER",
            "responsible_user_id INTEGER",
            "status_id INTEGER",
            "pipeline_id INTEGER",
            "created_at_iso TEXT",
            "updated_at_iso TEXT",
            "closed_at_iso TEXT",
            "tags_json TEXT",
            "contacts_json TEXT",
        ]
        dynamic_columns = [f"{_quote_identifier(column)} TEXT" for _, column in field_columns]
        cursor.execute(f"CREATE TABLE kommo_leads_wide ({', '.join(standard_columns + dynamic_columns)})")

        select_dynamic = [
            f"MAX(CASE WHEN v.field_id = {field_id} THEN v.value_text END) AS {_quote_identifier(column)}"
            for field_id, column in field_columns
        ]
        insert_columns = [
            "lead_id",
            "name",
            "price",
            "responsible_user_id",
            "status_id",
            "pipeline_id",
            "created_at_iso",
            "updated_at_iso",
            "closed_at_iso",
            "tags_json",
            "contacts_json",
        ] + [column for _, column in field_columns]
        select_columns = [
            "l.lead_id",
            "l.name",
            "l.price",
            "l.responsible_user_id",
            "l.status_id",
            "l.pipeline_id",
            "l.created_at_iso",
            "l.updated_at_iso",
            "l.closed_at_iso",
            "l.tags_json",
            "l.contacts_json",
        ] + select_dynamic

        cursor.execute(
            f"""
            INSERT INTO kommo_leads_wide ({', '.join(_quote_identifier(col) for col in insert_columns)})
            SELECT
                {', '.join(select_columns)}
            FROM kommo_leads l
            LEFT JOIN kommo_lead_field_values v
                ON v.lead_id = l.lead_id
            GROUP BY l.lead_id
            """
        )

    def export_sql_dump(self, output_path: Path) -> Path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        latest_run = self.conn.execute("SELECT MAX(run_id) AS run_id FROM kommo_sync_runs").fetchone()
        if latest_run and latest_run["run_id"]:
            self.conn.execute(
                "UPDATE kommo_sync_runs SET sql_dump_path = ? WHERE run_id = ?",
                (str(output_path), latest_run["run_id"]),
            )
            self.conn.commit()
        with output_path.open("w", encoding="utf-8", newline="\n") as file:
            file.write("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n")
            objects = self.conn.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_master
                WHERE sql IS NOT NULL
                  AND name NOT LIKE 'sqlite_%'
                ORDER BY
                    CASE type
                        WHEN 'table' THEN 1
                        WHEN 'index' THEN 3
                        WHEN 'trigger' THEN 4
                        WHEN 'view' THEN 5
                        ELSE 9
                    END,
                    name
                """
            ).fetchall()

            for row in objects:
                if row["type"] == "table":
                    file.write(f"{row['sql']};\n")
                    columns = [
                        column["name"]
                        for column in self.conn.execute(f"PRAGMA table_info({_quote_identifier(row['name'])})")
                    ]
                    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
                    for data_row in self.conn.execute(f"SELECT * FROM {_quote_identifier(row['name'])}"):
                        values = ", ".join(_sql_literal(data_row[column]) for column in columns)
                        file.write(
                            f"INSERT INTO {_quote_identifier(row['name'])} ({quoted_columns}) VALUES ({values});\n"
                        )
                elif row["type"] in {"index", "trigger", "view"}:
                    file.write(f"{row['sql']};\n")
            file.write("COMMIT;\n")
        return output_path


def fetch_kommo_data(
    session: requests.Session,
    base_url: str,
    pipeline_id: Optional[str],
    updated_from: Optional[int],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fields_url = f"{base_url}/api/v4/leads/custom_fields?limit=250"
    fields_data = _request_json(session, fields_url, logger)
    fields = fields_data.get("_embedded", {}).get("custom_fields", [])

    query: Dict[str, Any] = {
        "limit": 250,
        "page": 1,
        "with": "contacts,source,loss_reason",
    }
    if pipeline_id:
        query["filter[pipeline_id][]"] = pipeline_id
    if updated_from:
        query["filter[updated_at][from]"] = int(updated_from)

    leads: List[Dict[str, Any]] = []
    page = 1
    while True:
        query["page"] = page
        url = f"{base_url}/api/v4/leads?{urlencode(query, doseq=True)}"
        data = _request_json(session, url, logger)
        page_leads = data.get("_embedded", {}).get("leads", [])
        if not page_leads:
            break
        leads.extend(page_leads)
        logger.info("Pagina %s: %s leads coletados; total parcial=%s", page, len(page_leads), len(leads))
        if not data.get("_links", {}).get("next", {}).get("href"):
            break
        page += 1

    return leads, fields


def _create_browser_context(browser: Browser, state_path: Path) -> BrowserContext:
    kwargs: Dict[str, Any] = {
        "viewport": {"width": 1365, "height": 900},
        "ignore_https_errors": True,
    }
    if state_path.exists():
        kwargs["storage_state"] = str(state_path)
    return browser.new_context(**kwargs)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sincroniza leads do Kommo em SQLite e gera um dump SQL."
    )
    parser.add_argument("--base-url", default=os.getenv("KOMMO_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--email", default=os.getenv("KOMMO_EMAIL"))
    parser.add_argument("--password", default=os.getenv("KOMMO_PASSWORD"))
    parser.add_argument("--access-token", default=os.getenv("KOMMO_ACCESS_TOKEN"))
    parser.add_argument("--pipeline-id", default=os.getenv("KOMMO_PIPELINE_ID"), help="Opcional. Se omitido, busca todos os pipelines.")
    parser.add_argument("--sync-mode", choices=("full", "incremental"), default=os.getenv("KOMMO_SYNC_MODE", DEFAULT_SYNC_MODE))
    parser.add_argument("--incremental-lookback-seconds", type=int, default=int(os.getenv("KOMMO_INCREMENTAL_LOOKBACK_SECONDS", DEFAULT_INCREMENTAL_LOOKBACK_SECONDS)))
    parser.add_argument("--db-path", default=os.getenv("KOMMO_DB_PATH", str(DEFAULT_DB_PATH)))
    parser.add_argument("--output-dir", default=os.getenv("KOMMO_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)))
    parser.add_argument("--state-path", default=os.getenv("KOMMO_STATE_PATH", str(DEFAULT_STATE_PATH)))
    parser.add_argument("--headed", action="store_true", help="Abre o navegador visivel.")
    return parser


def main() -> None:
    load_env_file(Path(".env"))
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    logger = logging.getLogger("kommo_leads_sqlite")

    base_url = args.base_url.rstrip("/")
    pipeline_id = str(args.pipeline_id or "").strip() or None
    db_path = Path(args.db_path)
    output_dir = Path(args.output_dir)
    state_enc_path = Path(args.state_path)
    state_path = state_util.activate_manual(state_enc_path)
    updated_from = None if args.sync_mode == "full" else _determine_incremental_from(db_path, args.incremental_lookback_seconds)

    logger.info("============================================================")
    logger.info("Exportacao Kommo Leads")
    logger.info("Base URL: %s", base_url)
    logger.info("Pipeline: %s", pipeline_id or "todos")
    logger.info("Modo sync: %s", args.sync_mode)
    if updated_from:
        logger.info("Filtro updated_at[from]: %s", updated_from)
    logger.info("SQLite: %s", db_path)
    logger.info("Saida SQL: %s", output_dir)
    logger.info("============================================================")

    try:
        session = _create_http_session(base_url, state_path, args.access_token)
        try:
            leads, fields = fetch_kommo_data(session, base_url, pipeline_id, updated_from, logger)
        except KommoAuthError:
            if args.access_token:
                raise
            _refresh_session_with_browser(
                base_url=base_url,
                email=args.email,
                password=args.password,
                state_path=state_path,
                headed=args.headed,
                logger=logger,
            )
            session = _create_http_session(base_url, state_path, args.access_token)
            leads, fields = fetch_kommo_data(session, base_url, pipeline_id, updated_from, logger)
        if state_util.is_encrypted_path(state_enc_path):
            state_util.seal(state_enc_path)
    except Exception:
        if state_util.is_encrypted_path(state_enc_path):
            state_util.discard(state_enc_path)
        raise

    store = KommoSQLiteStore(db_path, logger)
    try:
        summary = store.upsert_all(leads=leads, fields=fields, base_url=base_url, pipeline_id=pipeline_id)
        sql_path = output_dir / "kommo_leads_latest.sql"
        store.export_sql_dump(sql_path)
    finally:
        store.close()

    logger.info(
        "Kommo sincronizado: leads=%s | campos=%s | linhas lead+campo=%s | versoes_novas=%s",
        summary["leads"],
        summary["fields"],
        summary["field_rows"],
        summary["versions_added"],
    )
    logger.info("SQLite gerado: %s", db_path)
    logger.info("Dump SQL gerado: %s", sql_path)


if __name__ == "__main__":
    main()
