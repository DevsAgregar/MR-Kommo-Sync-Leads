#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import random
import re
import time
from db_util import connect as db_connect, sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, TypeVar
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.exceptions import ProtocolError
from urllib3.util.retry import Retry
from env_config import load_env_file

from login import BASE_URL, DEFAULT_DB_PATH, DEFAULT_EMAIL, DEFAULT_ENV_FILE, DEFAULT_SENHA


# 30s e suficiente para uma pagina de paciente e libera o slot do worker mais
# cedo quando o socket virou zumbi (antes ficava ate 60s preso antes de o OS
# notar o RST do servidor).
DEFAULT_TIMEOUT = 30
DEFAULT_WORKERS = 4
# Retries sao aplicados em duas camadas:
#   1) urllib3.Retry no HTTPAdapter -> cobre falha ao estabelecer a conexao e
#      respostas 5xx. NAO cobre SSLEOFError que ocorre depois que os headers
#      ja comecaram a chegar (leitura do body), que e o nosso caso comum.
#   2) _request_with_retry na camada de aplicacao -> refaz a chamada inteira
#      com backoff exponencial + jitter em SSLError/ConnectionError/Timeout.
# Juntas evitam que uma desconexao pontual do servidor derrube um paciente.
_REQUEST_MAX_ATTEMPTS = 4
_REQUEST_BACKOFF_BASE = 0.8
_REQUEST_BACKOFF_CAP = 8.0

_T = TypeVar("_T")

_TRANSIENT_EXC = (
    requests.exceptions.SSLError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ProtocolError,
)


def _build_retry_adapter() -> HTTPAdapter:
    """HTTPAdapter com retry no handshake/conexao. Nao cobre SSL no meio da
    resposta (leitura do body) -- para isso ver _request_with_retry."""
    retry = Retry(
        total=3,
        connect=3,
        read=0,  # read-retry aqui e perigoso (pode reenviar POST); deixamos na camada de app.
        status=3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(("GET", "POST")),
        backoff_factor=0.6,
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    return HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)


def _request_with_retry(
    logger: logging.Logger,
    label: str,
    fn: Callable[[], _T],
) -> _T:
    """Executa `fn` com retry exponencial + jitter em falhas transitorias.

    O servidor da Clinica Agil fecha TLS abruptamente de forma esparsa (LB /
    KeepAlive). Sem retry, um paciente inteiro e perdido por uma unica
    desconexao. Aqui refazemos ate _REQUEST_MAX_ATTEMPTS com espera crescente,
    o que cobre tanto RemoteDisconnected quanto SSL: UNEXPECTED_EOF_WHILE_READING.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(1, _REQUEST_MAX_ATTEMPTS + 1):
        try:
            return fn()
        except _TRANSIENT_EXC as exc:
            last_exc = exc
            if attempt == _REQUEST_MAX_ATTEMPTS:
                break
            # backoff exponencial com jitter para evitar sincronia entre workers
            wait = min(_REQUEST_BACKOFF_CAP, _REQUEST_BACKOFF_BASE * (2 ** (attempt - 1)))
            wait += random.uniform(0, wait * 0.25)
            logger.debug(
                "Retry %s/%s em %.2fs apos %s: %s",
                attempt,
                _REQUEST_MAX_ATTEMPTS - 1,
                wait,
                label,
                exc.__class__.__name__,
            )
            time.sleep(wait)
    assert last_exc is not None
    raise last_exc
DEFAULT_KOMMO_DB_PATH = Path("mirella_kommo_leads.sqlite3")
TIMELINE_BLOCKED_STATUSES = {
    "bloqueado",
    "falta justificada",
    "nao compareceu",
    "não compareceu",
    "nao compareceu sem descontar",
    "não compareceu sem descontar",
    "desmarcado pelo profissional",
    "ausente",
    "reserva de agenda",
}
TIMELINE_BLOCKED_NEXT = TIMELINE_BLOCKED_STATUSES


@dataclass
class TimelineEntry:
    agenda_id: int
    when: datetime
    status_label: Optional[str]
    professional: Optional[str]
    specialty: Optional[str]


def _normalize_space(text: Any) -> Optional[str]:
    if text in (None, ""):
        return None
    value = re.sub(r"\s+", " ", str(text).strip())
    return value or None


def _to_iso_date(date_br: Optional[str]) -> Optional[str]:
    if not date_br:
        return None
    return datetime.strptime(date_br, "%d/%m/%Y").strftime("%Y-%m-%d")


def _to_iso_datetime(date_br: Optional[str], hour_text: Optional[str]) -> Optional[str]:
    if not date_br or not hour_text:
        return None
    return datetime.strptime(f"{date_br} {hour_text}", "%d/%m/%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S")


def _clean_service_name(raw: str) -> Optional[str]:
    text = _normalize_space(re.sub(r"<[^>]+>", " ", raw))
    if not text:
        return None
    text = re.sub(r"\s*-\s*\d{2}/\d{2}/\d{4}", "", text)
    text = re.sub(r"\s*-\s*\d+\s+Sess(?:ão|oes|ões?)", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" -")
    return text or None


def _dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _normalize_name(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = _normalize_space(value)
    if not text:
        return None
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = " ".join(text.split())
    return text or None


class ClinicaAgilOperationalExtractor:
    def __init__(self, email: str, senha: str, timeout: int, logger: logging.Logger) -> None:
        self.email = email
        self.senha = senha
        self.timeout = timeout
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/147.0.0.0 Safari/537.36"
                )
            }
        )
        # Adapter com retry no nivel urllib3 para falhas de conexao/5xx.
        # Read-retry fica desabilitado aqui porque o urllib3 reenviaria POSTs
        # tambem; a camada de retry de leitura ficou em _request_with_retry.
        adapter = _build_retry_adapter()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def login(self) -> None:
        response = self.session.post(
            f"{BASE_URL}/login",
            data={"identity": self.email, "password": self.senha},
            timeout=self.timeout,
        )
        if response.status_code not in (200, 302):
            raise RuntimeError(f"Falha no login da Clinica Agil. HTTP {response.status_code}")

        agenda = self.session.get(f"{BASE_URL}/agenda", timeout=self.timeout)
        if agenda.status_code != 200:
            raise RuntimeError(f"Sessao sem acesso a agenda. HTTP {agenda.status_code}")

    def export_cookies(self) -> Dict[str, str]:
        return requests.utils.dict_from_cookiejar(self.session.cookies)

    @classmethod
    def from_cookies(
        cls,
        email: str,
        senha: str,
        timeout: int,
        logger: logging.Logger,
        cookies: Dict[str, str],
    ) -> "ClinicaAgilOperationalExtractor":
        instance = cls(email=email, senha=senha, timeout=timeout, logger=logger)
        instance.session.cookies = requests.utils.cookiejar_from_dict(cookies, cookiejar=None, overwrite=True)
        return instance

    def get_patient_edit_html(self, patient_id: int) -> str:
        def _do() -> str:
            response = self.session.get(
                f"{BASE_URL}/pacientes/editar/{patient_id}", timeout=self.timeout
            )
            response.raise_for_status()
            return response.text

        return _request_with_retry(self.logger, f"edit/{patient_id}", _do)

    def get_patient_agendamentos_html(self, patient_id: int) -> str:
        def _do() -> str:
            response = self.session.get(
                f"{BASE_URL}/pacientes/visualizar/{patient_id}/agendamentos",
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.text

        return _request_with_retry(self.logger, f"agendamentos/{patient_id}", _do)

    def get_agenda_event(self, agenda_id: int) -> Dict[str, Any]:
        def _do() -> Dict[str, Any]:
            response = self.session.post(
                f"{BASE_URL}/agenda/busca_evento",
                data={"id": str(agenda_id)},
                headers={
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()

        return _request_with_retry(self.logger, f"agenda/{agenda_id}", _do)


def _extract_origin(edit_html: str) -> Optional[str]:
    soup = BeautifulSoup(edit_html, "html.parser")
    select = soup.select_one("select[name='indicacao']")
    if not select:
        return None

    option = select.select_one("option[selected]")
    if option is None:
        for candidate in select.select("option"):
            if candidate.has_attr("selected"):
                option = candidate
                break
    if option is None:
        return None

    value = option.get("value")
    if value in (None, ""):
        return None
    return _normalize_space(option.get_text(" ", strip=True))


def _extract_timeline_entries(agendamentos_html: str) -> List[TimelineEntry]:
    soup = BeautifulSoup(agendamentos_html, "html.parser")
    entries: List[TimelineEntry] = []
    for balloon in soup.select("div.linha_agendamentos_paciente"):
        time_text = balloon.select_one(".time-text")
        if time_text is None:
            continue
        parts = list(time_text.stripped_strings)
        if len(parts) < 2:
            continue
        date_br = _normalize_space(parts[0])
        hour_text = _normalize_space(parts[1])
        if not date_br or not hour_text:
            continue

        link = balloon.select_one("a[href*='/agenda/index/']")
        if link is None:
            continue
        href = link.get("href") or ""
        parsed = urlparse(href)
        try:
            agenda_id = int(parsed.path.rstrip("/").split("/")[-1])
        except (TypeError, ValueError):
            continue

        professional = _normalize_space(
            " ".join(balloon.select_one(".agendamentos-col-pr").stripped_strings)
            if balloon.select_one(".agendamentos-col-pr")
            else ""
        )
        specialty = _normalize_space(
            " ".join(balloon.select_one(".agendamentos-col-esp").stripped_strings)
            if balloon.select_one(".agendamentos-col-esp")
            else ""
        )
        status = _normalize_space(
            " ".join(balloon.select_one(".agendamentos-col-sta").stripped_strings)
            if balloon.select_one(".agendamentos-col-sta")
            else ""
        )
        professional = re.sub(r"^Profissional\s*", "", professional or "", flags=re.I) or None
        specialty = re.sub(r"^Especialidade\s*", "", specialty or "", flags=re.I) or None
        status = re.sub(r"^Status\s*", "", status or "", flags=re.I) or None

        entries.append(
            TimelineEntry(
                agenda_id=agenda_id,
                when=datetime.strptime(f"{date_br} {hour_text}", "%d/%m/%Y %H:%M"),
                status_label=status,
                professional=professional,
                specialty=specialty,
            )
        )
    return entries


def _is_valid_last_visit(entry: TimelineEntry, now: datetime) -> bool:
    if entry.when > now:
        return False
    if not entry.status_label:
        return True
    return entry.status_label.strip().lower() not in TIMELINE_BLOCKED_STATUSES


def _is_valid_next_visit(entry: TimelineEntry, now: datetime) -> bool:
    if entry.when <= now:
        return False
    if not entry.status_label:
        return True
    return entry.status_label.strip().lower() not in TIMELINE_BLOCKED_NEXT


def _extract_services_from_event(event_payload: Dict[str, Any]) -> List[str]:
    services: List[str] = []

    procedimentos_agendados = event_payload.get("procedimentos_agendados")
    if procedimentos_agendados:
        for raw in re.split(r"<br\s*/?>", str(procedimentos_agendados), flags=re.I):
            cleaned = _clean_service_name(raw)
            if cleaned:
                services.append(cleaned)

    for item in event_payload.get("combo") or []:
        cleaned = _clean_service_name(item.get("procedimento"))
        if cleaned:
            services.append(cleaned)

    procedimentos_2 = _normalize_space(event_payload.get("procedimentos_2"))
    if procedimentos_2:
        for raw in procedimentos_2.split(","):
            cleaned = _clean_service_name(raw)
            if cleaned:
                services.append(cleaned)

    return _dedupe_keep_order(services)


class PatientOperationalStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.conn = db_connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_schema()

    def close(self) -> None:
        self.conn.close()

    def _create_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS patient_operational_fields (
                patient_id INTEGER PRIMARY KEY,
                origem TEXT,
                ultima_visita TEXT,
                agendamento TEXT,
                proxima_consulta TEXT,
                servicos_text TEXT,
                servicos_json TEXT,
                ultima_visita_agenda_id INTEGER,
                proximo_agendamento_agenda_id INTEGER,
                ultima_visita_status TEXT,
                proximo_agendamento_status TEXT,
                ultima_visita_profissional TEXT,
                proximo_agendamento_profissional TEXT,
                ultima_visita_especialidade TEXT,
                proximo_agendamento_especialidade TEXT,
                imported_at TEXT NOT NULL,
                raw_payload_json TEXT NOT NULL,
                FOREIGN KEY(patient_id) REFERENCES patients(patient_id) ON DELETE CASCADE
            );

            DROP VIEW IF EXISTS vw_patients_complete_operational;

            CREATE VIEW vw_patients_complete_operational AS
            SELECT
                base.*,
                ops.origem,
                ops.ultima_visita,
                ops.agendamento,
                ops.proxima_consulta,
                ops.servicos_text,
                ops.servicos_json,
                ops.ultima_visita_agenda_id,
                ops.proximo_agendamento_agenda_id,
                ops.ultima_visita_status,
                ops.proximo_agendamento_status,
                ops.ultima_visita_profissional,
                ops.proximo_agendamento_profissional,
                ops.ultima_visita_especialidade,
                ops.proximo_agendamento_especialidade,
                ops.imported_at AS operational_imported_at
            FROM vw_patients_complete_financial base
            LEFT JOIN patient_operational_fields ops
                ON ops.patient_id = base.patient_id;
            """
        )
        self.conn.commit()

    def patient_ids(self, limit: Optional[int] = None) -> List[int]:
        sql = "SELECT patient_id FROM patients ORDER BY patient_id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return [int(row["patient_id"]) for row in self.conn.execute(sql)]

    def matched_patient_ids(self, kommo_db_path: Path, limit: Optional[int] = None) -> List[int]:
        kommo_db_path = Path(kommo_db_path)
        if not kommo_db_path.exists():
            return self.patient_ids(limit=limit)

        patient_rows = [
            (int(row["patient_id"]), _normalize_name(row["nome"]))
            for row in self.conn.execute("SELECT patient_id, nome FROM patients")
        ]
        patient_name_counts: Dict[str, int] = {}
        for _, name in patient_rows:
            if name:
                patient_name_counts[name] = patient_name_counts.get(name, 0) + 1

        kommo_conn = db_connect(kommo_db_path)
        kommo_conn.row_factory = sqlite3.Row
        try:
            lead_rows = [(_normalize_name(row["name"])) for row in kommo_conn.execute("SELECT name FROM kommo_leads")]
        finally:
            kommo_conn.close()

        lead_name_counts: Dict[str, int] = {}
        for name in lead_rows:
            if name:
                lead_name_counts[name] = lead_name_counts.get(name, 0) + 1

        matched_ids: List[int] = []
        for patient_id, name in patient_rows:
            if not name:
                continue
            if patient_name_counts.get(name) == 1 and lead_name_counts.get(name) == 1:
                matched_ids.append(patient_id)

        matched_ids.sort()
        if limit:
            matched_ids = matched_ids[: int(limit)]
        return matched_ids

    def _write_rows(self, rows: Sequence[Dict[str, Any]], clear_all: bool) -> None:
        imported_at = datetime.now().isoformat(timespec="seconds")
        cursor = self.conn.cursor()
        if clear_all:
            cursor.execute("DELETE FROM patient_operational_fields")
        else:
            patient_ids = [int(row["patient_id"]) for row in rows]
            if patient_ids:
                placeholders = ",".join("?" for _ in patient_ids)
                cursor.execute(
                    f"DELETE FROM patient_operational_fields WHERE patient_id IN ({placeholders})",
                    patient_ids,
                )
        for row in rows:
            payload = {**row, "imported_at": imported_at}
            payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            cursor.execute(
                """
                INSERT INTO patient_operational_fields (
                    patient_id,
                    origem,
                    ultima_visita,
                    agendamento,
                    proxima_consulta,
                    servicos_text,
                    servicos_json,
                    ultima_visita_agenda_id,
                    proximo_agendamento_agenda_id,
                    ultima_visita_status,
                    proximo_agendamento_status,
                    ultima_visita_profissional,
                    proximo_agendamento_profissional,
                    ultima_visita_especialidade,
                    proximo_agendamento_especialidade,
                    imported_at,
                    raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["patient_id"],
                    payload.get("origem"),
                    payload.get("ultima_visita"),
                    payload.get("agendamento"),
                    payload.get("proxima_consulta"),
                    payload.get("servicos_text"),
                    payload.get("servicos_json"),
                    payload.get("ultima_visita_agenda_id"),
                    payload.get("proximo_agendamento_agenda_id"),
                    payload.get("ultima_visita_status"),
                    payload.get("proximo_agendamento_status"),
                    payload.get("ultima_visita_profissional"),
                    payload.get("proximo_agendamento_profissional"),
                    payload.get("ultima_visita_especialidade"),
                    payload.get("proximo_agendamento_especialidade"),
                    imported_at,
                    payload_json,
                ),
            )
        self.conn.commit()

    def replace_all(self, rows: Sequence[Dict[str, Any]]) -> None:
        self._write_rows(rows, clear_all=True)

    def replace_subset(self, rows: Sequence[Dict[str, Any]]) -> None:
        self._write_rows(rows, clear_all=False)


def _build_snapshot(
    extractor: ClinicaAgilOperationalExtractor,
    patient_id: int,
    now: datetime,
) -> Dict[str, Any]:
    edit_html = extractor.get_patient_edit_html(patient_id)
    agendamentos_html = extractor.get_patient_agendamentos_html(patient_id)
    origem = _extract_origin(edit_html)
    entries = _extract_timeline_entries(agendamentos_html)

    last_entry = max((entry for entry in entries if _is_valid_last_visit(entry, now)), key=lambda item: item.when, default=None)
    next_entry = min((entry for entry in entries if _is_valid_next_visit(entry, now)), key=lambda item: item.when, default=None)

    services: List[str] = []
    for agenda_id in _dedupe_keep_order(
        [str(entry.agenda_id) for entry in (last_entry, next_entry) if entry is not None]
    ):
        event_payload = extractor.get_agenda_event(int(agenda_id))
        services.extend(_extract_services_from_event(event_payload))
    services = _dedupe_keep_order(services)

    return {
        "patient_id": patient_id,
        "origem": origem,
        "ultima_visita": last_entry.when.strftime("%Y-%m-%d %H:%M:%S") if last_entry else None,
        "agendamento": next_entry.when.strftime("%Y-%m-%d %H:%M:%S") if next_entry else None,
        "proxima_consulta": next_entry.when.strftime("%Y-%m-%d %H:%M:%S") if next_entry else None,
        "servicos_text": "; ".join(services) if services else None,
        "servicos_json": json.dumps(services, ensure_ascii=False) if services else None,
        "ultima_visita_agenda_id": last_entry.agenda_id if last_entry else None,
        "proximo_agendamento_agenda_id": next_entry.agenda_id if next_entry else None,
        "ultima_visita_status": last_entry.status_label if last_entry else None,
        "proximo_agendamento_status": next_entry.status_label if next_entry else None,
        "ultima_visita_profissional": last_entry.professional if last_entry else None,
        "proximo_agendamento_profissional": next_entry.professional if next_entry else None,
        "ultima_visita_especialidade": last_entry.specialty if last_entry else None,
        "proximo_agendamento_especialidade": next_entry.specialty if next_entry else None,
    }


def _build_snapshot_worker(
    patient_id: int,
    now_iso: str,
    email: str,
    senha: str,
    timeout: int,
    cookies: Dict[str, str],
) -> Dict[str, Any]:
    logger = logging.getLogger("clinic_operational_fields_sync.worker")
    extractor = ClinicaAgilOperationalExtractor.from_cookies(
        email=email,
        senha=senha,
        timeout=timeout,
        logger=logger,
        cookies=cookies,
    )
    return _build_snapshot(extractor=extractor, patient_id=patient_id, now=datetime.fromisoformat(now_iso))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Extrai campos operacionais da Clinica Agil para apoiar o preenchimento do Kommo."
    )
    parser.add_argument("--email", default=os.getenv("MIRELLA_EMAIL", DEFAULT_EMAIL))
    parser.add_argument("--senha", default=os.getenv("MIRELLA_SENHA", DEFAULT_SENHA))
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--kommo-db-path", default=str(DEFAULT_KOMMO_DB_PATH))
    parser.add_argument("--patient-scope", choices=("matched", "all"), default="matched")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--limit", type=int)
    return parser


def main() -> None:
    load_env_file(DEFAULT_ENV_FILE)
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    logger = logging.getLogger("clinic_operational_fields_sync")

    store = PatientOperationalStore(Path(args.db_path))
    extractor = ClinicaAgilOperationalExtractor(
        email=args.email,
        senha=args.senha,
        timeout=args.timeout,
        logger=logger,
    )
    try:
        extractor.login()
        if args.patient_scope == "matched":
            patient_ids = store.matched_patient_ids(Path(args.kommo_db_path), limit=args.limit)
            if not patient_ids:
                logger.warning("Nenhum paciente relevante encontrado no Kommo local; usando todos os pacientes.")
                patient_ids = store.patient_ids(limit=args.limit)
        else:
            patient_ids = store.patient_ids(limit=args.limit)

        logger.info(
            "Pacientes para extrair campos operacionais: %s | escopo=%s | workers=%s",
            len(patient_ids),
            args.patient_scope,
            args.workers,
        )
        rows: List[Dict[str, Any]] = []
        now = datetime.now()
        cookies = extractor.export_cookies()
        if args.workers <= 1:
            for index, patient_id in enumerate(patient_ids, start=1):
                try:
                    rows.append(_build_snapshot(extractor, patient_id, now))
                except Exception as exc:
                    logger.warning("Falha ao extrair paciente %s: %s", patient_id, exc)
                if index % 25 == 0 or index == len(patient_ids):
                    logger.info("Processados %s/%s pacientes", index, len(patient_ids))
        else:
            futures: Dict[concurrent.futures.Future[Dict[str, Any]], int] = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as pool:
                for patient_id in patient_ids:
                    future = pool.submit(
                        _build_snapshot_worker,
                        patient_id,
                        now.isoformat(),
                        args.email,
                        args.senha,
                        args.timeout,
                        cookies,
                    )
                    futures[future] = patient_id

                for index, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                    patient_id = futures[future]
                    try:
                        rows.append(future.result())
                    except Exception as exc:
                        logger.warning("Falha ao extrair paciente %s: %s", patient_id, exc)
                    if index % 25 == 0 or index == len(patient_ids):
                        logger.info("Processados %s/%s pacientes", index, len(patient_ids))

        rows.sort(key=lambda item: int(item["patient_id"]))
        if args.patient_scope == "all":
            store.replace_all(rows)
        else:
            store.replace_subset(rows)

        summary = store.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN origem IS NOT NULL AND TRIM(origem) <> '' THEN 1 ELSE 0 END) AS origem_count,
                SUM(CASE WHEN ultima_visita IS NOT NULL AND TRIM(ultima_visita) <> '' THEN 1 ELSE 0 END) AS ultima_visita_count,
                SUM(CASE WHEN agendamento IS NOT NULL AND TRIM(agendamento) <> '' THEN 1 ELSE 0 END) AS agendamento_count,
                SUM(CASE WHEN proxima_consulta IS NOT NULL AND TRIM(proxima_consulta) <> '' THEN 1 ELSE 0 END) AS proxima_consulta_count,
                SUM(CASE WHEN servicos_text IS NOT NULL AND TRIM(servicos_text) <> '' THEN 1 ELSE 0 END) AS servicos_count
            FROM patient_operational_fields
            """
        ).fetchone()
        logger.info(
            "Campos operacionais sincronizados: total=%s | origem=%s | ultima_visita=%s | agendamento=%s | proxima_consulta=%s | servicos=%s",
            summary["total"],
            summary["origem_count"],
            summary["ultima_visita_count"],
            summary["agendamento_count"],
            summary["proxima_consulta_count"],
            summary["servicos_count"],
        )
    finally:
        store.close()


if __name__ == "__main__":
    main()
