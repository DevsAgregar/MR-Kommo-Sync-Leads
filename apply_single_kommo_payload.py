#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

from env_config import load_env_file
import state_util
from kommo_leads_sqlite import (
    DEFAULT_BASE_URL,
    DEFAULT_STATE_PATH,
    KommoAuthError,
    _create_http_session,
    _refresh_session_with_browser,
)


DEFAULT_PAYLOADS_PATH = Path("exports") / "sync_preview" / "clinic_kommo_safe_payloads.json"
DEFAULT_OUTPUT_DIR = Path("exports") / "single_apply"
DEFAULT_TIMEZONE = "America/Sao_Paulo"
DATE_FIELD_IDS = {1561315, 1561317}
DATETIME_FIELD_IDS = {1555897, 1574511}


def _load_payload(payloads_path: Path, lead_id: int) -> Dict[str, Any]:
    payloads = json.loads(payloads_path.read_text(encoding="utf-8"))
    for payload in payloads:
        if int(payload["id"]) == int(lead_id):
            return payload
    raise RuntimeError(f"Lead {lead_id} nao encontrado em {payloads_path}")


def _date_to_timestamp(value: str, timezone: ZoneInfo) -> int:
    dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone)
    return int(dt.timestamp())


def _datetime_to_timestamp(value: str, timezone: ZoneInfo) -> int:
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=timezone)
    return int(dt.timestamp())


def _prepare_field_value(field: Dict[str, Any], timezone: ZoneInfo) -> Dict[str, Any]:
    field_id = int(field["field_id"])
    values: List[Dict[str, Any]] = []
    for item in field.get("values") or []:
        if "enum_id" in item:
            values.append({"enum_id": item["enum_id"], "value": item.get("value")})
            continue

        raw_value = item.get("value")
        if field_id in DATE_FIELD_IDS:
            values.append({"value": _date_to_timestamp(str(raw_value), timezone)})
        elif field_id in DATETIME_FIELD_IDS:
            values.append({"value": _datetime_to_timestamp(str(raw_value), timezone)})
        else:
            values.append({"value": raw_value})

    return {
        "field_id": field_id,
        "values": values,
    }


def _prepare_patch_payload(payload: Dict[str, Any], timezone_name: str) -> Dict[str, Any]:
    timezone = ZoneInfo(timezone_name)
    patch: Dict[str, Any] = {
        "custom_fields_values": [
            _prepare_field_value(field, timezone)
            for field in payload.get("custom_fields_values", [])
        ]
    }
    if "price" in payload and payload["price"] is not None:
        patch["price"] = round(float(payload["price"]), 2)
    return patch


def _request_json(session: requests.Session, method: str, url: str, **kwargs: Any) -> Dict[str, Any]:
    response = session.request(method, url, timeout=60, **kwargs)
    if response.status_code in (401, 403):
        raise KommoAuthError(f"Kommo retornou HTTP {response.status_code} para {url}")
    if response.status_code not in (200, 201):
        raise RuntimeError(f"Kommo retornou HTTP {response.status_code}: {response.text[:800]}")
    return response.json()


def _session(base_url: str, state_path: Path, email: Optional[str], password: Optional[str]) -> requests.Session:
    session = _create_http_session(base_url=base_url, state_path=state_path, access_token=None)
    try:
        _request_json(session, "GET", f"{base_url}/api/v4/account")
        return session
    except KommoAuthError:
        _refresh_session_with_browser(
            base_url=base_url,
            email=email,
            password=password,
            state_path=state_path,
            headed=False,
            logger=__import__("logging").getLogger("apply_single_kommo_payload"),
        )
        session = _create_http_session(base_url=base_url, state_path=state_path, access_token=None)
        _request_json(session, "GET", f"{base_url}/api/v4/account")
        return session


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aplica um payload seguro do preview em um unico lead do Kommo.")
    parser.add_argument("--lead-id", type=int, required=True)
    parser.add_argument("--payloads-path", default=str(DEFAULT_PAYLOADS_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--apply", action="store_true", help="Sem essa flag, apenas gera o plano.")
    return parser


def main() -> None:
    load_env_file(Path(".env"))
    parser = build_parser()
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    args.state_path = str(state_util.activate(Path(args.state_path)))
    lead_id = int(args.lead_id)
    output_dir = Path(args.output_dir)
    payload = _load_payload(Path(args.payloads_path), lead_id)
    patch_payload = _prepare_patch_payload(payload, args.timezone)
    _write_json(output_dir / f"{lead_id}_patch_plan.json", patch_payload)

    print(f"Lead: {lead_id} | {payload.get('lead_name')}")
    print(json.dumps(patch_payload, ensure_ascii=False, indent=2))

    if not args.apply:
        print("Modo preview. Use --apply para enviar ao Kommo.")
        return

    session = _session(
        base_url=base_url,
        state_path=Path(args.state_path),
        email=__import__("os").getenv("KOMMO_EMAIL"),
        password=__import__("os").getenv("KOMMO_PASSWORD"),
    )

    before = _request_json(session, "GET", f"{base_url}/api/v4/leads/{lead_id}")
    result = _request_json(
        session,
        "PATCH",
        f"{base_url}/api/v4/leads/{lead_id}",
        json=patch_payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    after = _request_json(session, "GET", f"{base_url}/api/v4/leads/{lead_id}")

    _write_json(output_dir / f"{lead_id}_before.json", before)
    _write_json(output_dir / f"{lead_id}_result.json", result)
    _write_json(output_dir / f"{lead_id}_after.json", after)
    print("Aplicado com sucesso.")


if __name__ == "__main__":
    main()
