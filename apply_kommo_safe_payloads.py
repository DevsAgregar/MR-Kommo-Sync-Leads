#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from env_config import load_env_file
from apply_single_kommo_payload import (
    DEFAULT_PAYLOADS_PATH,
    DEFAULT_TIMEZONE,
    _prepare_patch_payload,
    _request_json,
    _session,
)
from kommo_leads_sqlite import DEFAULT_BASE_URL, DEFAULT_STATE_PATH


DEFAULT_OUTPUT_DIR = Path("exports") / "apply_safe"


def _load_payloads(path: Path, lead_id: Optional[int], limit: Optional[int]) -> List[Dict[str, Any]]:
    payloads = json.loads(path.read_text(encoding="utf-8"))
    if lead_id is not None:
        payloads = [payload for payload in payloads if int(payload["id"]) == int(lead_id)]
    if limit is not None:
        payloads = payloads[: int(limit)]
    return payloads


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aplica payloads seguros do preview no Kommo.")
    parser.add_argument("--payloads-path", default=str(DEFAULT_PAYLOADS_PATH))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--state-path", default=str(DEFAULT_STATE_PATH))
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--lead-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true", help="Sem essa flag, apenas gera o plano.")
    return parser


def main() -> None:
    load_env_file(Path(".env"))
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
    logger = logging.getLogger("apply_kommo_safe_payloads")

    base_url = args.base_url.rstrip("/")
    output_dir = Path(args.output_dir)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    payloads = _load_payloads(Path(args.payloads_path), args.lead_id, args.limit)
    plan = [
        {
            "id": payload["id"],
            "lead_name": payload.get("lead_name"),
            "patch": _prepare_patch_payload(payload, args.timezone),
        }
        for payload in payloads
    ]
    _write_json(output_dir / f"{run_id}_plan.json", plan)
    logger.info("Payloads seguros carregados: %s", len(plan))

    if not args.apply:
        logger.info("Modo preview. Use --apply para enviar ao Kommo.")
        return

    try:
        session = _session(
            base_url=base_url,
            state_path=Path(args.state_path),
            email=__import__("os").getenv("KOMMO_EMAIL"),
            password=__import__("os").getenv("KOMMO_PASSWORD"),
        )
    except Exception as exc:
        logger.error("Falha ao autenticar no Kommo antes de iniciar a aplicacao: %s", exc)
        logger.debug("Detalhes tecnicos:\n%s", traceback.format_exc())
        raise SystemExit(1)

    results: List[Dict[str, Any]] = []
    total = len(plan)
    for index, item in enumerate(plan, start=1):
        lead_id = int(item["id"])
        lead_name = item.get("lead_name") or f"Lead {lead_id}"
        try:
            result = _request_json(
                session,
                "PATCH",
                f"{base_url}/api/v4/leads/{lead_id}",
                json=item["patch"],
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            results.append({"id": lead_id, "lead_name": item.get("lead_name"), "ok": True, "result": result})
            logger.info("[%s/%s] OK  %s (id=%s)", index, total, lead_name, lead_id)
        except Exception as exc:
            results.append({"id": lead_id, "lead_name": item.get("lead_name"), "ok": False, "error": str(exc)})
            logger.info("[%s/%s] ERR %s (id=%s) -> %s", index, total, lead_name, lead_id, exc)

    _write_json(output_dir / f"{run_id}_result.json", results)
    ok_count = sum(1 for item in results if item["ok"])
    error_count = len(results) - ok_count
    logger.info("Aplicacao concluida: sucesso=%s | erro=%s", ok_count, error_count)
    if error_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
