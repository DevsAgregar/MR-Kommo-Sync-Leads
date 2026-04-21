#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import unicodedata
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from clinic_kommo_field_mappings import (
    ScalarMapping,
    MultiMappingItem,
    map_origin,
    map_service_items,
    normalize_token,
)


DEFAULT_PATIENT_DB = Path("mirella_pacientes.sqlite3")
DEFAULT_KOMMO_DB = Path("mirella_kommo_leads.sqlite3")
DEFAULT_OUTPUT_DIR = Path("exports") / "sync_preview"
SAFE_ACTIONS = {"fill_empty", "update_if_greater", "update_if_newer", "merge"}


@dataclass(frozen=True)
class FieldSpec:
    field_id: Optional[int]
    slug: str
    label: str
    value_kind: str
    target: str = "custom_field"


FIELD_SPECS: Sequence[FieldSpec] = (
    FieldSpec(None, "sale_value", "Venda", "numeric", "lead_price"),
    FieldSpec(1561315, "birthday", "Data de aniversário", "date"),
    FieldSpec(1559593, "birthday_month", "Aniversariantes do Mês", "text"),
    FieldSpec(1561939, "age_bucket", "Faixa Etária", "text"),
    FieldSpec(1559591, "status", "Status do Cliente", "text"),
    FieldSpec(1561947, "billed_total", "Faturado", "numeric"),
    FieldSpec(1559587, "visits", "Visitas", "integer"),
    FieldSpec(1561317, "last_visit", "Última visita", "date"),
    FieldSpec(1555897, "appointment", "Agendamento", "datetime"),
    FieldSpec(1574511, "next_consultation", "Próxima consulta", "datetime"),
    FieldSpec(1561319, "origin", "Origem", "select"),
    FieldSpec(1561309, "service", "Serviço", "multiselect"),
)


def _normalize_name(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9]+", " ", text.lower())
    text = " ".join(text.split())
    return text or None


def _normalize_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _normalize_numeric(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}"
    text = str(value).strip().replace("R$", "").strip()
    if not text:
        return None
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return f"{float(text):.2f}"
    except ValueError:
        return None


def _normalize_integer(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"[^\d-]+", "", text)
    if not digits:
        return None
    try:
        return str(int(digits))
    except ValueError:
        return None


def _normalize_date(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(int(text)).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text[:10]


def _normalize_datetime(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            return datetime.fromtimestamp(int(text)).strftime("%Y-%m-%d %H:%M")
        except (ValueError, OSError):
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M")
        except ValueError:
            continue
    return text[:16]


def _comparable_value(kind: str, value: Any) -> Optional[str]:
    if kind == "numeric":
        return _normalize_numeric(value)
    if kind == "integer":
        return _normalize_integer(value)
    if kind == "date":
        return _normalize_date(value)
    if kind == "datetime":
        return _normalize_datetime(value)
    return _normalize_text(value)


def _numeric_float(value: Any) -> Optional[float]:
    normalized = _normalize_numeric(value)
    if normalized is None:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _lead_price_integer(value: Any) -> Optional[int]:
    numeric = _numeric_float(value)
    if numeric is None:
        return None
    return int(round(numeric))


def _integer_value(value: Any) -> Optional[int]:
    normalized = _normalize_integer(value)
    if normalized is None:
        return None
    try:
        return int(normalized)
    except ValueError:
        return None


def _datetime_sort_value(kind: str, value: Any) -> Optional[datetime]:
    normalized = _normalize_datetime(value) if kind == "datetime" else _normalize_date(value)
    if not normalized:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    return None


def _is_empty_value(value: Any) -> bool:
    if value in (None, ""):
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (int, float)):
        return float(value) == 0
    return False


def _current_multiselect_values(raw_current: Any) -> List[str]:
    if raw_current in (None, ""):
        return []
    values = [item.strip() for item in str(raw_current).split(";") if item.strip()]
    result: List[str] = []
    for value in values:
        if value not in result:
            result.append(value)
    return result


def _decide_direct_action(spec: FieldSpec, current_raw: Any, candidate_raw: Any) -> Tuple[str, Optional[str], Optional[str]]:
    current = _comparable_value(spec.value_kind, current_raw)
    candidate = _comparable_value(spec.value_kind, candidate_raw)
    if candidate in (None, ""):
        return "skip", current, candidate

    if spec.slug == "sale_value":
        current_number = _lead_price_integer(current_raw)
        candidate_number = _lead_price_integer(candidate_raw)
        if candidate_number is None or candidate_number <= 0:
            return "skip", current, None
        if current_number is None:
            return "fill_empty", current, f"{candidate_number:.2f}"
        if candidate_number > current_number:
            return "update_if_greater", current, f"{candidate_number:.2f}"
        return "skip", current, f"{candidate_number:.2f}"

    if _is_empty_value(current_raw):
        return "fill_empty", current, candidate

    if spec.slug == "billed_total":
        current_number = _numeric_float(current_raw)
        candidate_number = _numeric_float(candidate_raw)
        if current_number is not None and candidate_number is not None and candidate_number > current_number:
            return "update_if_greater", current, candidate
        return "skip", current, candidate

    if spec.slug == "visits":
        current_number = _integer_value(current_raw)
        candidate_number = _integer_value(candidate_raw)
        if current_number is not None and candidate_number is not None and candidate_number > current_number:
            return "update_if_greater", current, candidate
        return "skip", current, candidate

    if spec.slug in {"last_visit", "appointment", "next_consultation"}:
        current_date = _datetime_sort_value(spec.value_kind, current_raw)
        candidate_date = _datetime_sort_value(spec.value_kind, candidate_raw)
        if current_date is not None and candidate_date is not None and candidate_date > current_date:
            return "update_if_newer", current, candidate
        if current != candidate and spec.slug in {"appointment", "next_consultation"}:
            return "update_if_newer", current, candidate
        return "skip", current, candidate

    return "skip", current, candidate


def _calculate_age(birth_iso: Optional[str]) -> Optional[int]:
    normalized = _normalize_date(birth_iso)
    if not normalized:
        return None
    birth = datetime.strptime(normalized, "%Y-%m-%d").date()
    today = date.today()
    return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))


def _age_bucket(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    if age < 18:
        return "Menor de 18"
    if age <= 24:
        return "18-24"
    if age <= 34:
        return "25-34"
    if age <= 44:
        return "35-44"
    if age <= 54:
        return "45-54"
    if age <= 64:
        return "55-64"
    return "65+"


def _birthday_month_name(birth_iso: Optional[str]) -> Optional[str]:
    normalized = _normalize_date(birth_iso)
    if not normalized:
        return None
    month = int(normalized[5:7])
    names = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    return names.get(month)


def _load_patients(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        WITH ranked_sales AS (
            SELECT
                matched_patient_id AS patient_id,
                competencia AS last_sale_competencia,
                total_liquido AS last_sale_value,
                ROW_NUMBER() OVER (
                    PARTITION BY matched_patient_id
                    ORDER BY
                        substr(competencia, 7, 4) || '-' || substr(competencia, 4, 2) || '-' || substr(competencia, 1, 2) DESC,
                        sale_id DESC
                ) AS rn
            FROM patient_financial_sales
            WHERE matched_patient_id IS NOT NULL
        )
        SELECT
            ops.patient_id,
            ops.nome,
            ops.data_nascimento,
            ops.status,
            ops.total_vendido_liquido,
            ops.total_vendas_linhas,
            ops.origem,
            ops.ultima_visita,
            ops.agendamento,
            ops.proxima_consulta,
            ops.servicos_json,
            sales.last_sale_competencia,
            sales.last_sale_value
        FROM vw_patients_complete_operational ops
        LEFT JOIN ranked_sales sales
            ON sales.patient_id = ops.patient_id
           AND sales.rn = 1
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _load_leads(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute("SELECT lead_id, name, price FROM kommo_leads").fetchall()
    return [dict(row) for row in rows]


def _load_current_field_values(conn: sqlite3.Connection) -> Dict[int, Dict[int, Optional[str]]]:
    field_ids = ",".join(str(spec.field_id) for spec in FIELD_SPECS if spec.field_id is not None)
    current_values: Dict[int, Dict[int, Optional[str]]] = defaultdict(dict)
    for row in conn.execute(
        f"""
        SELECT lead_id, field_id, value_text
        FROM kommo_lead_field_values
        WHERE field_id IN ({field_ids})
        """
    ):
        current_values[row["lead_id"]][row["field_id"]] = row["value_text"]
    return current_values


def _load_field_enums(conn: sqlite3.Connection) -> Dict[int, Dict[str, Dict[str, object]]]:
    enum_map: Dict[int, Dict[str, Dict[str, object]]] = {}
    for field_id in (1561319, 1561309):
        row = conn.execute(
            "SELECT enums_json FROM kommo_lead_custom_fields WHERE field_id = ?",
            (field_id,),
        ).fetchone()
        if row is None or row["enums_json"] is None:
            enum_map[field_id] = {}
            continue
        values = json.loads(row["enums_json"])
        enum_map[field_id] = {
            normalize_token(item["value"]): {"id": item["id"], "value": item["value"]}
            for item in values
            if normalize_token(item["value"])
        }
    return enum_map


def _match_exact_unique_names(
    patients: Sequence[Dict[str, Any]],
    leads: Sequence[Dict[str, Any]],
) -> Tuple[List[Tuple[Dict[str, Any], Dict[str, Any]]], Dict[str, int]]:
    patient_name_counter = Counter(_normalize_name(row["nome"]) for row in patients if _normalize_name(row["nome"]))
    lead_name_counter = Counter(_normalize_name(row["name"]) for row in leads if _normalize_name(row["name"]))

    lead_by_name = {
        _normalize_name(row["name"]): row
        for row in leads
        if _normalize_name(row["name"]) and lead_name_counter[_normalize_name(row["name"])] == 1
    }

    matches: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    overlap_names = 0
    ambiguous_overlap_names = 0

    for normalized_name in set(patient_name_counter):
        if normalized_name in lead_name_counter:
            overlap_names += 1
            if patient_name_counter[normalized_name] != 1 or lead_name_counter[normalized_name] != 1:
                ambiguous_overlap_names += 1

    for patient in patients:
        normalized_name = _normalize_name(patient["nome"])
        if not normalized_name:
            continue
        if patient_name_counter[normalized_name] != 1 or lead_name_counter[normalized_name] != 1:
            continue
        lead = lead_by_name.get(normalized_name)
        if lead is None:
            continue
        matches.append((patient, lead))

    return matches, {
        "patient_count": len(patients),
        "lead_count": len(leads),
        "overlap_name_count": overlap_names,
        "ambiguous_overlap_name_count": ambiguous_overlap_names,
        "exact_unique_match_count": len(matches),
    }


def _build_patient_candidate_values(
    patient: Dict[str, Any],
    enum_maps: Dict[int, Dict[str, Dict[str, object]]],
) -> Dict[int, Dict[str, Any]]:
    age = _calculate_age(patient.get("data_nascimento"))
    origin_result = map_origin(_normalize_text(patient.get("origem")))
    raw_services = json.loads(patient["servicos_json"]) if patient.get("servicos_json") else []
    service_results = map_service_items(raw_services, enum_maps[1561309])
    flattened_services: List[str] = []
    for item in service_results:
        for mapped in item.mapped_values:
            if mapped not in flattened_services:
                flattened_services.append(mapped)
    all_services_mapped = bool(raw_services) and all(item.mapped_values for item in service_results)
    all_services_high = all_services_mapped and all(item.confidence == "high" for item in service_results)

    return {
        0: {
            "kind": "numeric",
            "candidate_value": patient.get("last_sale_value"),
            "confidence": "high",
            "rule": "financial_latest_sale_value",
        },
        1561315: {
            "kind": "date",
            "candidate_value": patient.get("data_nascimento"),
            "confidence": "high",
            "rule": "direct_patient_birthdate",
        },
        1559593: {
            "kind": "text",
            "candidate_value": _birthday_month_name(patient.get("data_nascimento")),
            "confidence": "high",
            "rule": "derived_birthday_month_name",
        },
        1561939: {
            "kind": "text",
            "candidate_value": _age_bucket(age),
            "confidence": "high",
            "rule": "derived_age_bucket",
        },
        1559591: {
            "kind": "text",
            "candidate_value": patient.get("status"),
            "confidence": "high",
            "rule": "direct_patient_status",
        },
        1561947: {
            "kind": "numeric",
            "candidate_value": patient.get("total_vendido_liquido"),
            "confidence": "high",
            "rule": "financial_summary_total_vendido_liquido",
        },
        1559587: {
            "kind": "integer",
            "candidate_value": patient.get("total_vendas_linhas"),
            "confidence": "high",
            "rule": "financial_summary_total_vendas_linhas",
        },
        1561317: {
            "kind": "date",
            "candidate_value": patient.get("ultima_visita"),
            "confidence": "high",
            "rule": "operational_last_visit",
        },
        1555897: {
            "kind": "datetime",
            "candidate_value": patient.get("agendamento"),
            "confidence": "high",
            "rule": "operational_next_appointment",
        },
        1574511: {
            "kind": "datetime",
            "candidate_value": patient.get("proxima_consulta"),
            "confidence": "high",
            "rule": "operational_next_consultation",
        },
        1561319: {
            "kind": "select",
            "candidate_value": origin_result.mapped_value,
            "confidence": origin_result.confidence,
            "rule": origin_result.rule,
            "raw_value": origin_result.raw_value,
        },
        1561309: {
            "kind": "multiselect",
            "candidate_value": flattened_services,
            "confidence": (
                "high" if all_services_high else
                "medium" if all_services_mapped else
                "none"
            ),
            "rule": "service_mapping_bundle",
            "raw_value": raw_services,
            "mapping_items": [item.__dict__ for item in service_results],
        },
    }


def _enum_preview_value(
    field_id: int,
    mapped_value: str,
    enum_maps: Dict[int, Dict[str, Dict[str, object]]],
) -> Optional[Dict[str, Any]]:
    enum_info = enum_maps.get(field_id, {}).get(normalize_token(mapped_value))
    if enum_info is None:
        return None
    return {
        "enum_id": enum_info["id"],
        "value": enum_info["value"],
    }


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(
    path: Path,
    match_summary: Dict[str, int],
    field_stats: Dict[str, Dict[str, int]],
    safe_lead_payloads: Sequence[Dict[str, Any]],
    safe_rows: Sequence[Dict[str, Any]],
    review_rows: Sequence[Dict[str, Any]],
) -> None:
    lines = [
        "# Kommo Payload Preview",
        "",
        "## Match Base",
        "",
        f"- Patients analyzed: `{match_summary['patient_count']}`",
        f"- Kommo leads analyzed: `{match_summary['lead_count']}`",
        f"- Exact unique matches: `{match_summary['exact_unique_match_count']}`",
        "",
        "## Safe Preview",
        "",
        f"- Leads with safe payloads: `{len(safe_lead_payloads)}`",
        f"- Safe field rows: `{len(safe_rows)}`",
        f"- Review field rows: `{len(review_rows)}`",
        "",
        "## Field Coverage",
        "",
        "| Field | Candidate | Safe fill | Needs review | Unmapped |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]

    for spec in FIELD_SPECS:
        stats = field_stats[spec.slug]
        lines.append(
            f"| {spec.label} | {stats['candidate']} | {stats['safe_fill']} | {stats['review_fill']} | {stats['unmapped']} |"
        )

    lines.extend(
        [
            "",
            "## Action Policy",
            "",
            "- `fill_empty`: Kommo value is empty and clinic value is available.",
            "- `update_if_greater`: clinic numeric value is greater than Kommo.",
            "- `update_if_newer`: clinic date/datetime is newer or the next appointment changed.",
            "- `merge`: multiselect receives mapped values without removing existing values.",
            "- `skip`: no safe change is needed.",
            "",
            "## Notes",
            "",
            "- Safe payloads include fill, greater/newer updates, and multiselect merges according to field policy.",
            "- `Venda` is previewed through the lead root field `price`, using the latest clinic sale value.",
            "- `Origem` and `Serviço` use mapping rules and can fall into review when confidence is not high.",
            "- `Serviço` preview may include one or more enum values because the Kommo field is multiselect.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run(patient_db: Path, kommo_db: Path, output_dir: Path) -> Dict[str, Any]:
    patient_conn = sqlite3.connect(str(patient_db))
    patient_conn.row_factory = sqlite3.Row
    kommo_conn = sqlite3.connect(str(kommo_db))
    kommo_conn.row_factory = sqlite3.Row

    try:
        patients = _load_patients(patient_conn)
        leads = _load_leads(kommo_conn)
        current_field_values = _load_current_field_values(kommo_conn)
        enum_maps = _load_field_enums(kommo_conn)
        matches, match_summary = _match_exact_unique_names(patients, leads)

        field_stats = {
            spec.slug: {
                "candidate": 0,
                "safe_fill": 0,
                "review_fill": 0,
                "unmapped": 0,
                "fill_empty": 0,
                "update_if_greater": 0,
                "update_if_newer": 0,
                "merge": 0,
                "skip": 0,
            }
            for spec in FIELD_SPECS
        }
        safe_rows: List[Dict[str, Any]] = []
        review_rows: List[Dict[str, Any]] = []
        all_rows: List[Dict[str, Any]] = []
        safe_payloads_by_lead: Dict[int, Dict[str, Any]] = {}

        for patient, lead in matches:
            candidates = _build_patient_candidate_values(patient, enum_maps)
            lead_current = current_field_values.get(lead["lead_id"], {})
            for spec in FIELD_SPECS:
                candidate_key = 0 if spec.field_id is None else spec.field_id
                candidate = candidates[candidate_key]
                kind = candidate["kind"]

                raw_current = lead.get("price") if spec.target == "lead_price" else lead_current.get(spec.field_id)
                if kind == "multiselect":
                    current_values = _current_multiselect_values(raw_current)
                    mapped_values = candidate["candidate_value"] or []
                    if candidate.get("raw_value"):
                        field_stats[spec.slug]["candidate"] += 1
                    if not mapped_values:
                        if candidate.get("raw_value"):
                            field_stats[spec.slug]["unmapped"] += 1
                            row = {
                                "match_strategy": "exact_unique_normalized_name",
                                "patient_id": patient["patient_id"],
                                "lead_id": lead["lead_id"],
                                "patient_name": patient["nome"],
                                "lead_name": lead["name"],
                                "field_slug": spec.slug,
                                "field_label": spec.label,
                                "action": "review",
                                "current_value": raw_current or "",
                                "candidate_value": json.dumps(candidate.get("raw_value") or [], ensure_ascii=False),
                                "mapped_value": "",
                                "confidence": candidate["confidence"],
                                "rule": candidate["rule"],
                            }
                            review_rows.append(row)
                            all_rows.append(row)
                        continue

                    preview_values = [
                        _enum_preview_value(spec.field_id, mapped_value, enum_maps)
                        for mapped_value in mapped_values
                    ]
                    preview_values = [value for value in preview_values if value is not None]
                    if not preview_values:
                        field_stats[spec.slug]["unmapped"] += 1
                        continue

                    confidence = candidate["confidence"]
                    new_preview_values = [
                        item for item in preview_values if item["value"] not in current_values
                    ]
                    if not new_preview_values:
                        field_stats[spec.slug]["skip"] += 1
                        all_rows.append(
                            {
                                "match_strategy": "exact_unique_normalized_name",
                                "patient_id": patient["patient_id"],
                                "lead_id": lead["lead_id"],
                                "patient_name": patient["nome"],
                                "lead_name": lead["name"],
                                "field_slug": spec.slug,
                                "field_label": spec.label,
                                "action": "skip",
                                "current_value": raw_current or "",
                                "candidate_value": json.dumps(candidate.get("raw_value") or [], ensure_ascii=False),
                                "mapped_value": json.dumps([item["value"] for item in preview_values], ensure_ascii=False),
                                "confidence": confidence,
                                "rule": candidate["rule"],
                            }
                        )
                        continue

                    action = "fill_empty" if not current_values else "merge"
                    row = {
                        "match_strategy": "exact_unique_normalized_name",
                        "patient_id": patient["patient_id"],
                        "lead_id": lead["lead_id"],
                        "patient_name": patient["nome"],
                        "lead_name": lead["name"],
                        "field_slug": spec.slug,
                        "field_label": spec.label,
                        "action": action,
                        "current_value": raw_current or "",
                        "candidate_value": json.dumps(candidate.get("raw_value") or [], ensure_ascii=False),
                        "mapped_value": json.dumps([item["value"] for item in preview_values], ensure_ascii=False),
                        "confidence": confidence,
                        "rule": candidate["rule"],
                    }
                    if confidence == "high":
                        field_stats[spec.slug]["safe_fill"] += 1
                        field_stats[spec.slug][action] += 1
                        safe_rows.append(row)
                        all_rows.append(row)
                        payload = safe_payloads_by_lead.setdefault(
                            lead["lead_id"],
                            {"id": lead["lead_id"], "lead_name": lead["name"], "custom_fields_values": []},
                        )
                        existing_preview_values = [
                            _enum_preview_value(spec.field_id, current_value, enum_maps)
                            for current_value in current_values
                        ]
                        existing_preview_values = [value for value in existing_preview_values if value is not None]
                        combined_values: List[Dict[str, Any]] = []
                        seen_enum_ids: set[Any] = set()
                        for value in [*existing_preview_values, *preview_values]:
                            enum_id = value.get("enum_id")
                            if enum_id in seen_enum_ids:
                                continue
                            seen_enum_ids.add(enum_id)
                            combined_values.append(value)
                        payload["custom_fields_values"].append(
                            {"field_id": spec.field_id, "field_name": spec.label, "values": combined_values}
                        )
                    else:
                        field_stats[spec.slug]["review_fill"] += 1
                        review_rows.append(row)
                        all_rows.append(row)
                    continue

                comparable_current = _comparable_value(spec.value_kind, raw_current)
                comparable_candidate = _comparable_value(spec.value_kind, candidate["candidate_value"])
                if comparable_candidate in (None, ""):
                    continue

                field_stats[spec.slug]["candidate"] += 1
                action, comparable_current, comparable_candidate = _decide_direct_action(
                    spec,
                    raw_current,
                    candidate["candidate_value"],
                )
                if action == "skip":
                    field_stats[spec.slug]["skip"] += 1
                    all_rows.append(
                        {
                            "match_strategy": "exact_unique_normalized_name",
                            "patient_id": patient["patient_id"],
                            "lead_id": lead["lead_id"],
                            "patient_name": patient["nome"],
                            "lead_name": lead["name"],
                            "field_slug": spec.slug,
                            "field_label": spec.label,
                            "action": action,
                            "current_value": comparable_current or "",
                            "candidate_value": comparable_candidate or "",
                            "mapped_value": comparable_candidate or "",
                            "confidence": candidate["confidence"],
                            "rule": candidate["rule"],
                        }
                    )
                    continue

                row = {
                    "match_strategy": "exact_unique_normalized_name",
                    "patient_id": patient["patient_id"],
                    "lead_id": lead["lead_id"],
                    "patient_name": patient["nome"],
                    "lead_name": lead["name"],
                    "field_slug": spec.slug,
                    "field_label": spec.label,
                    "action": action,
                    "current_value": comparable_current or "",
                    "candidate_value": comparable_candidate,
                    "mapped_value": comparable_candidate,
                    "confidence": candidate["confidence"],
                    "rule": candidate["rule"],
                }

                if spec.target == "lead_price":
                    field_stats[spec.slug]["safe_fill"] += 1
                    field_stats[spec.slug][action] += 1
                    safe_rows.append(row)
                    all_rows.append(row)
                    payload = safe_payloads_by_lead.setdefault(
                        lead["lead_id"],
                        {"id": lead["lead_id"], "lead_name": lead["name"], "custom_fields_values": []},
                    )
                    payload["price"] = float(comparable_candidate)
                    continue

                if kind == "select":
                    mapped = candidate["candidate_value"]
                    if mapped is None:
                        field_stats[spec.slug]["unmapped"] += 1
                        row["action"] = "review"
                        review_rows.append(row)
                        all_rows.append(row)
                        continue
                    preview_value = _enum_preview_value(spec.field_id, mapped, enum_maps)
                    if preview_value is None:
                        field_stats[spec.slug]["unmapped"] += 1
                        row["action"] = "review"
                        review_rows.append(row)
                        all_rows.append(row)
                        continue
                    row["mapped_value"] = preview_value["value"]
                    if candidate["confidence"] == "high":
                        field_stats[spec.slug]["safe_fill"] += 1
                        field_stats[spec.slug][action] += 1
                        safe_rows.append(row)
                        all_rows.append(row)
                        payload = safe_payloads_by_lead.setdefault(
                            lead["lead_id"],
                            {"id": lead["lead_id"], "lead_name": lead["name"], "custom_fields_values": []},
                        )
                        payload["custom_fields_values"].append(
                            {"field_id": spec.field_id, "field_name": spec.label, "values": [preview_value]}
                        )
                    else:
                        field_stats[spec.slug]["review_fill"] += 1
                        row["action"] = "review"
                        review_rows.append(row)
                        all_rows.append(row)
                    continue

                # direct fields
                field_stats[spec.slug]["safe_fill"] += 1
                field_stats[spec.slug][action] += 1
                safe_rows.append(row)
                all_rows.append(row)
                payload = safe_payloads_by_lead.setdefault(
                    lead["lead_id"],
                    {"id": lead["lead_id"], "lead_name": lead["name"], "custom_fields_values": []},
                )
                payload["custom_fields_values"].append(
                    {
                        "field_id": spec.field_id,
                        "field_name": spec.label,
                        "values": [{"value": comparable_candidate}],
                    }
                )

        safe_payloads = list(safe_payloads_by_lead.values())
        action_counts = Counter(row["action"] for row in all_rows)
        payload = {
            "match_summary": match_summary,
            "field_stats": field_stats,
            "action_counts": dict(action_counts),
            "safe_lead_count": len(safe_payloads),
            "safe_field_row_count": len(safe_rows),
            "review_field_row_count": len(review_rows),
            "all_action_row_count": len(all_rows),
        }

        output_dir.mkdir(parents=True, exist_ok=True)
        _write_json(output_dir / "clinic_kommo_safe_payloads.json", safe_payloads)
        _write_csv(output_dir / "clinic_kommo_all_actions.csv", all_rows)
        _write_csv(output_dir / "clinic_kommo_safe_rows.csv", safe_rows)
        _write_csv(output_dir / "clinic_kommo_review_rows.csv", review_rows)
        _write_json(output_dir / "clinic_kommo_preview_summary.json", payload)
        _write_markdown(
            output_dir / "clinic_kommo_preview_summary.md",
            match_summary=match_summary,
            field_stats=field_stats,
            safe_lead_payloads=safe_payloads,
            safe_rows=safe_rows,
            review_rows=review_rows,
        )
        return payload
    finally:
        patient_conn.close()
        kommo_conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate preview payloads for filling Kommo leads from Clínica Ágil data."
    )
    parser.add_argument("--patient-db", default=str(DEFAULT_PATIENT_DB))
    parser.add_argument("--kommo-db", default=str(DEFAULT_KOMMO_DB))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    payload = run(
        patient_db=Path(args.patient_db),
        kommo_db=Path(args.kommo_db),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
