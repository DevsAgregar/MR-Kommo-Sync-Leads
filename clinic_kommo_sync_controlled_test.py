#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from db_util import connect as db_connect, sqlite3
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


DEFAULT_PATIENT_DB = Path("mirella_pacientes.sqlite3")
DEFAULT_KOMMO_DB = Path("mirella_kommo_leads.sqlite3")
DEFAULT_OUTPUT_DIR = Path("exports") / "sync_test"


@dataclass(frozen=True)
class FieldSpec:
    field_id: int
    slug: str
    label: str
    value_kind: str


FIELD_SPECS: Sequence[FieldSpec] = (
    FieldSpec(1561315, "birthday", "Data de aniversário", "date"),
    FieldSpec(1561939, "age_bucket", "Faixa Etária", "text"),
    FieldSpec(1559591, "status", "Status do Cliente", "text"),
    FieldSpec(1561947, "billed_total", "Faturado", "numeric"),
    FieldSpec(1559587, "visits", "Visitas", "integer"),
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
    text = text.replace(".", "").replace(",", ".")
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
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def _comparable_value(kind: str, value: Any) -> Optional[str]:
    if kind == "numeric":
        return _normalize_numeric(value)
    if kind == "integer":
        return _normalize_integer(value)
    if kind == "date":
        return _normalize_date(value)
    return _normalize_text(value)


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


def _load_patients(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            patient_id,
            nome,
            data_nascimento,
            status,
            total_vendido_liquido,
            total_vendas_linhas
        FROM vw_patients_complete_financial
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _load_leads(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute("SELECT lead_id, name FROM kommo_leads").fetchall()
    return [dict(row) for row in rows]


def _load_current_field_values(conn: sqlite3.Connection) -> Dict[int, Dict[int, Optional[str]]]:
    field_ids = ",".join(str(spec.field_id) for spec in FIELD_SPECS)
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


def _build_patient_candidate_values(patient: Dict[str, Any]) -> Dict[int, Optional[str]]:
    age = _calculate_age(patient.get("data_nascimento"))
    return {
        1561315: _normalize_date(patient.get("data_nascimento")),
        1561939: _age_bucket(age),
        1559591: _normalize_text(patient.get("status")),
        1561947: _normalize_numeric(patient.get("total_vendido_liquido")),
        1559587: _normalize_integer(patient.get("total_vendas_linhas")),
    }


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
        "patient_unique_name_count": sum(1 for row in patients if (name := _normalize_name(row["nome"])) and patient_name_counter[name] == 1),
        "lead_unique_name_count": sum(1 for row in leads if (name := _normalize_name(row["name"])) and lead_name_counter[name] == 1),
        "overlap_name_count": overlap_names,
        "ambiguous_overlap_name_count": ambiguous_overlap_names,
        "exact_unique_match_count": len(matches),
    }


def _collect_rows(
    matches: Sequence[Tuple[Dict[str, Any], Dict[str, Any]]],
    current_field_values: Dict[int, Dict[int, Optional[str]]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
    summary = {
        spec.slug: {
            "matched_pairs": 0,
            "candidate_nonempty": 0,
            "kommo_filled": 0,
            "kommo_missing": 0,
            "fillable_missing": 0,
            "different_nonempty": 0,
        }
        for spec in FIELD_SPECS
    }
    rows: List[Dict[str, Any]] = []

    for patient, lead in matches:
        candidates = _build_patient_candidate_values(patient)
        current_values = current_field_values.get(lead["lead_id"], {})
        for spec in FIELD_SPECS:
            candidate_value = candidates.get(spec.field_id)
            if candidate_value in (None, ""):
                continue

            current_value_raw = current_values.get(spec.field_id)
            current_value = _comparable_value(spec.value_kind, current_value_raw)
            candidate_value = _comparable_value(spec.value_kind, candidate_value)
            if candidate_value in (None, ""):
                continue

            summary[spec.slug]["matched_pairs"] += 1
            summary[spec.slug]["candidate_nonempty"] += 1

            would_fill_missing = not current_value
            if would_fill_missing:
                summary[spec.slug]["kommo_missing"] += 1
                summary[spec.slug]["fillable_missing"] += 1
            else:
                summary[spec.slug]["kommo_filled"] += 1
                if current_value != candidate_value:
                    summary[spec.slug]["different_nonempty"] += 1

            rows.append(
                {
                    "match_strategy": "exact_unique_normalized_name",
                    "patient_id": patient["patient_id"],
                    "lead_id": lead["lead_id"],
                    "patient_name": patient["nome"],
                    "lead_name": lead["name"],
                    "field_id": spec.field_id,
                    "field_slug": spec.slug,
                    "field_label": spec.label,
                    "current_value": current_value or "",
                    "candidate_value": candidate_value or "",
                    "would_fill_missing": 1 if would_fill_missing else 0,
                    "would_overwrite_nonempty": 1 if current_value and current_value != candidate_value else 0,
                }
            )

    return rows, summary


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


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(
    path: Path,
    match_summary: Dict[str, int],
    field_summary: Dict[str, Dict[str, int]],
    candidate_rows: Sequence[Dict[str, Any]],
    contact_reference_shape: Sequence[str],
) -> None:
    safe_rows = [row for row in candidate_rows if row["would_fill_missing"] == 1]
    lines = [
        "# Controlled Sync Test",
        "",
        "## Baseline",
        "",
        f"- Patients analyzed: `{match_summary['patient_count']}`",
        f"- Kommo leads analyzed: `{match_summary['lead_count']}`",
        f"- Overlapping normalized names: `{match_summary['overlap_name_count']}`",
        f"- Ambiguous overlapping names: `{match_summary['ambiguous_overlap_name_count']}`",
        f"- Exact unique matches: `{match_summary['exact_unique_match_count']}`",
        "",
        "## Safe v1 Strategy",
        "",
        "Use only `exact_unique_normalized_name` matches and fill only empty Kommo fields.",
        "",
        "## Field Opportunities",
        "",
        "| Field | Candidate rows | Fill missing | Would overwrite non-empty |",
        "| --- | ---: | ---: | ---: |",
    ]

    for spec in FIELD_SPECS:
        stats = field_summary[spec.slug]
        lines.append(
            f"| {spec.label} | {stats['candidate_nonempty']} | {stats['fillable_missing']} | {stats['different_nonempty']} |"
        )

    lines.extend(
        [
            "",
            "## Kommo Contact Payload",
            "",
            "The cached lead dataset only stores contact references, not phone/email/address values.",
            f"- Contact keys in cache: `{', '.join(contact_reference_shape)}`",
            "- App implication: lead custom fields can be updated on `leads`, but phone/email/address should be handled on linked `contacts` after fetching the contact entity.",
            "",
            "## Recommended App Rollout",
            "",
            "1. v1: update only empty fields `Data de aniversário`, `Faixa Etária`, `Status do Cliente`, `Faturado`, `Visitas` on exact unique matches.",
            "2. v2: fetch Kommo contacts and add CPF/phone/email matching to expand match coverage safely.",
            "3. v3: only after audit approval, consider overwriting non-empty Kommo values when clinic data is fresher.",
            "",
            f"Safe candidate rows generated: `{len(safe_rows)}`",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run(patient_db: Path, kommo_db: Path, output_dir: Path) -> Dict[str, Any]:
    patient_conn = db_connect(patient_db)
    patient_conn.row_factory = sqlite3.Row
    kommo_conn = db_connect(kommo_db)
    kommo_conn.row_factory = sqlite3.Row

    try:
        patients = _load_patients(patient_conn)
        leads = _load_leads(kommo_conn)
        current_field_values = _load_current_field_values(kommo_conn)
        matches, match_summary = _match_exact_unique_names(patients, leads)
        candidate_rows, field_summary = _collect_rows(matches, current_field_values)
        safe_rows = [row for row in candidate_rows if row["would_fill_missing"] == 1]

        contact_shape_row = kommo_conn.execute(
            """
            SELECT contacts_json
            FROM kommo_leads
            WHERE contacts_json IS NOT NULL AND TRIM(contacts_json) <> ''
            LIMIT 1
            """
        ).fetchone()
        contact_reference_shape: List[str] = []
        if contact_shape_row:
            contact_reference = json.loads(contact_shape_row["contacts_json"])[0]
            contact_reference_shape = sorted(contact_reference.keys())

        payload = {
            "match_summary": match_summary,
            "field_summary": field_summary,
            "safe_candidate_row_count": len(safe_rows),
            "contact_reference_shape": contact_reference_shape,
            "field_specs": [spec.__dict__ for spec in FIELD_SPECS],
        }

        output_dir.mkdir(parents=True, exist_ok=True)
        _write_csv(output_dir / "clinic_kommo_controlled_candidates.csv", safe_rows)
        _write_json(output_dir / "clinic_kommo_controlled_summary.json", payload)
        _write_markdown(
            output_dir / "clinic_kommo_controlled_summary.md",
            match_summary=match_summary,
            field_summary=field_summary,
            candidate_rows=candidate_rows,
            contact_reference_shape=contact_reference_shape,
        )
        return payload
    finally:
        patient_conn.close()
        kommo_conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an offline controlled sync test between Clínica Ágil patients and Kommo leads."
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
