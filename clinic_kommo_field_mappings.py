from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


MAPPINGS_DIR = Path(__file__).resolve().parent / "mappings"
ORIGIN_MAPPING_CSV = MAPPINGS_DIR / "clinic_kommo_origin_mapping.csv"
SERVICE_MAPPING_CSV = MAPPINGS_DIR / "clinic_kommo_service_mapping.csv"


def normalize_token(value: object) -> Optional[str]:
    if value in (None, ""):
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = " ".join(text.split())
    return text or None


@dataclass(frozen=True)
class ScalarMapping:
    raw_value: str
    mapped_value: Optional[str]
    confidence: str
    rule: str


@dataclass(frozen=True)
class MultiMappingItem:
    raw_value: str
    mapped_values: Tuple[str, ...]
    confidence: str
    rule: str


def _load_mapping_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


@lru_cache(maxsize=1)
def origin_mapping_table() -> Dict[str, Tuple[str, str, str]]:
    table: Dict[str, Tuple[str, str, str]] = {}
    for row in _load_mapping_rows(ORIGIN_MAPPING_CSV):
        raw_value = normalize_token(row.get("raw_value"))
        mapped_value = row.get("mapped_value") or None
        confidence = row.get("confidence") or "none"
        rule = row.get("rule") or "table"
        if raw_value:
            table[raw_value] = (mapped_value or "", confidence, rule)
    return table


@lru_cache(maxsize=1)
def service_mapping_table() -> List[Tuple[str, str, str, str]]:
    rules: List[Tuple[str, str, str, str]] = []
    for row in _load_mapping_rows(SERVICE_MAPPING_CSV):
        raw_value = normalize_token(row.get("raw_value"))
        mapped_value = row.get("mapped_value") or None
        confidence = row.get("confidence") or "none"
        rule = row.get("rule") or "table"
        if raw_value and mapped_value:
            rules.append((raw_value, mapped_value, confidence, rule))
    return rules


def map_origin(raw_value: Optional[str]) -> ScalarMapping:
    cleaned = (raw_value or "").strip()
    normalized = normalize_token(cleaned)
    if not normalized:
        return ScalarMapping(raw_value=cleaned, mapped_value=None, confidence="none", rule="empty")

    origin_rules = origin_mapping_table()
    if normalized in origin_rules:
        mapped_value, confidence, rule = origin_rules[normalized]
        return ScalarMapping(
            raw_value=cleaned,
            mapped_value=mapped_value or None,
            confidence=confidence,
            rule=rule,
        )

    if normalized.startswith("dra ") or normalized.startswith("dr "):
        return ScalarMapping(raw_value=cleaned, mapped_value="Parceria", confidence="high", rule="doctor_prefix")

    return ScalarMapping(raw_value=cleaned, mapped_value=None, confidence="none", rule="unmapped")


def _best_direct_service_match(
    normalized: str,
    enum_by_normalized: Dict[str, Dict[str, object]],
) -> Optional[MultiMappingItem]:
    enum_info = enum_by_normalized.get(normalized)
    if enum_info is None:
        return None
    return MultiMappingItem(
        raw_value=normalized,
        mapped_values=(str(enum_info["value"]),),
        confidence="high",
        rule="direct_exact",
    )


def map_service_item(
    raw_value: Optional[str],
    enum_by_normalized: Dict[str, Dict[str, object]],
) -> MultiMappingItem:
    cleaned = (raw_value or "").strip()
    normalized = normalize_token(cleaned)
    if not normalized:
        return MultiMappingItem(raw_value=cleaned, mapped_values=(), confidence="none", rule="empty")

    direct = _best_direct_service_match(normalized, enum_by_normalized)
    if direct is not None:
        return MultiMappingItem(
            raw_value=cleaned,
            mapped_values=direct.mapped_values,
            confidence=direct.confidence,
            rule=direct.rule,
        )

    for alias, mapped_value, confidence, rule in service_mapping_table():
        if alias in normalized:
            mapped_values = tuple(value.strip() for value in mapped_value.split("|") if value.strip())
            return MultiMappingItem(raw_value=cleaned, mapped_values=mapped_values, confidence=confidence, rule=rule)

    contains: List[Tuple[int, str]] = []
    for enum_normalized, enum_info in enum_by_normalized.items():
        if enum_normalized in normalized or normalized in enum_normalized:
            contains.append((len(enum_normalized), str(enum_info["value"])))
    if len(contains) == 1:
        return MultiMappingItem(raw_value=cleaned, mapped_values=(contains[0][1],), confidence="medium", rule="contains_single")
    if contains:
        contains.sort(reverse=True)
        if len(contains) == 1 or contains[0][0] > contains[1][0]:
            return MultiMappingItem(raw_value=cleaned, mapped_values=(contains[0][1],), confidence="medium", rule="contains_longest")

    return MultiMappingItem(raw_value=cleaned, mapped_values=(), confidence="none", rule="unmapped")


def map_service_items(
    raw_values: Sequence[str],
    enum_by_normalized: Dict[str, Dict[str, object]],
) -> List[MultiMappingItem]:
    return [map_service_item(value, enum_by_normalized) for value in raw_values]
