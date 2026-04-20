from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


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
    mapped_value: Optional[str]
    confidence: str
    rule: str


ORIGIN_DIRECT_RULES: Dict[str, Tuple[str, str, str]] = {
    "anuncio": ("Anúncio Meta", "high", "direct_exact"),
    "facebook": ("Anúncio Meta", "medium", "channel_family"),
    "instagran": ("Instagram", "high", "direct_exact_typo"),
    "instagram": ("Instagram", "high", "direct_exact"),
    "site": ("Site", "high", "direct_exact"),
    "google": ("Site", "medium", "search_to_site"),
    "internet": ("Site", "medium", "generic_web"),
    "passou em frente": ("Agendamento Espontâneo", "high", "walk_in"),
    "amigo": ("Indicação", "high", "referral"),
    "atraves de outro paciente": ("Indicação", "high", "referral"),
    "parente": ("Indicação", "high", "referral"),
    "funcionaria": ("Indicação", "medium", "internal_referral"),
    "funcionaria": ("Indicação", "medium", "internal_referral"),
    "funcionaria": ("Indicação", "medium", "internal_referral"),
    "funcionaria": ("Indicação", "medium", "internal_referral"),
    "dra mirella": ("Parceria", "medium", "professional_referral"),
    "dra eloisa": ("Parceria", "medium", "professional_referral"),
    "dra diandra": ("Parceria", "medium", "professional_referral"),
    "daniela barbosa": ("Parceria", "medium", "professional_referral"),
    "bruna costa": ("Parceria", "medium", "professional_referral"),
    "raquel rocha": ("Parceria", "medium", "professional_referral"),
    "renan simoes": ("Indicação", "medium", "named_referral"),
    "medico": ("Parceria", "high", "medical_partner"),
    "atraves de outra clinica": ("Parceria", "high", "clinic_partner"),
    "salao absolut": ("Parceria", "high", "business_partner"),
    "dr luis neto cirurgiao plastico": ("Parceria", "high", "medical_partner"),
    "colegio ipe indicacao da dra mirella": ("Parceria", "medium", "partner_network"),
    "monyke secretaria dr waldivino guimaraes": ("Parceria", "high", "medical_partner"),
}


SERVICE_ALIAS_RULES: Sequence[Tuple[str, str, str, str]] = (
    ("botox", "Botox", "high", "substring"),
    ("microagulhamento", "Microagulhamento", "high", "substring"),
    ("skinbooster", "Skinbooster", "high", "substring"),
    ("lavieen", "Lavieen", "high", "substring"),
    ("criolipolise", "Criolipólise", "high", "substring"),
    ("criofrequencia", "Criofrequência", "high", "substring"),
    ("enzima", "Enzimas", "medium", "family"),
    ("luz pulsada", "Luz Pulsada", "high", "substring"),
    ("microfocado full face", "Ultrassom Microfocado Full Face", "high", "substring"),
    ("microfocado papada", "Ultrassom Microfocado Papada", "high", "substring"),
    ("microfocado pescoco", "Ultrassom Microfocado Pescoço", "high", "substring"),
    ("microfocado pescoço", "Ultrassom Microfocado Pescoço", "high", "substring"),
    ("microfocado olhos", "Ultrassom Microfocado Olhos", "high", "substring"),
    ("area dos olhos", "Ultrassom Microfocado Olhos", "medium", "semantic"),
    ("microfocado abdomen", "Ultrassom Microfocado Abdômen", "high", "substring"),
    ("microfocado abdômen", "Ultrassom Microfocado Abdômen", "high", "substring"),
    ("microfocado contorno", "Ultrassom Contorno", "medium", "semantic"),
    ("ultrassom contorno", "Ultrassom Contorno", "high", "substring"),
    ("peeling", "Peeling", "high", "substring"),
    ("pdrn", "PDRN", "high", "substring"),
    ("ozonioterapia", "Ozonioterapia", "high", "substring"),
    ("jato de plasma", "Jato de Plasma", "high", "substring"),
    ("soroterapia", "Soroterapia", "high", "substring"),
    ("radiofrequencia", "Radiofrequência", "high", "substring"),
    ("radiofrequência", "Radiofrequência", "high", "substring"),
    ("fios pdo", "Fios de PDO", "high", "substring"),
    ("bioestimulador", "Bioestimulador de Colágeno", "high", "family"),
    ("acido hialuronico", "Ácido Hialurônico", "high", "substring"),
    ("acido hialurônico", "Ácido Hialurônico", "high", "substring"),
    ("preenchimento labial", "Ácido Hialurônico", "medium", "treatment_family"),
    ("preenchimento facial", "Ácido Hialurônico", "medium", "treatment_family"),
    ("preenchimento olheira", "Ácido Hialurônico", "medium", "treatment_family"),
    ("pontilhismo facial", "Pontilhismo Facial AH", "high", "substring"),
    ("rinomodelacao", "Rinomodelação", "high", "substring"),
    ("rinomodelação", "Rinomodelação", "high", "substring"),
    ("co2 glow up", "CO2 Glow-Up", "high", "substring"),
    ("co2 full face", "CO2 Resurfacing", "medium", "co2_family"),
    ("co2 colo", "CO2 Resurfacing", "medium", "co2_family"),
    ("co2 pescoco", "CO2 Resurfacing", "medium", "co2_family"),
    ("co2 pescoço", "CO2 Resurfacing", "medium", "co2_family"),
    ("glowing complexion plus", "Glowing Complexion Plus", "high", "substring"),
    ("glowing complexion", "Glowing Complexion Bioregenere Plus", "medium", "family"),
    ("massagem facial relaxante", "Massagem S.O.S Relaxante", "medium", "family"),
    ("drenagem local", "Drenagem Linfática", "medium", "family"),
    ("drenagem linfatica", "Drenagem Linfática", "high", "substring"),
    ("drenagem linfática", "Drenagem Linfática", "high", "substring"),
    ("tratamento capilar", "Protocolo Capilar", "high", "semantic"),
)


def map_origin(raw_value: Optional[str]) -> ScalarMapping:
    cleaned = (raw_value or "").strip()
    normalized = normalize_token(cleaned)
    if not normalized:
        return ScalarMapping(raw_value=cleaned, mapped_value=None, confidence="none", rule="empty")

    if normalized in ORIGIN_DIRECT_RULES:
        mapped_value, confidence, rule = ORIGIN_DIRECT_RULES[normalized]
        return ScalarMapping(raw_value=cleaned, mapped_value=mapped_value, confidence=confidence, rule=rule)

    if normalized.startswith("dra ") or normalized.startswith("dr "):
        return ScalarMapping(raw_value=cleaned, mapped_value="Parceria", confidence="medium", rule="doctor_prefix")

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
        mapped_value=str(enum_info["value"]),
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
        return MultiMappingItem(raw_value=cleaned, mapped_value=None, confidence="none", rule="empty")

    direct = _best_direct_service_match(normalized, enum_by_normalized)
    if direct is not None:
        return MultiMappingItem(
            raw_value=cleaned,
            mapped_value=direct.mapped_value,
            confidence=direct.confidence,
            rule=direct.rule,
        )

    for alias, mapped_value, confidence, rule in SERVICE_ALIAS_RULES:
        if alias in normalized:
            return MultiMappingItem(raw_value=cleaned, mapped_value=mapped_value, confidence=confidence, rule=rule)

    contains: List[Tuple[int, str]] = []
    for enum_normalized, enum_info in enum_by_normalized.items():
        if enum_normalized in normalized or normalized in enum_normalized:
            contains.append((len(enum_normalized), str(enum_info["value"])))
    if len(contains) == 1:
        return MultiMappingItem(raw_value=cleaned, mapped_value=contains[0][1], confidence="medium", rule="contains_single")
    if contains:
        contains.sort(reverse=True)
        if len(contains) == 1 or contains[0][0] > contains[1][0]:
            return MultiMappingItem(raw_value=cleaned, mapped_value=contains[0][1], confidence="medium", rule="contains_longest")

    return MultiMappingItem(raw_value=cleaned, mapped_value=None, confidence="none", rule="unmapped")


def map_service_items(
    raw_values: Sequence[str],
    enum_by_normalized: Dict[str, Dict[str, object]],
) -> List[MultiMappingItem]:
    return [map_service_item(value, enum_by_normalized) for value in raw_values]
