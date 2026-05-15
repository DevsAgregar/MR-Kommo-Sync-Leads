import unittest
import logging
import tempfile
from datetime import datetime
from pathlib import Path

from clinic_kommo_payload_preview import FIELD_SPECS, _build_patient_candidate_values, _decide_direct_action
from clinic_operational_fields_sync import _build_snapshot, _extract_patient_financial_summary
from env_config import load_env_file
from login import SQLitePatientStore, _normalizar_documento_generico, _normalizar_nome_busca


PATIENT_FINANCIAL_HTML = """
<div class="paciente-financeiro">
  <a id="href-aberto"><span class="span-dinheiro">R$ <br>0,00</span></a>
  <a id="href-atrasado"><span class="span-dinheiro">R$ <br>198,00</span></a>
  <a id="href-pago"><span class="span-dinheiro">R$ <br>180,00</span></a>
  <table>
    <tbody>
      <tr class="pago">
        <td>Receitas Particulares</td>
        <td>Estetica Facial</td>
        <td>1</td>
        <td>30/04/2026</td>
        <td>30/04/2026</td>
        <td>R$ 180,00</td>
        <td>R$ 180,00</td>
        <td>Matriz</td>
      </tr>
      <tr class="atrasado">
        <td>Receitas Particulares</td>
        <td>Estetica Facial</td>
        <td>1</td>
        <td>30/04/2026</td>
        <td></td>
        <td>R$ 289,90</td>
        <td>R$ 0,00</td>
        <td>Matriz</td>
      </tr>
    </tbody>
  </table>
</div>
"""


class _FakeOperationalLogger:
    def warning(self, *_args, **_kwargs) -> None:
        pass


class _FakeOperationalExtractor:
    def __init__(self, agendamentos_html: str) -> None:
        self.agendamentos_html = agendamentos_html
        self.logger = _FakeOperationalLogger()

    def get_patient_edit_html(self, _patient_id: int) -> str:
        return "<html></html>"

    def get_patient_agendamentos_html(self, _patient_id: int) -> str:
        return self.agendamentos_html

    def get_patient_financial_html(self, _patient_id: int) -> str:
        return PATIENT_FINANCIAL_HTML

    def get_agenda_event(self, _agenda_id: int) -> dict:
        return {}


class PatientPaidFinancialsTest(unittest.TestCase):
    def test_extract_patient_financial_summary_tracks_paid_not_overdue(self) -> None:
        summary = _extract_patient_financial_summary(PATIENT_FINANCIAL_HTML)

        self.assertEqual(summary["financeiro_pago_total"], 180.0)
        self.assertEqual(summary["financeiro_atraso_total"], 198.0)
        self.assertEqual(summary["financeiro_aberto_total"], 0.0)
        self.assertEqual(summary["financeiro_pago_linhas"], 1)
        self.assertEqual(summary["financeiro_ultimo_pago"], 180.0)
        self.assertEqual(summary["financeiro_ultimo_pago_data"], "2026-04-30")

    def test_kommo_payload_prefers_paid_financials_over_sales_report(self) -> None:
        patient = {
            "patient_id": 764,
            "nome": "Allan Carlos Guimaraes",
            "data_nascimento": "1994-01-17",
            "status": "Ativo",
            "total_vendido_liquido": 756.0,
            "total_vendas_linhas": 4,
            "last_sale_value": 198.0,
            "financeiro_pago_total": 180.0,
            "financeiro_pago_linhas": 1,
            "financeiro_ultimo_pago": 180.0,
            "financeiro_ultimo_pago_data": "2026-04-30",
            "servicos_json": "[]",
        }

        candidates = _build_patient_candidate_values(patient, {1561319: {}, 1561309: {}})

        self.assertEqual(candidates[0]["candidate_value"], 180.0)
        self.assertEqual(candidates[0]["rule"], "patient_financial_latest_paid_value")
        self.assertEqual(candidates[1561947]["candidate_value"], 180.0)
        self.assertEqual(candidates[1561947]["rule"], "patient_financial_paid_total")
        self.assertIsNone(candidates[1559587]["candidate_value"])
        self.assertEqual(candidates[1559587]["rule"], "operational_visit_count")

    def test_kommo_payload_uses_operational_visit_count(self) -> None:
        patient = {
            "patient_id": 764,
            "nome": "Allan Carlos Guimaraes",
            "data_nascimento": "1994-01-17",
            "status": "Ativo",
            "total_vendido_liquido": 756.0,
            "total_vendas_linhas": 4,
            "last_sale_value": 198.0,
            "financeiro_pago_total": 180.0,
            "financeiro_pago_linhas": 1,
            "financeiro_ultimo_pago": 180.0,
            "financeiro_ultimo_pago_data": "2026-04-30",
            "visitas_count": 7,
            "servicos_json": "[]",
        }

        candidates = _build_patient_candidate_values(patient, {1561319: {}, 1561309: {}})

        self.assertEqual(candidates[1559587]["candidate_value"], 7)
        self.assertEqual(candidates[1559587]["rule"], "operational_visit_count")

    def test_operational_snapshot_counts_valid_past_visits(self) -> None:
        extractor = _FakeOperationalExtractor(
            agendamentos_html="""
            <div class="linha_agendamentos_paciente">
              <div class="time-text"><span>01/05/2026</span><span>10:00</span></div>
              <a href="/agenda/index/101"></a>
              <div class="agendamentos-col-sta">Status Finalizado</div>
            </div>
            <div class="linha_agendamentos_paciente">
              <div class="time-text"><span>10/05/2026</span><span>11:00</span></div>
              <a href="/agenda/index/102"></a>
              <div class="agendamentos-col-sta">Status Não compareceu</div>
            </div>
            <div class="linha_agendamentos_paciente">
              <div class="time-text"><span>12/05/2026</span><span>09:30</span></div>
              <a href="/agenda/index/103"></a>
              <div class="agendamentos-col-sta">Status Atendido</div>
            </div>
            <div class="linha_agendamentos_paciente">
              <div class="time-text"><span>16/05/2026</span><span>09:00</span></div>
              <a href="/agenda/index/104"></a>
              <div class="agendamentos-col-sta">Status Agendado</div>
            </div>
            """
        )

        snapshot = _build_snapshot(extractor, patient_id=764, now=datetime(2026, 5, 15, 12, 0))

        self.assertEqual(snapshot["visitas_count"], 2)
        self.assertEqual(snapshot["ultima_visita"], "2026-05-12 09:30:00")
        self.assertEqual(snapshot["agendamento"], "2026-05-16 09:00:00")

    def test_paid_financial_values_are_authoritative_even_when_lower(self) -> None:
        sale_spec = next(spec for spec in FIELD_SPECS if spec.slug == "sale_value")
        billed_spec = next(spec for spec in FIELD_SPECS if spec.slug == "billed_total")

        self.assertEqual(
            _decide_direct_action(sale_spec, current_raw=198, candidate_raw=180)[0],
            "sync_authoritative",
        )
        self.assertEqual(
            _decide_direct_action(billed_spec, current_raw=756, candidate_raw=180)[0],
            "sync_authoritative",
        )

    def test_financial_sales_full_refresh_replaces_same_start_date(self) -> None:
        logger = logging.getLogger("test_financial_sales_full_refresh")
        logger.addHandler(logging.NullHandler())
        load_env_file(Path(".env"))
        with tempfile.TemporaryDirectory() as temp_dir:
            store = SQLitePatientStore(Path(temp_dir) / "patients.sqlite3", logger)
            try:
                store.upsert_patients(
                    [
                        {
                            "patient_id": "764",
                            "nome": "Allan Carlos Guimaraes",
                            "data_nasc": "17/01/1994",
                            "telefone_1": None,
                            "telefone_2": None,
                            "telefone_3": None,
                            "matricula": None,
                            "convenio": None,
                            "sexo": None,
                            "etnia": None,
                            "responsaveis": None,
                            "nome_mae": None,
                            "cpf": "040.518.001-21",
                            "identidade": None,
                            "cep": None,
                            "endereco": None,
                            "email": None,
                            "profissao": None,
                            "status": "Ativo",
                            "cidade": None,
                            "bairro": None,
                            "plano": None,
                            "cpf_responsavel": None,
                            "cns": None,
                        }
                    ],
                    "01/01/1900",
                    "15/05/2026",
                    "pacientes.xlsx",
                )
                rows = [
                    _sale_row(744, 180.0, "Pix", "Daniela Barbosa"),
                    _sale_row(1605, 198.0, "Cartao de Credito", None),
                ]

                store.replace_financial_sales(rows, "01/01/1900", "14/05/2026", "vendas.xlsx")
                store.replace_financial_sales(rows, "01/01/1900", "15/05/2026", "vendas.xlsx")

                summary = store.conn.execute(
                    "SELECT total_vendas_linhas, total_vendido_liquido FROM vw_patient_financial_summary WHERE patient_id = 764"
                ).fetchone()
                self.assertEqual(summary["total_vendas_linhas"], 2)
                self.assertEqual(summary["total_vendido_liquido"], 378.0)
            finally:
                store.close()


def _sale_row(source_row_number: int, value: float, payment_type: str, professional: str | None) -> dict:
    name = "Allan Carlos Guimaraes"
    document = "040.518.001-21"
    return {
        "source_row_number": source_row_number,
        "competencia": "30/04/2026",
        "interessado": name,
        "interessado_norm": _normalizar_nome_busca(name),
        "interessado_documento": _normalizar_documento_generico(document),
        "categoria": "Receitas Particulares",
        "subcategoria": "Estetica Facial",
        "observacoes": None,
        "vezes": None,
        "total_bruto": value,
        "total_liquido": value,
        "descontos": 0.0,
        "tipo_pagamento": payment_type,
        "cpf_cnpj_interessado": document,
        "profissional": professional,
    }


if __name__ == "__main__":
    unittest.main()
