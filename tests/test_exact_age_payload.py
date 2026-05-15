import unittest

from clinic_kommo_payload_preview import FIELD_SPECS, _build_patient_candidate_values, _decide_direct_action


class ExactAgePayloadTest(unittest.TestCase):
    def test_age_field_uses_exact_age_not_bucket(self) -> None:
        patient = {
            "patient_id": 549,
            "nome": "Elisangela Quirino de Melo",
            "data_nascimento": "1984-11-24",
            "status": "Ativo",
            "servicos_json": "[]",
        }

        candidates = _build_patient_candidate_values(patient, {1561319: {}, 1561309: {}})

        self.assertEqual(candidates[1561939]["kind"], "integer")
        self.assertEqual(candidates[1561939]["candidate_value"], 41)
        self.assertEqual(candidates[1561939]["rule"], "derived_exact_age")

    def test_age_field_corrects_existing_bucket_value(self) -> None:
        age_spec = next(spec for spec in FIELD_SPECS if spec.field_id == 1561939)

        action, _current, candidate = _decide_direct_action(
            age_spec,
            current_raw="35-44",
            candidate_raw=41,
        )

        self.assertEqual(action, "sync_authoritative")
        self.assertEqual(candidate, "41")


if __name__ == "__main__":
    unittest.main()
