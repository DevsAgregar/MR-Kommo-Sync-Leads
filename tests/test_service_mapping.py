import unittest

from clinic_kommo_field_mappings import map_service_items


SERVICE_ENUMS = {
    "bioregeneradores": {"value": "Bioregeneradores", "enum_id": 10},
    "botox": {"value": "Botox", "enum_id": 11},
    "co2 resurfacing": {"value": "CO2 Resurfacing", "enum_id": 12},
    "glowing complexion essencial": {"value": "Glowing Complexion Essencial", "enum_id": 13},
    "massagem sos": {"value": "Massagem SOS", "enum_id": 14},
    "peeling": {"value": "Peeling", "enum_id": 15},
    "synergy prime": {"value": "Synergy Prime", "enum_id": 16},
}


class ServiceMappingTest(unittest.TestCase):
    def test_dermo_mr_pdrn_exossomos_maps_to_bioregeneradores(self) -> None:
        [result] = map_service_items(
            ["Dermo Mr (Sérum Bioregenerativo - Pdrn + Exossomos)"],
            SERVICE_ENUMS,
        )

        self.assertEqual(result.mapped_values, ("Bioregeneradores",))
        self.assertEqual(result.confidence, "high")

    def test_spa_urbano_does_not_block_known_service_bundle(self) -> None:
        results = map_service_items(
            [
                "Retorno - Botox",
                "Co2 Glow-Up",
                "Peeling Químico - Pós Botox",
                "Limpeza de Pele Express",
                "Massagem Facial Relaxante",
                "Synergy Prime - Botox Rosto Completo Feminino",
                "Spa Urbano",
            ],
            SERVICE_ENUMS,
        )

        actionable = [item for item in results if item.rule != "no_equivalent"]
        self.assertTrue(actionable)
        self.assertTrue(all(item.mapped_values for item in actionable))
        self.assertTrue(all(item.confidence == "high" for item in actionable))
        self.assertEqual(results[-1].mapped_values, ())
        self.assertEqual(results[-1].rule, "no_equivalent")


if __name__ == "__main__":
    unittest.main()
