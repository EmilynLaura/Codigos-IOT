import unittest

import pandas as pd

from channel_integrations import extract_whatsapp_text_messages, parse_order_message
from pipeline_pdf_fretes import FreightKMeansSegmenter, FreightRegressionModel


class ParseOrderMessageTests(unittest.TestCase):
    def test_parses_portuguese_labeled_order(self):
        order = parse_order_message(
            "Origem: Cascavel - PR\n"
            "Destino: Curitiba - PR\n"
            "Peso da carga: 12 toneladas\n"
            "Valor do frete: R$ 4.500,00\n"
            "Data da solicitação: 12/10/2026"
        )

        self.assertEqual(order["origem"], "Cascavel - PR")
        self.assertEqual(order["destino"], "Curitiba - PR")
        self.assertEqual(order["peso"], "12 toneladas")
        self.assertEqual(order["valor_frete"], "R$ 4.500,00")
        self.assertEqual(order["data"], "12/10/2026")

    def test_rejects_order_without_required_fields(self):
        with self.assertRaisesRegex(ValueError, "destino, peso"):
            parse_order_message("Origem: Cascavel - PR")

    def test_rejects_non_numeric_weight(self):
        with self.assertRaisesRegex(ValueError, "Peso invalido"):
            parse_order_message("Origem: X\nDestino: Y\nPeso: desconhecido")

    def test_extracts_text_messages_from_meta_webhook(self):
        messages = extract_whatsapp_text_messages(
            {
                "entry": [
                    {
                        "changes": [
                            {
                                "value": {
                                    "messages": [
                                        {
                                            "id": "wamid.example",
                                            "type": "text",
                                            "text": {"body": "Origem: X\nDestino: Y\nPeso: 2 t"},
                                        },
                                        {"id": "wamid.image", "type": "image"},
                                    ]
                                }
                            }
                        ]
                    }
                ]
            }
        )

        self.assertEqual(
            messages,
            [{"id": "wamid.example", "body": "Origem: X\nDestino: Y\nPeso: 2 t"}],
        )


class FreightKMeansSegmenterTests(unittest.TestCase):
    def test_segments_new_demand_without_training_record(self):
        historical = pd.DataFrame(
            {
                "peso_kg": [5000, 8000, 14000, 18000, 26000, 32000],
                "valor_frete": [2200, 3000, 4900, 5100, 6800, 8400],
            }
        )
        segmenter = FreightKMeansSegmenter(k=3).fit(historical)

        result = segmenter.predict(35000, 14500)

        self.assertIn("perfil", result)
        self.assertIn("cluster", result)
        self.assertIn("anomalia", result)
        self.assertEqual(result["peso_kg"], 35000.0)
        self.assertEqual(result["valor_frete"], 14500.0)

    def test_rejects_negative_new_demand(self):
        segmenter = FreightKMeansSegmenter(k=2).fit(
            pd.DataFrame({"peso_kg": [1000, 2000], "valor_frete": [500, 800]})
        )

        with self.assertRaisesRegex(ValueError, "não podem ser negativos"):
            segmenter.predict(-1, 500)


class FreightRegressionModelTests(unittest.TestCase):
    def setUp(self):
        self.historical = pd.DataFrame(
            {
                "peso_kg": [5000, 8000, 12000, 16000, 20000, 24000, 28000, 32000],
                "valor_frete": [1500, 1800, 2200, 2600, 3000, 3400, 3800, 4200],
            }
        )

    def test_estimates_new_case_and_marks_extrapolation(self):
        model = FreightRegressionModel().fit(self.historical)

        estimate = model.predict(35000)

        self.assertAlmostEqual(estimate["frete_estimado"], 4500.0, places=2)
        self.assertTrue(estimate["fora_da_faixa_treinada"])

    def test_holdout_metrics_are_deterministic_and_beat_mean_baseline(self):
        first = FreightRegressionModel.evaluate(self.historical)
        second = FreightRegressionModel.evaluate(self.historical)

        self.assertEqual(first, second)
        self.assertEqual(first["registros_teste"], 2)
        self.assertEqual(first["mae_regressao"], 0.0)
        self.assertGreater(first["mae_linha_base"], first["mae_regressao"])
        self.assertEqual(len(first["casos_teste"]), 2)

    def test_rejects_insufficient_training_data(self):
        with self.assertRaisesRegex(ValueError, "ao menos dois"):
            FreightRegressionModel().fit(self.historical.iloc[:1])


if __name__ == "__main__":
    unittest.main()
