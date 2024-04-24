import unittest
from unittest.mock import MagicMock
from utils.bigquery_operations import list_tables_and_schema


class TestBigQueryOperations(unittest.TestCase):
    def test_list_tables_and_schema(self):
        # Mock del client BigQuery
        client_mock = MagicMock()
        # Definizione di un dataset fittizio
        dataset_id = "fake_dataset"
        # Simulazione della lista delle tabelle
        client_mock.list_tables.return_value = [
            MagicMock(table_id="table1", schema=[MagicMock(name="column1", field_type="STRING")]),
            MagicMock(table_id="table2", schema=[MagicMock(name="column2", field_type="INTEGER")])
        ]

        # Esecuzione della funzione da testare
        with unittest.mock.patch('builtins.print') as mock_print:
            list_tables_and_schema(client_mock, dataset_id)

        # Verifica che la funzione print sia stata chiamata con i valori corretti
        mock_print.assert_any_call("Nome tabella: table1")
        mock_print.assert_any_call("\tCampo: column1, Tipo: STRING")
        mock_print.assert_any_call("Nome tabella: table2")
        mock_print.assert_any_call("\tCampo: column2, Tipo: INTEGER")


if __name__ == '__main__':
    unittest.main()
