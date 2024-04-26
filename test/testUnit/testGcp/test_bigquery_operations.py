import unittest
from unittest.mock import Mock
from src.main.gcp.utils.bigquery_operations import list_tables, get_table_schema

class TestBigQueryOperations(unittest.TestCase):
    def test_list_tables(self):
        # Mock del client BigQuery
        mock_client = Mock()
        # Mock del risultato della lista delle tabelle
        mock_tables = [Mock(table_id='table1'), Mock(table_id='table2')]
        mock_client.list_tables.return_value = mock_tables

        # Chiamata alla funzione da testare
        dataset_id = 'fake_dataset'
        result = list_tables(mock_client, dataset_id)

        # Verifica che la funzione restituisca correttamente i nomi delle tabelle
        expected_result = ['table1', 'table2']
        self.assertEqual(result, expected_result)
        mock_client.list_tables.assert_called_once_with(dataset_id)

def test_get_table_schema(self):
    # Mock del client BigQuery
    mock_client = Mock()
    # Mock dello schema della tabella
    mock_schema = [{'name': 'col1', 'type': 'STRING'}, {'name': 'col2', 'type': 'INTEGER'}]
    mock_table = Mock(schema=mock_schema)

    # Impostiamo la chiamata a get_table per restituire il mock della tabella
    mock_client.get_table.return_value = mock_table

    # Chiamata alla funzione da testare
    dataset_id = 'fake_dataset'
    table_id = 'fake_table'
    result = get_table_schema(mock_client, dataset_id, table_id)

    # Verifica che la funzione restituisca correttamente lo schema della tabella
    expected_result = [{'name': 'col1', 'type': 'STRING'}, {'name': 'col2', 'type': 'INTEGER'}]
    self.assertEqual(result, expected_result)

    # Verifica che la funzione get_table sia stata chiamata correttamente
    mock_client.get_table.assert_called_once_with(f"fake-project.{dataset_id}.{table_id}")


if __name__ == "__main__":
    unittest.main()
