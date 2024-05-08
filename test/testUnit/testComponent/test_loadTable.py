import unittest
import json  # Add this import statement
import os
from unittest.mock import patch, MagicMock
from src.component.table_creator.conf.utils.loadTable import BigQueryTableCreator

class TestBigQueryTableCreator(unittest.TestCase):
    @patch('src.component.table_creator.conf.utils.loadTable.bigquery.Client')
    def test_create_or_update_table_existing(self, mock_client):
        # Mock the configuration file path
        config_file = "test_config.json"

        # Mock the return values for retrieve_query and retrieve_schema
        mock_query = "SELECT * FROM table"
        mock_schema = [{"name": "field1", "type": "STRING"}, {"name": "field2", "type": "INTEGER"}]

        # Mock the content of the config file
        config_content = {
            "dataset_name": "test_dataset",
            "table_name": "test_table",
            "query_location": "test_query_location",
            "schema_location": "test_schema_location"
        }
        with open(config_file, "w") as f:
            json.dump(config_content, f)

        # Initialize the class with the mocked config file path
        table_creator = BigQueryTableCreator(config_file)

        # Mock the return values for retrieve_query and retrieve_schema methods
        table_creator.retrieve_query = MagicMock(return_value=mock_query)
        table_creator.retrieve_schema = MagicMock(return_value=mock_schema)

        # Mock the return value for get_table method
        mock_table = MagicMock()
        mock_client.return_value.get_table.return_value = mock_table

        # Call the method being tested
        table_creator.create_or_update_table()

        # Assertions
        mock_client.return_value.update_table.assert_called_once_with(mock_table, ["schema"])

        # Clean up: Remove the temporary config file
        os.remove(config_file)

    @patch('src.component.table_creator.conf.utils.loadTable.bigquery.Client')
    def test_create_or_update_table_not_found(self, mock_client):
        # Mock the configuration file path
        config_file = "test_config.json"

        # Mock the return values for retrieve_query and retrieve_schema
        mock_query = "SELECT * FROM table"
        mock_schema = [{"name": "field1", "type": "STRING"}, {"name": "field2", "type": "INTEGER"}]

        # Mock the content of the config file
        config_content = {
            "dataset_name": "test_dataset",
            "table_name": "test_table",
            "query_location": "test_query_location",
            "schema_location": "test_schema_location"
        }
        with open(config_file, "w") as f:
            json.dump(config_content, f)

        # Initialize the class with the mocked config file path
        table_creator = BigQueryTableCreator(config_file)

        # Mock the return values for retrieve_query and retrieve_schema methods
        table_creator.retrieve_query = MagicMock(return_value=mock_query)
        table_creator.retrieve_schema = MagicMock(return_value=mock_schema)

        # Mock the behavior for get_table method to raise an exception indicating table not found
        mock_client.return_value.get_table.side_effect = Exception("Not found")

        # Call the method being tested
        table_creator.create_or_update_table()

        # Assertions
        mock_client.return_value.create_table.assert_called_once()

        # Clean up: Remove the temporary config file
        os.remove(config_file)

if __name__ == '__main__':
    unittest.main()
