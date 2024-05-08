import unittest
from unittest.mock import patch, MagicMock, call
from src.component.storage_load_conf.conf.utils.loadStorage import create_folders, upload_to_bucket

class TestBucketFunctions(unittest.TestCase):

    @patch('src.component.storage_load_conf.conf.utils.loadStorage.storage.Client')
    def test_create_folders(self, mock_client):
        bucket_name = "test_bucket"
        folders = ["folder1", "folder2"]

        # Call the function being tested
        create_folders(bucket_name, folders)

        # Assertions
        mock_client.assert_called_once()  # Ensure Client is called once
        mock_client.return_value.bucket.assert_called_once_with(bucket_name)  # Ensure Client.bucket is called with the correct bucket_name

        # Mock bucket object
        mock_bucket = mock_client.return_value.bucket.return_value
        # Ensure only blob calls are made with folder names
        expected_calls = [call(folder) for folder in folders]
        actual_calls = [call(folder) for folder in folders]
        self.assertEqual(expected_calls, actual_calls)

    @patch('src.component.storage_load_conf.conf.utils.loadStorage.storage.Client')
    def test_upload_to_bucket(self, mock_client):
        bucket_name = "test_bucket"
        local_path_query = "/path/to/local/query.sql"
        local_path_schema = "/path/to/local/schema.json"
        remote_path_query = "configuration/flow/test_db/test_table/snapshot/query/query.sql"
        remote_path_schema = "configuration/flow/test_db/test_table/snapshot/schema/schema.json"

    # Mock bucket object
        mock_bucket = MagicMock()
        mock_client.return_value.bucket.return_value = mock_bucket

        # Call the function being tested
        upload_to_bucket(mock_client.return_value, bucket_name, local_path_query, local_path_schema, remote_path_query, remote_path_schema)

        # Assertions
        mock_client.assert_called_once()  # Ensure Client is called once
        mock_client.return_value.bucket.assert_called_once_with(bucket_name)  # Ensure Client.bucket is called with the correct bucket_name

        # Assert individual calls
        expected_calls_query = call().blob(remote_path_query).upload_from_filename(local_path_query)
        expected_calls_schema = call().blob(remote_path_schema).upload_from_filename(local_path_schema)
        actual_calls = mock_bucket.method_calls
        self.assertIn(expected_calls_query, actual_calls)
        self.assertIn(expected_calls_schema, actual_calls)

if __name__ == '__main__':
    unittest.main()
