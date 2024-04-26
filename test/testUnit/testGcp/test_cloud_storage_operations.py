import unittest
from unittest.mock import Mock, patch
from src.main.gcp.utils.cloud_storage_operations import list_files_in_bucket

class TestListFilesInBucket(unittest.TestCase):

    def test_list_files_in_bucket(self):
        # Mock del client Cloud Storage
        mock_client = Mock()
        # Mock del risultato della lista dei file
        mock_blob1 = Mock()
        mock_blob1.name = 'file1.txt'
        mock_blob2 = Mock()
        mock_blob2.name = 'file2.txt'
        mock_bucket = Mock()
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_client.get_bucket.return_value = mock_bucket

        # Chiamata alla funzione da testare
        bucket_name = 'fake_bucket'
        result = list_files_in_bucket(mock_client, bucket_name)

        # Verifica che la funzione restituisca correttamente i nomi dei file
        expected_result = ['file1.txt', 'file2.txt']
        self.assertEqual(result, expected_result)
        # Verifica che la funzione abbia chiamato correttamente i metodi del client Cloud Storage
        mock_client.get_bucket.assert_called_once_with(bucket_name)
        mock_bucket.list_blobs.assert_called_once_with()

if __name__ == "__main__":
    unittest.main()
