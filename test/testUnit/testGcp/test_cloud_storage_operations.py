import unittest
from unittest.mock import MagicMock
from utils.cloud_storage_operations import list_files_in_bucket


class TestCloudStorageOperations(unittest.TestCase):
    def test_list_files_in_bucket(self):
        # Mock del client Cloud Storage
        client_mock = MagicMock()
        # Definizione di un nome fittizio del bucket
        bucket_name = "fake_bucket"
        # Simulazione della lista dei file nel bucket
        client_mock.get_bucket.return_value.list_blobs.return_value = [
            MagicMock(name="file1.txt"),
            MagicMock(name="file2.csv")
        ]

        # Esecuzione della funzione da testare
        with unittest.mock.patch('builtins.print') as mock_print:
            list_files_in_bucket(client_mock, bucket_name)

        # Verifica che la funzione print sia stata chiamata con i valori corretti
        mock_print.assert_any_call("Nome file: file1.txt")
        mock_print.assert_any_call("Nome file: file2.csv")


if __name__ == '__main__':
    unittest.main()
