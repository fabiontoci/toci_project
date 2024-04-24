from google.cloud import storage

def create_cloud_storage_client():
    """Crea e restituisce un client Cloud Storage."""
    return storage.Client()

