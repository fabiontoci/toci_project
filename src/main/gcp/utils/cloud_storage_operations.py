def list_files_in_bucket(client, bucket_name):
    """Restituisce una lista dei nomi dei file all'interno del bucket specificato."""
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    file_names = [blob.name for blob in blobs]
    return file_names
