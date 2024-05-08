import os
from google.cloud import storage

def create_folders(bucket_name, folders):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for folder in folders:
        blob = bucket.blob(folder)
        blob.upload_from_string('')  # Upload an empty file to create the folder

def upload_to_bucket(bucket_name, local_path_query, local_path_schema, remote_path_query, remote_path_schema):
    os.environ["GOOGLE_CLOUD_PROJECT"] = "training-gcp-309207"
    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create folders if they don't exist
    create_folders(bucket_name, os.path.dirname(remote_path_query).split('/'))

    # Upload the query file
    blob_query = bucket.blob(remote_path_query)
    blob_query.upload_from_filename(local_path_query)

    print(f"File {local_path_query} uploaded to gs://{bucket_name}/{remote_path_query}")

    # Upload the schema file
    blob_schema = bucket.blob(remote_path_schema)
    blob_schema.upload_from_filename(local_path_schema)

    print(f"File {local_path_schema} uploaded to gs://{bucket_name}/{remote_path_schema}")

if __name__ == "__main__":
    # Specify your bucket name
    bucket_name = "bucket_toci"

    # Specify the local path of the files you want to upload
    local_path_query = "C:\\Users\\ETOCIFADF\\OneDrive - NTT DATA EMEAL\\Desktop\\project_ntt\\toci_project\\conf\\flow\\test_db\\test_table\\snapshot\\query\\query.sql"
    local_path_schema = "C:\\Users\\ETOCIFADF\\OneDrive - NTT DATA EMEAL\\Desktop\\project_ntt\\toci_project\\conf\\flow\\test_db\\test_table\\snapshot\\schema\\schema.json"

    # Specify the remote paths where you want to store the files in the bucket
    remote_path_query = "configuration/flow/test_db/test_table/snapshot/query/query.sql"
    remote_path_schema = "configuration/flow/test_db/test_table/snapshot/schema/schema.json"

    # Upload the files to the bucket
    upload_to_bucket(bucket_name, local_path_query, local_path_schema, remote_path_query, remote_path_schema)
