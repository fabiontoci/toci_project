import csv
import chardet
from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery

def process_message(message):
    # Extract relevant information from the Pub/Sub message
    bucket_name = message.attributes['bucketId']
    file_name = message.attributes['objectId']

    # Initialize Cloud Storage client
    storage_client = storage.Client()

    # Get the file from Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content_bytes = blob.download_as_bytes()

    # Detect the encoding
    detected_encoding = chardet.detect(file_content_bytes)['encoding']
    if detected_encoding is None:
        detected_encoding = 'latin1'  # Fallback encoding

    file_content = file_content_bytes.decode(detected_encoding)

    # Filter out null bytes
    file_content = file_content.replace('\x00', '')

    # Initialize BigQuery client
    bq_client = bigquery.Client()

    # Upload the file to BigQuery
    dataset_id = 'toci_dataset'
    table_id = 'test_table'
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bq_client.get_table(table_ref)

    # Parse the CSV content with custom delimiter
    csv_reader = csv.DictReader(file_content.splitlines(), delimiter=';')

    # Print the fieldnames for debugging purposes
    print(f"CSV Headers: {csv_reader.fieldnames}")

    rows_to_insert = []
    for row in csv_reader:
        try:
            # Check if the 'id' field is not empty
            if row["id"]:
                rows_to_insert.append({
                    "id": int(row["id"]),
                    "name": row["name"]
                })
            else:
                print("Empty 'id' field encountered. Skipping row.")
        except KeyError as e:
            print(f"Missing key in row: {e}. Row content: {row}")
        except ValueError as e:
            print(f"Invalid value in 'id' field: {row['id']}. Skipping row.")

    # Insert rows into BigQuery
    if rows_to_insert:
        errors = bq_client.insert_rows(table, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
        else:
            print(f"File {file_name} uploaded to BigQuery table {dataset_id}.{table_id}")
    else:
        print("No valid rows to insert.")

def receive_messages(project_id, subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    def callback(message):
        process_message(message)
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)
    print(f'Started listening for messages on subscription {subscription_name}...')

    # Keep the main thread alive
    while True:
        pass

if __name__ == '__main__':
    project_id = 'training-gcp-309207'
    subscription_name = 'my-subscription'
    receive_messages(project_id, subscription_name)
