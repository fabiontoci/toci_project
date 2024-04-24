from utils import bigquery_connector, bigquery_operations, cloud_storage_connector, cloud_storage_operations


def main():
    # Connetti a BigQuery
    bq_client = bigquery_connector.create_bigquery_client()

    # Esegui operazioni su BigQuery
    dataset_id = 'il_tuo_dataset'
    tables = bigquery_operations.list_tables(bq_client, dataset_id)
    print("Nomi delle tabelle nel dataset:")
    print(tables)

    table_id = 'il_tuo_table'
    schema = bigquery_operations.get_table_schema(bq_client, dataset_id, table_id)
    print("Schema della tabella:")
    print(schema)

    # Connetti a Cloud Storage
    storage_client = cloud_storage_connector.create_cloud_storage_client()

    # Esegui operazioni su Cloud Storage
    bucket_name = 'il_tuo_bucket'
    files = cloud_storage_operations.list_files_in_bucket(storage_client, bucket_name)
    print("Nomi dei file nel bucket:")
    print(files)


if __name__ == "__main__":
    main()
