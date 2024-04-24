def list_tables(client, dataset_id):
    """Restituisce una lista dei nomi delle tabelle all'interno del dataset specificato."""
    tables = client.list_tables(dataset_id)
    table_names = [table.table_id for table in tables]
    return table_names

def get_table_schema(client, dataset_id, table_id):
    """Restituisce lo schema della tabella specificata nel dataset."""
    table_ref = client.get_table(f"{client.project}.{dataset_id}.{table_id}")
    return table_ref.schema
