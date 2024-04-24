from google.cloud import bigquery

def create_bigquery_client():
    """Crea e restituisce un client BigQuery."""
    return bigquery.Client()

