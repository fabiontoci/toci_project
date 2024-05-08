import json
import os
import logging
from google.cloud import bigquery

class BigQueryTableCreator:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def load_config(self, config_file):
        with open(config_file, "r") as f:
            return json.load(f)

    def retrieve_query(self):
        with open(self.config["query_location"], "r") as f:
            return f.read()

    def retrieve_schema(self):
        with open(self.config["schema_location"], "r") as f:
            return json.load(f)

    def create_or_update_table(self):
        client = bigquery.Client()
        dataset_name = self.config["dataset_name"]
        table_name = self.config["table_name"]
        query = self.retrieve_query()
        schema = self.retrieve_schema()

        dataset_ref = client.dataset(dataset_name)
        table_ref = dataset_ref.table(table_name)

        table = bigquery.Table(table_ref, schema=schema)

        try:
            existing_table = client.get_table(table_ref)
            self.logger.info(f"Table '{table_name}' already exists in dataset '{dataset_name}'. Updating...")
            existing_table.schema = schema
            client.update_table(existing_table, ["schema"])
            self.logger.info(f"Table '{table_name}' updated successfully.")
        except Exception as e:
            if "Not found" in str(e):
                self.logger.info(f"Table '{table_name}' does not exist. Creating...")
                client.create_table(table)
                self.logger.info(f"Table '{table_name}' created successfully in dataset '{dataset_name}'.")
            else:
                self.logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    os.environ["GOOGLE_CLOUD_PROJECT"] = "training-gcp-309207"
    config_file = "C:\\Users\\ETOCIFADF\\OneDrive - NTT DATA EMEAL\\Desktop\\project_ntt\\toci_project\\src\\data\\application\\table_saving\\snapshot.json"
    table_creator = BigQueryTableCreator(config_file)
    table_creator.create_or_update_table()
