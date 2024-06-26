from google.cloud import bigquery

class BigQuerySetup:
    def __init__(self,project_id:str,dataset_id:str,table_id) -> None:
        self.project_id = project_id
        self.dataset_id:str = dataset_id
        self.table_id:str = table_id
        self.unique_project = f"{project_id}.{dataset_id}.{table_id}"
        self.client = bigquery.Client()
    
    def create_table(self,schema:list[object]):
        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)
        try:
            table = bigquery.Table(table_ref,schema=schema)
            table = self.client.create_table(table=table)
            print(f"Created table {self.table_id} in dataset {self.dataset_id}.")
        except Exception as e:
            print(f"An error occurred while creating the table: {e}")
    def insert_data(self, rows: list) -> None:
        """
        Inserts rows of data into the table.

        Args:
            rows (list): A list of dictionaries, each representing a row to insert.
        """
        
        table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
        errors = self.client.insert_rows_json(table_ref, rows)  # API request

        if errors == []:
            print(f"Inserted {len(rows)} rows into {self.table_id}.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
        print("insert data was been records")

if __name__ == "__main__":
    import os
    project_id = os.getenv('project_id')
    dataset_id = "test_lake"
    table_id = "tester"
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("exchange_rate", "FLOAT"),
        bigquery.SchemaField("change", "FLOAT"),
    ]
    bigquery_client = BigQuerySetup(project_id,dataset_id,table_id)
    bigquery_client.create_table(schema)