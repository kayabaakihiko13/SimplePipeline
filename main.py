import os
from get_data import get_data_currency
from control_bigquery import BigQuerySetup

def call_data():
    result = get_data_currency("usd", "idr")
    return result

def runner_creact():
    project_id = "belajar-big-data-427517"
    dataset_id = "test_lake"
    table_id = "tester"
    
    if not project_id:
        raise ValueError("Environment variable 'project_id' not set")
    
    try:
        bigquery_client = BigQuerySetup(project_id, dataset_id, table_id)
        df_time_series = call_data()
        df_time_series['timestamp'] = df_time_series['timestamp'].astype(str)
        rows_to_insert = df_time_series.to_dict(orient='records')
        bigquery_client.insert_data(rows_to_insert)
        print("Data successfully inserted into BigQuery.")
    except Exception as e:
        print(f"Error inserting data into BigQuery: {str(e)}")

if __name__ == "__main__":
    runner_creact()
