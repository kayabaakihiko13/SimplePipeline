from google.cloud import bigquery

def people_roles():
    client = bigquery.Client()
    query_job = client.query("select session_user() as whoami")
    results =  query_job.result()
    for row in results :
        print(f"iam {row.whoami}")

if __name__ == "__main__":
    people_roles()
    