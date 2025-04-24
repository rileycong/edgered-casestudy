from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound

def table_exists(project_id, dataset_id: str, table_id: str):
    bq_client = bq.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    try:
        bq_client.get_table(table_ref)
    except NotFound:
        return False
    return True

def cleaned_payments_bq(project_id: str):
     
    dataset_id = 'edgered_case_study'
    table_name = 'payments'
    table_id = f"{dataset_id}.{table_name}"

    table_exists_flag = table_exists(project_id, dataset_id, table_name)

    if not table_exists_flag:
        print("Table does not exist")
    else:
        print("Table exists")

project_id= "edgered-case-study"
# dataset_id = 'edgered_case_study'
# table_id = "payments"

cleaned_payments_bq(project_id)