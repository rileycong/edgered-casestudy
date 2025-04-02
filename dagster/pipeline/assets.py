import pandas as pd
import os
from dagster import asset, Definitions, AssetExecutionContext
from google.cloud import bigquery as bq
from google.cloud.exceptions import NotFound
from dagster_gcp import GCSResource, BigQueryResource
from dagster_dbt import (DbtCliResource, dbt_assets)
from pipeline.constants import dbt_manifest_path, dbt_project_dir


@asset(group_name="raw_to_bq")
def raw_payments(gcs: GCSResource) -> pd.DataFrame:
    raw_payments = pd.read_csv("gs://edgered_casestudy/Payments.csv")
    return raw_payments

@asset(group_name="raw_to_bq")
def raw_clients(gcs: GCSResource) -> pd.DataFrame:
    raw_clients = pd.read_csv("gs://edgered_casestudy/Clients.csv")  # Adjust with your actual GCS path
    return raw_clients


@asset(group_name="raw_to_bq")
def cleaned_payments(context:AssetExecutionContext, raw_payments: pd.DataFrame) -> pd.DataFrame:
    raw_payments = raw_payments
    raw_payments['transaction_datetime'] = pd.to_datetime(raw_payments['transaction_date'], unit='s')
    cleaned_payments = raw_payments.drop(columns=['transaction_date'])
    return cleaned_payments

@asset(group_name="raw_to_bq")
def cleaned_clients(context:AssetExecutionContext, raw_clients: pd.DataFrame) -> pd.DataFrame:
    raw_clients = raw_clients
    clients_duplicates = raw_clients[raw_clients.duplicated(subset=['client_id'], keep=False)]
    filtered_clients_duplicates = raw_clients.loc[clients_duplicates.groupby('client_id')['entity_year_established'].idxmax()]
    other_clients = raw_clients[~raw_clients['client_id'].isin(clients_duplicates['client_id'])]
    cleaned_clients = pd.concat([other_clients, filtered_clients_duplicates])
    return cleaned_clients


def create_dataset_if_not_exists(project_id: str, dataset_id: str):
    # BigQuery client
    bq_client = bq.Client(project=project_id)
    dataset_ref = bq_client.dataset(dataset_id)

    # Check if the dataset exists
    try:
        bq_client.get_dataset(dataset_ref)  # Fetch dataset metadata to check existence
    except NotFound:
        # If dataset is not found, create it
        dataset = bq.Dataset(dataset_ref)
        dataset = bq_client.create_dataset(dataset)

def table_exists(project_id: str, dataset_id: str, table_id: str):
    bq_client = bq.Client(project=project_id)
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    try:
        bq_client.get_table(table_ref)
    except NotFound:
        return False
    return True

# Load cleaned data to BigQuery
@asset(group_name="raw_to_bq")
def cleaned_payments_bq(bigquery: BigQueryResource, cleaned_payments: pd.DataFrame):
     
    dataset_id = 'edgered_case_study'
    table_name = 'raw_payments'
    table_id = f"{dataset_id}.{table_name}"

    with bigquery.get_client() as client:
        create_dataset_if_not_exists(client.project, dataset_id)
        table_exists_flag = table_exists(client.project, dataset_id, table_name)

        if not table_exists_flag:
            job = client.load_table_from_dataframe(
                dataframe=cleaned_payments,
                destination=table_id,
            )
            job.result()
        else:
            print(f"Table {table_name} already exists, skipping load.")

@asset(group_name="raw_to_bq")
def cleaned_clients_bq(bigquery: BigQueryResource, cleaned_clients: pd.DataFrame):
    dataset_id = 'edgered_case_study'
    table_name = 'raw_clients'
    table_id = f"{dataset_id}.{table_name}"

    with bigquery.get_client() as client:
        create_dataset_if_not_exists(client.project, dataset_id)
        table_exists_flag = table_exists(client.project, dataset_id, table_name)

        if not table_exists_flag:
            job = client.load_table_from_dataframe(
                dataframe=cleaned_clients,
                destination=table_id,
            )
            job.result()
        else:
            print(f"Table {table_name} already exists, skipping load.")

# dbt assets
@dbt_assets(manifest=dbt_manifest_path)
def edgered_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions (
    assets=[raw_clients, raw_payments, cleaned_clients, cleaned_payments, cleaned_payments_bq, cleaned_clients_bq, edgered_dbt_assets],
    resources= {
        "gcs": GCSResource(
            project="edgered-case-study",
            bucket="edgered_casestudy",
        ),
        "bigquery": BigQueryResource(
            project="edgered-case-study",
            # location="us-east5",
        ),
         "dbt": DbtCliResource(
            project_dir=os.fspath(dbt_project_dir)
        )
    }
)