# create a bucket in GCP and upload the data files
gsutil mb gs://edgered_casestudy/
gsutil cp .\data\Clients.csv gs://edgered_casestudy/
gsutil cp .\data\Payments.csv gs://edgered_casestudy/

# set up big query access
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your\service_account_key.json

# set up the dagster pipeline
mkdir dagster
cd dagster
mkdir pipeline

# run the dagster pipeline (cd to the asset directory first)
dagster dev -f pipeline/assets.py
