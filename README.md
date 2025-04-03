
# Detecting Defaults: An EdgeRed Case Study

## Problem Statement

Payment defaults are detrimental to the business and are a significant cost factor.

Are there any key trends in the data which can help me avoid default-prone customers in the future?

## Project Overview

The project consists of several main components:

1. **EDA Notebooks**
    - Python notebooks to uncover the patterns behind default behaviours.

2. **Data Pipeline**:

   - **Data Ingestion**: Upload **clients** and **payments** CSV files to **Google Cloud Storage**.
   - **Data Cleaning & Transformation**: Clean and preprocess the data, then upload the cleaned datasets to **BigQuery**.
   - **Data Modeling**: Use **dbt** to build **transaction models** and **client risk models** in **BigQuery** using a **Kimball-style schema**.

3. **Predictive Models**: 
   - A model that predicts whether a **future transaction** is at risk of default based on past transaction records, estimated transaction time, and client attributes.
   - Experiment with a model to classify new clients based solely on inherent characteristics.


## Installation

1. Clone this repository:
    ```bash
    git clone https://github.com/rileycong/edgered-casestudy.git
    ```

2. Install the required dependencies (make sure you have Python 3.7+):

    ```bash
    pip install -r requirements.txt
    ```

3. Set up GCP:

    - Ensure you have Google Cloud SDK installed and authenticated.

    - Set up BigQuery and Cloud Storage in your GCP project.

4. Set up dbt:
    -  Follow the instructions in the dbt documentation to configure your BigQuery connection.

## Usage
1. Upload Data to GCP:
    - Upload the clients.csv and payments.csv files to Google Cloud Storage.

2. Run the Data Pipeline:

    - The pipeline cleans and uploads data to BigQuery using Dagster orchestration.

    - Trigger the pipeline using Dagster UI or command line:

    ```bash
    dagster dev -f pipeline/assets.py
    ```

3. Run dbt Models:

    - Connect dbt to BigQuery and run the models:

    ```bash
    dbt run
    ```

4. Run the EDA notebooks:
    - Run to see visualisations and read about the patterns of default behaviours.
    - Several AI models are implemented in the eda notebooks.

## Outputs

- BigQuery tables for cleaned and transformed data.
- Client Risk Model and Transaction Model created with dbt in BigQuery.
- AI models for predicting future transaction defaults.

## To Do
- Implement incremental materialisation for the transaction model.
- Deploy ML components and orchestrate with Dagster.