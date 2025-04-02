from setuptools import find_packages, setup

setup(
    name="dagster",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-gcp",
        "google-cloud-bigquery",
        # "dagster-gcp-pandas",
        "pandas",
        "dbt",
        "dbt-bigquery",
        "dagster-dbt",

    ],
    extras_require={"dev": ["dagster-webserver"]},
)