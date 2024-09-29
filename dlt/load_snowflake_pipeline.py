import dlt
import requests
import json
from pathlib import Path
import os

def _get_ads(url_for_search, params):
    """
    Fetch job ads from the API.
    """
    headers = {"accept": "application/json"}
    response = requests.get(url_for_search, headers=headers, params=params)
    response.raise_for_status()  # Check for HTTP errors
    return json.loads(response.content.decode("utf8"))

@dlt.resource(write_disposition="replace")
def jobsearch_resource(params):
    """
    Resource function to fetch job ads from Jobtech API.
    """
    url = "https://jobsearch.api.jobtechdev.se"
    url_for_search = f"{url}/search"

    # Fetch data from the API and yield the results
    for ad in _get_ads(url_for_search, params)["hits"]:
        yield ad

def run_pipeline(queries, table_name):
    """
    Runs the DLT pipeline for multiple queries and loads them into Snowflake.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jobsearch_pipeline",
        destination="snowflake",
        dataset_name="staging",
    )

    for query in queries:
        params = {"q": query, "limit": 100}  # Adjust 'limit' if necessary
        load_info = pipeline.run(jobsearch_resource(params=params), table_name=table_name)
        print(f"Loaded data for query: {query} into table: {table_name}")
        print(load_info)

if __name__ == "__main__":
    # Change the working directory to the script's directory
    working_directory = Path(__file__).parent
    os.chdir(working_directory)

    # List of job search queries (keywords)
    queries = [
        'gymnasielärare'
        # , 'grundskolelärare', 'förskollärare', 'yrkeshögskolejobb', 
        # 'högskolejobb', 'universitetsjobb', 'data science', 'data analyst', 
        # 'business intelligence', 'högskoleingenjör', 'civillingengör', 
        # 'ekonom', 'redovisning', 'controller', 'projektledare', 'lagerjobb', 
        # 'sjuksköterska'
    ]

    # Define the Snowflake table name where the data will be stored
    table_name = "data_field_job_ads"

    # Run the pipeline with the defined queries and table name
    run_pipeline(queries, table_name)
