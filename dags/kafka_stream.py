from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'Nelson',  # Who created the DAG (optional)
    'start_date': datetime(2024, 6, 18, 13, 45),  # When the DAG should first run
}

# Function to stream data from the API
def stream_data():
    import requests
    import json

    # Make a GET request to the random user API
    response = requests.get("http://randomuser.me/api/")

    # Check for successful response
    if response.status_code == 200:
        # Convert the response to JSON (assuming it's JSON data)
        data = json.loads(response.text)
        # Print the JSON data (for demonstration purposes)
        print(data)
    else:
        print(f"Error: API request failed with status code {response.status_code}")

# Define the DAG with configuration
with DAG(
    dag_id='user_automation',  # Unique ID for the DAG
    default_args=default_args,
    schedule_interval="@daily",  # Run the DAG once every day
    catchup=False,  # Don't run for missed scheduling periods
) as dag:

    # Task to stream data from the API
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',  # Unique ID for the task
        python_callable=stream_data,  # Function to be executed
    )
