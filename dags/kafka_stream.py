import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'Nelson',  # Who created the DAG (optional)
    'start_date': datetime(2024, 6, 18, 13, 45),  # When the DAG should first run
}

def get_data():
    import requests
    import json
    # Make a GET request to the random user API
    res = requests.get("http://randomuser.me/api/")

    # Check for successful response
    if res.status_code == 200:
        # Convert the response to JSON (assuming it's JSON data)
        res = json.loads(res.text)
        res = res['results'][0]
        # print(json.dumps(data, indent = 3))
    else:
        print(f"Error: API request failed with status code {res.status_code}")

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

# Function to stream data from the API
def stream_data():
    import json
    res = get_data()
    res = format_data(res)
    print(json.dumps(res, indent = 3))


# Define the DAG with configuration
with DAG(
    dag_id = 'user_automation',  # Unique ID for the DAG
    default_args = default_args,
    schedule = "@daily",  # Run the DAG once every day
    catchup = False,  # Don't run for missed scheduling periods
) as dag:


    # Task to stream data from the API
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',  # Unique ID for the task
        python_callable = stream_data,  # Function to be executed
    )

# print(format_data(get_data()))
stream_data()