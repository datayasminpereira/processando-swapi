from datetime import datetime, timedelta
import os
import requests
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='swapi_data_processing',
    default_args=default_args,
    description='A DAG to process data from SWAPI',
    schedule_interval="@daily",
)

def fetch_data(endpoint, ti):
    url = f"http://swapi.dev/api/{endpoint}"
    data = {"results": []}

    while url:
        response = requests.get(url)
        if response.status_code == 200:
            data["results"].extend(response.json()["results"])
            url = response.json()["next"]
        else:
            print(f"Failed to fetch data from {url}")
            break

    ti.xcom_push(key="data", value=data)


def save_data(directory, ti):
    data = ti.xcom_pull(task_ids=f"fetch_data_{directory}", key="data")
    resulting_dict = {"results": []}
    for _, values in data.items():
        for value in values:
            created_year = value["created"].split("-")[0]
            if not os.path.exists(directory.capitalize()):
                os.makedirs(directory.capitalize())
            filename = f"{directory.capitalize()}/{created_year}.json"

            resulting_dict["results"].append(value)

    with open(filename, "w") as file:
        json.dump(resulting_dict, file, indent=4)


def count_records(directory):
    total_records = 0
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        with open(filepath, "r") as file:
            data = json.load(file)
            total_records += len(data["results"])

    print(f'{total_records} records were fetched from the {directory.lower()} endpoint and saved in the files!')

def transform_people_data():
    transformation = {"results": []}
    
    filepath = os.path.join("People", "2014.json") 
    with open(filepath, "r") as file:
            data = json.load(file)
    
    for person in data["results"]:
        name = person["name"]
        height = person["height"]
        gender = person["gender"]
        
        titles = []
        for film_url in person["films"]: 
            response_film = requests.get(film_url)
            if response_film.status_code == 200:
                filme_data = response_film.json()
                film_title = filme_data["title"]
                titles.append(film_title)
            else:
                print(f"Failed to fetch data from {film_url}")
        
        transformed_value = {
            "name": name,
            "height": height,
            "gender": gender,
            "title": titles 
        }
        
        transformation["results"].append(transformed_value)
    
    with open("transformation.json", "w") as file:
        json.dump(transformation, file, indent=4)

with dag:
    fetch_data_people = PythonOperator(
        task_id='fetch_data_people',
        python_callable=fetch_data,
        op_kwargs={'endpoint': 'people'},
    )

    save_data_people = PythonOperator(
        task_id='save_data_people',
        python_callable=save_data,
        op_kwargs={'directory': 'people'},
    )

    fetch_data_films = PythonOperator(
        task_id='fetch_data_films',
        python_callable=fetch_data,
        op_kwargs={'endpoint': 'films'},
    )

    save_data_films = PythonOperator(
        task_id='save_data_films',
        python_callable=save_data,
        op_kwargs={'directory': 'films'},
    )

    fetch_data_vehicles = PythonOperator(
        task_id='fetch_data_vehicles',
        python_callable=fetch_data,
        op_kwargs={'endpoint': 'vehicles'},
    )

    save_data_vehicles = PythonOperator(
        task_id='save_data_vehicles',
        python_callable=save_data,
        op_kwargs={'directory': 'vehicles'},
    )   

    count_records_people = PythonOperator(
        task_id='count_records_people',
        python_callable=count_records,
        op_kwargs={'directory': 'People'},
    )

    transform_people = PythonOperator(
        task_id='transform_people_data',
        python_callable=transform_people_data,
    )

    fetch_data_people >> save_data_people >> count_records_people >> transform_people
    fetch_data_films >> save_data_films
    fetch_data_vehicles >> save_data_vehicles