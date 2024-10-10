import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import json
import os
import sys
import re

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_parameters(**kwargs):
    ti = kwargs["ti"]
    selected_llm = kwargs["dag_run"].conf.get("selectedLLM")
    file_format = kwargs["dag_run"].conf.get("fileFormat")
    file_content = kwargs["dag_run"].conf.get("file_content")
    file_name = kwargs["dag_run"].conf.get("fileName")

    user_data = kwargs["dag_run"].conf.get("user_data")

    # Here you can take custom arguments from config objects
    custom_parameter = user_data.get("custom_parameter")

    params = {
        "selected_llm": selected_llm,
        "file_format": file_format,
        "file_content": file_content,
        "file_name": file_name,
        "custom_parameter": custom_parameter,
    }

    ti.xcom_push(key="params", value=params)


# This function creates JSONLD contents
def convert_to_jsonld(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(key="params", task_ids="convert_csv_to_json_array_task")
    file_name = params["file_name"]
    selected_llm = params["selected_llm"]
    json_array = params["json_array"]

    try:
        ka = from_json_arr_to_jsonld(
            json_array, file_name, publisher_name, publisher_domain, file_name
        )

        ti.xcom_push(key="ka", value=ka)
    except Exception as e:
        logging.error(f"Failed to convert to JSON LD: {str(e)}", exc_info=True)
        return "Failed to convert to JSON LD"


with DAG(
    "exampleDAG",
    default_args=default_args,
    description="Example DAG",
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_parameters_task = PythonOperator(
        task_id="extract_parameters",
        provide_context=True,
        python_callable=extract_parameters,
        dag=dag,
    )

    convert_to_jsonld = PythonOperator(
        task_id="convert_to_ka",
        provide_context=True,
        python_callable=convert_to_jsonld,
        dag=dag,
    )

    (extract_parameters_task >> convert_to_jsonld)

logging.info("DAG exampleDAG loaded")
