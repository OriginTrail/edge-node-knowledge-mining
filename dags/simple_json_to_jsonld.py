import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from plugins.services.ka_service import simple_json_to_ka
from plugins.utils.jsonld_utils import is_valid_jsonld

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
    file_content = kwargs["dag_run"].conf.get("file_content")
    selected_llm = kwargs["dag_run"].conf.get("selectedLLM")
    file_format = kwargs["dag_run"].conf.get("fileFormat")
    file_name = kwargs["dag_run"].conf.get("fileName")

    params = {
        "file_content": file_content,
        "selected_llm": selected_llm,
        "file_format": file_format,
        "file_name": file_name,
    }

    ti.xcom_push(key="params", value=params)


def convert_to_ka(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(key="params", task_ids="extract_parameters")
    file_name = params["file_name"]
    selected_llm = params["selected_llm"]
    file_content = params["file_content"]

    try:
        if is_valid_jsonld(file_content):
            logging.info(f"File content is already valid JSON-LD.")
            ka = json.loads(file_content)
        else:
            logging.info(f"File content is not valid JSON-LD, transforming into JSON.")
            ka = simple_json_to_ka(file_content, file_name, selected_llm)

        ti.xcom_push(key="ka", value=ka)
        logging.info(f"Got JSON LD KA from JSON {json.dumps(ka)}")
    except Exception as e:
        logging.error(f"Failed to convert chunks to JSON LD: {str(e)}", exc_info=True)
        return "Failed to convert chunks to JSON LD"


with DAG(
    "simple_json_to_jsonld",
    default_args=default_args,
    description="Process parameters for simple JSON to JSON-LD conversion",
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_parameters_task = PythonOperator(
        task_id="extract_parameters",
        provide_context=True,
        python_callable=extract_parameters,
        dag=dag,
    )

    convert_to_ka = PythonOperator(
        task_id="convert_to_ka",
        provide_context=True,
        python_callable=convert_to_ka,
        dag=dag,
    )

    (extract_parameters_task >> convert_to_ka)

logging.info("DAG simple_json_to_jsonld loaded")
