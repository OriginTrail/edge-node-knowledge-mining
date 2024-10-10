import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from plugins.services.unstructured_service import unstructured_convert_pdf_to_json_array
from plugins.services.ka_service import json_arr_to_ka

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
    file = kwargs["dag_run"].conf.get("file")
    file_name = kwargs["dag_run"].conf.get("fileName")

    params = {
        "selected_llm": selected_llm,
        "file_format": file_format,
        "file": file,
        "file_name": file_name,
    }

    ti.xcom_push(key="params", value=params)


def convert_pdf_to_json_array(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(key="params", task_ids="extract_parameters")
    file = params["file"]
    file_format = params["file_format"].lower()
    file_name = params["file_name"]
    selected_llm = params["selected_llm"]

    if file_format != "pdf":
        logging.error("File format is not PDF")
        return

    try:
        logging.info("Calling unstructured convert function")
        unstr_json_array = unstructured_convert_pdf_to_json_array(file, file_name)
        logging.info(
            f"Successfully converted PDF to JSON array with length {len(unstr_json_array)}"
        )
        params = {
            "selected_llm": selected_llm,
            "unstr_json_array": unstr_json_array,
            "file_name": file_name,
            "selected_llm": selected_llm,
        }
        ti.xcom_push(key="params", value=params)
    except Exception as e:
        logging.error(
            f"Failed to parse PDF using Unstructured: {str(e)}", exc_info=True
        )
        return "Failed to parse PDF using Unstructured"


def convert_to_ka(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(key="params", task_ids="convert_pdf_to_json_array")
    file_name = params["file_name"]
    selected_llm = params["selected_llm"]
    unstr_json_array = params["unstr_json_array"]

    try:
        ka = json_arr_to_ka(unstr_json_array, file_name, selected_llm)
        logging.info(f"Got JSON LD KA from PDF {json.dumps(ka)}")

        ti.xcom_push(key="ka", value=ka)
    except Exception as e:
        logging.error(f"Failed to convert chunks to JSON LD: {str(e)}", exc_info=True)
        return "Failed to convert chunks to JSON LD"


with DAG(
    "pdf_to_jsonld",
    default_args=default_args,
    description="Process parameters for PDF to JSON-LD conversion",
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_parameters_task = PythonOperator(
        task_id="extract_parameters",
        provide_context=True,
        python_callable=extract_parameters,
        dag=dag,
    )

    convert_pdf_to_json_array_task = PythonOperator(
        task_id="convert_pdf_to_json_array",
        provide_context=True,
        python_callable=convert_pdf_to_json_array,
        dag=dag,
    )

    convert_to_ka = PythonOperator(
        task_id="convert_to_ka",
        provide_context=True,
        python_callable=convert_to_ka,
        dag=dag,
    )

    (extract_parameters_task >> convert_pdf_to_json_array_task >> convert_to_ka)

logging.info("DAG pdf_to_jsonld loaded")
