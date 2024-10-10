import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from plugins.utils.python_utils import str_to_bool
from plugins.services.vectorization_service import (
    vectorize_knowledge_assets,
)  # Ensure this service is implemented

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_params(**kwargs):
    ti = kwargs["ti"]
    params = kwargs["dag_run"].conf
    ti.xcom_push(key="params", value=params)


def process_params(**kwargs):
    ti = kwargs["ti"]
    params = ti.xcom_pull(key="params", task_ids="extract_params")
    json_ld_array = params.get("knowledge_assets", [])
    embedding_model_name = params.get("embedding_model_name", "")
    llm_model_name = params.get("llm_model_name", "")
    use_case = params.get("use_case", None)

    if not json_ld_array:
        logging.error("No JSON-LD array provided")
        return "No JSON-LD array provided"

    if not embedding_model_name:
        logging.error("No embedding model name provided")
        return "No embedding model name provided"

    if not llm_model_name:
        logging.error("No llm model name provided")
        return "No llm model name provided"

    try:
        json_ld_data = json.loads(json_ld_array)
        ti.xcom_push(key="json_ld_data", value=json_ld_data)
        ti.xcom_push(key="embedding_model_name", value=embedding_model_name)
        ti.xcom_push(key="llm_model_name", value=llm_model_name)
        ti.xcom_push(key="use_case", value=use_case)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON-LD array: {e}")
        return "Failed to decode JSON-LD array"


def vectorize(**kwargs):
    ti = kwargs["ti"]
    json_ld_data = ti.xcom_pull(key="json_ld_data", task_ids="process_params")
    embedding_model_name = ti.xcom_pull(
        key="embedding_model_name", task_ids="process_params"
    )
    llm_model_name = ti.xcom_pull(key="llm_model_name", task_ids="process_params")
    use_case = ti.xcom_pull(key="use_case", task_ids="process_params")

    if not json_ld_data or not embedding_model_name or not llm_model_name:
        logging.error("Missing data for vectorization")
        return "Missing data for vectorization"

    try:
        result = vectorize_knowledge_assets(
            llm_model_name, json_ld_data, embedding_model_name, use_case=use_case
        )
        logging.info(
            f"Vectorization result: - embedded {len(result['embeddings'])} texts, producing {len(result['metadatas'])} metadatas"
        )

        ti.xcom_push(key="vectors", value=result)
    except Exception as e:
        logging.error(f"Vectorization failed: {e}")
        return "Vectorization failed"


with DAG(
    "vectorize_ka",
    default_args=default_args,
    description="Vectorize JSON-LD data into a vector database",
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_params_task = PythonOperator(
        task_id="extract_params",
        provide_context=True,
        python_callable=extract_params,
        dag=dag,
    )

    process_params_task = PythonOperator(
        task_id="process_params",
        provide_context=True,
        python_callable=process_params,
        dag=dag,
    )

    vectorize_task = PythonOperator(
        task_id="vectorize",
        provide_context=True,
        python_callable=vectorize,
        dag=dag,
    )

    extract_params_task >> process_params_task >> vectorize_task

logging.info("DAG vectorize_ka loaded")
