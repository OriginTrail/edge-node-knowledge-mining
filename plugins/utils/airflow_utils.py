import time
import logging
from flask import Flask, request, jsonify, g
from airflow.models import DagRun, TaskInstance
from airflow.settings import Session
import io


def get_status_of_dag_until_finished(
    session, dag_id, run_id, task_id, xcom_key, poll_interval=10, max_wait_time=300
):
    start_time = time.time()
    dag_run_status = None

    while time.time() - start_time < max_wait_time:
        logging.info(
            f"Polling for DAG run status... elapsed time: {int(time.time() - start_time)} seconds"
        )

        # Refresh session
        session.expire_all()

        dag_run = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
            .first()
        )

        if dag_run:
            logging.info(f"DAG run state: {dag_run.state}")
        else:
            logging.info("DAG run not found yet.")

        if dag_run and dag_run.state in ["success", "failed"]:
            dag_run_status = dag_run.state
            break

        time.sleep(poll_interval)

    if dag_run_status == "success":
        logging.info("DAG run completed successfully. Fetching XCom value...")
        task_instance = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.task_id == task_id,
            )
            .first()
        )

        if task_instance:
            xcom_value = task_instance.xcom_pull(task_ids=task_id, key=xcom_key)
            return {"status": "success", "xcom_value": xcom_value}

    elif dag_run_status == "failed":
        logging.error("DAG run failed.")
        return {"status": "failed"}

    logging.error("DAG did not complete in the expected time.")
    return {"status": "timeout"}


def get_status_of_dag(session, dag_id, run_id, task_id=None, xcom_key=None):
    # Refresh session
    session.expire_all()

    # Fetch the DAG run
    dag_run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
        .first()
    )

    if not dag_run:
        logging.info("DAG run not found.")
        return {"status": "not_found"}

    logging.info(f"DAG run state: {dag_run.state}")

    # If task_id and xcom_key are provided, fetch the XCom value
    if dag_run.state == "success" and task_id and xcom_key:
        task_instance = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.task_id == task_id,
            )
            .first()
        )

        if task_instance:
            xcom_value = task_instance.xcom_pull(task_ids=task_id, key=xcom_key)
            return {"status": dag_run.state, "xcom_value": xcom_value}

    return {"status": dag_run.state}


def handle_vectorize_ka_pipeline(pipeline_id, file, conf, userData):
    file_content = file.read().decode("utf-8")
    embedding_model_name = "guidecare/all-mpnet-base-v2-feature-extraction"
    llm_model_name = getattr(g, "user_data", {}).get("model")
    use_case = request.form.get("use_case")

    if not embedding_model_name:
        return jsonify({"error": "Missing embedding_model_name"}), 400

    if not llm_model_name:
        return jsonify({"error": "Missing llm_model_name"}), 400

    conf.update(
        {
            "knowledge_assets": file_content,
            "embedding_model_name": embedding_model_name,
            "llm_model_name": llm_model_name,
            "use_case": use_case,
            "user_data": userData,
        }
    )


def handle_file_to_jsonld_pipeline(pipeline_id, file, conf, userData):
    selected_llm = getattr(g, "user_data", {}).get("model")
    file_format = request.form.get("fileFormat")

    logging.info(f"LLM MODEL: {selected_llm}")

    if not file_format:
        return jsonify({"error": "Missing fileFormat"}), 400

    conf.update({"selectedLLM": selected_llm, "fileFormat": file_format})

    logging.info(f"Reading file and passing content to DAG {pipeline_id}")

    conf["user_data"] = userData
    logging.info(f"User Data: {userData}")

    if file_format.lower() == "pdf":
        pdf_file = io.BytesIO(file.read())
        conf["file"] = pdf_file
    elif file_format.lower() in ["json", "csv"]:
        file_content = file.read().decode("utf-8")
        conf["file_content"] = file_content

    else:
        return jsonify({"error": "Unsupported file format"}), 400
