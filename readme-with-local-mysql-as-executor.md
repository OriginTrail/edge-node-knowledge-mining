# Edge node Knowledge mining

## Environment and DB Setup
1. cp .env.example .env
2. create mysql DB named ‘airflow_db’
3. Install MYSQL development lib (if not installed) - brew install mysql-client
   1. After installation check mysql-client path -> brew info mysql-client
   2. Update your .zshrc or ~/.bash_profile
```sh
echo 'export PATH="YOUR_PATH_TO/mysql-client/bin:$PATH"' >> ~/.zshrc && \
echo 'export PKG_CONFIG_PATH="YOUR_PATH_TO/mysql-client/lib/pkgconfig:$PKG_CONFIG_PATH"' >> ~/.zshrc && \
source ~/.zshrc
```
Those variables needs to available when installing project requirements.

## Install Python environment

1. It's recommended to use **pyenv** and to install **Python 3.11** locally inside the app's directory so it doesn't clash with other Python version on your machine
   ```sh
   pyenv local 3.11.7
      ```
2. Now that Python is available (python -v), Virtual environment should be set in order to install requirements
   ```sh
   python -m venv .venv && source .venv/bin/activate
   ```
3. Install Python requirements
   ```sh
   pip install -r requirements.txt
   ```

## Apache airflow setup
Airflow pipelines are part of Knowledge mining service, which are used for creation of automated data processing pipelines. Main purpose of pipelines is to create content for Knowledge assets based on the input file.

**Generate default airflow config**

```sh
airflow config list --defaults
```

### This is path for Airflow config file: **~/airflow/airflow.cfg file**

Change the following lines in the config:

```sh
executor = LocalExecutor
load_examples = False
sql_alchemy_conn = mysql+pymysql://{YOUR_MYSQL_USERNAME}:{YOUR_MYSQL_PASSWORD}@localhost/airflow_db
dags_folder = YOUR_PATH_TO/edge-node-knowledge-mining/dags
parallelism = 32
max_active_tasks_per_dag = 16
max_active_runs_per_dag = 16
enable_xcom_pickling = True
```

### Airflow db init

```sh
airflow db init

airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```

### Airflow scheduler
In order to have Airflow running, first Scheduler should be started:
```sh
airflow scheduler (to pick up new DAGs/jobs)
```

### Unpause JOBS
```sh
airflow dags unpause exampleDAG
airflow dags unpause pdf_to_jsonld
airflow dags unpause simple_json_to_jsonld
```

### Airflow webserver
To keep track how your pipelines perform, webserver should be installed. It will be available on http://localhost:8080. After starting everything pipelines should be available on page http://localhost:8080/home and un-paused \
**Start airflow server**
```sh
airflow webserver --port 8080 (port where you can open the dashboard)
```

### Start server for Edge node Knowledge mining

```sh
python app.py
```

### MYSQL for logging

```sh
CREATE DATABASE ka-mining-api-logging CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
```

## Potential errors

#### Error: When installing pip requirements.txt -> Trying pkg-config --exists mysqlclient Command 'pkg-config --exists mysqlclient' returned non-zero exit status 1
```sh
brew link --force mysql-client
CFLAGS="-I/opt/homebrew/opt/mysql-client/include/mysql" \
LDFLAGS="-L/opt/homebrew/opt/mysql-client/lib" \
pip install mysqlclient
```


```sh
    curl -X POST http://localhost:5001/trigger_pipeline \
    -F "file=@test_pdfs/22pages_eng.pdf" \
    -F "pipelineId=pdf_to_jsonld" \
    -F "fileFormat=pdf" \
    -b "connect.sid=s%3A9XCAe7sos-iY4Z_jIjyVcQYjLaYHVi0H.UeghM8ZRS97nVkZPukbL8Zu%2F%2BbRZSAuOLpq3BMepiD0; Path=/; HttpOnly;"
```

```sh
    curl -X POST http://localhost:5001/trigger_pipeline \
    -F "file=@test_jsons/entertainment_test.json" \
    -F "pipelineId=simple_json_to_jsonld" \
    -F "fileFormat=json" \
    -b "connect.sid=s%3A9XCAe7sos-iY4Z_jIjyVcQYjLaYHVi0H.UeghM8ZRS97nVkZPukbL8Zu%2F%2BbRZSAuOLpq3BMepiD0; Path=/; HttpOnly;"
```

**Trigger the vectorization DAG via POST request**

```sh
curl -X POST http://localhost:5000/trigger_pipeline \
     -F "file=@test_jsonlds/vectorize_test.json" \
     -F "pipelineId=vectorize_ka" \
     -b "connect.sid=s%3AjLYArFLH7IadiB4dkEDrppgEEQJEqNss.35WzNEW3PySPRIxrDpL5tsRZ%2F%2B%2FNo%2BnZgRPDoRz0y7g; Path=/; HttpOnly;"
```

