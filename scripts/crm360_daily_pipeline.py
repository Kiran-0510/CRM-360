"""
crm360_daily_pipeline.py
-------------------------
Daily CRM 360 pipeline DAG.
Runs at 9 AM every day.

Flow:
  check_raw_data → pyspark_cleaning → dbt_staging → test_staging
  → dbt_intermediate → dbt_marts → test_marts → notify_success
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── paths ────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/Users/kiranyadav/Desktop/crm-360/crm360_dbt"
SCRIPTS_DIR     = "/Users/kiranyadav/Desktop/crm-360/scripts"
DATA_DIR        = "/Users/kiranyadav/Desktop/crm-360/data"

# ── default args ─────────────────────────────────────────────────────────────
# These apply to every task in the DAG unless overridden individually.
# retries=1 means if a task fails, Airflow automatically tries once more
# before marking it as failed. retry_delay gives it 5 minutes to recover
# (e.g. a transient Snowflake connection timeout).
default_args = {
    "owner": "kiran",
    "depends_on_past": False,       # don't wait for yesterday's run to succeed
    "email_on_failure": False,      # set to True + add email in prod
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="crm360_daily_pipeline",
    default_args=default_args,
    description="Daily CRM 360 pipeline: PySpark → Snowflake → dbt",
    schedule="0 9 * * *",   # 9 AM every day (cron syntax)
    start_date=datetime(2026, 7, 1),
    catchup=False,                   # don't backfill missed runs
    tags=["crm360", "dbt", "pyspark"],
) as dag:

    # ── Task 1: check raw data freshness ─────────────────────────────────────
    # In production this would check S3 for new files or query a metadata
    # table. For now it verifies the local raw data directory exists and
    # has files. Fails fast if there's nothing to process.
    def check_raw_data():
        import os
        raw_dir = f"{DATA_DIR}/raw"
        if not os.path.exists(raw_dir):
            raise FileNotFoundError(
                f"Raw data directory not found: {raw_dir}. "
                "Run generate_data.py first."
            )
        files = os.listdir(raw_dir)
        if not files:
            raise ValueError("Raw data directory is empty — nothing to process.")
        print(f"Raw data check passed. Found {len(files)} files: {files}")

    check_raw_data_task = PythonOperator(
        task_id="check_raw_data_freshness",
        python_callable=check_raw_data,
    )

    # ── Task 2: PySpark cleaning ──────────────────────────────────────────────
    # Runs your spark_clean_transform.py — dedup, rolling windows,
    # flatten tickets, quarantine bad records.
    # In production this would submit to EMR or Databricks.
    def run_pyspark():
        import subprocess
        result = subprocess.run(
            [
                "python3",
                f"{SCRIPTS_DIR}/spark_clean_transform.py",
                "--input-dir",  f"{DATA_DIR}/raw",
                "--output-dir", f"{DATA_DIR}/processed",
            ],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"PySpark job failed:\n{result.stderr}")
        print("PySpark cleaning completed successfully.")

    pyspark_task = PythonOperator(
        task_id="run_pyspark_cleaning",
        python_callable=run_pyspark,
        execution_timeout=timedelta(minutes=30),  # fail if spark takes > 30 min
    )

    # ── Task 3: dbt run staging ───────────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && /Users/kiranyadav/airflow_project/.venv/bin/dbt run --select staging",
    )

    # ── Task 4: dbt test staging ──────────────────────────────────────────────
    # This is the quality gate. If staging tests fail, nothing downstream runs.
    # A null customer_id here means marts never get built on bad data.
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging",
    )

    # ── Task 5: dbt run intermediate ─────────────────────────────────────────
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"cd {DBT_PROJECT_DIR} && /Users/kiranyadav/airflow_project/.venv/bin/dbt run --select intermediate",
    )

    # ── Task 6: dbt run marts ─────────────────────────────────────────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && /Users/kiranyadav/airflow_project/.venv/bin/dbt run --select marts",
    )

    # ── Task 7: dbt test marts ────────────────────────────────────────────────
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select marts",
    )

    # ── Task 8: notify success ────────────────────────────────────────────────
    # In production: send Slack message, PagerDuty all-clear, update a
    # pipeline status dashboard. For now just prints a summary.
    def notify_success(**context):
        dag_run = context["dag_run"]
        print(f"""
        ✅ CRM 360 pipeline completed successfully
        DAG run: {dag_run.run_id}
        Execution date: {context['execution_date']}
        All dbt tests passed — marts are fresh and validated.
        """)

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,

    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    # This is the DAG structure. >> means "then".
    # Read it as: check data, then clean it, then build and test staging,
    # then build intermediate, then build and test marts, then notify.
    (
        check_raw_data_task
        >> pyspark_task
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test_marts
        >> notify_task
    )
