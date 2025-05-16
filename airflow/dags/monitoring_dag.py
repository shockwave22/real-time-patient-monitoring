from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def generate_daily_report():
    # Just as a placeholder - load logs and generate CSV report
    df = pd.read_json("data/vitals_log.json", lines=True)
    daily_summary = df.groupby("patient_id").agg({
        "heart_rate": "mean",
        "bp_systolic": "mean",
        "bp_diastolic": "mean"
    })
    daily_summary.to_csv(f"data/daily_report_{datetime.now().date()}.csv")

with DAG("monitoring_daily_summary",
         start_date=datetime(2024, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    generate_summary = PythonOperator(
        task_id="generate_report",
        python_callable=generate_daily_report
    )
