from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('weather_scada_data_import', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    # Task to import SCADA data
    import_scada_task = BashOperator(
        task_id='import_scada',
        bash_command='/usr/local/bin/python /app/src/imports/import_scada_10mn.py',
        dag=dag,
        schedule_interval="*/10 * * * *"
    )

    # Task to import forecast weather data
    import_forecast_task = BashOperator(
        task_id='import_forecast',
        bash_command='/usr/local/bin/python /app/src/imports/import_forecastweather.py',
        dag=dag,
        schedule_interval="0 */3 * * *"
    )

    # Task to import current weather data
    import_current_task = BashOperator(
        task_id='import_current',
        bash_command='/usr/local/bin/python /app/src/imports/import_currentweather.py',
        dag=dag,
        schedule_interval="0 * * * *"
    )

    import_scada_task >> import_forecast_task >> import_current_task
