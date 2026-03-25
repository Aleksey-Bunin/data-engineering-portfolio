"""
Network Health Check DAG
Runs every 5 minutes to monitor real-time network health
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'network_health_check',
    default_args=default_args,
    description='5-minute network health monitoring',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['network', 'monitoring', 'real-time']
)

def extract_current_metrics(**context):
    """Extract metrics from last 5 minutes"""
    client = Client(host='clickhouse', port=9000)
    
    # Get execution time
    execution_date = context['execution_date']
    time_window = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    
    # Query last 5 minutes of data
    query = """
        SELECT 
            COUNT(*) as total_events,
            countIf(error_code != '') as error_count,
            AVG(call_duration) as avg_call_duration
        FROM network.events
        WHERE timestamp >= now() - INTERVAL 5 MINUTE
    """
    
    result = client.execute(query)
    
    if result:
        total_events, error_count, avg_duration = result[0]
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='total_events', value=total_events)
        context['task_instance'].xcom_push(key='error_count', value=error_count)
        context['task_instance'].xcom_push(key='avg_duration', value=avg_duration)
        
        print(f"✅ Extracted metrics: {total_events} events, {error_count} errors")
    else:
        print("⚠️ No data in last 5 minutes")

def calculate_error_rate(**context):
    """Calculate error rate"""
    ti = context['task_instance']
    
    total_events = ti.xcom_pull(key='total_events', task_ids='extract_current_metrics')
    error_count = ti.xcom_pull(key='error_count', task_ids='extract_current_metrics')
    
    if total_events and total_events > 0:
        error_rate = (error_count / total_events) * 100
        ti.xcom_push(key='error_rate', value=error_rate)
        print(f"📊 Error rate: {error_rate:.2f}%")
    else:
        ti.xcom_push(key='error_rate', value=0.0)

def update_health_status(**context):
    """Insert aggregated metrics into health table"""
    client = Client(host='clickhouse', port=9000)
    ti = context['task_instance']
    execution_date = context['execution_date']
    
    # Get metrics from XCom
    total_events = ti.xcom_pull(key='total_events', task_ids='extract_current_metrics') or 0
    error_count = ti.xcom_pull(key='error_count', task_ids='extract_current_metrics') or 0
    avg_duration = ti.xcom_pull(key='avg_duration', task_ids='extract_current_metrics') or 0.0
    error_rate = ti.xcom_pull(key='error_rate', task_ids='calculate_error_rate') or 0.0
    
    # Insert into aggregation table
    insert_query = """
        INSERT INTO network.health_5min (time_window, total_events, error_count, error_rate, avg_call_duration)
        VALUES
    """
    
    time_window = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    values = f"('{time_window}', {total_events}, {error_count}, {error_rate}, {avg_duration})"
    
    client.execute(insert_query + values)
    print(f"✅ Health metrics saved for {time_window}")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_current_metrics',
    python_callable=extract_current_metrics,
    dag=dag
)

task_calculate = PythonOperator(
    task_id='calculate_error_rate',
    python_callable=calculate_error_rate,
    dag=dag
)

task_update = PythonOperator(
    task_id='update_health_status',
    python_callable=update_health_status,
    dag=dag
)

# Set task dependencies
task_extract >> task_calculate >> task_update
