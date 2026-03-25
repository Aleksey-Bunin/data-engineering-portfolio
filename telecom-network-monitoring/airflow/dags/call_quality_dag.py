"""
Call Quality Metrics DAG
Runs every 30 minutes to analyze regional call quality
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
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'call_quality_metrics',
    default_args=default_args,
    description='30-minute call quality analysis by region',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    tags=['network', 'call-quality', 'regional']
)

def extract_call_logs(**context):
    """Extract call logs from last 30 minutes"""
    client = Client(host='clickhouse', port=9000)
    execution_date = context['execution_date']
    
    query = """
        SELECT 
            region,
            countIf(event_type IN ('call_start', 'call_end')) as total_calls,
            countIf(event_type = 'call_end' AND error_code != '') as dropped_calls,
            AVG(call_duration) as avg_call_duration
        FROM network.events
        WHERE timestamp >= now() - INTERVAL 30 MINUTE
          AND event_type IN ('call_start', 'call_end')
        GROUP BY region
    """
    
    results = client.execute(query)
    
    # Store results in XCom
    context['task_instance'].xcom_push(key='call_data', value=results)
    print(f"✅ Extracted call logs for {len(results)} regions")

def calculate_drop_rate_by_region(**context):
    """Calculate call drop rate per region"""
    ti = context['task_instance']
    call_data = ti.xcom_pull(key='call_data', task_ids='extract_call_logs')
    
    if not call_data:
        print("⚠️ No call data available")
        return
    
    drop_rates = []
    for region, total_calls, dropped_calls, avg_duration in call_data:
        if total_calls > 0:
            drop_rate = (dropped_calls / total_calls) * 100
        else:
            drop_rate = 0.0
        
        drop_rates.append({
            'region': region,
            'total_calls': total_calls,
            'dropped_calls': dropped_calls,
            'drop_rate': drop_rate,
            'avg_duration': avg_duration
        })
        
        print(f"📊 {region}: {drop_rate:.2f}% drop rate ({dropped_calls}/{total_calls} calls)")
    
    ti.xcom_push(key='drop_rates', value=drop_rates)

def alert_if_threshold_exceeded(**context):
    """Send alert if drop rate exceeds threshold"""
    ti = context['task_instance']
    execution_date = context['execution_date']
    drop_rates = ti.xcom_pull(key='drop_rates', task_ids='calculate_drop_rate_by_region')
    
    if not drop_rates:
        return
    
    client = Client(host='clickhouse', port=9000)
    time_window = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    
    ALERT_THRESHOLD = 5.0  # Alert if drop rate > 5%
    
    for data in drop_rates:
        # Insert into aggregation table
        insert_query = f"""
            INSERT INTO network.call_quality_30min 
            (time_window, region, total_calls, dropped_calls, drop_rate, avg_call_duration)
            VALUES ('{time_window}', '{data['region']}', {data['total_calls']}, 
                    {data['dropped_calls']}, {data['drop_rate']}, {data['avg_duration']})
        """
        client.execute(insert_query)
        
        # Check alert threshold
        if data['drop_rate'] > ALERT_THRESHOLD:
            print(f"🚨 ALERT: {data['region']} drop rate {data['drop_rate']:.2f}% exceeds threshold!")
            # In production: send email/Slack notification here
        else:
            print(f"✅ {data['region']} drop rate {data['drop_rate']:.2f}% within threshold")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_call_logs',
    python_callable=extract_call_logs,
    dag=dag
)

task_calculate = PythonOperator(
    task_id='calculate_drop_rate_by_region',
    python_callable=calculate_drop_rate_by_region,
    dag=dag
)

task_alert = PythonOperator(
    task_id='alert_if_threshold_exceeded',
    python_callable=alert_if_threshold_exceeded,
    dag=dag
)

# Set task dependencies
task_extract >> task_calculate >> task_alert
