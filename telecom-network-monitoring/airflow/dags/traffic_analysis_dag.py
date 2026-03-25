"""
Traffic Analysis DAG
Runs every hour to analyze network traffic patterns
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
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'traffic_analysis',
    default_args=default_args,
    description='Hourly network traffic analysis',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['network', 'traffic', 'capacity']
)

def extract_data_sessions(**context):
    """Extract data session metrics from last hour"""
    client = Client(host='clickhouse', port=9000)
    execution_date = context['execution_date']
    
    query = """
        SELECT 
            base_station_id,
            region,
            SUM(data_usage_mb) as total_data_mb,
            MAX(data_usage_mb) as peak_usage_mb,
            COUNT(*) as active_sessions
        FROM network.events
        WHERE timestamp >= now() - INTERVAL 1 HOUR
          AND event_type = 'data_session'
        GROUP BY base_station_id, region
    """
    
    results = client.execute(query)
    
    context['task_instance'].xcom_push(key='traffic_data', value=results)
    print(f"✅ Extracted traffic data for {len(results)} base stations")

def aggregate_by_tower(**context):
    """Aggregate traffic by base station"""
    ti = context['task_instance']
    traffic_data = ti.xcom_pull(key='traffic_data', task_ids='extract_data_sessions')
    
    if not traffic_data:
        print("⚠️ No traffic data available")
        return
    
    aggregated = []
    for bs_id, region, total_mb, peak_mb, sessions in traffic_data:
        aggregated.append({
            'base_station_id': bs_id,
            'region': region,
            'total_data_mb': total_mb,
            'peak_usage_mb': peak_mb,
            'active_sessions': sessions
        })
        
        print(f"📊 {bs_id} ({region}): {total_mb:.2f} MB, {sessions} sessions")
    
    ti.xcom_push(key='aggregated_traffic', value=aggregated)

def identify_peak_loads(**context):
    """Identify base stations with peak loads"""
    ti = context['task_instance']
    execution_date = context['execution_date']
    aggregated = ti.xcom_pull(key='aggregated_traffic', task_ids='aggregate_by_tower')
    
    if not aggregated:
        return
    
    client = Client(host='clickhouse', port=9000)
    time_window = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    
    CONGESTION_THRESHOLD = 1000.0  # MB
    
    for data in aggregated:
        # Insert into aggregation table
        insert_query = f"""
            INSERT INTO network.traffic_hourly 
            (time_window, base_station_id, region, total_data_mb, peak_usage_mb, active_sessions)
            VALUES ('{time_window}', '{data['base_station_id']}', '{data['region']}', 
                    {data['total_data_mb']}, {data['peak_usage_mb']}, {data['active_sessions']})
        """
        client.execute(insert_query)
        
        # Check congestion
        if data['total_data_mb'] > CONGESTION_THRESHOLD:
            print(f"🚨 CONGESTION: {data['base_station_id']} at {data['total_data_mb']:.2f} MB!")
            # In production: trigger capacity planning alert
        else:
            print(f"✅ {data['base_station_id']} traffic normal: {data['total_data_mb']:.2f} MB")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_data_sessions',
    python_callable=extract_data_sessions,
    dag=dag
)

task_aggregate = PythonOperator(
    task_id='aggregate_by_tower',
    python_callable=aggregate_by_tower,
    dag=dag
)

task_identify = PythonOperator(
    task_id='identify_peak_loads',
    python_callable=identify_peak_loads,
    dag=dag
)

# Set task dependencies
task_extract >> task_aggregate >> task_identify
