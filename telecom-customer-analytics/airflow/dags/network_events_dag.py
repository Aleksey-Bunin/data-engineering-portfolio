"""
Network Events Pipeline DAG
Runs every 5 minutes to capture real-time customer network activity
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import random
from datetime import datetime as dt

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
    'network_events_pipeline',
    default_args=default_args,
    description='Real-time customer network activity (every 5 minutes)',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['customer', 'network', 'real-time']
)

def extract_real_time_events(**context):
    """Extract real-time network events from API"""
    # In production: call actual network monitoring API
    # For demo: generate sample data
    
    events = []
    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 101)]
    event_types = ['data_session', 'call_start', 'call_end', 'sms_sent']
    
    # Generate 50 random events
    for _ in range(50):
        event = {
            'customer_id': random.choice(customer_ids),
            'event_time': dt.now(),
            'event_type': random.choice(event_types),
            'data_usage_mb': round(random.uniform(0.1, 100.0), 2) if random.random() > 0.5 else 0,
            'call_duration_seconds': random.randint(10, 3600) if random.random() > 0.5 else 0
        }
        events.append(event)
    
    context['task_instance'].xcom_push(key='events', value=events)
    print(f"✅ Extracted {len(events)} network events")

def aggregate_active_sessions(**context):
    """Aggregate and load active sessions into PostgreSQL"""
    ti = context['task_instance']
    events = ti.xcom_pull(key='events', task_ids='extract_real_time_events')
    
    if not events:
        print("⚠️ No events to process")
        return
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    
    # Insert events
    inserted = 0
    for event in events:
        insert_query = """
            INSERT INTO network_events 
            (customer_id, event_time, event_type, data_usage_mb, call_duration_seconds)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            event['customer_id'],
            event['event_time'],
            event['event_type'],
            event['data_usage_mb'],
            event['call_duration_seconds']
        ))
        inserted += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {inserted} network events to PostgreSQL")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_real_time_events',
    python_callable=extract_real_time_events,
    dag=dag
)

task_aggregate = PythonOperator(
    task_id='aggregate_active_sessions',
    python_callable=aggregate_active_sessions,
    dag=dag
)

# Set task dependencies
task_extract >> task_aggregate
