"""
SMS Delivery Pipeline DAG
Runs every 30 minutes to monitor SMS delivery quality
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
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'sms_delivery_pipeline',
    default_args=default_args,
    description='SMS service quality monitoring (every 30 minutes)',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    tags=['customer', 'sms', 'quality']
)

def extract_sms_logs(**context):
    """Extract SMS delivery logs from API"""
    # In production: call actual SMS gateway API
    # For demo: generate sample data
    
    sms_logs = []
    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 101)]
    statuses = ['delivered', 'delivered', 'delivered', 'delivered', 'failed']  # 80% success rate
    failure_reasons = ['', '', '', '', 'Network timeout', 'Invalid number', 'Carrier rejected']
    
    # Generate 200 SMS records
    for i in range(200):
        status = random.choice(statuses)
        sms_log = {
            'sms_id': f"SMS{str(random.randint(100000, 999999))}",
            'customer_id': random.choice(customer_ids),
            'sent_time': dt.now() - timedelta(minutes=random.randint(1, 30)),
            'delivery_status': status,
            'failure_reason': random.choice(failure_reasons) if status == 'failed' else ''
        }
        sms_logs.append(sms_log)
    
    context['task_instance'].xcom_push(key='sms_logs', value=sms_logs)
    print(f"✅ Extracted {len(sms_logs)} SMS delivery logs")

def calculate_delivery_rate(**context):
    """Calculate SMS delivery success rate"""
    ti = context['task_instance']
    sms_logs = ti.xcom_pull(key='sms_logs', task_ids='extract_sms_logs')
    
    if not sms_logs:
        print("⚠️ No SMS logs available")
        return
    
    total_sms = len(sms_logs)
    delivered = sum(1 for log in sms_logs if log['delivery_status'] == 'delivered')
    failed = total_sms - delivered
    
    delivery_rate = (delivered / total_sms) * 100 if total_sms > 0 else 0
    
    stats = {
        'total_sms': total_sms,
        'delivered': delivered,
        'failed': failed,
        'delivery_rate': delivery_rate
    }
    
    ti.xcom_push(key='delivery_stats', value=stats)
    ti.xcom_push(key='sms_logs', value=sms_logs)
    
    print(f"📊 SMS Delivery Rate: {delivery_rate:.2f}% ({delivered}/{total_sms})")

def alert_if_below_threshold(**context):
    """Alert if delivery rate below threshold and load to PostgreSQL"""
    ti = context['task_instance']
    stats = ti.xcom_pull(key='delivery_stats', task_ids='calculate_delivery_rate')
    sms_logs = ti.xcom_pull(key='sms_logs', task_ids='calculate
