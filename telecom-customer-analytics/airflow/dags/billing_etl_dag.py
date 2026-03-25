"""
Billing ETL Pipeline DAG
Runs hourly to extract customer usage and billing data
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
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'billing_etl_pipeline',
    default_args=default_args,
    description='Hourly billing and usage data ETL',
    schedule_interval='0 * * * *',  # Every hour
    catchup=False,
    tags=['customer', 'billing', 'usage']
)

def extract_billing_data(**context):
    """Extract billing data from billing system API"""
    # In production: call actual billing system API
    # For demo: generate sample data
    
    billing_records = []
    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 101)]
    payment_statuses = ['paid', 'paid', 'paid', 'pending', 'overdue']  # 60% paid
    
    # Generate 50 billing records
    for _ in range(50):
        amount_charged = round(random.uniform(20.0, 150.0), 2)
        status = random.choice(payment_statuses)
        
        record = {
            'customer_id': random.choice(customer_ids),
            'billing_date': (dt.now() - timedelta(days=random.randint(1, 30))).date(),
            'amount_charged': amount_charged,
            'amount_paid': amount_charged if status == 'paid' else 0.0,
            'payment_status': status,
            'data_usage_mb': round(random.uniform(500, 15000), 2),
            'call_minutes': random.randint(100, 3000),
            'sms_count': random.randint(50, 500)
        }
        billing_records.append(record)
    
    context['task_instance'].xcom_push(key='billing_data', value=billing_records)
    print(f"✅ Extracted {len(billing_records)} billing records from API")

def clean_transform(**context):
    """Clean and transform billing data"""
    ti = context['task_instance']
    billing_data = ti.xcom_pull(key='billing_data', task_ids='extract_billing_data')
    
    if not billing_data:
        print("⚠️ No billing data to transform")
        return
    
    # Data quality checks
    cleaned = []
    for record in billing_data:
        # Validate amount
        if record['amount_charged'] < 0:
            print(f"⚠️ Negative amount for {record['customer_id']}, skipping")
            continue
        
        # Validate usage
        if record['data_usage_mb'] < 0 or record['call_minutes'] < 0:
            print(f"⚠️ Negative usage for {record['customer_id']}, skipping")
            continue
        
        cleaned.append(record)
    
    ti.xcom_push(key='cleaned_billing', value=cleaned)
    print(f"✅ Cleaned billing data: {len(cleaned)}/{len(billing_data)} records valid")

def load_to_postgres(**context):
    """Load billing data to PostgreSQL"""
    ti = context['task_instance']
    cleaned_billing = ti.xcom_pull(key='cleaned_billing', task_ids='clean_transform')
    
    if not cleaned_billing:
        print("⚠️ No data to load")
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
    
    # Insert billing records
    inserted = 0
    for record in cleaned_billing:
        insert_query = """
            INSERT INTO billing_history 
            (customer_id, billing_date, amount_charged, amount_paid, payment_status,
             data_usage_mb, call_minutes, sms_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            record['customer_id'],
            record['billing_date'],
            record['amount_charged'],
            record['amount_paid'],
            record['payment_status'],
            record['data_usage_mb'],
            record['call_minutes'],
            record['sms_count']
        ))
        inserted += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {inserted} billing records to PostgreSQL")
    
    # Calculate ARPU (Average Revenue Per User)
    total_revenue = sum(r['amount_charged'] for r in cleaned_billing)
    unique_customers = len(set(r['customer_id'] for r in cleaned_billing))
    arpu = total_revenue / unique_customers if unique_customers > 0 else 0
    
    print(f"📊 ARPU (Average Revenue Per User): ${arpu:.2f}")

# Define tasks
task_extract = PythonOperator(
    task_id='extract_billing_data',
    python_callable=extract_billing_data,
    dag=dag
)

task_transform = PythonOperator(
    task_id='clean_transform',
    python_callable=clean_transform,
    dag=dag
)

task_load = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

# Set task dependencies
task_extract >> task_transform >> task_load
