"""
SLA Reports DAG
Runs daily to calculate SLA compliance and send reports
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
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'sla_reports',
    default_args=default_args,
    description='Daily SLA compliance reporting',
    schedule_interval='0 2 * * *',  # Every day at 2 AM
    catchup=False,
    tags=['network', 'sla', 'reporting']
)

def aggregate_daily_metrics(**context):
    """Aggregate metrics from previous day"""
    client = Client(host='clickhouse', port=9000)
    execution_date = context['execution_date']
    
    # Get yesterday's date
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    query = f"""
        SELECT 
            region,
            COUNT(*) as total_events,
            countIf(error_code != '') as error_events
        FROM network.events
        WHERE toDate(timestamp) = '{yesterday}'
        GROUP BY region
    """
    
    results = client.execute(query)
    
    context['task_instance'].xcom_push(key='daily_metrics', value=results)
    context['task_instance'].xcom_push(key='report_date', value=yesterday)
    print(f"✅ Aggregated metrics for {yesterday}: {len(results)} regions")

def calculate_sla_compliance(**context):
    """Calculate SLA compliance (uptime %)"""
    ti = context['task_instance']
    daily_metrics = ti.xcom_pull(key='daily_metrics', task_ids='aggregate_daily_metrics')
    
    if not daily_metrics:
        print("⚠️ No daily metrics available")
        return
    
    SLA_TARGET = 99.5  # 99.5% uptime required
    
    sla_results = []
    for region, total_events, error_events in daily_metrics:
        if total_events > 0:
            uptime_pct = ((total_events - error_events) / total_events) * 100
        else:
            uptime_pct = 100.0
        
        sla_met = uptime_pct >= SLA_TARGET
        
        sla_results.append({
            'region': region,
            'total_events': total_events,
            'error_events': error_events,
            'uptime_pct': uptime_pct,
            'sla_met': sla_met
        })
        
        status = "✅ MET" if sla_met else "❌ FAILED"
        print(f"📊 {region}: {uptime_pct:.2f}% uptime {status}")
    
    ti.xcom_push(key='sla_results', value=sla_results)

def send_email_alerts(**context):
    """Send email alerts for SLA violations"""
    ti = context['task_instance']
    sla_results = ti.xcom_pull(key='sla_results', task_ids='calculate_sla_compliance')
    report_date = ti.xcom_pull(key='report_date', task_ids='aggregate_daily_metrics')
    
    if not sla_results:
        return
    
    client = Client(host='clickhouse', port=9000)
    
    violations = []
    for data in sla_results:
        # Insert into SLA table
        insert_query = f"""
            INSERT INTO network.sla_daily 
            (date, region, total_events, error_events, uptime_percentage, sla_met)
            VALUES ('{report_date}', '{data['region']}', {data['total_events']}, 
                    {data['error_events']}, {data['uptime_pct']}, {1 if data['sla_met'] else 0})
        """
        client.execute(insert_query)
        
        # Collect violations
        if not data['sla_met']:
            violations.append(f"{data['region']}: {data['uptime_pct']:.2f}%")
    
    if violations:
        print(f"🚨 SLA VIOLATIONS on {report_date}:")
        for v in violations:
            print(f"   - {v}")
        # In production: send email with violation details
    else:
        print(f"✅ All regions met SLA on {report_date}")

# Define tasks
task_aggregate = PythonOperator(
    task_id='aggregate_daily_metrics',
    python_callable=aggregate_daily_metrics,
    dag=dag
)

task_calculate = PythonOperator(
    task_id='calculate_sla_compliance',
    python_callable=calculate_sla_compliance,
    dag=dag
)

task_alert = PythonOperator(
    task_id='send_email_alerts',
    python_callable=send_email_alerts,
    dag=dag
)

# Set task dependencies
task_aggregate >> task_calculate >> task_alert
