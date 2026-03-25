"""
CRM ETL Pipeline DAG with Churn Scoring
Runs daily to extract CRM data and calculate churn risk
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
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'crm_etl_pipeline',
    default_args=default_args,
    description='Daily CRM data ETL with churn prediction',
    schedule_interval='0 3 * * *',  # Every day at 3 AM
    catchup=False,
    tags=['customer', 'crm', 'churn']
)

def extract_crm_data(**context):
    """Extract support tickets from CRM API"""
    # In production: call actual CRM system API
    # For demo: generate sample data
    
    tickets = []
    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 101)]
    issue_types = ['Network Issue', 'Billing Problem', 'Technical Support', 'Account Change', 'Complaint']
    priorities = ['Low', 'Medium', 'High', 'Critical']
    statuses = ['Open', 'In Progress', 'Resolved', 'Closed']
    
    # Generate 30 support tickets
    for i in range(30):
        ticket = {
            'ticket_id': f"TKT{str(random.randint(100000, 999999))}",
            'customer_id': random.choice(customer_ids),
            'ticket_date': (dt.now() - timedelta(days=random.randint(1, 30))).date(),
            'issue_type': random.choice(issue_types),
            'priority': random.choice(priorities),
            'status': random.choice(statuses),
            'resolution_time_hours': random.randint(1, 72) if random.random() > 0.3 else None
        }
        tickets.append(ticket)
    
    context['task_instance'].xcom_push(key='crm_tickets', value=tickets)
    print(f"✅ Extracted {len(tickets)} support tickets from CRM")

def enrich_customer_profile(**context):
    """Enrich customer data with ticket history"""
    ti = context['task_instance']
    tickets = ti.xcom_pull(key='crm_tickets', task_ids='extract_crm_data')
    
    if not tickets:
        print("⚠️ No tickets to process")
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
    
    # Insert tickets
    inserted = 0
    for ticket in tickets:
        insert_query = """
            INSERT INTO support_tickets 
            (ticket_id, customer_id, ticket_date, issue_type, priority, status, resolution_time_hours)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO NOTHING
        """
        
        cursor.execute(insert_query, (
            ticket['ticket_id'],
            ticket['customer_id'],
            ticket['ticket_date'],
            ticket['issue_type'],
            ticket['priority'],
            ticket['status'],
            ticket['resolution_time_hours']
        ))
        inserted += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {inserted} support tickets to PostgreSQL")
    
    ti.xcom_push(key='tickets', value=tickets)

def calculate_churn_score(**context):
    """Calculate churn probability score for each customer"""
    ti = context['task_instance']
    tickets = ti.xcom_pull(key='tickets', task_ids='enrich_customer_profile')
    
    if not tickets:
        print("⚠️ No data for churn scoring")
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
    
    # Get unique customers from tickets
    customer_ids = list(set(t['customer_id'] for t in tickets))
    
    churn_scores = []
    for customer_id in customer_ids:
        # Count tickets for this customer
        cursor.execute("""
            SELECT COUNT(*) FROM support_tickets 
            WHERE customer_id = %s AND ticket_date >= CURRENT_DATE - INTERVAL '30 days'
        """, (customer_id,))
        ticket_count = cursor.fetchone()[0]
        
        # Get billing history
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE payment_status = 'overdue') as late_payments,
                AVG(data_usage_mb) as avg_usage
            FROM billing_history 
            WHERE customer_id = %s AND billing_date >= CURRENT_DATE - INTERVAL '90 days'
        """, (customer_id,))
        
        billing_data = cursor.fetchone()
        late_payments = billing_data[0] if billing_data else 0
        avg_usage = billing_data[1] if billing_data and billing_data[1] else 0
        
        # Churn scoring algorithm
        # Formula: 0.3*late_payments + 0.25*ticket_count + 0.2*usage_decline + 0.15*contract_expiry + 0.1*competitor_price
        
        late_payment_score = min(late_payments * 10, 30)  # Max 30 points
        support_ticket_score = min(ticket_count * 5, 25)   # Max 25 points
        usage_decline_score = 10 if avg_usage < 1000 else 0  # 10 points if low usage
        
        # Simplified churn score (0-100)
        churn_score = late_payment_score + support_ticket_score + usage_decline_score
        churn_probability = min(churn_score, 100)
        
        # Risk category
        if churn_probability >= 70:
            risk_category = 'High'
        elif churn_probability >= 40:
            risk_category = 'Medium'
        else:
            risk_category = 'Low'
        
        churn_scores.append({
            'customer_id': customer_id,
            'churn_probability': churn_probability,
            'risk_category': risk_category,
            'late_payment_score': late_payment_score,
            'support_ticket_score': support_ticket_score,
            'usage_decline_score': usage_decline_score
        })
        
        print(f"📊 {customer_id}: {churn_probability}% churn risk ({risk_category})")
    
    cursor.close()
    conn.close()
    
    ti.xcom_push(key='churn_scores', value=churn_scores)

def load_to_postgres(**context):
    """Load churn scores to PostgreSQL"""
    ti = context['task_instance']
    churn_scores = ti.xcom_pull(key='churn_scores', task_ids='calculate_churn_score')
    
    if not churn_scores:
        print("⚠️ No churn scores to load")
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
    
    score_date = dt.now().date()
    
    # Insert churn scores
    inserted = 0
    high_risk_count = 0
    
    for score in churn_scores:
        insert_query = """
            INSERT INTO churn_scores 
            (customer_id, score_date, churn_probability, risk_category,
             late_payment_score, support_ticket_score, usage_decline_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            score['customer_id'],
            score_date,
            score['churn_probability'],
            score['risk_category'],
            score['late_payment_score'],
            score['support_ticket_score'],
            score['usage_decline_score']
        ))
        inserted += 1
        
        if score['risk_category'] == 'High':
            high_risk_count += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {inserted} churn scores to PostgreSQL")
    print(f"🚨 High-risk customers: {high_risk_count}")
    
    if high_risk_count > 0:
        print(f"⚠️ ACTION REQUIRED: {high_risk_count} customers at high churn risk")
        # In production: trigger retention campaign automation

# Define tasks
task_extract = PythonOperator(
    task_id='extract_crm_data',
    python_callable=extract_crm_data,
    dag=dag
)

task_enrich = PythonOperator(
    task_id='enrich_customer_profile',
    python_callable=enrich_customer_profile,
    dag=dag
)

task_calculate = PythonOperator(
    task_id='calculate_churn_score',
    python_callable=calculate_churn_score,
    dag=dag
)

task_load = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

# Set task dependencies
task_extract >> task_enrich >> task_calculate >> task_load
