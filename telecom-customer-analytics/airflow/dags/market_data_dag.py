"""
Market Intelligence Pipeline DAG
Runs weekly to extract competitor pricing and market data
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
    'retry_delay': timedelta(minutes=15),
}

dag = DAG(
    'market_data_pipeline',
    default_args=default_args,
    description='Weekly competitive intelligence gathering',
    schedule_interval='0 4 * * 0',  # Every Sunday at 4 AM
    catchup=False,
    tags=['market', 'competitive', 'pricing']
)

def extract_market_data(**context):
    """Extract competitor pricing from market intelligence API"""
    # In production: call actual market data API or web scraping
    # For demo: generate sample competitor data
    
    market_data = []
    competitors = ['Competitor A', 'Competitor B', 'Competitor C', 'Competitor D']
    plan_types = ['Basic', 'Standard', 'Premium', 'Unlimited']
    
    our_prices = {
        'Basic': 25.00,
        'Standard': 45.00,
        'Premium': 65.00,
        'Unlimited': 85.00
    }
    
    for competitor in competitors:
        for plan in plan_types:
            our_price = our_prices[plan]
            # Competitor prices vary ±20% from our prices
            competitor_price = round(our_price * random.uniform(0.8, 1.2), 2)
            
            record = {
                'competitor_name': competitor,
                'plan_type': plan,
                'price': competitor_price,
                'data_allowance_gb': random.choice([5, 10, 20, 50, 999]),  # 999 = unlimited
                'our_price': our_price
            }
            market_data.append(record)
    
    context['task_instance'].xcom_push(key='market_data', value=market_data)
    print(f"✅ Extracted {len(market_data)} competitor pricing records")

def compare_competitor_pricing(**context):
    """Analyze competitive positioning"""
    ti = context['task_instance']
    market_data = ti.xcom_pull(key='market_data', task_ids='extract_market_data')
    
    if not market_data:
        print("⚠️ No market data to analyze")
        return
    
    analyzed = []
    for record in market_data:
        price_diff = record['our_price'] - record['price']
        
        # Determine competitive position
        if price_diff < -5:
            position = 'More Expensive'
        elif price_diff > 5:
            position = 'Cheaper'
        else:
            position = 'Competitive'
        
        record['price_difference'] = price_diff
        record['competitive_position'] = position
        
        analyzed.append(record)
        
        print(f"📊 {record['competitor_name']} {record['plan_type']}: "
              f"${record['price']} vs Our ${record['our_price']} ({position})")
    
    ti.xcom_push(key='analyzed_data', value=analyzed)

def calculate_competitive_position(**context):
    """Calculate overall competitive position and load to PostgreSQL"""
    ti = context['task_instance']
    analyzed_data = ti.xcom_pull(key='analyzed_data', task_ids='compare_competitor_pricing')
    
    if not analyzed_data:
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
    
    report_date = dt.now().date()
    
    # Insert market intelligence data
    inserted = 0
    cheaper_count = 0
    expensive_count = 0
    
    for record in analyzed_data:
        insert_query = """
            INSERT INTO market_intelligence 
            (report_date, competitor_name, plan_type, price, data_allowance_gb,
             our_price, price_difference, competitive_position)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            report_date,
            record['competitor_name'],
            record['plan_type'],
            record['price'],
            record['data_allowance_gb'],
            record['our_price'],
            record['price_difference'],
            record['competitive_position']
        ))
        inserted += 1
        
        if record['competitive_position'] == 'Cheaper':
            cheaper_count += 1
        elif record['competitive_position'] == 'More Expensive':
            expensive_count += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {inserted} market intelligence records to PostgreSQL")
    print(f"📊 Competitive Summary:")
    print(f"   - We are cheaper: {cheaper_count} plans")
    print(f"   - We are more expensive: {expensive_count} plans")
    print(f"   - Competitive parity: {inserted - cheaper_count - expensive_count} plans")
    
    if expensive_count > cheaper_count:
        print(f"⚠️ ACTION REQUIRED: Review pricing strategy - we are more expensive on {expensive_count} plans")
        # In production: alert marketing team

# Define tasks
task_extract = PythonOperator(
    task_id='extract_market_data',
    python_callable=extract_market_data,
    dag=dag
)

task_compare = PythonOperator(
    task_id='compare_competitor_pricing',
    python_callable=compare_competitor_pricing,
    dag=dag
)

task_calculate = PythonOperator(
    task_id='calculate_competitive_position',
    python_callable=calculate_competitive_position,
    dag=dag
)

# Set task dependencies
task_extract >> task_compare >> task_calculate
```

4. **Commit:** `Add weekly market intelligence DAG`

---

## **ШАГ 15: Создаём requirements.txt для обоих проектов**

1. **Add file** → **Create new file**
2. Напиши: `telecom-network-monitoring/requirements.txt`
3. **ВСТАВЬ:**
```
kafka-python==2.0.2
clickhouse-driver==0.2.6
```

4. **Commit:** `Add requirements for network monitoring`

5. **Add file** → **Create new file**
6. Напиши: `telecom-customer-analytics/requirements.txt`
7. **ВСТАВЬ:**
```
psycopg2-binary==2.9.9
pandas==2.1.4
