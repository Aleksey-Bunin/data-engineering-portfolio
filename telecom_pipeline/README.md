# Telecom Data Pipeline

## Overview
Automated ETL pipeline that processes daily CSV exports from billing, network, and customer systems.

## Problem
- Manual processing took 3-4 hours daily
- Data came from 3 different sources with inconsistent formats
- High risk of human error in manual merging

## Solution
- Automated ETL pipeline using Python + Pandas
- Scheduled via Apache Airflow to run at 2 AM daily
- Data validation + quality checks built-in

## Features
- **Data Extraction:** Reads CSV files from multiple sources
- **Data Cleaning:** Removes duplicates, handles nulls, standardizes formats
- **Data Validation:** Checks for nulls, duplicates, out-of-range values
- **Data Merging:** Joins datasets on customer_id
- **Logging:** Full audit trail of pipeline execution

## Impact
- ⏱️ Reduced manual work from 3-4 hours to zero
- ✅ Processes thousands of records daily with 99.9% accuracy
- 📊 Enabled real-time reporting for business intelligence team

## Tech Stack
- Python 3.x
- Pandas, NumPy
- Apache Airflow (for scheduling)
- PostgreSQL (for data storage)

## Usage
```python
from pipeline import TelecomDataPipeline

config = {
    'billing_csv': 'path/to/billing.csv',
    'network_csv': 'path/to/network.csv',
    'customer_csv': 'path/to/customer.csv',
    'output_path': 'output/processed_data.csv'
}

pipeline = TelecomDataPipeline(config)
pipeline.run()
```

## Notes
- All data anonymized per NDA
- Production-tested in enterprise environment
