# Data Engineering Portfolio

## About Me
Data Engineer specializing in ETL pipelines, data automation, and analytics dashboards. Experience across telecom and manufacturing industries.

**Core Skills:**
- Python (Pandas, NumPy, GeoPandas)
- SQL (PostgreSQL, ClickHouse)
- ETL/Data Pipelines (Apache Airflow)
- Data Visualization (Power BI, Apache Superset)
- API Integration & Automation

---

## Projects

### 1. Telecom Data Pipeline Automation
**Problem:** Manual data processing from multiple CSV exports took 3-4 hours daily.

**Solution:**
- Built automated ETL pipeline using Python (Pandas) and Apache Airflow
- Cleaned and merged data from 3+ sources (billing systems, network logs, customer databases)
- Added data validation checks (duplicates, null values, format consistency)
- Scheduled pipeline to run automatically at 2 AM daily

**Impact:**
- Reduced manual work from 3-4 hours to zero
- Processed thousands of records daily with 99.9% accuracy
- Enabled real-time reporting for business intelligence team

**Tech Stack:** Python, Pandas, Apache Airflow, PostgreSQL

**Code:** See `telecom_pipeline/` folder

---

### 2. Manufacturing Data Migration & Cleaning
**Problem:** Legacy ERP system exports contained inconsistent data formats, duplicates, and missing values.

**Solution:**
- Migrated 50,000+ records from legacy system to new database
- Built Python scripts for data profiling and quality checks
- Implemented validation rules for 15+ data fields
- Automated duplicate detection and null value handling

**Impact:**
- Zero data loss during migration
- Reduced manual review time from 2 weeks to 3 days
- Ensured data integrity across customer IDs, product codes, and transaction records

**Tech Stack:** Python, Pandas, SQL, data validation libraries

**Code:** See `data_migration/` folder

---

### 3. CSV Data Cleaning & Validation Toolkit
**Problem:** Messy CSV exports from various sources required manual cleaning.

**Solution:**
- Created reusable Python scripts for common data cleaning tasks
- Automated duplicate removal, null value handling, and format standardization
- Built validation layer with configurable business rules

**Impact:**
- Reusable across multiple projects
- 80% reduction in data prep time

**Tech Stack:** Python, Pandas, NumPy

**Code:** See `csv_toolkit/` folder

---

### 4. Telecom Network Monitoring Pipeline
**Problem:** Network Operations Center learned about network issues from customer complaints, not from monitoring systems.

**Solution:**
- Built real-time event streaming pipeline using Apache Kafka
- ClickHouse for storing billions of network events
- Apache Airflow orchestrating 4 scheduled aggregations (5 min, 30 min, hourly, daily)
- Grafana dashboards for NOC monitoring

**Impact:**
- ⏱️ NOC detects network issues in 5 minutes (was 24 hours)
- ✅ Call drop rate reduced by 12%
- 📊 Processing 10M+ events per hour

**Tech Stack:** Kafka, ClickHouse, Apache Airflow, Grafana, Python, Docker

**Code:** See `telecom-network-monitoring/` folder

---

### 5. Telecom Customer Analytics Pipeline
**Problem:** Customer data fragmented across systems. Analysts spent 3 hours/day manually collecting data. Marketing unable to identify churn risk.

**Solution:**
- Built automated ETL pipeline using Apache Airflow
- 5 pipelines at different schedules (5 min, 30 min, hourly, daily, weekly)
- Consolidated billing, CRM, and market data into PostgreSQL
- Churn prediction scoring algorithm
- Grafana dashboards for marketing team

**Impact:**
- ✅ Churn rate decreased by 8%
- ⏱️ Eliminated 3 hours/day of manual work
- 💰 Retained $2M+ annual revenue through early churn detection

**Tech Stack:** Apache Airflow, PostgreSQL, Python, Grafana, Docker

**Code:** See `telecom-customer-analytics/` folder

---

## Technical Skills

**Languages & Tools:**
- Python, SQL
- Apache Airflow, Git
- PostgreSQL, ClickHouse
- Power BI, Apache Superset

**Specializations:**
- ETL Pipeline Development
- Data Quality & Validation
- Data Migration & Integration
- Process Automation

---

## Notes
- All code examples use anonymized data
- Client names and sensitive information removed per NDA agreements
