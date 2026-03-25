# Data Migration & Cleaning Tool

## Overview
Migrated 50,000+ records from legacy ERP system to new database with zero data loss.

## Problem
- Legacy ERP exports had inconsistent formats
- Duplicates and missing values throughout dataset
- Manual review would take 2 weeks

## Solution
- Automated data profiling to identify quality issues
- Built validation rules for 15+ critical fields
- Implemented smart null-value handling
- Automated duplicate detection and removal

## Features
- **Data Profiling:** Analyze data quality before migration
- **Customer ID Cleaning:** Standardize and validate IDs
- **Field Validation:** Check required fields, data types, value ranges
- **Missing Value Handling:** Smart fill strategies based on column type
- **Outlier Detection:** IQR-based outlier identification
- **Migration Report:** Detailed audit trail

## Impact
- ✅ Migrated 50,000+ records with zero data loss
- ⏱️ Reduced manual review from 2 weeks to 3 days
- 📊 Ensured data integrity across 15+ fields

## Tech Stack
- Python 3.x
- Pandas, NumPy
- SQL (PostgreSQL)

## Usage
```python
from migration_script import DataMigrationTool

migrator = DataMigrationTool(
    source_path="legacy_export.csv",
    target_db={'host': 'localhost', 'database': 'new_db'}
)

report = migrator.run_migration()
```

## Notes
- Sensitive data anonymized per NDA
- Production-tested in enterprise environment
