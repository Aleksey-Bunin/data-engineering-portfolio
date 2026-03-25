"""
Data Migration & Cleaning Script
Migrates data from legacy ERP to new database with validation
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataMigrationTool:
    """
    Tool for migrating and cleaning data from legacy systems
    """
    
    def __init__(self, source_path, target_db_config):
        self.source_path = source_path
        self.target_db = target_db_config
        self.validation_report = []
        
    def profile_data(self, df):
        """Generate data quality profile"""
        logging.info("Profiling source data...")
        
        profile = {
            'total_records': len(df),
            'columns': len(df.columns),
            'duplicates': df.duplicated().sum(),
            'null_values': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.to_dict()
        }
        
        logging.info(f"Profile: {profile['total_records']} records, {profile['duplicates']} duplicates")
        
        return profile
    
    def clean_customer_ids(self, df, id_column='customer_id'):
        """Clean and validate customer IDs"""
        logging.info(f"Cleaning {id_column}...")
        
        initial_count = len(df)
        
        # Remove nulls
        df = df[df[id_column].notnull()]
        
        # Remove duplicates
        df = df.drop_duplicates(subset=[id_column])
        
        # Standardize format (remove spaces, uppercase)
        df[id_column] = df[id_column].astype(str).str.strip().str.upper()
        
        removed = initial_count - len(df)
        logging.info(f"Removed {removed} invalid/duplicate {id_column}")
        
        return df
    
    def validate_fields(self, df, field_rules):
        """Validate data fields against business rules"""
        logging.info("Validating data fields...")
        
        errors = []
        
        for field, rules in field_rules.items():
            if field not in df.columns:
                errors.append(f"Missing field: {field}")
                continue
            
            # Check required fields
            if rules.get('required') and df[field].isnull().any():
                null_count = df[field].isnull().sum()
                errors.append(f"{field}: {null_count} null values (required field)")
            
            # Check data type
            if 'type' in rules:
                expected_type = rules['type']
                if expected_type == 'numeric':
                    non_numeric = pd.to_numeric(df[field], errors='coerce').isnull().sum()
                    if non_numeric > 0:
                        errors.append(f"{field}: {non_numeric} non-numeric values")
            
            # Check value range
            if 'min' in rules or 'max' in rules:
                numeric_series = pd.to_numeric(df[field], errors='coerce')
                if 'min' in rules:
                    below_min = (numeric_series < rules['min']).sum()
                    if below_min > 0:
                        errors.append(f"{field}: {below_min} values below minimum {rules['min']}")
                if 'max' in rules:
                    above_max = (numeric_series > rules['max']).sum()
                    if above_max > 0:
                        errors.append(f"{field}: {above_max} values above maximum {rules['max']}")
        
        self.validation_report.extend(errors)
        
        if errors:
            logging.warning(f"Validation errors: {len(errors)}")
            for error in errors:
                logging.warning(f"  - {error}")
        else:
            logging.info("All validation checks passed")
        
        return errors
    
    def handle_missing_values(self, df, strategy='smart'):
        """Handle missing values based on column type"""
        logging.info("Handling missing values...")
        
        for col in df.columns:
            null_count = df[col].isnull().sum()
            
            if null_count > 0:
                if df[col].dtype == 'object':
                    # Text fields: fill with 'UNKNOWN'
                    df[col].fillna('UNKNOWN', inplace=True)
                elif df[col].dtype in ['int64', 'float64']:
                    # Numeric fields: fill with 0 or median
                    if strategy == 'smart':
                        df[col].fillna(df[col].median(), inplace=True)
                    else:
                        df[col].fillna(0, inplace=True)
                
                logging.info(f"Filled {null_count} nulls in {col}")
        
        return df
    
    def detect_outliers(self, df, numeric_columns):
        """Detect outliers using IQR method"""
        logging.info("Detecting outliers...")
        
        outliers_summary = {}
        
        for col in numeric_columns:
            if col in df.columns and df[col].dtype in ['int64', 'float64']:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                
                if outliers > 0:
                    outliers_summary[col] = outliers
                    logging.warning(f"{col}: {outliers} outliers detected")
        
        return outliers_summary
    
    def export_to_database(self, df, table_name):
        """Export cleaned data to target database"""
        logging.info(f"Exporting to database table: {table_name}")
        
        # In production, this would use SQLAlchemy or similar
        # For demo purposes, we'll export to CSV
        output_path = f"output/{table_name}_cleaned.csv"
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        logging.info(f"Exported {len(df)} records to {output_path}")
    
    def generate_report(self):
        """Generate migration report"""
        logging.info("Generating migration report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'validation_errors': self.validation_report,
            'status': 'completed' if not self.validation_report else 'completed_with_warnings'
        }
        
        return report
    
    def run_migration(self):
        """Execute full migration workflow"""
        start_time = datetime.now()
        logging.info("=== Migration started ===")
        
        # Load source data
        df = pd.read_csv(self.source_path, encoding='utf-8')
        logging.info(f"Loaded {len(df)} records from source")
        
        # Profile data
        profile = self.profile_data(df)
        
        # Clean customer IDs
        df = self.clean_customer_ids(df)
        
        # Validate fields
        field_rules = {
            'customer_id': {'required': True},
            'product_code': {'required': True},
            'transaction_amount': {'required': True, 'type': 'numeric', 'min': 0, 'max': 1000000},
            'transaction_date': {'required': True}
        }
        self.validate_fields(df, field_rules)
        
        # Handle missing values
        df = self.handle_missing_values(df, strategy='smart')
        
        # Detect outliers
        outliers = self.detect_outliers(df, ['transaction_amount', 'quantity'])
        
        # Export to database
        self.export_to_database(df, 'cleaned_transactions')
        
        # Generate report
        report = self.generate_report()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"=== Migration completed in {duration:.2f} seconds ===")
        logging.info(f"Final record count: {len(df)}")
        
        return report


# Example usage
if __name__ == "__main__":
    source_path = "data/legacy_erp_export.csv"
    target_db = {'host': 'localhost', 'database': 'new_erp'}
    
    migrator = DataMigrationTool(source_path, target_db)
    report = migrator.run_migration()
    
    print("Migration Report:")
    print(report)
