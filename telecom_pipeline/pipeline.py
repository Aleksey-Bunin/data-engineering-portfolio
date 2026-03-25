"""
Telecom Data Pipeline - Automated ETL
Processes daily CSV exports from billing, network, and customer systems
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TelecomDataPipeline:
    """
    ETL pipeline for processing telecom data from multiple sources
    """
    
    def __init__(self, config):
        self.billing_path = config.get('billing_csv')
        self.network_path = config.get('network_csv')
        self.customer_path = config.get('customer_csv')
        self.output_path = config.get('output_path')
        
    def extract_data(self):
        """Extract data from CSV sources"""
        logging.info("Starting data extraction...")
        
        try:
            # Read CSV files
            billing_df = pd.read_csv(self.billing_path, encoding='utf-8')
            network_df = pd.read_csv(self.network_path, encoding='utf-8')
            customer_df = pd.read_csv(self.customer_path, encoding='utf-8')
            
            logging.info(f"Billing records: {len(billing_df)}")
            logging.info(f"Network records: {len(network_df)}")
            logging.info(f"Customer records: {len(customer_df)}")
            
            return billing_df, network_df, customer_df
            
        except Exception as e:
            logging.error(f"Error extracting data: {e}")
            raise
    
    def clean_data(self, df, dataset_name):
        """Clean and validate data"""
        logging.info(f"Cleaning {dataset_name} data...")
        
        initial_rows = len(df)
        
        # Remove duplicates
        df = df.drop_duplicates()
        duplicates_removed = initial_rows - len(df)
        
        # Handle null values
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            logging.warning(f"Null values found: {null_counts[null_counts > 0].to_dict()}")
        
        # Fill or drop nulls based on column type
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col].fillna('UNKNOWN', inplace=True)
            else:
                df[col].fillna(0, inplace=True)
        
        # Standardize text fields
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            df[col] = df[col].str.strip().str.upper()
        
        logging.info(f"Cleaned {dataset_name}: removed {duplicates_removed} duplicates")
        
        return df
    
    def validate_data(self, df, validation_rules):
        """Run validation checks"""
        logging.info("Running data validation...")
        
        errors = []
        
        for rule in validation_rules:
            field = rule['field']
            check_type = rule['type']
            
            if check_type == 'not_null':
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    errors.append(f"{field}: {null_count} null values found")
            
            elif check_type == 'unique':
                duplicate_count = df[field].duplicated().sum()
                if duplicate_count > 0:
                    errors.append(f"{field}: {duplicate_count} duplicate values found")
            
            elif check_type == 'range':
                min_val, max_val = rule['min'], rule['max']
                out_of_range = ((df[field] < min_val) | (df[field] > max_val)).sum()
                if out_of_range > 0:
                    errors.append(f"{field}: {out_of_range} values out of range [{min_val}, {max_val}]")
        
        if errors:
            logging.warning(f"Validation errors: {errors}")
        else:
            logging.info("All validation checks passed")
        
        return errors
    
    def merge_data(self, billing_df, network_df, customer_df):
        """Merge data from multiple sources"""
        logging.info("Merging datasets...")
        
        # Merge billing + customer on customer_id
        merged_df = pd.merge(
            billing_df, 
            customer_df, 
            on='customer_id', 
            how='left',
            suffixes=('_billing', '_customer')
        )
        
        # Merge with network data on customer_id
        merged_df = pd.merge(
            merged_df,
            network_df,
            on='customer_id',
            how='left'
        )
        
        logging.info(f"Merged dataset: {len(merged_df)} records")
        
        return merged_df
    
    def export_data(self, df):
        """Export processed data"""
        logging.info("Exporting data...")
        
        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8')
            logging.info(f"Data exported to {self.output_path}")
        except Exception as e:
            logging.error(f"Error exporting data: {e}")
            raise
    
    def run(self):
        """Execute full pipeline"""
        start_time = datetime.now()
        logging.info("=== Pipeline started ===")
        
        # Extract
        billing_df, network_df, customer_df = self.extract_data()
        
        # Clean
        billing_df = self.clean_data(billing_df, "billing")
        network_df = self.clean_data(network_df, "network")
        customer_df = self.clean_data(customer_df, "customer")
        
        # Validate
        validation_rules = [
            {'field': 'customer_id', 'type': 'not_null'},
            {'field': 'customer_id', 'type': 'unique'},
            {'field': 'billing_amount', 'type': 'range', 'min': 0, 'max': 10000}
        ]
        self.validate_data(billing_df, validation_rules)
        
        # Merge
        final_df = self.merge_data(billing_df, network_df, customer_df)
        
        # Export
        self.export_data(final_df)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"=== Pipeline completed in {duration:.2f} seconds ===")


# Example usage (when run with Apache Airflow or as standalone script)
if __name__ == "__main__":
    config = {
        'billing_csv': 'data/billing_export.csv',
        'network_csv': 'data/network_logs.csv',
        'customer_csv': 'data/customer_data.csv',
        'output_path': 'output/processed_data.csv'
    }
    
    pipeline = TelecomDataPipeline(config)
    pipeline.run()
