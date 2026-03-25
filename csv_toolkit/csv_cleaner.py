"""
CSV Data Cleaning Toolkit
Reusable scripts for common CSV cleaning tasks
"""

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)

def remove_duplicates(df, subset=None, keep='first'):
    """
    Remove duplicate rows from DataFrame
    
    Args:
        df: Input DataFrame
        subset: Column(s) to check for duplicates
        keep: Which duplicates to keep ('first', 'last', False)
    
    Returns:
        Cleaned DataFrame
    """
    initial_count = len(df)
    df_clean = df.drop_duplicates(subset=subset, keep=keep)
    removed = initial_count - len(df_clean)
    
    logging.info(f"Removed {removed} duplicate rows ({removed/initial_count*100:.1f}%)")
    
    return df_clean


def handle_null_values(df, strategy='drop', fill_value=None):
    """
    Handle null values in DataFrame
    
    Args:
        df: Input DataFrame
        strategy: 'drop', 'fill', 'smart'
        fill_value: Value to fill nulls (if strategy='fill')
    
    Returns:
        Cleaned DataFrame
    """
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    
    if total_nulls == 0:
        logging.info("No null values found")
        return df
    
    logging.info(f"Found {total_nulls} null values across {(null_counts > 0).sum()} columns")
    
    if strategy == 'drop':
        df_clean = df.dropna()
    elif strategy == 'fill':
        df_clean = df.fillna(fill_value)
    elif strategy == 'smart':
        df_clean = df.copy()
        for col in df_clean.columns:
            if df_clean[col].isnull().sum() > 0:
                if df_clean[col].dtype == 'object':
                    df_clean[col].fillna('UNKNOWN', inplace=True)
                else:
                    df_clean[col].fillna(df_clean[col].median(), inplace=True)
    
    return df_clean


def standardize_text(df, columns=None):
    """
    Standardize text columns (trim, uppercase)
    
    Args:
        df: Input DataFrame
        columns: List of columns to standardize (None = all text columns)
    
    Returns:
        Cleaned DataFrame
    """
    df_clean = df.copy()
    
    if columns is None:
        columns = df_clean.select_dtypes(include=['object']).columns
    
    for col in columns:
        if col in df_clean.columns and df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].str.strip().str.upper()
            logging.info(f"Standardized text in column: {col}")
    
    return df_clean


def validate_format(df, column, pattern, regex=True):
    """
    Validate column format against pattern
    
    Args:
        df: Input DataFrame
        column: Column to validate
        pattern: Regex pattern or fixed string
        regex: Whether pattern is regex
    
    Returns:
        Boolean Series indicating valid rows
    """
    if regex:
        valid_mask = df[column].str.match(pattern, na=False)
    else:
        valid_mask = df[column] == pattern
    
    invalid_count = (~valid_mask).sum()
    
    if invalid_count > 0:
        logging.warning(f"{column}: {invalid_count} rows don't match pattern")
    else:
        logging.info(f"{column}: all rows match pattern")
    
    return valid_mask


def detect_outliers_iqr(df, column, multiplier=1.5):
    """
    Detect outliers using IQR method
    
    Args:
        df: Input DataFrame
        column: Numeric column to check
        multiplier: IQR multiplier (default 1.5)
    
    Returns:
        Boolean Series indicating outlier rows
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    outliers = (df[column] < lower_bound) | (df[column] > upper_bound)
    outlier_count = outliers.sum()
    
    logging.info(f"{column}: {outlier_count} outliers detected (bounds: [{lower_bound:.2f}, {upper_bound:.2f}])")
    
    return outliers


def clean_csv_file(input_path, output_path, cleaning_config):
    """
    Clean CSV file based on configuration
    
    Args:
        input_path: Path to input CSV
        output_path: Path to output CSV
        cleaning_config: Dict with cleaning options
    
    Example config:
        {
            'remove_duplicates': True,
            'handle_nulls': 'smart',
            'standardize_text': True,
            'text_columns': ['name', 'city']
        }
    """
    logging.info(f"Loading CSV from {input_path}")
    df = pd.read_csv(input_path, encoding='utf-8')
    
    initial_count = len(df)
    logging.info(f"Initial records: {initial_count}")
    
    # Remove duplicates
    if cleaning_config.get('remove_duplicates'):
        df = remove_duplicates(df)
    
    # Handle nulls
    if 'handle_nulls' in cleaning_config:
        df = handle_null_values(df, strategy=cleaning_config['handle_nulls'])
    
    # Standardize text
    if cleaning_config.get('standardize_text'):
        columns = cleaning_config.get('text_columns')
        df = standardize_text(df, columns=columns)
    
    # Export
    df.to_csv(output_path, index=False, encoding='utf-8')
    logging.info(f"Cleaned CSV saved to {output_path}")
    logging.info(f"Final records: {len(df)} ({len(df)/initial_count*100:.1f}% of original)")


# Example usage
if __name__ == "__main__":
    config = {
        'remove_duplicates': True,
        'handle_nulls': 'smart',
        'standardize_text': True,
        'text_columns': ['customer_name', 'city', 'product']
    }
    
    clean_csv_file(
        input_path="data/messy_export.csv",
        output_path="output/cleaned_export.csv",
        cleaning_config=config
    )
