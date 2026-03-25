# CSV Data Cleaning Toolkit

## Overview
Reusable Python scripts for common CSV cleaning tasks.

## Features
- **Duplicate Removal:** Remove duplicate rows based on key columns
- **Null Handling:** Drop, fill, or smart-fill missing values
- **Text Standardization:** Trim whitespace, convert to uppercase
- **Format Validation:** Check columns against regex patterns
- **Outlier Detection:** IQR-based outlier identification

## Impact
- 80% reduction in data prep time
- Reusable across multiple projects
- Consistent data quality standards

## Usage
```python
from csv_cleaner import clean_csv_file

config = {
    'remove_duplicates': True,
    'handle_nulls': 'smart',
    'standardize_text': True,
    'text_columns': ['name', 'city']
}

clean_csv_file(
    input_path="messy_data.csv",
    output_path="clean_data.csv",
    cleaning_config=config
)
```

## Functions

### `remove_duplicates(df, subset=None, keep='first')`
Remove duplicate rows from DataFrame.

### `handle_null_values(df, strategy='drop', fill_value=None)`
Handle null values. Strategies: 'drop', 'fill', 'smart'.

### `standardize_text(df, columns=None)`
Standardize text columns (trim, uppercase).

### `validate_format(df, column, pattern, regex=True)`
Validate column format against pattern.

### `detect_outliers_iqr(df, column, multiplier=1.5)`
Detect outliers using IQR method.

## Tech Stack
- Python 3.x
- Pandas, NumPy

## Notes
- Designed for reusability across projects
- Production-tested on telecom and manufacturing datasets
```

