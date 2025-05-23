"""Configuration settings for the comparison framework"""

# Source types supported by the framework
SOURCE_TYPES = [
    "CSV File",
    "DAT File",
    "SQL Server",
    "Stored Procedure",
    "Teradata",
    "API",
    "Parquet File",
    "Zipped Flat Files"
]

# Data type mapping dictionary
TYPE_MAPPING = {
    'int': 'int32',
    'int64': 'int64',
    'numeric': 'int64',
    'bigint': 'int64',
    'smallint': 'int64',
    'varchar': 'string',
    'nvarchar': 'string',
    'char': 'string',
    'date': 'datetime64[ns]',
    'datetime': 'datetime64[ns]',
    'decimal': 'float',
    'float': 'float',
    'bit': 'bool',
    'nchar': 'char',
    'boolean': 'bool'
}

# Chunk size for reading large files (100MB)
CHUNK_SIZE = 100 * 1024 * 1024

# Default delimiters for different file types
DEFAULT_DELIMITERS = {
    'CSV File': ',',
    'DAT File': '|',
    'Zipped Flat Files': '|'
}

# Report directory
REPORTS_DIR = "reports"

# Excel styling
EXCEL_STYLES = {
    'PASS': {
        'fill': {'fgColor': '90EE90'},  # Light green
        'font': {'color': '006100'}     # Dark green
    },
    'FAIL': {
        'fill': {'fgColor': 'FFB6C1'},  # Light pink
        'font': {'color': '9C0006'}     # Dark red
    }
}
