SOURCE_TYPES = [
    'CSV File',
    'DAT File',
    'Parquet File',
    'Zipped Flat Files',
    'SQL Server',
    'Teradata',
    'Stored Procedure',
    'API'
]

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

DEFAULT_DELIMITERS = {
    'CSV File': ',',
    'DAT File': '|',
    'Zipped Flat Files': '|'
}

REPORTS_DIR = "reports"

CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB chunk size for Dask
