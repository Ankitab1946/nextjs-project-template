import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_comparison_framework.log')
    ]
)

logger = logging.getLogger(__name__)

def log_error(error_message: str) -> None:
    logger.error(error_message)

def check_file_size(file) -> bool:
    try:
        if hasattr(file, 'seek'):
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(0)
        else:
            size = os.path.getsize(file)
        GB_3 = 3 * 1024 * 1024 * 1024
        return size <= GB_3
    except Exception as e:
        log_error(f"Error checking file size: {str(e)}")
        return False

def format_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def validate_connection_params(params: dict) -> bool:
    try:
        if not isinstance(params, dict):
            log_error("Connection parameters must be a dictionary")
            return False
        if params.get('use_windows_auth', True):
            if not params.get('database'):
                log_error("Database name is required")
                return False
            if not (params.get('host') or params.get('server')):
                log_error("Server/Host is required")
                return False
            return True
        else:
            required_params = ['database', 'username', 'password']
            if not (params.get('host') or params.get('server')):
                log_error("Server/Host is required")
                return False
            for param in required_params:
                if not params.get(param):
                    log_error(f"{param} is required for SQL Authentication")
                    return False
            return True
    except Exception as e:
        log_error(f"Error validating connection parameters: {str(e)}")
        return False

def sanitize_filename(filename: str) -> str:
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

def get_file_extension(filename: str) -> str:
    return os.path.splitext(filename)[1][1:].lower()

def clean_df_columns(df) -> 'pd.DataFrame':
    try:
        df_copy = df.copy()
        df_copy.columns = df_copy.columns.str.strip()
        df_copy.columns = df_copy.columns.str.lower()
        df_copy.columns = df_copy.columns.str.replace(r'[^a-z0-9]', '', regex=True)
        return df_copy
    except Exception as e:
        log_error(f"Error cleaning DataFrame columns: {str(e)}")
        return df
