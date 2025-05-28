import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('comparison_framework.log')
    ]
)

logger = logging.getLogger(__name__)

def log_error(error_message: str) -> None:
    """
    Log error messages to both console and file.
    
    Args:
        error_message (str): The error message to log
    """
    logger.error(error_message)

def check_file_size(file) -> bool:
    """
    Check if file size exceeds 3GB threshold.
    
    Args:
        file: File object or path
        
    Returns:
        bool: True if file size is within limits, False otherwise
    """
    try:
        if hasattr(file, 'seek'):
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(0)  # Reset file pointer
        else:
            size = os.path.getsize(file)
        
        # 3GB in bytes
        GB_3 = 3 * 1024 * 1024 * 1024
        return size <= GB_3
    except Exception as e:
        log_error(f"Error checking file size: {str(e)}")
        return False

def format_timestamp() -> str:
    """
    Get formatted timestamp for file naming.
    
    Returns:
        str: Formatted timestamp string
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def validate_connection_params(params: dict) -> bool:
    """
    Validate database connection parameters.
    
    Args:
        params (dict): Dictionary containing connection parameters
        
    Returns:
        bool: True if parameters are valid, False otherwise
    """
    try:
        if not isinstance(params, dict):
            log_error("Connection parameters must be a dictionary")
            return False

        # Check if using Windows Authentication
        if params.get('use_windows_auth', True):
            # For Windows Auth, we need either host or server, and database
            if not params.get('database'):
                log_error("Database name is required")
                return False
            
            if not (params.get('host') or params.get('server')):
                log_error("Server/Host is required")
                return False
            
            return True
        else:
            # For SQL Auth, we need host/server, database, username, and password
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
    """
    Sanitize filename by removing invalid characters.
    
    Args:
        filename (str): Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Replace invalid characters with underscore
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename

def get_file_extension(filename: str) -> str:
    """
    Get file extension from filename.
    
    Args:
        filename (str): Name of the file
        
    Returns:
        str: File extension without dot
    """
    return os.path.splitext(filename)[1][1:].lower()

def clean_df_columns(df) -> 'pd.DataFrame':
    """
    Returns a copy of the DataFrame with whitespace trimmed, lowercased, and special characters removed from all column names.
    
    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with cleaned column names.
    """
    try:
        df_copy = df.copy()
        # Strip whitespace
        df_copy.columns = df_copy.columns.str.strip()
        # Lowercase
        df_copy.columns = df_copy.columns.str.lower()
        # Remove special characters, keep only alphanumeric
        df_copy.columns = df_copy.columns.str.replace(r'[^a-z0-9]', '', regex=True)
        return df_copy
    except Exception as e:
        log_error(f"Error cleaning DataFrame columns: {str(e)}")
        return df  # Return original DataFrame if cleaning fails
