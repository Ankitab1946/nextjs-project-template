import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Union, Optional, Any
import pyodbc
import teradatasql
from utils import log_error, validate_connection_params

class DatabaseConnector:
    """Class to handle database connections and data retrieval"""

    @staticmethod
    def get_sqlserver_data(connection_params: Dict[str, str], query: str) -> Optional[pd.DataFrame]:
        """
        Retrieve data from SQL Server using provided connection parameters and query.
        
        Args:
            connection_params: Dictionary containing connection parameters
            query: SQL query to execute
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing query results, None if error occurs
        """
        try:
            if not validate_connection_params(connection_params):
                log_error("Invalid connection parameters for SQL Server")
                return None

            try:
                # Get server name from either 'server' or 'host' parameter
                server = connection_params.get('server') or connection_params.get('host')
                if not server:
                    raise ValueError("Server/Host is required")

                database = connection_params.get('database')
                if not database:
                    raise ValueError("Database name is required")

                # Build connection string based on authentication type
                if connection_params.get('use_windows_auth', True):
                    conn_str = (
                        f"Driver={{ODBC Driver 17 for SQL Server}};"
                        f"Server={server};"
                        f"Database={database};"
                        "Trusted_Connection=yes"
                    )
                else:
                    username = connection_params.get('username')
                    password = connection_params.get('password')
                    if not username or not password:
                        raise ValueError("Username and password are required for SQL authentication")

                    conn_str = (
                        f"Driver={{ODBC Driver 17 for SQL Server}};"
                        f"Server={server};"
                        f"Database={database};"
                        f"Uid={username};"
                        f"Pwd={password}"
                    )

                # Create engine using the pyodbc connection string
                engine = create_engine(
                    f"mssql+pyodbc:///?odbc_connect={conn_str}",
                    fast_executemany=True
                )
            with engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
                return df

        except Exception as e:
            log_error(f"Error retrieving data from SQL Server: {str(e)}")
            return None

    @staticmethod
    def get_teradata_data(connection_params: Dict[str, str], query: str) -> Optional[pd.DataFrame]:
        """
        Retrieve data from Teradata using provided connection parameters and query.
        
        Args:
            connection_params: Dictionary containing connection parameters
            query: SQL query to execute
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing query results, None if error occurs
        """
        try:
            if not validate_connection_params(connection_params):
                log_error("Invalid connection parameters for Teradata")
                return None

            # Create Teradata connection
            conn = teradatasql.connect(
                host=connection_params['host'],
                user=connection_params['username'],
                password=connection_params['password'],
                database=connection_params['database']
            )

            # Execute query and return results as DataFrame
            df = pd.read_sql(query, conn)
            conn.close()
            return df

        except Exception as e:
            log_error(f"Error retrieving data from Teradata: {str(e)}")
            return None

    @staticmethod
    def get_data_from_stored_proc(connection_params: Dict[str, str], 
                                 proc_name: str,
                                 params: Optional[Dict[str, Any]] = None) -> Optional[pd.DataFrame]:
        """
        Execute a stored procedure and retrieve its results.
        
        Args:
            connection_params: Dictionary containing connection parameters
            proc_name: Name of the stored procedure to execute
            params: Optional dictionary of parameters for the stored procedure
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing procedure results, None if error occurs
        """
        try:
            if not validate_connection_params(connection_params):
                log_error("Invalid connection parameters for stored procedure execution")
                return None

            try:
                # Get server name from either 'server' or 'host' parameter
                server = connection_params.get('server') or connection_params.get('host')
                if not server:
                    raise ValueError("Server/Host is required")

                database = connection_params.get('database')
                if not database:
                    raise ValueError("Database name is required")

                # Build connection string based on authentication type
                if connection_params.get('use_windows_auth', True):
                    conn_str = (
                        f"Driver={{ODBC Driver 17 for SQL Server}};"
                        f"Server={server};"
                        f"Database={database};"
                        "Trusted_Connection=yes"
                    )
                else:
                    username = connection_params.get('username')
                    password = connection_params.get('password')
                    if not username or not password:
                        raise ValueError("Username and password are required for SQL authentication")

                    conn_str = (
                        f"Driver={{ODBC Driver 17 for SQL Server}};"
                        f"Server={server};"
                        f"Database={database};"
                        f"Uid={username};"
                        f"Pwd={password}"
                    )

                # Connect and execute stored procedure
                with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                
                if params:
                    # Build parameter string
                    param_str = ', '.join([f"@{k}=?" for k in params.keys()])
                    exec_str = f"EXEC {proc_name} {param_str}"
                    cursor.execute(exec_str, list(params.values()))
                else:
                    cursor.execute(f"EXEC {proc_name}")

                # Fetch results
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                
                # Convert to DataFrame
                df = pd.DataFrame.from_records(results, columns=columns)
                return df

        except Exception as e:
            log_error(f"Error executing stored procedure: {str(e)}")
            return None

    @staticmethod
    def test_connection(connection_params: Dict[str, str], db_type: str) -> bool:
        """
        Test database connection without executing any queries.
        
        Args:
            connection_params: Dictionary containing connection parameters
            db_type: Type of database ('sqlserver' or 'teradata')
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if not validate_connection_params(connection_params):
                return False

            if db_type.lower() == 'sqlserver':
                try:
                    # Get server name from either 'server' or 'host' parameter
                    server = connection_params.get('server') or connection_params.get('host')
                    if not server:
                        raise ValueError("Server/Host is required")

                    database = connection_params.get('database')
                    if not database:
                        raise ValueError("Database name is required")

                    # Build connection string based on authentication type
                    if connection_params.get('use_windows_auth', True):
                        conn_str = (
                            f"Driver={{ODBC Driver 17 for SQL Server}};"
                            f"Server={server};"
                            f"Database={database};"
                            "Trusted_Connection=yes"
                        )
                    else:
                        username = connection_params.get('username')
                        password = connection_params.get('password')
                        if not username or not password:
                            raise ValueError("Username and password are required for SQL authentication")

                        conn_str = (
                            f"Driver={{ODBC Driver 17 for SQL Server}};"
                            f"Server={server};"
                            f"Database={database};"
                            f"Uid={username};"
                            f"Pwd={password}"
                        )

                    # Test connection
                    conn = pyodbc.connect(conn_str)
                    conn.close()
                    return True

                except Exception as e:
                    log_error(f"SQL Server connection test failed: {str(e)}")
                    return False

            elif db_type.lower() == 'teradata':
                conn = teradatasql.connect(
                    host=connection_params['host'],
                    user=connection_params['username'],
                    password=connection_params['password'],
                    database=connection_params['database']
                )
                conn.close()
                return True

            else:
                log_error(f"Unsupported database type: {db_type}")
                return False

        except Exception as e:
            log_error(f"Error testing connection: {str(e)}")
            return False

    @staticmethod
    def get_table_schema(connection_params: Dict[str, str], 
                        table_name: str,
                        db_type: str) -> Optional[pd.DataFrame]:
        """
        Retrieve schema information for a specified table.
        
        Args:
            connection_params: Dictionary containing connection parameters
            table_name: Name of the table
            db_type: Type of database ('sqlserver' or 'teradata')
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing schema information, None if error occurs
        """
        try:
            if db_type.lower() == 'sqlserver':
                query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
                """
                return DatabaseConnector.get_sqlserver_data(connection_params, query)

            elif db_type.lower() == 'teradata':
                query = f"""
                SELECT 
                    ColumnName,
                    ColumnType,
                    CharacterLength,
                    Nullable
                FROM DBC.Columns
                WHERE TableName = '{table_name}'
                ORDER BY ColumnId
                """
                return DatabaseConnector.get_teradata_data(connection_params, query)

            else:
                log_error(f"Unsupported database type: {db_type}")
                return None

        except Exception as e:
            log_error(f"Error retrieving table schema: {str(e)}")
            return None
