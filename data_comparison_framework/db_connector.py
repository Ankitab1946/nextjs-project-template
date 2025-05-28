import pyodbc
import teradatasql
from typing import Dict, Any

class DatabaseConnector:
    @staticmethod
    def connect_sql_server(conn_params: Dict[str, Any]):
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={conn_params['server']};"
                f"DATABASE={conn_params['database']};"
            )
            if conn_params.get('use_windows_auth', True):
                conn_str += "Trusted_Connection=yes;"
            else:
                conn_str += f"UID={conn_params['username']};PWD={conn_params['password']};"
            conn = pyodbc.connect(conn_str)
            return conn
        except Exception as e:
            raise Exception(f"Error connecting to SQL Server: {str(e)}")

    @staticmethod
    def connect_teradata(conn_params: Dict[str, Any]):
        try:
            conn = teradatasql.connect(
                host=conn_params['host'],
                user=conn_params['username'],
                password=conn_params['password']
            )
            return conn
        except Exception as e:
            raise Exception(f"Error connecting to Teradata: {str(e)}")
