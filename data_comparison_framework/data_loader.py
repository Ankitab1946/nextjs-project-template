import pandas as pd
import dask.dataframe as dd
import pyodbc
import teradatasql
import requests
import zipfile
import io
from typing import Optional, Dict, Any
from config import CHUNK_SIZE

class DataLoader:
    @staticmethod
    def read_csv_in_chunks(file_obj: Any, delimiter: str = ',', **kwargs) -> pd.DataFrame:
        try:
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    if hasattr(file_obj, 'read'):
                        file_obj.seek(0)
                        try:
                            return pd.read_csv(file_obj, delimiter=delimiter, encoding=encoding, **kwargs)
                        except Exception:
                            file_obj.seek(0)
                            ddf = dd.read_csv(file_obj, blocksize=CHUNK_SIZE, delimiter=delimiter, encoding=encoding, **kwargs)
                            return ddf.compute()
                    else:
                        try:
                            return pd.read_csv(file_obj, delimiter=delimiter, encoding=encoding, **kwargs)
                        except Exception:
                            ddf = dd.read_csv(file_obj, blocksize=CHUNK_SIZE, delimiter=delimiter, encoding=encoding, **kwargs)
                            return ddf.compute()
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    if encoding == encodings[-1]:
                        raise e
                    continue
            raise Exception("Unable to read file with any supported encoding")
        except Exception as e:
            raise Exception(f"Error reading CSV file: {str(e)}")

    @staticmethod
    def read_dat_file(file_obj: Any, delimiter: str = '|', **kwargs) -> pd.DataFrame:
        try:
            return DataLoader.read_csv_in_chunks(file_obj, delimiter=delimiter, **kwargs)
        except Exception as e:
            raise Exception(f"Error reading DAT file: {str(e)}")

    @staticmethod
    def read_parquet(file_path: str) -> pd.DataFrame:
        try:
            return dd.read_parquet(file_path).compute()
        except Exception as e:
            raise Exception(f"Error reading Parquet file: {str(e)}")

    @staticmethod
    def read_sql_server(conn_params: Dict[str, Any], query: str) -> pd.DataFrame:
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
            with pyodbc.connect(conn_str) as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            raise Exception(f"Error connecting to SQL Server: {str(e)}")

    @staticmethod
    def read_teradata(conn_params: Dict[str, Any], query: str) -> pd.DataFrame:
        try:
            with teradatasql.connect(
                host=conn_params['host'],
                user=conn_params['username'],
                password=conn_params['password']
            ) as conn:
                return pd.read_sql(query, conn)
        except Exception as e:
            raise Exception(f"Error connecting to Teradata: {str(e)}")

    @staticmethod
    def read_stored_proc(conn_params: Dict[str, Any], proc_name: str, params: Optional[Dict] = None) -> pd.DataFrame:
        try:
            param_str = ""
            if params:
                param_str = ",".join([f"@{k}=?" for k in params.keys()])
            exec_str = f"EXEC {proc_name} {param_str}"
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={conn_params['server']};"
                f"DATABASE={conn_params['database']};"
            )
            if conn_params.get('use_windows_auth', True):
                conn_str += "Trusted_Connection=yes;"
            else:
                conn_str += f"UID={conn_params['username']};PWD={conn_params['password']};"
            with pyodbc.connect(conn_str) as conn:
                if params:
                    return pd.read_sql(exec_str, conn, params=list(params.values()))
                else:
                    return pd.read_sql(exec_str, conn)
        except Exception as e:
            raise Exception(f"Error executing stored procedure: {str(e)}")

    @staticmethod
    def read_api(url: str, method: str = 'GET', headers: Optional[Dict] = None, params: Optional[Dict] = None) -> pd.DataFrame:
        try:
            response = requests.request(method, url, headers=headers, params=params)
            response.raise_for_status()
            return pd.DataFrame(response.json())
        except Exception as e:
            raise Exception(f"Error fetching API data: {str(e)}")

    @staticmethod
    def read_zipped_flat_files(file_obj: io.BytesIO, delimiter: str = '|') -> pd.DataFrame:
        try:
            with zipfile.ZipFile(file_obj) as z:
                dfs = []
                for filename in z.namelist():
                    if filename.endswith('.csv') or filename.endswith('.dat') or filename.endswith('.txt'):
                        with z.open(filename) as f:
                            df = pd.read_csv(f, delimiter=delimiter)
                            dfs.append(df)
                if dfs:
                    return pd.concat(dfs, ignore_index=True)
                else:
                    raise Exception("No suitable flat files found in zip archive")
        except Exception as e:
            raise Exception(f"Error reading zipped flat files: {str(e)}")
