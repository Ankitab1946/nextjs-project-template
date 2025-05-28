import streamlit as st
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
import os
from datetime import datetime
import datacompy
from ydata_profiling import ProfileReport
import streamlit.components.v1 as components

# Import our modules
from data_reader import DataReader
from mapping_manager import MappingManager
from report_generator import ReportGenerator
from db_connector import DatabaseConnector
from api_fetcher import APIFetcher
from utils import log_error, format_timestamp

# Set page config
st.set_page_config(
    page_title="Data Comparison Framework",
    page_icon="📊",
    layout="wide"
)

# Define source types
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

def main():
    """Main application function"""
    
    # Add modern header with enhanced styling
    st.markdown(
        """
        <div style='text-align: center; background-color: #f8f9fa; padding: 2rem; 
             border-radius: 10px; margin-bottom: 2rem; border: 1px solid #dee2e6;'>
            <h1 style='color: #1f77b4; margin-bottom: 0.5rem; font-size: 2.5em;'>
                Data Comparison Framework
            </h1>
            <p style='color: #6c757d; font-size: 1.1em; margin: 1rem 0;'>
                Compare data between SQL Server databases and feed files with advanced mapping capabilities
            </p>
            <div style='display: flex; justify-content: center; gap: 1rem; margin-top: 1rem;'>
                <div style='background-color: #e8f4f8; padding: 0.5rem 1rem; border-radius: 5px;'>
                    <span style='color: #1f77b4;'>✨ Automatic Column Mapping</span>
                </div>
                <div style='background-color: #e8f4f8; padding: 0.5rem 1rem; border-radius: 5px;'>
                    <span style='color: #1f77b4;'>🔍 Smart Join Detection</span>
                </div>
                <div style='background-color: #e8f4f8; padding: 0.5rem 1rem; border-radius: 5px;'>
                    <span style='color: #1f77b4;'>📊 Detailed Reports</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Add error handling for the entire app
    try:

        # Initialize session state variables
        if 'source_df' not in st.session_state:
            st.session_state.source_df = None
        if 'target_df' not in st.session_state:
            st.session_state.target_df = None
        if 'column_mapping' not in st.session_state:
            st.session_state.column_mapping = {}
        if 'excluded_columns' not in st.session_state:
            st.session_state.excluded_columns = []
        if 'report_paths' not in st.session_state:
            st.session_state.report_paths = {}

        # Create two columns for source and target selection with enhanced styling
        st.markdown(
            """
            <div style='display: flex; gap: 2rem; margin: 1rem 0;'>
                <div style='flex: 1; background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; border: 1px solid #dee2e6;'>
                    <h3 style='margin: 0 0 1rem 0; color: #1f77b4;'>Source Configuration</h3>
                    <p style='color: #6c757d; margin-bottom: 1rem;'>Select and configure your source data</p>
                </div>
                <div style='flex: 1; background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; border: 1px solid #dee2e6;'>
                    <h3 style='margin: 0 0 1rem 0; color: #1f77b4;'>Target Configuration</h3>
                    <p style='color: #6c757d; margin-bottom: 1rem;'>Select and configure your target data</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )

        col1, col2 = st.columns(2)

        with col1:
            source_type = st.selectbox(
                "Select Source Type",
                SOURCE_TYPES,
                key="source_type",
                help="Choose the type of source data you want to compare"
            )
            source_data = handle_data_source(source_type, "source")
            if source_data is not None:
                st.success(f"✅ Source data loaded successfully: {len(source_data)} rows, {len(source_data.columns)} columns")

        with col2:
            target_type = st.selectbox(
                "Select Target Type",
                SOURCE_TYPES,
                key="target_type",
                help="Choose the type of target data you want to compare"
            )
            target_data = handle_data_source(target_type, "target")
            if target_data is not None:
                st.success(f"✅ Target data loaded successfully: {len(target_data)} rows, {len(target_data.columns)} columns")

    except Exception as e:
        st.error(f"❌ An error occurred: {str(e)}")
        log_error(f"Application error: {str(e)}")
        return

    # Check if we have data either from direct loading or session state
    if ((source_data is not None and target_data is not None) or 
        (isinstance(st.session_state.source_df, pd.DataFrame) and isinstance(st.session_state.target_df, pd.DataFrame))):
        try:
            # Get data from direct load or session state
            from utils import clean_df_columns
            
            # Validate and prepare source data
            source_df = source_data if source_data is not None else st.session_state.get('source_df')
            if not isinstance(source_df, pd.DataFrame):
                st.error("❌ Source data is not available or invalid. Please load source data first.")
                return
            
            # Convert all columns to string type for consistent comparison
            source_df = source_df.astype(str)
            
            # Validate and prepare target data
            target_df = target_data if target_data is not None else st.session_state.get('target_df')
            if not isinstance(target_df, pd.DataFrame):
                st.error("❌ Target data is not available or invalid. Please load target data first.")
                return
            
            # Convert all columns to string type for consistent comparison
            target_df = target_df.astype(str)
            
            try:
                # Normalize columns for consistent mapping
                cleaned_source = clean_df_columns(source_df)
                cleaned_target = clean_df_columns(target_df)
                
                if cleaned_source.empty or cleaned_target.empty:
                    st.error("❌ One or both datasets are empty. Please check your data.")
                    return
                
                # Reset index to ensure consistent comparison
                cleaned_source = cleaned_source.reset_index(drop=True)
                cleaned_target = cleaned_target.reset_index(drop=True)
            except Exception as e:
                st.error(f"❌ Error cleaning data: {str(e)}")
                return
            
            # Store cleaned DataFrames in session state if they came from direct load
            if source_data is not None:
                st.session_state.source_df = cleaned_source
            if target_data is not None:
                st.session_state.target_df = cleaned_target
            
            # Debug information
            st.write("Source columns:", list(cleaned_source.columns))
            st.write("Target columns:", list(cleaned_target.columns))
            
            # Force re-mapping if source columns have changed
            current_source_cols = list(cleaned_source.columns)
            if (not st.session_state.get('column_mapping') or 
                st.session_state.get('last_source_columns') != current_source_cols):
                
                # Clean column names before mapping
                cleaned_source.columns = [col.strip().lower() for col in cleaned_source.columns]
                cleaned_target.columns = [col.strip().lower() for col in cleaned_target.columns]
                
                st.session_state.column_mapping = MappingManager.auto_map_columns(cleaned_source, cleaned_target)
                st.session_state.last_source_columns = current_source_cols
                
                # Debug mapping results
                st.write("Mapped columns:", st.session_state.column_mapping)
                
                if st.session_state.column_mapping:
                    st.success(f"✅ Successfully mapped {len(st.session_state.column_mapping)} columns automatically!")
                else:
                    st.warning("⚠️ No automatic column mappings found. Please map columns manually below.")
        except Exception as e:
            st.error(f"❌ Error initializing data: {str(e)}")
            log_error(f"Data initialization error: {str(e)}")
            return

        # Show column mapping interface
        st.subheader("Column Mapping")
        show_column_mapping_interface(source_data, target_data)

        # Add join key selection with enhanced styling
        st.markdown(
            """
            <div style='background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; margin: 2rem 0; border: 1px solid #dee2e6;'>
                <h3 style='margin: 0; color: #1f77b4;'>Join Key Selection</h3>
                <p style='margin: 0.5rem 0 0 0; color: #6c757d;'>
                    Select columns to use as join keys for comparing records between source and target data.
                </p>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        # Get mapped columns (where source and target are mapped)
        mapped_columns = {source_col: target_col 
                        for source_col, target_col in st.session_state.column_mapping.items()
                        if source_col in source_data.columns and target_col in target_data.columns}
        
        if not mapped_columns:
            st.warning("⚠️ No mapped columns available for join keys. Please map at least one column first.")
            join_keys = None
        else:
            # Create columns for join key selection and preview
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Select join keys from mapped columns with enhanced styling
                st.markdown("##### Select Join Key Columns")
                selected_keys = st.multiselect(
                    "Choose columns that uniquely identify records",
                    options=list(mapped_columns.keys()),
                    help="These columns will be used to match records between source and target data"
                )
                
                if selected_keys:
                    join_keys = [(source_col, mapped_columns[source_col]) for source_col in selected_keys]
                    st.markdown(
                        """
                        <div style='background-color: #e8f4f8; padding: 0.75rem; border-radius: 5px; margin-top: 1rem;'>
                            <p style='margin: 0; color: #1f77b4;'>Selected Join Keys:</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                    for source_col, target_col in join_keys:
                        st.markdown(f"- {source_col} → {target_col}")
                else:
                    st.info("ℹ️ No join keys selected. Comparison will be done row by row.")
                    join_keys = None
            
            with col2:
                # Show preview of mapped columns with samples
                st.markdown("##### Join Keys Preview")
                mapped_preview = []
                for source_col, target_col in mapped_columns.items():
                    source_sample = source_data[source_col].head(3).tolist()
                    target_sample = target_data[target_col].head(3).tolist()
                    mapped_preview.append({
                        'Source Column': source_col,
                        'Target Column': target_col,
                        'Source Sample': str(source_sample),
                        'Target Sample': str(target_sample)
                    })
                
                if mapped_preview:
                    st.dataframe(
                        pd.DataFrame(mapped_preview),
                        use_container_width=True,
                        height=200
                    )

        # Store join keys in session state
        st.session_state.join_keys = join_keys

        # Compare button section with enhanced styling
        st.markdown(
            """
            <div style='background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; margin: 2rem 0; 
                 border: 1px solid #dee2e6; text-align: center;'>
                <h3 style='margin: 0 0 1rem 0; color: #1f77b4;'>Generate Comparison Reports</h3>
            </div>
            """,
            unsafe_allow_html=True
        )

        # Center the compare button
        col1, col2, col3 = st.columns([2, 1, 2])
        with col2:
            if st.button("🔄 Compare Data", type="primary", use_container_width=True):
                with st.spinner("Generating comparison reports..."):
                    perform_comparison()

def handle_data_source(source_type: str, prefix: str) -> Optional[pd.DataFrame]:
    """Handle different types of data sources"""
    
    try:
        if source_type in ["CSV File", "DAT File", "Parquet File", "Zipped Flat Files"]:
            return handle_file_upload(source_type, prefix)
        elif source_type in ["SQL Server", "Teradata", "Stored Procedure"]:
            return handle_database_connection(source_type, prefix)
        elif source_type == "API":
            return handle_api_connection(prefix)
        
    except Exception as e:
        log_error(f"Error handling {source_type}: {str(e)}")
        st.error(f"Error processing {source_type}: {str(e)}")
    
    return None

def handle_file_upload(file_type: str, prefix: str) -> Optional[pd.DataFrame]:
    """Handle file upload for different file types with enhanced UI and error handling"""
    
    # Add file upload UI with modern styling
    st.markdown(
        f"""
        <div style='background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem; border: 1px solid #dee2e6;'>
            <h4 style='margin: 0; color: #1f77b4;'>{file_type} Upload</h4>
            <p style='margin: 0.5rem 0 0 0; color: #6c757d;'>
                Upload your {file_type.lower()} and configure import settings below.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # File upload section
    uploaded_file = st.file_uploader(
        f"Choose {file_type}",
        key=f"{prefix}_file",
        help=f"Select a {file_type.lower()} to upload"
    )
    
    if uploaded_file:
        try:
            # Show file info
            file_details = {
                "Filename": uploaded_file.name,
                "File size": f"{uploaded_file.size / 1024:.2f} KB",
                "File type": uploaded_file.type
            }
            
            st.markdown("##### File Details")
            for key, value in file_details.items():
                st.text(f"{key}: {value}")
            
            # Configuration options based on file type
            with st.expander("Import Configuration", expanded=True):
                if file_type in ["CSV File", "DAT File"]:
                    col1, col2 = st.columns(2)
                    with col1:
                        delimiter = st.text_input(
                            "Delimiter",
                            value=',' if file_type == "CSV File" else '|',
                            key=f"{prefix}_delimiter",
                            help="Enter the character used to separate columns"
                        )
                    with col2:
                        encoding = st.selectbox(
                            "File Encoding",
                            ["utf-8", "latin1", "ascii"],
                            key=f"{prefix}_encoding",
                            help="Select the file encoding"
                        )
            
            # Load the file with progress indicator
            with st.spinner(f"Loading {file_type}..."):
                if file_type == "CSV File":
                    df = DataReader.load_csv(uploaded_file, delimiter=delimiter)
                elif file_type == "DAT File":
                    df = DataReader.load_dat(uploaded_file, delimiter=delimiter)
                elif file_type == "Parquet File":
                    df = DataReader.load_parquet(uploaded_file)
                elif file_type == "Zipped Flat Files":
                    df = DataReader.load_zipped_flat_files(uploaded_file, separator=delimiter)
                
                if df is not None:
                    st.success(f"✅ {file_type} loaded successfully!")
                    st.info(f"Retrieved {len(df)} rows and {len(df.columns)} columns")
                    
                    # Show data preview in an expander
                    with st.expander("Data Preview"):
                        st.dataframe(
                            df.head(5),
                            use_container_width=True,
                            height=200
                        )
                    return df
                else:
                    st.error(f"❌ Failed to load {file_type}. Please check the file format and try again.")
                
        except Exception as e:
            st.error(f"❌ Error reading {file_type}: {str(e)}")
            log_error(f"File upload error ({file_type}): {str(e)}")
            
            # Show detailed error message in an expander
            with st.expander("Error Details"):
                st.code(str(e))
    
    return None

def handle_database_connection(db_type: str, prefix: str) -> Optional[pd.DataFrame]:
    """Handle database connections with enhanced UI and error handling"""
    
    # Add database connection UI with modern styling
    st.markdown(
        f"""
        <div style='background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem; border: 1px solid #dee2e6;'>
            <h4 style='margin: 0; color: #1f77b4;'>{db_type} Connection</h4>
            <p style='margin: 0.5rem 0 0 0; color: #6c757d;'>
                Enter your {db_type} connection details below. Column names will be automatically standardized for better mapping.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Create tabs for connection and query
    conn_tab, query_tab = st.tabs(["Connection Details", "Query Configuration"])
    
    with conn_tab:
        # Connection details with improved layout
        col1, col2 = st.columns(2)
        
        with col1:
            host = st.text_input(
                "Server/Host",
                key=f"{prefix}_host",
                help="Enter the server name or IP address"
            )
        
        with col2:
            database = st.text_input(
                "Database",
                key=f"{prefix}_database",
                help="Enter the database name"
            )
        
        # Authentication section
        st.markdown("##### Authentication")
        
        if db_type in ["SQL Server", "Stored Procedure"]:
            use_windows_auth = st.checkbox(
                "Use Windows Authentication",
                value=True,
                key=f"{prefix}_use_windows_auth",
                help="Check to use Windows Authentication, uncheck for SQL Authentication"
            )
            
            if not use_windows_auth:
                col1, col2 = st.columns(2)
                with col1:
                    username = st.text_input(
                        "Username",
                        key=f"{prefix}_username",
                        help="Enter your SQL Server username"
                    )
                with col2:
                    password = st.text_input(
                        "Password",
                        type="password",
                        key=f"{prefix}_password",
                        help="Enter your SQL Server password"
                    )
        else:
            use_windows_auth = False
            col1, col2 = st.columns(2)
            with col1:
                username = st.text_input(
                    "Username",
                    key=f"{prefix}_username"
                )
            with col2:
                password = st.text_input(
                    "Password",
                    type="password",
                    key=f"{prefix}_password"
                )
    
    with query_tab:
        if db_type == "Stored Procedure":
            proc_name = st.text_input(
                "Stored Procedure Name",
                key=f"{prefix}_proc",
                help="Enter the name of the stored procedure"
            )
            params = st.text_area(
                "Parameters (JSON format)",
                key=f"{prefix}_params",
                help="Enter parameters as JSON, e.g., {\"param1\": \"value1\"}"
            )
        else:
            query = st.text_area(
                "SQL Query",
                key=f"{prefix}_query",
                height=150,
                help="Enter your SQL query here. The query will be executed to fetch the data."
            )
    
    # Connection button with loading state
    if st.button("🔌 Connect to Database", key=f"{prefix}_connect", use_container_width=True):
        with st.spinner(f"Connecting to {db_type}..."):
            try:
                # Build connection parameters
                conn_params = {
                    'host': host,
                    'database': database,
                    'use_windows_auth': use_windows_auth if db_type in ["SQL Server", "Stored Procedure"] else False
                }
                
                # Add username/password only if not using Windows Auth
                if not (db_type in ["SQL Server", "Stored Procedure"] and use_windows_auth):
                    if not username or not password:
                        st.error("❌ Username and password are required for SQL Authentication")
                        return None
                    conn_params.update({
                        'username': username,
                        'password': password
                    })
                
                # Validate connection parameters
                if not host or not database:
                    st.error("❌ Server/Host and Database are required")
                    return None
                
                # Execute query based on database type
                if db_type == "SQL Server":
                    if not query:
                        st.error("❌ SQL Query is required")
                        return None
                    try:
                        df = DatabaseConnector.get_sqlserver_data(conn_params, query)
                        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                            st.success("✅ SQL Server connection successful!")
                            st.info(f"Retrieved {len(df)} rows and {len(df.columns)} columns")
                            
                            # Store DataFrame in session state
                            if prefix == "source":
                                st.session_state['source_df'] = df.copy()
                            else:
                                st.session_state['target_df'] = df.copy()
                                
                            # Verify the DataFrame was stored correctly
                            stored_df = st.session_state.get('source_df' if prefix == "source" else 'target_df')
                            if not isinstance(stored_df, pd.DataFrame):
                                st.error("❌ Error storing DataFrame in session state")
                                return None
                                
                            return df
                        else:
                            st.error("❌ No data returned from SQL Server or empty result")
                            return None
                    except Exception as e:
                        st.error(f"❌ Error processing SQL Server data: {str(e)}")
                        return None
                
                elif db_type == "Teradata":
                    if not query:
                        st.error("❌ SQL Query is required")
                        return None
                    try:
                        df = DatabaseConnector.get_teradata_data(conn_params, query)
                        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                            st.success("✅ Teradata connection successful!")
                            st.info(f"Retrieved {len(df)} rows and {len(df.columns)} columns")
                            
                            # Store DataFrame in session state
                            if prefix == "source":
                                st.session_state['source_df'] = df.copy()
                            else:
                                st.session_state['target_df'] = df.copy()
                                
                            # Verify the DataFrame was stored correctly
                            stored_df = st.session_state.get('source_df' if prefix == "source" else 'target_df')
                            if not isinstance(stored_df, pd.DataFrame):
                                st.error("❌ Error storing DataFrame in session state")
                                return None
                                
                            return df
                        else:
                            st.error("❌ No data returned from Teradata or empty result")
                            return None
                    except Exception as e:
                        st.error(f"❌ Error processing Teradata data: {str(e)}")
                        return None
                
                elif db_type == "Stored Procedure":
                    if not proc_name:
                        st.error("❌ Stored Procedure name is required")
                        return None
                    try:
                        df = DatabaseConnector.get_data_from_stored_proc(
                            conn_params, proc_name, eval(params) if params else None)
                        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
                            st.success("✅ Stored Procedure executed successfully!")
                            st.info(f"Retrieved {len(df)} rows and {len(df.columns)} columns")
                            
                            # Store DataFrame in session state
                            if prefix == "source":
                                st.session_state['source_df'] = df.copy()
                            else:
                                st.session_state['target_df'] = df.copy()
                                
                            # Verify the DataFrame was stored correctly
                            stored_df = st.session_state.get('source_df' if prefix == "source" else 'target_df')
                            if not isinstance(stored_df, pd.DataFrame):
                                st.error("❌ Error storing DataFrame in session state")
                                return None
                                
                            return df
                        else:
                            st.error("❌ No data returned from Stored Procedure or empty result")
                            return None
                    except Exception as e:
                        st.error(f"❌ Error processing Stored Procedure data: {str(e)}")
                        return None
                        
            except Exception as e:
                st.error(f"❌ Database connection error: {str(e)}")
                log_error(f"Database connection error ({db_type}): {str(e)}")
    
    return None

def handle_api_connection(prefix: str) -> Optional[pd.DataFrame]:
    """Handle API connections with enhanced UI and error handling"""
    
    # Add API connection UI with modern styling
    st.markdown(
        """
        <div style='background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem; border: 1px solid #dee2e6;'>
            <h4 style='margin: 0; color: #1f77b4;'>API Connection</h4>
            <p style='margin: 0.5rem 0 0 0; color: #6c757d;'>
                Configure your API connection details and parameters below.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Create tabs for basic and advanced configuration
    basic_tab, advanced_tab = st.tabs(["Basic Configuration", "Advanced Settings"])
    
    with basic_tab:
        # URL and Method
        col1, col2 = st.columns([3, 1])
        
        with col1:
            api_url = st.text_input(
                "API URL",
                key=f"{prefix}_api_url",
                help="Enter the complete API endpoint URL",
                placeholder="https://api.example.com/data"
            )
        
        with col2:
            method = st.selectbox(
                "Method",
                ["GET", "POST"],
                key=f"{prefix}_method",
                help="Select the HTTP method to use"
            )
    
    with advanced_tab:
        # Headers
        st.markdown("##### Headers")
        headers = st.text_area(
            "Headers (JSON format)",
            key=f"{prefix}_headers",
            help="Enter headers as JSON object, e.g., {\"Authorization\": \"Bearer token\"}",
            height=100,
            placeholder="""
{
    "Authorization": "Bearer your_token",
    "Content-Type": "application/json"
}"""
        )
        
        # Parameters
        st.markdown("##### Parameters")
        params = st.text_area(
            "Parameters (JSON format)",
            key=f"{prefix}_params",
            help="Enter query parameters or POST body as JSON object",
            height=100,
            placeholder="""
{
    "limit": 1000,
    "offset": 0
}"""
        )
    
    # Connection button with loading state
    if st.button("🔌 Connect to API", key=f"{prefix}_connect", use_container_width=True):
        with st.spinner("Connecting to API..."):
            try:
                # Validate inputs
                if not api_url:
                    st.error("❌ API URL is required")
                    return None
                
                # Parse JSON inputs
                try:
                    headers_dict = eval(headers) if headers else None
                    params_dict = eval(params) if params else None
                except Exception as e:
                    st.error(f"❌ Invalid JSON format: {str(e)}")
                    return None
                
                # Fetch data
                df = APIFetcher.fetch_api_data(
                    api_url=api_url,
                    method=method,
                    headers=headers_dict,
                    params=params_dict
                )
                
                if df is not None:
                    st.success("✅ API connection successful!")
                    st.info(f"Retrieved {len(df)} rows and {len(df.columns)} columns")
                    
                    # Show data preview in an expander
                    with st.expander("Data Preview"):
                        st.dataframe(
                            df.head(5),
                            use_container_width=True,
                            height=200
                        )
                    return df
                else:
                    st.error("❌ No data received from API")
                    
            except Exception as e:
                st.error(f"❌ API connection error: {str(e)}")
                log_error(f"API connection error: {str(e)}")
                
                # Show detailed error message in an expander
                with st.expander("Error Details"):
                    st.code(str(e))
    
    return None

def show_column_mapping_interface(source_df: pd.DataFrame, target_df: pd.DataFrame):
    """Show interface for column mapping"""
    
    if not isinstance(source_df, pd.DataFrame) or not isinstance(target_df, pd.DataFrame):
        st.error("❌ Source or target data is not available for mapping")
        return
        
    st.markdown(
        """
        <div style='background-color: #f8f9fa; padding: 1.5rem; border-radius: 10px; margin-bottom: 1rem; border: 1px solid #dee2e6;'>
            <h3 style='margin: 0; color: #1f77b4;'>Column Mapping Configuration</h3>
            <p style='margin: 0.5rem 0 0 0; color: #6c757d;'>
                Map columns between source and target data sources. Use auto-mapping or manually adjust the mappings below.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Create tabs for auto and manual mapping
    auto_tab, manual_tab = st.tabs(["Automatic Mapping", "Manual Mapping"])
    
    with auto_tab:
        # Create two columns for the auto-map button and mapping status
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if st.button("🔄 Auto-Map Columns", key="auto_map_btn", use_container_width=True):
                with st.spinner("Mapping columns..."):
                    try:
                        st.session_state.column_mapping = MappingManager.auto_map_columns(source_df, target_df)
                        if st.session_state.column_mapping:
                            st.success(f"✅ Successfully mapped {len(st.session_state.column_mapping)} columns!")
                        else:
                            st.warning("⚠️ No automatic matches found. Please map columns manually.")
                    except Exception as e:
                        st.error(f"❌ Error during auto-mapping: {str(e)}")
        
        with col2:
            if st.session_state.get('column_mapping'):
                total_cols = len(source_df.columns)
                mapped_cols = len(st.session_state.column_mapping)
                mapping_percentage = (mapped_cols / total_cols) * 100
                st.progress(mapping_percentage / 100, text=f"Mapped {mapped_cols} of {total_cols} columns ({mapping_percentage:.1f}%)")
    
    with manual_tab:
        st.markdown("### Manual Column Mapping")
        st.markdown("Select target columns for each source column below:")
        
        # Initialize column mapping in session state if not exists
        if 'column_mapping' not in st.session_state:
            st.session_state.column_mapping = {}
        
        # Create mapping interface for each source column
        for source_col in source_df.columns:
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.markdown(f"**Source:** {source_col}")
                st.caption(f"Sample: {str(source_df[source_col].head(2).tolist())}")
            
            with col2:
                # Get current mapping for this column
                current_mapping = st.session_state.column_mapping.get(source_col, '')
                
                # Create dropdown with target columns
                target_options = [''] + list(target_df.columns)
                selected_target = st.selectbox(
                    "Map to target column",
                    options=target_options,
                    index=target_options.index(current_mapping) if current_mapping in target_options else 0,
                    key=f"manual_mapping_{source_col}"
                )
                
                # Update mapping if changed
                if selected_target:
                    if selected_target != current_mapping:
                        st.session_state.column_mapping[source_col] = selected_target
                        # Show sample of selected target column
                        st.caption(f"Target sample: {str(target_df[selected_target].head(2).tolist())}")
                elif source_col in st.session_state.column_mapping:
                    del st.session_state.column_mapping[source_col]
            
            st.divider()
        
        # Show current mapping summary
        if st.session_state.column_mapping:
            st.success(f"✅ Currently mapped: {len(st.session_state.column_mapping)} columns")
            with st.expander("View Current Mappings"):
                for source_col, target_col in st.session_state.column_mapping.items():
                    st.write(f"{source_col} → {target_col}")
    
    # Show mapping table with enhanced styling
    st.markdown("### Current Mapping Overview")
    
    # Create a DataFrame to display the mapping
    mapping_data = []
    for source_col in source_df.columns:
        source_sample = str(source_df[source_col].head(2).tolist())
        target_col = st.session_state.column_mapping.get(source_col, '')
        target_sample = str(target_df[target_col].head(2).tolist()) if target_col else ''
        
        mapping_data.append({
            'Source Column': source_col,
            'Source Sample': source_sample,
            'Target Column': target_col,
            'Target Sample': target_sample,
            'Status': '✅' if target_col else '❌'
        })
    
    mapping_df = pd.DataFrame(mapping_data)
    
    # Display the mapping table with custom styling
    st.dataframe(
        mapping_df,
        use_container_width=True,
        height=400,
        column_config={
            "Status": st.column_config.Column(
                "Mapping Status",
                help="✅: Mapped | ❌: Unmapped",
                width="small"
            )
        }
    )

def perform_comparison():
    """Perform the comparison and generate reports"""
    
    try:
        # Initial validation
        if not isinstance(st.session_state.get('source_df'), pd.DataFrame):
            st.error("❌ Source data is not available. Please load source data first.")
            return
            
        if not isinstance(st.session_state.get('target_df'), pd.DataFrame):
            st.error("❌ Target data is not available. Please load target data first.")
            return
            
        if not st.session_state.get('column_mapping'):
            st.error("❌ No column mappings defined. Please map columns before comparing.")
            return
            
        # Validate column mappings
        source_df = st.session_state.source_df
        target_df = st.session_state.target_df
        
        invalid_mappings = []
        for source_col, target_col in st.session_state.column_mapping.items():
            if source_col not in source_df.columns:
                invalid_mappings.append(f"Source column '{source_col}' not found")
            if target_col not in target_df.columns:
                invalid_mappings.append(f"Target column '{target_col}' not found")
                
        if invalid_mappings:
            st.error("❌ Invalid column mappings detected:")
            for msg in invalid_mappings:
                st.write(f"- {msg}")
            return
            
        st.markdown("### Comparison Results")
        
        # Create reports directory with full path
        reports_dir = os.path.join(os.getcwd(), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        timestamp = format_timestamp()
        
        # Log report generation start
        st.info("📊 Generating comparison reports...")
        
        # Generate reports
        join_keys = st.session_state.get('join_keys', None)
        
        with st.spinner("Generating comparison reports..."):
            try:
                from ydata_profiling import ProfileReport
                import streamlit.components.v1 as components
                
                # Generate Y-Data HTML profiling reports
                st.write("Generating Y-Data Profiling Reports...")
                
                # Source profiling
                source_profile = ProfileReport(
                    st.session_state.source_df,
                    title="Source Data Profile Report",
                    minimal=True
                )
                source_html = os.path.join(reports_dir, f"SourceProfile_{timestamp}.html")
                source_profile.to_file(source_html)
                st.success("✅ Source profile report generated")
                
                # Target profiling
                target_profile = ProfileReport(
                    st.session_state.target_df,
                    title="Target Data Profile Report",
                    minimal=True
                )
                target_html = os.path.join(reports_dir, f"TargetProfile_{timestamp}.html")
                target_profile.to_file(target_html)
                st.success("✅ Target profile report generated")
                
                # Generate other reports
                st.write("Generating Comparison Reports...")
                
                # Generate difference report
                diff_df, has_differences = ReportGenerator.generate_diff_report(
                    st.session_state.source_df,
                    st.session_state.target_df,
                    st.session_state.column_mapping,
                    st.session_state.excluded_columns,
                    join_keys=join_keys
                )
                
                # Generate profiling report
                profile_df = ReportGenerator.generate_profiling_report(
                    st.session_state.source_df,
                    st.session_state.target_df,
                    st.session_state.column_mapping,
                    join_keys=join_keys
                )
                
                # Generate regression report (independent of join keys)
                regression_path = f"reports/RegressionReport_{timestamp}.xlsx"
                ReportGenerator.generate_regression_report(
                    st.session_state.source_df,
                    st.session_state.target_df,
                    st.session_state.column_mapping,
                    regression_path
                )
                
                # Generate side-by-side report
                side_by_side_path = f"reports/DifferenceReport_{timestamp}.xlsx"
                side_by_side_df, _ = ReportGenerator.generate_side_by_side_report(
                    st.session_state.source_df,
                    st.session_state.target_df,
                    st.session_state.column_mapping,
                    side_by_side_path,
                    join_keys=join_keys
                )
                
                # Display Y-Data Profiling Reports in expandable sections
                st.subheader("Y-Data Profiling Reports")

                # Generate comparison profile with side-by-side data
                comparison_df = pd.DataFrame()
                
                # Process each column in the mapping
                for source_col, target_col in st.session_state.column_mapping.items():
                    if source_col in st.session_state.source_df.columns and target_col in st.session_state.target_df.columns:
                        comparison_df[f'Source_{source_col}'] = st.session_state.source_df[source_col]
                        comparison_df[f'Target_{target_col}'] = st.session_state.target_df[target_col]
                
                # Generate profile report with enhanced configuration
                comparison_profile = ProfileReport(
                    comparison_df,
                    title="Source vs Target Comparison Profile",
                    minimal=False,  # Use full report for more detailed comparison
                    correlations={"cramers": True},  # Add correlation analysis
                    vars={"num": {"low_categorical_threshold": 0}},  # Treat numeric columns as continuous
                    interactions={"continuous": True}  # Show interactions between continuous variables
                )
                comparison_html = os.path.join(reports_dir, f"ComparisonProfile_{timestamp}.html")
                comparison_profile.to_file(comparison_html)
                st.success("✅ Comparison profile report generated")

                # Generate DataCompy comparison report
                try:
                    # Prepare join keys for DataCompy
                    join_columns = []
                    source_df = st.session_state.source_df.copy()
                    target_df = st.session_state.target_df.copy()
                    
                    if join_keys:
                        join_columns = [key[0] for key in join_keys]  # Use source column names
                    else:
                        # If no join keys specified, use index
                        source_df['_index'] = source_df.index
                        target_df['_index'] = target_df.index
                        join_columns = ['_index']

                    # Create DataCompy comparison
                    comparison = datacompy.Compare(
                        source_df,
                        target_df,
                        join_columns=join_columns,
                        df1_name='Source',
                        df2_name='Target'
                    )

                    # Get column statistics safely
                    column_stats_html = '<p>No column statistics available.</p>'
                    try:
                        if (hasattr(comparison, 'column_stats') and 
                            isinstance(comparison.column_stats, pd.DataFrame) and 
                            not comparison.column_stats.empty):
                            column_stats_html = comparison.column_stats.to_html()
                    except Exception as e:
                        st.warning(f"Could not generate column statistics: {str(e)}")

                    # Generate HTML report
                    datacompy_html = os.path.join(reports_dir, f"DataCompyReport_{timestamp}.html")
                    with open(datacompy_html, 'w') as f:
                        f.write(f"""
                        <html>
                        <head>
                            <title>DataCompy Comparison Report</title>
                            <style>
                                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                                .report {{ max-width: 1200px; margin: 0 auto; }}
                                .section {{ margin: 20px 0; padding: 20px; border: 1px solid #ddd; border-radius: 5px; }}
                                .match {{ color: green; }}
                                .mismatch {{ color: red; }}
                                table {{ border-collapse: collapse; width: 100%; }}
                                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                                th {{ background-color: #f5f5f5; }}
                            </style>
                        </head>
                        <body>
                            <div class="report">
                                <h1>DataCompy Comparison Report</h1>
                                <div class="section">
                                    <h2>Summary</h2>
                                    <pre>{comparison.report()}</pre>
                                </div>
                                <div class="section">
                                    <h2>Detailed Statistics</h2>
                                    <h3>Matches</h3>
                                    <div class="match">
                                        <p>Number of rows match: {comparison.count_matching_rows()}</p>
                                        <p>Number of columns match: {len(set(source_df.columns).intersection(target_df.columns))}</p>
                                    </div>
                                    <h3>Mismatches</h3>
                                    <div class="mismatch">
                                        <p>Rows only in Source: {len(comparison.df1_unq_rows)}</p>
                                        <p>Rows only in Target: {len(comparison.df2_unq_rows)}</p>
                                        <p>Source-only columns: {len(set(source_df.columns) - set(target_df.columns))}</p>
                                        <p>Target-only columns: {len(set(target_df.columns) - set(source_df.columns))}</p>
                                    </div>
                                </div>
                                <div class="section">
                                    <h2>Column Statistics</h2>
                                    {column_stats_html}
                                </div>
                            </div>
                        </body>
                        </html>
                        """)
                except Exception as e:
                    st.error(f"Error generating DataCompy report: {str(e)}")
                    datacompy_html = None

                # Store DataCompy report path
                st.session_state.report_paths['datacompy_html'] = datacompy_html
                
                # Store report paths in session state to prevent page reset
                if 'report_paths' not in st.session_state:
                    st.session_state.report_paths = {}
                st.session_state.report_paths.update({
                    'source_html': source_html,
                    'target_html': target_html,
                    'comparison_html': comparison_html
                })
                
                tab1, tab2, tab3, tab4 = st.tabs(["Source Profile", "Target Profile", "Comparison Profile", "DataCompy Report"])
                
                with tab1:
                    with open(source_html, 'r', encoding='utf-8') as f:
                        components.html(f.read(), height=600, scrolling=True)
                    
                    # Download button for source profile
                    with open(source_html, 'rb') as f:
                        st.download_button(
                            "Download Source Profile Report",
                            f,
                            file_name=f"SourceProfile_{timestamp}.html",
                            mime="text/html",
                            key="download_source"
                        )
                
                with tab2:
                    with open(target_html, 'r', encoding='utf-8') as f:
                        components.html(f.read(), height=600, scrolling=True)
                    
                    # Download button for target profile
                    with open(target_html, 'rb') as f:
                        st.download_button(
                            "Download Target Profile Report",
                            f,
                            file_name=f"TargetProfile_{timestamp}.html",
                            mime="text/html",
                            key="download_target"
                        )

                with tab3:
                    with open(comparison_html, 'r', encoding='utf-8') as f:
                        components.html(f.read(), height=600, scrolling=True)
                    
                    # Download button for comparison profile
                    with open(comparison_html, 'rb') as f:
                        st.download_button(
                            "Download Comparison Profile Report",
                            f,
                            file_name=f"ComparisonProfile_{timestamp}.html",
                            mime="text/html",
                            key="download_comparison"
                        )

                with tab4:
                    if datacompy_html and os.path.exists(datacompy_html):
                        with open(datacompy_html, 'r', encoding='utf-8') as f:
                            components.html(f.read(), height=600, scrolling=True)
                        
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            # Download button for DataCompy report
                            with open(datacompy_html, 'rb') as f:
                                st.download_button(
                                    "📥 Download DataCompy Report",
                                    f,
                                    file_name=f"DataCompyReport_{timestamp}.html",
                                    mime="text/html",
                                    key="download_datacompy",
                                    use_container_width=True
                                )
                        
                        # Show additional DataCompy insights
                        with st.expander("📊 DataCompy Insights", expanded=False):
                            try:
                                st.write("### Match Statistics")
                                col1, col2, col3 = st.columns(3)
                                
                                matching_rows = comparison.count_matching_rows()
                                source_only_rows = len(comparison.df1_unq_rows)
                                target_only_rows = len(comparison.df2_unq_rows)
                                
                                with col1:
                                    st.metric(
                                        "Matching Rows",
                                        matching_rows,
                                        delta=f"{matching_rows / len(source_df) * 100:.1f}%"
                                    )
                                
                                with col2:
                                    st.metric(
                                        "Source Only Rows",
                                        source_only_rows,
                                        delta=f"{source_only_rows / len(source_df) * 100:.1f}%"
                                    )
                                
                                with col3:
                                    st.metric(
                                        "Target Only Rows",
                                        target_only_rows,
                                        delta=f"{target_only_rows / len(target_df) * 100:.1f}%"
                                    )
                                
                                st.write("### Column Analysis")
                                
                                # Get column sets
                                source_cols = set(source_df.columns)
                                target_cols = set(target_df.columns)
                                common_cols = source_cols.intersection(target_cols)
                                source_only = source_cols - target_cols
                                target_only = target_cols - source_cols

                                # Display column sets
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.write("Source-only Columns:")
                                    if source_only:
                                        for col in sorted(source_only):
                                            st.info(f"- {col}")
                                    else:
                                        st.success("No source-only columns")
                                
                                with col2:
                                    st.write("Target-only Columns:")
                                    if target_only:
                                        for col in sorted(target_only):
                                            st.info(f"- {col}")
                                    else:
                                        st.success("No target-only columns")
                                
                                st.write("### Common Column Analysis")
                                if common_cols:
                                    for col in sorted(common_cols):
                                        # Calculate match rate for common columns
                                        source_values = set(source_df[col].dropna())
                                        target_values = set(target_df[col].dropna())
                                        common_values = source_values.intersection(target_values)
                                        all_values = source_values.union(target_values)
                                        match_rate = len(common_values) / len(all_values) if all_values else 1.0
                                        
                                        # Display progress bar with details
                                        st.progress(
                                            match_rate,
                                            text=f"{col}: {match_rate*100:.1f}% unique values match"
                                        )
                                        
                                        with st.expander(f"Details for {col}", expanded=False):
                                            st.write(f"- Unique values in Source: {len(source_values)}")
                                            st.write(f"- Unique values in Target: {len(target_values)}")
                                            st.write(f"- Common unique values: {len(common_values)}")
                                            if len(source_values - target_values) > 0:
                                                st.write("- Values only in Source:", list(source_values - target_values)[:5])
                                            if len(target_values - source_values) > 0:
                                                st.write("- Values only in Target:", list(target_values - source_values)[:5])
                                else:
                                    st.warning("No common columns found between source and target datasets")
                            except Exception as e:
                                st.error(f"Error displaying insights: {str(e)}")
                    else:
                        st.error("❌ DataCompy report generation failed. Please check the data and try again.")
                        
                        with col2:
                            st.write("Target-only Columns:")
                            if target_only:
                                for col in sorted(target_only):
                                    st.info(f"- {col}")
                            else:
                                st.success("No target-only columns")
                        
                        st.write("### Common Column Analysis")
                        if common_cols:
                            for col in sorted(common_cols):
                                # Calculate match rate for common columns
                                source_values = set(st.session_state.source_df[col].dropna())
                                target_values = set(st.session_state.target_df[col].dropna())
                                common_values = source_values.intersection(target_values)
                                all_values = source_values.union(target_values)
                                match_rate = len(common_values) / len(all_values) if all_values else 1.0
                                
                                # Display progress bar with details
                                st.progress(
                                    match_rate,
                                    text=f"{col}: {match_rate*100:.1f}% unique values match"
                                )
                                
                                with st.expander(f"Details for {col}", expanded=False):
                                    st.write(f"- Unique values in Source: {len(source_values)}")
                                    st.write(f"- Unique values in Target: {len(target_values)}")
                                    st.write(f"- Common unique values: {len(common_values)}")
                                    if len(source_values - target_values) > 0:
                                        st.write("- Values only in Source:", list(source_values - target_values)[:5])
                                    if len(target_values - source_values) > 0:
                                        st.write("- Values only in Target:", list(target_values - source_values)[:5])
                        else:
                            st.warning("No common columns found between source and target datasets")
                
            except ImportError:
                st.warning("Y-Data Profiling package not installed. Please install ydata-profiling package.")
                # Continue with other reports...

        # Store report paths in session state
        st.session_state.report_paths.update({
            'side_by_side_path': side_by_side_path,
            'profiling_path': f"reports/ProfilingReport_{timestamp}.xlsx",
            'regression_path': regression_path
        })

        # Create zip file containing all reports
        st.divider()
        zip_path = f"reports/FinalComparison_{timestamp}.zip"
        try:
            import zipfile
            with zipfile.ZipFile(zip_path, 'w') as zipf:
                # Add all reports to zip
                for report_name, report_path in st.session_state.report_paths.items():
                    if report_path and os.path.exists(report_path):
                        zipf.write(report_path, os.path.basename(report_path))
            
            # Add download button for zip file
            st.markdown("### 📦 Download Complete Comparison Package")
            st.caption("Contains all reports: Regression, Side by Side, DataCompy, and Profile Reports")
            with open(zip_path, 'rb') as f:
                st.download_button(
                    "📥 Download All Reports (ZIP)",
                    f,
                    file_name=f"FinalComparison_{timestamp}.zip",
                    mime="application/zip",
                    key="download_all",
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"Error creating zip file: {str(e)}")
        st.divider()

        # Display results and download buttons
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Data Comparison Results")
            st.dataframe(diff_df)
            
            if os.path.exists(side_by_side_path):
                with open(side_by_side_path, 'rb') as f:
                    st.download_button(
                        "Download Difference Report",
                        f,
                        file_name=f"DifferenceReport_{timestamp}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_difference"
                    )
        
        with col2:
            st.markdown("#### Y-Data Profiling Results")
            if not profile_df.empty:
                # Format the dataframe for better display
                st.write("Column-wise Statistical Comparison:")
                
                # Create tabs for different views
                metrics_tab, details_tab = st.tabs(["Main Metrics", "Detailed Statistics"])
                
                with metrics_tab:
                    main_metrics = profile_df[['Column', 'Source_Count', 'Target_Count', 'Match_Percentage']]
                    st.dataframe(main_metrics, use_container_width=True)
                
                with details_tab:
                    st.dataframe(profile_df, use_container_width=True)
                
                # Save profiling report
                profiling_path = st.session_state.report_paths['profiling_path']
                profile_df.to_excel(profiling_path, index=False)
                
                # Download buttons with unique keys
                col3, col4 = st.columns(2)
                with col3:
                    with open(profiling_path, 'rb') as f:
                        st.download_button(
                            "Download Profiling Report",
                            f,
                            file_name=f"ProfilingReport_{timestamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_profiling"
                        )
                
                with col4:
                    if os.path.exists(regression_path):
                        with open(regression_path, 'rb') as f:
                            st.download_button(
                                "Download Regression Report",
                                f,
                                file_name=f"RegressionReport_{timestamp}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_regression"
                            )
            else:
                st.error("Unable to generate profiling report. Please check the data and try again.")

    except Exception as e:
        log_error(f"Error performing comparison: {str(e)}")
        st.error(f"Error performing comparison: {str(e)}")

if __name__ == "__main__":
    main()
