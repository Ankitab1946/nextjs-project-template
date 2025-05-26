"""Main Streamlit application for data comparison framework"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import io
import tempfile

from config import SOURCE_TYPES, TYPE_MAPPING, DEFAULT_DELIMITERS, REPORTS_DIR
from data_loader import DataLoader
from report_generator import ReportGenerator

def init_session_state():
    """Initialize session state variables"""
    if 'source_df' not in st.session_state:
        st.session_state.source_df = None
    if 'target_df' not in st.session_state:
        st.session_state.target_df = None
    if 'column_mapping' not in st.session_state:
        st.session_state.column_mapping = {}
    if 'join_columns' not in st.session_state:
        st.session_state.join_columns = []
    if 'excluded_columns' not in st.session_state:
        st.session_state.excluded_columns = []

def show_data_source_config(prefix: str) -> None:
    """Show configuration options for data source/target"""
    st.subheader(f"{'Source' if prefix == 'source' else 'Target'} Configuration")
    
    source_type = st.selectbox(
        f"Select {'Source' if prefix == 'source' else 'Target'} Type",
        options=SOURCE_TYPES,
        key=f"{prefix}_type"
    )
    
    try:
        if source_type in ['CSV File', 'DAT File', 'Parquet File', 'Zipped Flat Files']:
            file = st.file_uploader(
                f"Upload {source_type}",
                type=['csv', 'dat', 'parquet', 'zip'],
                key=f"{prefix}_file"
            )
            
            if file:
                if source_type in ['CSV File', 'DAT File', 'Zipped Flat Files']:
                    delimiter = st.text_input(
                        "Delimiter",
                        value=DEFAULT_DELIMITERS.get(source_type, ','),
                        key=f"{prefix}_delimiter"
                    )
                    
                    if st.button(f"Load {source_type}", key=f"load_{prefix}"):
                        with st.spinner("Loading data..."):
                            try:
                                # Reset file pointer
                                file.seek(0)
                                
                                if source_type == 'Zipped Flat Files':
                                    df = DataLoader.read_zipped_flat_files(io.BytesIO(file.read()), delimiter)
                                elif source_type == 'CSV File':
                                    df = DataLoader.read_csv_in_chunks(file, delimiter=delimiter)
                                else:  # DAT File
                                    df = DataLoader.read_dat_file(file, delimiter=delimiter)
                                
                                if df is not None and len(df) > 0:
                                    st.session_state[f"{prefix}_df"] = df
                                    st.success(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                                else:
                                    st.error("❌ No data was loaded from the file")
                            except Exception as e:
                                st.error(f"❌ Error loading file: {str(e)}")
                                
                elif source_type == 'Parquet File':
                    if st.button(f"Load {source_type}", key=f"load_{prefix}"):
                        with st.spinner("Loading data..."):
                            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                                tmp.write(file.read())
                                df = DataLoader.read_parquet(tmp.name)
                            os.unlink(tmp.name)
                
                if 'df' in locals():
                    st.session_state[f"{prefix}_df"] = df
                    st.success(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                    
        elif source_type in ['SQL Server', 'Teradata']:
            with st.expander(f"{source_type} Connection Details"):
                if source_type == 'SQL Server':
                    server = st.text_input("Server", key=f"{prefix}_server")
                    database = st.text_input("Database", key=f"{prefix}_database")
                    use_windows_auth = st.checkbox("Use Windows Authentication", key=f"{prefix}_windows_auth")
                    if not use_windows_auth:
                        username = st.text_input("Username", key=f"{prefix}_username")
                        password = st.text_input("Password", type="password", key=f"{prefix}_password")
                else:  # Teradata
                    host = st.text_input("Host", key=f"{prefix}_host")
                    username = st.text_input("Username", key=f"{prefix}_username")
                    password = st.text_input("Password", type="password", key=f"{prefix}_password")
                
                query = st.text_area("SQL Query", key=f"{prefix}_query")
                
                if st.button("Execute Query", key=f"execute_{prefix}"):
                    with st.spinner("Executing query..."):
                        try:
                            if source_type == 'SQL Server':
                                conn_params = {
                                    'server': server,
                                    'database': database,
                                    'use_windows_auth': use_windows_auth
                                }
                                if not use_windows_auth:
                                    conn_params.update({'username': username, 'password': password})
                                df = DataLoader.read_sql_server(conn_params, query)
                            else:  # Teradata
                                conn_params = {
                                    'host': host,
                                    'username': username,
                                    'password': password
                                }
                                df = DataLoader.read_teradata(conn_params, query)
                                
                            st.session_state[f"{prefix}_df"] = df
                            st.success(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                            
        elif source_type == 'Stored Procedure':
            with st.expander("Stored Procedure Details"):
                server = st.text_input("Server", key=f"{prefix}_sp_server")
                database = st.text_input("Database", key=f"{prefix}_sp_database")
                use_windows_auth = st.checkbox("Use Windows Authentication", key=f"{prefix}_sp_windows_auth")
                if not use_windows_auth:
                    username = st.text_input("Username", key=f"{prefix}_sp_username")
                    password = st.text_input("Password", type="password", key=f"{prefix}_sp_password")
                
                proc_name = st.text_input("Stored Procedure Name", key=f"{prefix}_sp_name")
                params = st.text_area(
                    "Parameters (as Python dict, e.g., {'param1': 'value1'})",
                    key=f"{prefix}_sp_params"
                )
                
                if st.button("Execute Stored Procedure", key=f"execute_sp_{prefix}"):
                    with st.spinner("Executing stored procedure..."):
                        try:
                            conn_params = {
                                'server': server,
                                'database': database,
                                'use_windows_auth': use_windows_auth
                            }
                            if not use_windows_auth:
                                conn_params.update({'username': username, 'password': password})
                                
                            df = DataLoader.read_stored_proc(
                                conn_params,
                                proc_name,
                                eval(params) if params else None
                            )
                            
                            st.session_state[f"{prefix}_df"] = df
                            st.success(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                            
        elif source_type == 'API':
            with st.expander("API Details"):
                url = st.text_input("API URL", key=f"{prefix}_api_url")
                method = st.selectbox(
                    "HTTP Method",
                    options=['GET', 'POST', 'PUT', 'DELETE'],
                    key=f"{prefix}_api_method"
                )
                headers = st.text_area(
                    "Headers (as Python dict)",
                    key=f"{prefix}_api_headers"
                )
                params = st.text_area(
                    "Parameters (as Python dict)",
                    key=f"{prefix}_api_params"
                )
                
                if st.button("Fetch API Data", key=f"fetch_api_{prefix}"):
                    with st.spinner("Fetching API data..."):
                        try:
                            df = DataLoader.read_api(
                                url,
                                method=method,
                                headers=eval(headers) if headers else None,
                                params=eval(params) if params else None
                            )
                            
                            st.session_state[f"{prefix}_df"] = df
                            st.success(f"✅ Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
                            
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

def show_column_mapping_interface():
    """Show interface for column mapping"""
    st.subheader("Column Mapping Configuration")
    
    if not isinstance(st.session_state.source_df, pd.DataFrame):
        st.error("❌ Source data not loaded")
        return
    if not isinstance(st.session_state.target_df, pd.DataFrame):
        st.error("❌ Target data not loaded")
        return
        
    # Create tabs for auto and manual mapping
    auto_tab, manual_tab = st.tabs(["Automatic Mapping", "Manual Mapping"])
    
    with auto_tab:
        if st.button("🔄 Auto-Map Columns", use_container_width=True):
            # Simple auto-mapping based on column names
            mapping = {}
            source_cols = {col.lower().strip(): col for col in st.session_state.source_df.columns}
            target_cols = {col.lower().strip(): col for col in st.session_state.target_df.columns}
            
            for s_norm, s_orig in source_cols.items():
                if s_norm in target_cols:
                    mapping[s_orig] = target_cols[s_norm]
            
            st.session_state.column_mapping = mapping
            if mapping:
                st.success(f"✅ Successfully mapped {len(mapping)} columns!")
            else:
                st.warning("⚠️ No automatic matches found. Please map columns manually.")
    
    with manual_tab:
        st.markdown("### Manual Column Mapping")
        
        # Show mapping interface for each source column
        for source_col in st.session_state.source_df.columns:
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                st.markdown(f"**Source:** {source_col}")
                st.caption(f"Sample: {str(st.session_state.source_df[source_col].head(2).tolist())}")
            
            with col2:
                # Get current mapping
                current_mapping = st.session_state.column_mapping.get(source_col, '')
                
                # Create dropdown with target columns
                target_options = [''] + list(st.session_state.target_df.columns)
                selected_target = st.selectbox(
                    "Map to target column",
                    options=target_options,
                    index=target_options.index(current_mapping) if current_mapping in target_options else 0,
                    key=f"mapping_{source_col}"
                )
                
                if selected_target:
                    if selected_target != current_mapping:
                        st.session_state.column_mapping[source_col] = selected_target
                        st.caption(f"Target sample: {str(st.session_state.target_df[selected_target].head(2).tolist())}")
                elif source_col in st.session_state.column_mapping:
                    del st.session_state.column_mapping[source_col]
            
            with col3:
                excluded = st.checkbox(
                    "Exclude",
                    key=f"exclude_{source_col}",
                    value=source_col in st.session_state.excluded_columns
                )
                
                if excluded and source_col not in st.session_state.excluded_columns:
                    st.session_state.excluded_columns.append(source_col)
                elif not excluded and source_col in st.session_state.excluded_columns:
                    st.session_state.excluded_columns.remove(source_col)
    
    # Show current mapping summary
    if st.session_state.column_mapping:
        st.success(f"✅ Currently mapped: {len(st.session_state.column_mapping)} columns")
        with st.expander("View Current Mappings"):
            for source_col, target_col in st.session_state.column_mapping.items():
                st.write(f"{source_col} → {target_col}")

def show_join_column_selection():
    """Show interface for selecting join columns"""
    st.subheader("Join Column Selection")
    
    if not st.session_state.column_mapping:
        st.warning("⚠️ Please map columns first")
        return
    
    # Initialize join_columns in session state if not present
    if 'join_columns' not in st.session_state:
        st.session_state.join_columns = []
    
    # Get valid columns that exist in both dataframes
    valid_columns = [
        col for col in st.session_state.column_mapping.keys()
        if col in st.session_state.source_df.columns and 
        st.session_state.column_mapping[col] in st.session_state.target_df.columns
    ]
    
    # Show multiselect for join columns
    selected_columns = st.multiselect(
        "Select columns to use as join keys",
        options=valid_columns,
        default=st.session_state.join_columns,
        help="Select one or more columns to use as join keys for comparison"
    )
    
    # Update session state
    st.session_state.join_columns = selected_columns
    
    if selected_columns:
        st.success(f"✅ Selected {len(selected_columns)} join column(s)")
        # Show selected mappings
        st.write("Selected Join Keys:")
        for col in selected_columns:
            st.write(f"- Source: {col} → Target: {st.session_state.column_mapping[col]}")
    else:
        st.info("ℹ️ No join columns selected. Index-based comparison will be used.")

def perform_comparison():
    """Perform the comparison and generate reports"""
    st.subheader("Comparison Results")
    
    if not st.session_state.column_mapping:
        st.error("❌ Please map columns first")
        return
        
    if not st.session_state.join_columns:
        st.error("❌ Please select at least one join column")
        return
        
    try:
        # Create reports directory
        os.makedirs(REPORTS_DIR, exist_ok=True)
        
        # Generate timestamp for report files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        with st.spinner("Generating reports..."):
            # Generate comparison report
            comparison_path = os.path.join(REPORTS_DIR, f"ComparisonReport_{timestamp}.html")
            # Use join columns directly as a list of strings
            join_keys = list(st.session_state.join_columns) if st.session_state.join_columns else []
            ReportGenerator.generate_comparison_report(
                st.session_state.source_df,
                st.session_state.target_df,
                st.session_state.column_mapping,
                join_keys,
                comparison_path
            )
            
            # Generate Y-Data Profile reports
            profile_reports = ReportGenerator.generate_profile_reports(
                st.session_state.source_df,
                st.session_state.target_df,
                timestamp
            )
            
            # Generate regression report
            regression_path = os.path.join(REPORTS_DIR, f"RegressionReport_{timestamp}.xlsx")
            ReportGenerator.generate_regression_report(
                st.session_state.source_df,
                st.session_state.target_df,
                st.session_state.column_mapping,
                regression_path
            )
            
            # Generate side by side comparison
            diff_path = os.path.join(REPORTS_DIR, f"DifferenceReport_{timestamp}.xlsx")
            has_differences = ReportGenerator.generate_side_by_side_report(
                st.session_state.source_df,
                st.session_state.target_df,
                st.session_state.column_mapping,
                st.session_state.join_columns,
                diff_path
            )
            
            # Show download links
            st.success("✅ Reports generated successfully!")
            
            st.markdown("### Download Reports")
            col1, col2 = st.columns(2)
            
            with col1:
                with open(comparison_path, 'rb') as f:
                    st.download_button(
                        "📊 Download Comparison Report",
                        f,
                        file_name=f"ComparisonReport_{timestamp}.html",
                        mime="text/html"
                    )
                    
                with open(profile_reports['source'], 'rb') as f:
                    st.download_button(
                        "📈 Download Source Profile",
                        f,
                        file_name=f"SourceProfile_{timestamp}.html",
                        mime="text/html"
                    )
            
            with col2:
                with open(regression_path, 'rb') as f:
                    st.download_button(
                        "📑 Download Regression Report",
                        f,
                        file_name=f"RegressionReport_{timestamp}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                with open(profile_reports['target'], 'rb') as f:
                    st.download_button(
                        "📉 Download Target Profile",
                        f,
                        file_name=f"TargetProfile_{timestamp}.html",
                        mime="text/html"
                    )
            
            # Show comparison profile and difference report
            st.markdown("### Additional Reports")
            col3, col4 = st.columns(2)
            
            with col3:
                with open(profile_reports['comparison'], 'rb') as f:
                    st.download_button(
                        "🔄 Download Comparison Profile",
                        f,
                        file_name=f"ComparisonProfile_{timestamp}.html",
                        mime="text/html"
                    )
            
            with col4:
                if has_differences:
                    with open(diff_path, 'rb') as f:
                        st.download_button(
                            "❗ Download Difference Report",
                            f,
                            file_name=f"DifferenceReport_{timestamp}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                else:
                    st.info("✅ No differences found between source and target")
                    
    except Exception as e:
        st.error(f"❌ Error generating reports: {str(e)}")

def main():
    """Main application"""
    st.title("Data Comparison Framework")
    
    # Initialize session state
    init_session_state()
    
    # Create tabs for different sections
    source_tab, target_tab, mapping_tab, compare_tab = st.tabs([
        "Source Configuration",
        "Target Configuration",
        "Column Mapping",
        "Compare & Reports"
    ])
    
    with source_tab:
        show_data_source_config("source")
        
    with target_tab:
        show_data_source_config("target")
        
    with mapping_tab:
        show_column_mapping_interface()
        show_join_column_selection()
        
    with compare_tab:
        if st.button("🔍 Compare Data", use_container_width=True):
            perform_comparison()

if __name__ == "__main__":
    main()
